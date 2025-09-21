"""
Media Storage Utility for Ad Pipeline

Handles classification and storage of ad media files (images/videos) at ingestion time.
Solves both the media type classification bug and CDN URL expiration issue.
"""
import os
import requests
import hashlib
from typing import Tuple, Optional
from google.cloud import storage
import tempfile
import time

# Storage configuration
BUCKET_NAME = "ads-media-storage-bigquery-ai-kaggle"
MEDIA_BASE_PATH = "ad-media"

class MediaStorageManager:
    """Handles classification and storage of ad media at ingestion time"""

    def __init__(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(BUCKET_NAME)

    def classify_and_store_media(self, ad_data: dict, brand_name: str) -> Tuple[str, Optional[str]]:
        """
        Classify media type based on API fields and download/store the media.
        Returns (media_type, storage_path)

        Cost-optimized approach:
        - Use resized_image_url -> original_image_url (fallback) for images
        - Use video_preview_image_url for videos
        """

        # Extract API fields for classification
        original_image_url = ad_data.get('original_image_url')
        resized_image_url = ad_data.get('resized_image_url')
        video_preview_url = ad_data.get('video_preview_image_url')
        ad_id = ad_data.get('ad_id') or ad_data.get('ad_archive_id', 'unknown')

        # Classify based on API field nullability
        if video_preview_url:
            # It's a video - download video preview image
            media_type = 'video'
            url_to_download = video_preview_url
        elif resized_image_url or original_image_url:
            # It's an image/carousel - prefer resized for cost optimization
            media_type = 'image'
            url_to_download = resized_image_url or original_image_url
        else:
            # No media URLs found
            return 'unknown', None

        # Download and store the media
        try:
            storage_path = self._download_and_store(
                url_to_download,
                ad_id,
                brand_name,
                media_type
            )
            return media_type, storage_path

        except Exception as e:
            print(f"   ⚠️  Failed to download media for ad {ad_id}: {str(e)}")
            return media_type, None  # Classification succeeded, storage failed

    def _download_and_store(self, url: str, ad_id: str, brand_name: str, media_type: str) -> str:
        """Download media from URL and store in GCS bucket"""

        # Determine file extension from URL or default
        if url.lower().endswith(('.jpg', '.jpeg')):
            ext = 'jpg'
        elif url.lower().endswith('.png'):
            ext = 'png'
        elif url.lower().endswith('.gif'):
            ext = 'gif'
        elif url.lower().endswith('.webp'):
            ext = 'webp'
        else:
            ext = 'jpg'  # Default fallback

        # Create storage path using ad_archive_id for natural deduplication: ad-media/brand/media_type/ad_id.ext
        blob_name = f"{MEDIA_BASE_PATH}/{brand_name.lower().replace(' ', '_')}/{media_type}/{ad_id}.{ext}"

        # Check if already exists to avoid re-downloading
        blob = self.bucket.blob(blob_name)
        if blob.exists():
            print(f"   ♻️  Skipping duplicate: {ad_id} already stored")
            return f"gs://{BUCKET_NAME}/{blob_name}"

        # Download with timeout and error handling
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers, timeout=30, stream=True)
        response.raise_for_status()

        # Upload to GCS
        with tempfile.NamedTemporaryFile() as temp_file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)

            temp_file.seek(0)
            blob.upload_from_file(temp_file, content_type=response.headers.get('content-type', 'image/jpeg'))

        return f"gs://{BUCKET_NAME}/{blob_name}"

    def cleanup_duplicate_media(self, dry_run: bool = True):
        """
        Clean up duplicate media files that were created with old URL-based naming.

        Enhanced strategy:
        1. If canonical exists: Remove all duplicates with hash suffixes
        2. If canonical doesn't exist: Keep one random duplicate and rename it to canonical

        Returns dict with cleanup stats including any renames needed for BigQuery updates.
        """
        print(f"🧹 {'[DRY RUN] ' if dry_run else ''}Cleaning up duplicate media files from old URL-based naming...")

        # Get all blobs in the ad-media directory
        blobs = list(self.bucket.list_blobs(prefix=f"{MEDIA_BASE_PATH}/"))

        # Group files by their canonical name
        file_groups = {}
        for blob in blobs:
            filename = blob.name.split('/')[-1]  # Get just the filename part

            # Check if this looks like an old URL-hash based filename
            # Pattern: {ad_archive_id}_{hash}.{ext}
            if '_' in filename and '.' in filename:
                base_name = filename.split('_')[0]  # Get ad_archive_id part
                ext = filename.split('.')[-1]       # Get extension
                canonical_name = f"{base_name}.{ext}"
                canonical_path = blob.name.replace(filename, canonical_name)

                if canonical_path not in file_groups:
                    file_groups[canonical_path] = {'canonical': None, 'duplicates': []}
                file_groups[canonical_path]['duplicates'].append(blob)
            else:
                # This might be a canonical file
                if blob.name not in file_groups:
                    file_groups[blob.name] = {'canonical': None, 'duplicates': []}
                file_groups[blob.name]['canonical'] = blob

        duplicates_to_delete = []
        renames_needed = []  # Track renames for BigQuery path updates
        orphan_groups = 0

        for canonical_path, group in file_groups.items():
            if group['canonical'] and group['duplicates']:
                # Case 1: Canonical exists, remove all duplicates
                duplicates_to_delete.extend(group['duplicates'])
                for dup in group['duplicates']:
                    print(f"   🔍 Found duplicate: {dup.name} (canonical exists: {canonical_path})")

            elif not group['canonical'] and group['duplicates']:
                # Case 2: No canonical, promote one duplicate to canonical
                orphan_groups += 1
                if group['duplicates']:
                    # Keep the first duplicate, delete the rest
                    promote_blob = group['duplicates'][0]
                    duplicates_to_delete.extend(group['duplicates'][1:])

                    # Track the rename operation
                    old_path = promote_blob.name
                    new_path = canonical_path
                    renames_needed.append((old_path, new_path))

                    print(f"   🔄 Will promote: {old_path} → {new_path}")
                    if len(group['duplicates']) > 1:
                        print(f"   🔍 Will delete {len(group['duplicates']) - 1} orphan duplicates for {canonical_path}")

        print(f"   📊 Found {len(duplicates_to_delete)} files to delete")
        print(f"   🔄 Found {len(renames_needed)} files to rename to canonical")
        print(f"   🏝️  Found {orphan_groups} orphan groups without canonical files")

        if dry_run:
            print("   🔍 DRY RUN - Would perform these operations:")
            for old_path, new_path in renames_needed[:5]:
                print(f"      RENAME: {old_path} → {new_path}")
            if len(renames_needed) > 5:
                print(f"      ... and {len(renames_needed) - 5} more renames")

            for blob in duplicates_to_delete[:10]:
                print(f"      DELETE: {blob.name}")
            if len(duplicates_to_delete) > 10:
                print(f"      ... and {len(duplicates_to_delete) - 10} more deletions")

            return {
                'deleted_count': len(duplicates_to_delete),
                'renamed_count': len(renames_needed),
                'renames': renames_needed
            }

        # Perform actual cleanup
        deleted_count = 0
        renamed_count = 0

        # First, handle renames (promote orphans to canonical)
        for old_path, new_path in renames_needed:
            try:
                old_blob = self.bucket.blob(old_path)
                new_blob = self.bucket.blob(new_path)

                # Copy to new location
                new_blob.rewrite(old_blob)
                # Delete old location
                old_blob.delete()

                renamed_count += 1
                print(f"   🔄 Renamed: {old_path} → {new_path}")
            except Exception as e:
                print(f"   ⚠️  Failed to rename {old_path}: {e}")

        # Then delete remaining duplicates
        for blob in duplicates_to_delete:
            try:
                blob.delete()
                deleted_count += 1
                if deleted_count % 50 == 0:
                    print(f"   🗑️  Deleted {deleted_count}/{len(duplicates_to_delete)} duplicates...")
            except Exception as e:
                print(f"   ⚠️  Failed to delete {blob.name}: {e}")

        print(f"   ✅ Cleanup complete: renamed {renamed_count} files, deleted {deleted_count} duplicates")

        return {
            'deleted_count': deleted_count,
            'renamed_count': renamed_count,
            'renames': renames_needed
        }