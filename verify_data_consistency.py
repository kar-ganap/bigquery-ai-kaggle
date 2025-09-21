#!/usr/bin/env python3
"""
Data Consistency Verification Script

Verifies that BigQuery ads_with_dates table and GCS storage are in sync
after the cleanup operations.
"""

import os
from google.cloud import storage
from src.utils.bigquery_client import get_bigquery_client, run_query

def main():
    """Verify data consistency between BigQuery and GCS"""

    print("📊 DATA CONSISTENCY VERIFICATION")
    print("=" * 50)

    # Configuration
    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
    BUCKET_NAME = "ads-media-storage-bigquery-ai-kaggle"

    try:
        # 1. Check BigQuery ads_with_dates table
        print("\n🔍 Checking BigQuery ads_with_dates table...")

        # First check table schema
        schema_query = f"""
        SELECT column_name
        FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'ads_with_dates'
        ORDER BY ordinal_position
        """

        schema_result = run_query(schema_query)
        columns = schema_result['column_name'].tolist() if not schema_result.empty else []
        print(f"   📋 Table columns: {', '.join(columns)}")

        has_media_storage_path = 'media_storage_path' in columns
        has_computed_media_type = 'computed_media_type' in columns

        print(f"   🔍 Has media_storage_path: {has_media_storage_path}")
        print(f"   🔍 Has computed_media_type: {has_computed_media_type}")

        # Adjust query based on available fields
        if has_media_storage_path:
            bq_query = f"""
            SELECT
                COUNT(*) as total_ads,
                COUNT(CASE WHEN media_storage_path IS NOT NULL THEN 1 END) as ads_with_media,
                COUNT(DISTINCT brand) as unique_brands,
                COUNT(CASE WHEN media_type = 'image' THEN 1 END) as image_ads,
                COUNT(CASE WHEN media_type = 'video' THEN 1 END) as video_ads
            FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
            """
        else:
            # Use old schema fields
            bq_query = f"""
            SELECT
                COUNT(*) as total_ads,
                COUNT(CASE WHEN JSON_TYPE(image_urls) = 'array' AND ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as ads_with_media,
                COUNT(DISTINCT brand) as unique_brands,
                COUNT(CASE WHEN media_type = 'image' THEN 1 END) as image_ads,
                COUNT(CASE WHEN media_type = 'video' THEN 1 END) as video_ads
            FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
            """

        bq_result = run_query(bq_query)
        if not bq_result.empty:
            row = bq_result.iloc[0]
            total_ads = row['total_ads']
            ads_with_media = row['ads_with_media']
            unique_brands = row['unique_brands']
            image_ads = row['image_ads']
            video_ads = row['video_ads']

            print(f"   📊 Total ads in BigQuery: {total_ads}")
            print(f"   🖼️  Ads with media: {ads_with_media}")
            print(f"   🏢 Unique brands: {unique_brands}")
            print(f"   📸 Image ads: {image_ads}")
            print(f"   🎬 Video ads: {video_ads}")
        else:
            print("   ❌ No data found in ads_with_dates table")
            return False

        # 2. Check GCS bucket
        print("\n🔍 Checking GCS bucket storage...")

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        # Count files by brand and media type
        brand_counts = {}
        media_type_counts = {"image": 0, "video": 0}
        total_files = 0

        blobs = bucket.list_blobs(prefix="ad-media/")
        for blob in blobs:
            # Parse path: ad-media/brand/media_type/filename
            path_parts = blob.name.split('/')
            if len(path_parts) >= 4:
                brand = path_parts[1]
                media_type = path_parts[2]

                # Count by brand
                if brand not in brand_counts:
                    brand_counts[brand] = 0
                brand_counts[brand] += 1

                # Count by media type
                if media_type in media_type_counts:
                    media_type_counts[media_type] += 1

                total_files += 1

        print(f"   📁 Total files in GCS: {total_files}")
        print(f"   📸 Image files: {media_type_counts['image']}")
        print(f"   🎬 Video files: {media_type_counts['video']}")
        print(f"   🏢 Brands with files: {len(brand_counts)}")

        # Show per-brand breakdown
        print("\n📋 Per-brand file counts:")
        for brand, count in sorted(brand_counts.items()):
            print(f"   • {brand}: {count} files")

        # 3. Consistency analysis
        print("\n🔍 CONSISTENCY ANALYSIS")
        print("=" * 30)

        # Check if totals match
        if ads_with_media == total_files:
            print("   ✅ PERFECT MATCH: BigQuery ads with media == GCS files")
        else:
            diff = abs(ads_with_media - total_files)
            print(f"   ⚠️  MISMATCH: {diff} difference between BigQuery ({ads_with_media}) and GCS ({total_files})")

        # Check media type distribution
        bq_image_pct = (image_ads / total_ads * 100) if total_ads > 0 else 0
        bq_video_pct = (video_ads / total_ads * 100) if total_ads > 0 else 0
        gcs_image_pct = (media_type_counts['image'] / total_files * 100) if total_files > 0 else 0
        gcs_video_pct = (media_type_counts['video'] / total_files * 100) if total_files > 0 else 0

        print(f"   📊 Media type distribution:")
        print(f"      BigQuery: {bq_image_pct:.1f}% images, {bq_video_pct:.1f}% videos")
        print(f"      GCS:      {gcs_image_pct:.1f}% images, {gcs_video_pct:.1f}% videos")

        # Check brand counts
        if unique_brands == len(brand_counts):
            print("   ✅ Brand count matches")
        else:
            print(f"   ⚠️  Brand count mismatch: BQ={unique_brands}, GCS={len(brand_counts)}")

        # 4. Summary
        print(f"\n📈 SUMMARY")
        print("=" * 20)

        if ads_with_media == total_files and unique_brands == len(brand_counts):
            print("   🎉 PERFECT DATA CONSISTENCY!")
            print("   ✅ All ads with media have corresponding GCS files")
            print("   ✅ All brands accounted for")
            return True
        else:
            print("   ⚠️  Data inconsistencies detected")
            print("   🔧 May need investigation or re-sync")
            return False

    except Exception as e:
        print(f"   ❌ Verification failed: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Data consistency verification completed successfully!")
    else:
        print("\n❌ Data consistency issues detected!")