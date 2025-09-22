#!/usr/bin/env python3
"""
Validate 1:1 sync between BigQuery ads_with_dates and GCS media files
"""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available, use system environment variables

from google.cloud import storage
from src.utils.bigquery_client import run_query

def validate_sync():
    """Validate 1:1 sync between BigQuery and GCS"""

    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
    GCS_BUCKET = os.environ.get("GCS_BUCKET", "ads-media-storage-bigquery-ai-kaggle")

    print("🔍 VALIDATING 1:1 SYNC: BigQuery ↔ GCS")
    print("=" * 50)

    # 1. Count BigQuery rows with media
    bq_query = f"""
    SELECT COUNT(*) as total_ads,
           COUNT(CASE WHEN media_storage_path IS NOT NULL THEN 1 END) as ads_with_media,
           COUNT(DISTINCT brand) as unique_brands,
           STRING_AGG(DISTINCT brand ORDER BY brand) as brands
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    """

    try:
        bq_result = run_query(bq_query)
        if bq_result.empty:
            print("❌ No BigQuery data found")
            return False

        row = bq_result.iloc[0]
        total_ads = row['total_ads']
        ads_with_media = row['ads_with_media']
        unique_brands = row['unique_brands']
        brands = row['brands']

        print(f"📊 BIGQUERY SUMMARY:")
        print(f"   Total ads: {total_ads}")
        print(f"   Ads with media: {ads_with_media}")
        print(f"   Unique brands: {unique_brands}")
        print(f"   Brands: {brands}")

    except Exception as e:
        print(f"❌ BigQuery query failed: {e}")
        return False

    # 2. Count GCS files
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)

        # Count files by brand
        brand_counts = {}
        total_files = 0

        # List all files under ad-media/
        blobs = bucket.list_blobs(prefix="ad-media/")

        for blob in blobs:
            # Parse path: ad-media/brand/media_type/filename
            path_parts = blob.name.split('/')
            if len(path_parts) >= 4:  # ad-media/brand/media_type/filename
                brand = path_parts[1]
                media_type = path_parts[2]

                if brand not in brand_counts:
                    brand_counts[brand] = {'image': 0, 'video': 0, 'total': 0}

                if media_type in ['image', 'video']:
                    brand_counts[brand][media_type] += 1
                    brand_counts[brand]['total'] += 1
                    total_files += 1

        print(f"\n📁 GCS SUMMARY:")
        print(f"   Bucket: {GCS_BUCKET}")
        print(f"   Total media files: {total_files}")
        print(f"   Brands with media: {len(brand_counts)}")

        for brand, counts in sorted(brand_counts.items()):
            print(f"   • {brand}: {counts['total']} files ({counts['image']} images, {counts['video']} videos)")

    except Exception as e:
        print(f"❌ GCS query failed: {e}")
        return False

    # 3. Validate sync
    print(f"\n🎯 SYNC VALIDATION:")

    if ads_with_media == total_files:
        print(f"   ✅ PERFECT SYNC: {ads_with_media} BigQuery rows = {total_files} GCS files")
        sync_status = True
    else:
        print(f"   ❌ SYNC MISMATCH: {ads_with_media} BigQuery rows ≠ {total_files} GCS files")
        print(f"   📊 Difference: {abs(ads_with_media - total_files)} files")
        sync_status = False

    # 4. Brand-level validation
    print(f"\n📋 BRAND-LEVEL VALIDATION:")

    # Get brand breakdown from BigQuery
    brand_bq_query = f"""
    SELECT brand,
           COUNT(*) as total_ads,
           COUNT(CASE WHEN media_storage_path IS NOT NULL THEN 1 END) as ads_with_media
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    GROUP BY brand
    ORDER BY brand
    """

    try:
        brand_bq_result = run_query(brand_bq_query)

        all_brands_match = True
        for _, row in brand_bq_result.iterrows():
            brand = row['brand']
            bq_count = row['ads_with_media']

            # Find GCS count with normalized matching (case + space/underscore)
            gcs_count = 0
            matched_gcs_brand = None
            brand_normalized = brand.lower().replace(' ', '_')
            for gcs_brand, counts in brand_counts.items():
                gcs_brand_normalized = gcs_brand.lower().replace(' ', '_')
                if gcs_brand_normalized == brand_normalized:
                    gcs_count = counts.get('total', 0)
                    matched_gcs_brand = gcs_brand
                    break

            if bq_count == gcs_count:
                if matched_gcs_brand and matched_gcs_brand != brand:
                    print(f"   ✅ {brand}: {bq_count} BigQuery = {gcs_count} GCS (case normalized: {matched_gcs_brand})")
                else:
                    print(f"   ✅ {brand}: {bq_count} BigQuery = {gcs_count} GCS")
            else:
                print(f"   ❌ {brand}: {bq_count} BigQuery ≠ {gcs_count} GCS (GCS brand: {matched_gcs_brand})")
                all_brands_match = False

        if all_brands_match:
            print(f"\n🎉 ALL BRANDS IN PERFECT SYNC!")
        else:
            print(f"\n⚠️  Some brands have sync mismatches")

    except Exception as e:
        print(f"❌ Brand validation failed: {e}")
        return False

    return sync_status and all_brands_match

if __name__ == "__main__":
    success = validate_sync()
    exit(0 if success else 1)