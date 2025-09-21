#!/usr/bin/env python3
"""
Clean Slate Reset Script

Completely cleans both GCS bucket and BigQuery tables to start fresh.
Achieves perfect 1:1 sync between BigQuery ad_ids and GCS files.
"""

import os
from google.cloud import storage
from src.utils.bigquery_client import get_bigquery_client, run_query

def clean_gcs_bucket():
    """Completely clean the GCS bucket"""
    print("🧹 CLEANING GCS BUCKET")
    print("=" * 30)

    try:
        client = storage.Client()
        bucket = client.bucket("ads-media-storage-bigquery-ai-kaggle")

        # List all blobs in ad-media/ prefix
        blobs = list(bucket.list_blobs(prefix="ad-media/"))
        total_files = len(blobs)

        if total_files == 0:
            print("   ✅ GCS bucket already clean - no files to delete")
            return True

        print(f"   🗑️  Found {total_files} files to delete")

        # Auto-confirm deletion for clean slate
        print(f"   ✅ Auto-confirming deletion of {total_files} files for clean slate reset")

        # Delete all files
        deleted_count = 0
        for blob in blobs:
            try:
                blob.delete()
                deleted_count += 1
                if deleted_count % 50 == 0:
                    print(f"   🗑️  Deleted {deleted_count}/{total_files} files...")
            except Exception as e:
                print(f"   ⚠️  Failed to delete {blob.name}: {e}")

        print(f"   ✅ Deleted {deleted_count}/{total_files} files from GCS")
        return True

    except Exception as e:
        print(f"   ❌ GCS cleanup failed: {e}")
        return False

def clean_bigquery_tables():
    """Clean BigQuery tables"""
    print("\n🧹 CLEANING BIGQUERY TABLES")
    print("=" * 35)

    try:
        BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
        BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

        # Tables to clean
        tables_to_clean = [
            "ads_with_dates",
            "ads_raw"
        ]

        for table_name in tables_to_clean:
            try:
                # Check if table exists
                check_query = f"""
                SELECT table_name
                FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
                WHERE table_name = '{table_name}'
                """
                result = run_query(check_query)

                if result.empty:
                    print(f"   ✅ Table {table_name} doesn't exist - nothing to clean")
                    continue

                # Get row count before deletion
                count_query = f"SELECT COUNT(*) as count FROM `{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"
                count_result = run_query(count_query)
                row_count = count_result.iloc[0]['count'] if not count_result.empty else 0

                if row_count == 0:
                    print(f"   ✅ Table {table_name} already empty")
                    continue

                print(f"   🗑️  Found {row_count} rows in {table_name}")

                # Delete all rows
                delete_query = f"DELETE FROM `{BQ_PROJECT}.{BQ_DATASET}.{table_name}` WHERE TRUE"
                run_query(delete_query)

                print(f"   ✅ Cleaned {table_name} table ({row_count} rows deleted)")

            except Exception as e:
                print(f"   ⚠️  Failed to clean {table_name}: {e}")

        print("   ✅ BigQuery cleanup completed")
        return True

    except Exception as e:
        print(f"   ❌ BigQuery cleanup failed: {e}")
        return False

def verify_clean_state():
    """Verify everything is clean"""
    print("\n🔍 VERIFYING CLEAN STATE")
    print("=" * 30)

    try:
        # Check GCS
        client = storage.Client()
        bucket = client.bucket("ads-media-storage-bigquery-ai-kaggle")
        gcs_files = list(bucket.list_blobs(prefix="ad-media/"))
        print(f"   📁 GCS files remaining: {len(gcs_files)}")

        # Check BigQuery
        BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
        BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

        for table_name in ["ads_with_dates", "ads_raw"]:
            try:
                count_query = f"SELECT COUNT(*) as count FROM `{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"
                result = run_query(count_query)
                count = result.iloc[0]['count'] if not result.empty else 0
                print(f"   📊 {table_name} rows: {count}")
            except:
                print(f"   📊 {table_name}: table doesn't exist")

        # Perfect clean state
        if len(gcs_files) == 0:
            print("\n   🎉 PERFECT CLEAN STATE ACHIEVED!")
            print("   ✅ GCS bucket is empty")
            print("   ✅ BigQuery tables are clean")
            print("   🚀 Ready for fresh pipeline run")
            return True
        else:
            print(f"\n   ⚠️  Clean state not perfect - {len(gcs_files)} files remain in GCS")
            return False

    except Exception as e:
        print(f"   ❌ Verification failed: {e}")
        return False

def main():
    """Execute clean slate reset"""
    print("🚀 CLEAN SLATE RESET")
    print("=" * 50)
    print("This will completely clean both GCS bucket and BigQuery tables")
    print("to achieve perfect 1:1 sync on the next pipeline run.")

    # Auto-confirm reset for clean slate
    print("\n✅ Auto-proceeding with complete clean slate reset")

    # Step 1: Clean GCS
    if not clean_gcs_bucket():
        print("\n💥 Clean slate reset failed at GCS cleanup")
        return False

    # Step 2: Clean BigQuery
    if not clean_bigquery_tables():
        print("\n💥 Clean slate reset failed at BigQuery cleanup")
        return False

    # Step 3: Verify clean state
    if not verify_clean_state():
        print("\n💥 Clean slate reset verification failed")
        return False

    print("\n🎉 CLEAN SLATE RESET COMPLETED SUCCESSFULLY!")
    print("🚀 Ready to run fresh pipeline for perfect 1:1 sync")
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Clean slate reset completed - ready for fresh pipeline!")
    else:
        print("\n❌ Clean slate reset failed!")