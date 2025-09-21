#!/usr/bin/env python3
"""
GCS Bucket Cleanup Script

Removes duplicate media files that were created with old URL-based naming scheme.
"""

from src.utils.media_storage import MediaStorageManager

def update_bigquery_paths(renames):
    """Update media_storage_path in BigQuery tables after GCS cleanup"""
    if not renames:
        print("   ✅ No BigQuery path updates needed")
        return

    print(f"\n🔄 Updating {len(renames)} media_storage_path references in BigQuery...")

    try:
        from src.utils.bigquery_client import get_bigquery_client, run_query
        import os

        BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
        BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

        # Create update SQL for ads_with_dates
        update_statements = []
        for old_path, new_path in renames:
            old_gs_path = f"gs://ads-media-storage-bigquery-ai-kaggle/{old_path}"
            new_gs_path = f"gs://ads-media-storage-bigquery-ai-kaggle/{new_path}"

            update_statements.append(f"""
            UPDATE `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
            SET media_storage_path = '{new_gs_path}'
            WHERE media_storage_path = '{old_gs_path}';
            """)

            # Also update ads_raw if it exists
            update_statements.append(f"""
            UPDATE `{BQ_PROJECT}.{BQ_DATASET}.ads_raw`
            SET media_storage_path = '{new_gs_path}'
            WHERE media_storage_path = '{old_gs_path}';
            """)

        # Execute updates in batches
        for i, statement in enumerate(update_statements):
            try:
                run_query(statement)
                if (i + 1) % 10 == 0:
                    print(f"   📊 Updated {i + 1}/{len(update_statements)} path references...")
            except Exception as e:
                print(f"   ⚠️  Failed to update path reference: {e}")

        print("   ✅ BigQuery path updates completed")

    except ImportError:
        print("   ⚠️  BigQuery client not available - skipping path updates")
    except Exception as e:
        print(f"   ⚠️  BigQuery path update failed: {e}")


def main():
    """Run GCS bucket cleanup"""

    print("🧹 GCS Bucket Cleanup for Ad Media")
    print("=" * 50)

    try:
        manager = MediaStorageManager()
        print("✅ Connected to GCS bucket successfully")

        # First run dry-run to see what would be cleaned
        print("\n🔍 Running dry-run to identify duplicates...")
        cleanup_stats = manager.cleanup_duplicate_media(dry_run=True)

        total_operations = cleanup_stats['deleted_count'] + cleanup_stats['renamed_count']
        if total_operations == 0:
            print("✅ No cleanup needed - bucket is already optimized!")
            return

        # Show summary
        print(f"\n📊 Cleanup Summary:")
        print(f"   🔄 {cleanup_stats['renamed_count']} orphan files to promote to canonical")
        print(f"   🗑️  {cleanup_stats['deleted_count']} duplicate files to delete")
        print(f"   📁 Total operations: {total_operations}")

        # Ask for confirmation
        response = input("\n🤔 Proceed with actual cleanup? (y/N): ").strip().lower()

        if response == 'y':
            print("\n🧹 Performing actual cleanup...")
            final_stats = manager.cleanup_duplicate_media(dry_run=False)

            print(f"\n✅ Cleanup complete!")
            print(f"   🔄 Renamed {final_stats['renamed_count']} files to canonical")
            print(f"   🗑️  Deleted {final_stats['deleted_count']} duplicate files")

            # Calculate storage savings (rough estimate)
            estimated_savings_mb = final_stats['deleted_count'] * 0.5  # Assume ~500KB per image
            print(f"   💰 Estimated storage savings: ~{estimated_savings_mb:.1f} MB")

            # Update BigQuery paths if needed
            if final_stats['renames']:
                update_bigquery_paths(final_stats['renames'])

        else:
            print("❌ Cleanup cancelled by user")

    except Exception as e:
        print(f"❌ Cleanup failed: {str(e)}")
        return False

    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 GCS cleanup completed successfully!")
    else:
        print("\n💥 GCS cleanup failed!")