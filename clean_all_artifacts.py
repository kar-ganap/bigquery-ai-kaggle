#!/usr/bin/env python3
"""
Clean All BigQuery Artifacts - Complete Reset
"""
from src.utils.bigquery_client import get_bigquery_client, run_query

def clean_all_bigquery_artifacts():
    """Delete all tables and start completely fresh"""

    print("🧹 COMPLETE CLEAN SLATE - Deleting All BigQuery Artifacts")
    print("=" * 70)

    try:
        client = get_bigquery_client()
        dataset_id = "bigquery-ai-kaggle-469620.ads_demo"

        # List all tables in the dataset
        tables = client.list_tables(dataset_id)
        table_list = [table.table_id for table in tables]

        print(f"📋 Found {len(table_list)} tables to delete:")
        for table_id in table_list:
            print(f"   • {table_id}")

        # Delete each table
        deleted_count = 0
        for table_id in table_list:
            try:
                table_ref = f"{dataset_id}.{table_id}"
                delete_query = f"DROP TABLE IF EXISTS `{table_ref}`"
                run_query(delete_query)
                print(f"   ✅ Deleted: {table_id}")
                deleted_count += 1
            except Exception as e:
                print(f"   ⚠️  Failed to delete {table_id}: {e}")

        print(f"\n🗑️  Clean up completed!")
        print(f"   📊 Deleted {deleted_count}/{len(table_list)} tables")
        print(f"   🆕 Ready for fresh pipeline run")

        return True

    except Exception as e:
        print(f"❌ Clean up failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def clean_local_artifacts():
    """Clean up local output files"""
    import os
    import glob

    print(f"\n🧹 Cleaning Local Artifacts...")

    # Clean checkpoint files
    checkpoint_pattern = "data/output/clean_checkpoints/*"
    checkpoint_files = glob.glob(checkpoint_pattern)

    for file_path in checkpoint_files:
        try:
            os.remove(file_path)
            print(f"   ✅ Deleted: {os.path.basename(file_path)}")
        except Exception as e:
            print(f"   ⚠️  Failed to delete {file_path}: {e}")

    print(f"   🗑️  Cleaned {len(checkpoint_files)} local files")

if __name__ == "__main__":
    # Clean BigQuery tables
    bigquery_success = clean_all_bigquery_artifacts()

    # Clean local files
    clean_local_artifacts()

    if bigquery_success:
        print(f"\n🚀 READY FOR CLEAN SLATE PIPELINE RUN!")
        print(f"   All artifacts cleaned - starting fresh")
        print(f"   Run: uv run python -m src.pipeline.orchestrator --brand 'Warby Parker' --vertical 'eyewear' --verbose")
    else:
        print(f"\n❌ Clean up had issues - check logs before proceeding")