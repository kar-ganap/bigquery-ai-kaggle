#!/usr/bin/env python3
"""
Clean Only BigQuery Tables - Preserve Local Files
"""
from src.utils.bigquery_client import get_bigquery_client, run_query

def clean_only_bigquery_tables():
    """Delete only BigQuery tables, preserve all local files"""

    print("🧹 CLEANING ONLY BIGQUERY TABLES")
    print("=" * 50)

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

        print(f"\n🗑️  BigQuery cleanup completed!")
        print(f"   📊 Deleted {deleted_count}/{len(table_list)} tables")
        print(f"   📁 Local files preserved (including clean_checkpoints)")

        return True

    except Exception as e:
        print(f"❌ Clean up failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    clean_only_bigquery_tables()