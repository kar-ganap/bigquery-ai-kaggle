#!/usr/bin/env python3
"""
Check BigQuery table schema for media_storage_path field
"""
import os
from src.utils.bigquery_client import run_query

def check_schema():
    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

    print("🔍 CHECKING RAW TABLE SCHEMA FOR MEDIA_STORAGE_PATH")
    print("=" * 50)

    # Get the most recent raw table
    table_query = f"""
    SELECT table_name
    FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'ads_raw_%'
    ORDER BY creation_time DESC
    LIMIT 1
    """

    result = run_query(table_query)
    if not result.empty:
        raw_table = result.iloc[0]['table_name']
        print(f"📊 Latest raw table: {raw_table}")

        # Check table schema
        schema_query = f"""
        SELECT column_name, data_type
        FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{raw_table}'
        ORDER BY ordinal_position
        """

        schema_result = run_query(schema_query)
        print(f"\n📋 TABLE SCHEMA ({len(schema_result)} columns):")

        media_fields = []
        storage_field_found = False

        for i, row in schema_result.iterrows():
            col_name = row['column_name']
            if 'media' in col_name.lower():
                media_fields.append(f"   • {col_name}: {row['data_type']}")
                if col_name == 'media_storage_path':
                    storage_field_found = True

        if media_fields:
            print("\n🎯 MEDIA-RELATED FIELDS:")
            for field in media_fields:
                print(field)
        else:
            print("\n❌ NO MEDIA-RELATED FIELDS FOUND!")

        if storage_field_found:
            print("\n✅ media_storage_path field EXISTS in raw table")
        else:
            print("\n❌ media_storage_path field NOT FOUND in raw table")
            print("   This explains why Strategic Labeling SQL fails!")

    else:
        print("❌ No raw table found")

if __name__ == "__main__":
    check_schema()