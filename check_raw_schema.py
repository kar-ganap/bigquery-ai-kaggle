#!/usr/bin/env python3
"""
Check raw table schema and sample data for missing media ads
"""
import os
from src.utils.bigquery_client import run_query

def check_raw_schema():
    """Check raw table schema and data"""
    
    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
    
    print("🔍 CHECKING RAW TABLE SCHEMA AND SAMPLE DATA")
    print("============================================")
    
    # Get the most recent raw table
    table_query = f"""
    SELECT table_name 
    FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'ads_raw_%'
    ORDER BY creation_time DESC
    LIMIT 1
    """
    
    table_result = run_query(table_query)
    if table_result.empty:
        print("❌ No raw table found")
        return
    
    raw_table = table_result.iloc[0]['table_name']
    print(f"📊 Using raw table: {raw_table}")
    
    # Check table schema
    schema_query = f"""
    SELECT column_name, data_type
    FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{raw_table}'
    ORDER BY ordinal_position
    """
    
    schema_result = run_query(schema_query)
    print(f"\n📋 TABLE SCHEMA ({len(schema_result)} columns):")
    url_columns = []
    for i, row in schema_result.iterrows():
        col_name = row['column_name']
        print(f"   • {col_name}: {row['data_type']}")
        if 'url' in col_name.lower():
            url_columns.append(col_name)
    
    print(f"\n🔗 URL-related columns found: {url_columns}")
    
    # Check sample data for missing media ads
    missing_ads_query = f"""
    SELECT ad_archive_id 
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    WHERE media_storage_path IS NULL
    LIMIT 3
    """
    
    missing_ads = run_query(missing_ads_query)
    if missing_ads.empty:
        print("\n✅ No ads without media storage found")
        return
    
    sample_ids = missing_ads['ad_archive_id'].tolist()
    print(f"\n📊 Sample ads without media: {sample_ids}")
    
    # Build dynamic query for URL columns
    url_select = ""
    if url_columns:
        url_select = ", " + ", ".join(url_columns)
    
    # Check raw data for these sample ads
    sample_query = f"""
    SELECT 
        ad_archive_id,
        brand,
        media_type,
        computed_media_type,
        media_storage_path
        {url_select}
    FROM `{BQ_PROJECT}.{BQ_DATASET}.{raw_table}`
    WHERE ad_archive_id IN UNNEST({sample_ids})
    """
    
    sample_result = run_query(sample_query)
    
    print(f"\n📋 RAW DATA FOR SAMPLE ADS:")
    print("-" * 80)
    
    for i, row in sample_result.iterrows():
        print(f"ID: {row['ad_archive_id']}")
        print(f"Brand: {row['brand']}")
        print(f"Media Type: {row['media_type']}")
        print(f"Computed Media Type: {row['computed_media_type']}")
        print(f"Media Storage Path: {row['media_storage_path']}")
        
        # Show URL fields
        for col in url_columns:
            if col in row:
                value = row[col]
                if value is not None and str(value).strip():
                    print(f"{col}: {value}")
                else:
                    print(f"{col}: NULL/EMPTY")
        print("-" * 60)

if __name__ == "__main__":
    check_raw_schema()
