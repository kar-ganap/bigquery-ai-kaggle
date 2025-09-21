#!/usr/bin/env python3
"""
Check the raw data URLs for ads that don't have media storage
"""
import os
from src.utils.bigquery_client import run_query

def check_raw_urls():
    """Check URL fields in raw data for missing media ads"""
    
    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
    
    print("🔍 CHECKING RAW URL DATA FOR MISSING MEDIA ADS")
    print("==============================================")
    
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
    
    # Check URL fields for ads without media storage
    url_query = f"""
    SELECT 
        r.ad_archive_id,
        r.brand,
        r.media_type as raw_media_type,
        r.computed_media_type,
        r.media_storage_path,
        
        -- Check for URL fields using column inspection
        CASE 
            WHEN EXISTS (
                SELECT column_name 
                FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS` 
                WHERE table_name = '{raw_table}' AND column_name = 'original_image_url'
            ) THEN 'HAS_FIELD'
            ELSE 'NO_FIELD'
        END as has_original_image_url_field,
        
        CASE 
            WHEN EXISTS (
                SELECT column_name 
                FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS` 
                WHERE table_name = '{raw_table}' AND column_name = 'resized_image_url'
            ) THEN 'HAS_FIELD'
            ELSE 'NO_FIELD'
        END as has_resized_image_url_field,
        
        CASE 
            WHEN EXISTS (
                SELECT column_name 
                FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS` 
                WHERE table_name = '{raw_table}' AND column_name = 'video_preview_image_url'
            ) THEN 'HAS_FIELD'
            ELSE 'NO_FIELD'
        END as has_video_preview_url_field
    
    FROM `{BQ_PROJECT}.{BQ_DATASET}.{raw_table}` r
    WHERE r.ad_archive_id IN (
        SELECT ad_archive_id 
        FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
        WHERE media_storage_path IS NULL
    )
    LIMIT 5
    """
    
    result = run_query(url_query)
    
    if result.empty:
        print("❌ No matching ads found in raw data")
        return
    
    print(f"📊 Found {len(result)} matching ads in raw data")
    print("")
    
    # Show results
    for i, row in result.iterrows():
        print(f"ID: {row['ad_archive_id']}")
        print(f"Brand: {row['brand']}")
        print(f"Raw Media Type: {row['raw_media_type']}")
        print(f"Computed Media Type: {row['computed_media_type']}")
        print(f"Media Storage Path: {row['media_storage_path']}")
        print(f"Has Original Image URL Field: {row['has_original_image_url_field']}")
        print(f"Has Resized Image URL Field: {row['has_resized_image_url_field']}")
        print(f"Has Video Preview URL Field: {row['has_video_preview_url_field']}")
        print("-" * 60)

if __name__ == "__main__":
    check_raw_urls()
