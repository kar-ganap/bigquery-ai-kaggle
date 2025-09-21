#!/usr/bin/env python3
"""
Investigate the 61 ads that don't have media storage
"""
import os
import pandas as pd
from src.utils.bigquery_client import run_query

def investigate_missing_media():
    """Analyze ads without media storage"""
    
    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
    
    print("🔍 INVESTIGATING MISSING MEDIA STORAGE")
    print("=====================================")
    
    # Query to find ads without media storage
    query = f"""
    SELECT 
        ad_archive_id,
        brand,
        creative_text,
        title,
        media_type,
        media_storage_path,
        -- Check what URLs were in the original data
        JSON_EXTRACT_SCALAR(publisher_platforms, '$[0]') as platform,
        CASE 
            WHEN creative_text IS NULL AND title IS NULL THEN 'NO_TEXT'
            WHEN LENGTH(COALESCE(creative_text, '') + COALESCE(title, '')) < 10 THEN 'MINIMAL_TEXT'
            ELSE 'HAS_TEXT'
        END as text_content_status
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    WHERE media_storage_path IS NULL
    ORDER BY brand, ad_archive_id
    """
    
    result = run_query(query)
    
    if result.empty:
        print("   ✅ No ads found without media storage")
        return
    
    print(f"   📊 Found {len(result)} ads without media storage")
    print("")
    
    # Analyze by brand
    brand_counts = result['brand'].value_counts()
    print("📋 BREAKDOWN BY BRAND:")
    for brand, count in brand_counts.items():
        print(f"   • {brand}: {count} ads")
    print("")
    
    # Analyze by media type
    media_type_counts = result['media_type'].value_counts()
    print("📋 BREAKDOWN BY MEDIA TYPE:")
    for media_type, count in media_type_counts.items():
        print(f"   • {media_type}: {count} ads")
    print("")
    
    # Analyze by text content
    text_status_counts = result['text_content_status'].value_counts()
    print("📋 BREAKDOWN BY TEXT CONTENT:")
    for status, count in text_status_counts.items():
        print(f"   • {status}: {count} ads")
    print("")
    
    # Show sample ads
    print("📋 SAMPLE ADS WITHOUT MEDIA:")
    print("-" * 80)
    for i, row in result.head(10).iterrows():
        print(f"ID: {row['ad_archive_id']}")
        print(f"Brand: {row['brand']}")
        print(f"Media Type: {row['media_type']}")
        print(f"Text: {(row['creative_text'] or '')[:100]}...")
        print(f"Title: {(row['title'] or '')[:50]}...")
        print("-" * 40)
    
    # Check if we can find these in the raw data to see original URLs
    print("\n🔍 CHECKING ORIGINAL URL DATA...")
    
    # Get the most recent run's raw data
    raw_query = f"""
    SELECT table_name 
    FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'ads_raw_%'
    ORDER BY creation_time DESC
    LIMIT 1
    """
    
    raw_table_result = run_query(raw_query)
    
    if not raw_table_result.empty:
        raw_table = raw_table_result.iloc[0]['table_name']
        print(f"   📊 Found recent raw table: {raw_table}")
        
        # Check URLs in raw data for missing media ads
        url_query = f"""
        SELECT 
            ad_archive_id,
            brand,
            media_type as raw_media_type,
            computed_media_type,
            media_storage_path,
            -- Check for URL fields if they exist
            CASE 
                WHEN REGEXP_CONTAINS(TO_JSON_STRING(STRUCT(*)), r'"original_image_url"') 
                THEN JSON_EXTRACT_SCALAR(TO_JSON_STRING(STRUCT(*)), '$.original_image_url')
                ELSE 'NO_FIELD'
            END as original_image_url_check,
            CASE 
                WHEN REGEXP_CONTAINS(TO_JSON_STRING(STRUCT(*)), r'"resized_image_url"') 
                THEN JSON_EXTRACT_SCALAR(TO_JSON_STRING(STRUCT(*)), '$.resized_image_url')
                ELSE 'NO_FIELD'
            END as resized_image_url_check,
            CASE 
                WHEN REGEXP_CONTAINS(TO_JSON_STRING(STRUCT(*)), r'"video_preview_image_url"') 
                THEN JSON_EXTRACT_SCALAR(TO_JSON_STRING(STRUCT(*)), '$.video_preview_image_url')
                ELSE 'NO_FIELD'
            END as video_preview_url_check
        FROM `{BQ_PROJECT}.{BQ_DATASET}.{raw_table}`
        WHERE ad_archive_id IN (
            SELECT ad_archive_id 
            FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
            WHERE media_storage_path IS NULL
        )
        LIMIT 10
        """
        
        url_result = run_query(url_query)
        
        if not url_result.empty:
            print(f"   📊 Found {len(url_result)} matching ads in raw data")
            print("\n📋 URL ANALYSIS FOR MISSING MEDIA:")
            print("-" * 80)
            
            for i, row in url_result.iterrows():
                print(f"ID: {row['ad_archive_id']}")
                print(f"Brand: {row['brand']}")
                print(f"Raw Media Type: {row['raw_media_type']}")
                print(f"Computed Media Type: {row['computed_media_type']}")
                print(f"Original Image URL: {row['original_image_url_check']}")
                print(f"Resized Image URL: {row['resized_image_url_check']}")
                print(f"Video Preview URL: {row['video_preview_url_check']}")
                print("-" * 40)
        else:
            print("   ❌ No matching ads found in raw data")
    else:
        print("   ❌ No recent raw table found")

if __name__ == "__main__":
    investigate_missing_media()
