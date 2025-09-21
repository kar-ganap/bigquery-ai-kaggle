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
        JSON_EXTRACT_SCALAR(publisher_platforms, '$[0]') as platform,
        CASE 
            WHEN creative_text IS NULL AND title IS NULL THEN 'NO_TEXT'
            WHEN LENGTH(COALESCE(creative_text, '') || COALESCE(title, '')) < 10 THEN 'MINIMAL_TEXT'
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
        print(f"Text: {str(row['creative_text'] or '')[:100]}...")
        print(f"Title: {str(row['title'] or '')[:50]}...")
        print("-" * 40)

if __name__ == "__main__":
    investigate_missing_media()
