#!/usr/bin/env python3
"""
Get all text-only ad IDs for manual verification
"""
import os
from src.utils.bigquery_client import run_query

def get_all_text_only_ads():
    """Get all ad IDs that don't have media storage"""
    
    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
    
    query = f"""
    SELECT 
        ad_archive_id,
        brand,
        LEFT(creative_text, 60) as text_preview
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    WHERE media_storage_path IS NULL
    ORDER BY brand, ad_archive_id
    """
    
    result = run_query(query)
    
    print(f"📋 ALL {len(result)} TEXT-ONLY AD IDs:")
    print("=" * 50)
    
    current_brand = None
    for i, row in result.iterrows():
        if row['brand'] != current_brand:
            current_brand = row['brand']
            print(f"\n🏢 {current_brand} ({len(result[result['brand'] == current_brand])} ads):")
        
        print(f"   • {row['ad_archive_id']} - \"{row['text_preview']}...\"")
    
    print(f"\n🔗 Manual verification URLs:")
    print("Copy any ID and visit: https://www.facebook.com/ads/library?id=[AD_ID]")

if __name__ == "__main__":
    get_all_text_only_ads()
