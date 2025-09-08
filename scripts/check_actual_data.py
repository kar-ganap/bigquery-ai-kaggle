#!/usr/bin/env python3
"""
Check what actual data we have available for testing
"""

import os
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def check_fb_ads_scale_test():
    """Check the fb_ads_scale_test table"""
    try:
        query = f"""
        SELECT 
          COUNT(*) as total_rows,
          COUNT(DISTINCT brand) as unique_brands,
          STRING_AGG(DISTINCT brand) as brand_list,
          MIN(first_seen) as earliest_date,
          MAX(first_seen) as latest_date
        FROM `{PROJECT_ID}.{DATASET_ID}.fb_ads_scale_test`
        """
        
        result = client.query(query).result()
        
        for row in result:
            print("fb_ads_scale_test summary:")
            print(f"  Total ads: {row.total_rows}")
            print(f"  Unique brands: {row.unique_brands}")
            print(f"  Brands: {row.brand_list}")
            print(f"  Date range: {row.earliest_date} to {row.latest_date}")
            
    except Exception as e:
        print(f"Error: {e}")

def check_strategic_labels():
    """Check what strategic labels data we have"""
    try:
        query = f"""
        SELECT 
          COUNT(*) as total_rows,
          COUNT(DISTINCT brand) as unique_brands,
          COUNTIF(funnel IS NOT NULL) as has_funnel,
          COUNTIF(urgency_score IS NOT NULL) as has_urgency
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels`
        """
        
        result = client.query(query).result()
        
        for row in result:
            print(f"\nads_strategic_labels summary:")
            print(f"  Total rows: {row.total_rows}")
            print(f"  Unique brands: {row.unique_brands}")
            print(f"  Has funnel: {row.has_funnel}")
            print(f"  Has urgency: {row.has_urgency}")
            
    except Exception as e:
        print(f"Error: {e}")

def sample_actual_data():
    """Get sample of what we actually have"""
    try:
        query = f"""
        SELECT 
          brand,
          creative_text,
          media_type,
          first_seen,
          publisher_platforms
        FROM `{PROJECT_ID}.{DATASET_ID}.fb_ads_scale_test`
        ORDER BY first_seen DESC
        LIMIT 5
        """
        
        result = client.query(query).result()
        
        print(f"\nSample fb_ads_scale_test data:")
        for row in result:
            print(f"  {row.brand}: {row.creative_text[:50]}... ({row.media_type})")
            print(f"    Platforms: {row.publisher_platforms}, Date: {row.first_seen}")
            
    except Exception as e:
        print(f"Error: {e}")

def check_table_schema():
    """Check what columns are available"""
    try:
        table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.fb_ads_scale_test")
        
        print(f"\nfb_ads_scale_test schema:")
        for field in table.schema:
            print(f"  {field.name}: {field.field_type}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_fb_ads_scale_test()
    check_strategic_labels()
    sample_actual_data()
    check_table_schema()