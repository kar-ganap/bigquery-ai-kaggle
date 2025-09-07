#!/usr/bin/env python3
"""
Check what data exists in BigQuery ads_demo dataset
"""

import os
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def check_dataset_tables():
    """List all tables in the dataset"""
    print(f"Checking dataset: {PROJECT_ID}.{DATASET_ID}")
    print("="*60)
    
    try:
        # List tables in dataset
        tables = client.list_tables(DATASET_ID)
        table_list = list(tables)
        
        print(f"Found {len(table_list)} tables/views:")
        
        for table in table_list:
            table_ref = client.get_table(table.reference)
            table_type = "VIEW" if table_ref.table_type == "VIEW" else "TABLE"
            rows = table_ref.num_rows if table_ref.num_rows is not None else "N/A"
            print(f"  {table_type}: {table.table_id} ({rows} rows)")
            
        return table_list
        
    except Exception as e:
        print(f"Error accessing dataset: {e}")
        return None

def sample_ads_raw():
    """Check if we have basic ads data"""
    try:
        query = f"""
        SELECT 
          COUNT(*) as total_rows,
          COUNT(DISTINCT brand) as unique_brands,
          MIN(first_seen) as earliest_date,
          MAX(first_seen) as latest_date
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_raw`
        LIMIT 1
        """
        
        query_job = client.query(query)
        result = query_job.result()
        
        for row in result:
            print(f"\nads_raw table summary:")
            print(f"  Total rows: {row.total_rows}")
            print(f"  Unique brands: {row.unique_brands}")
            print(f"  Date range: {row.earliest_date} to {row.latest_date}")
            
    except Exception as e:
        print(f"Error querying ads_raw: {e}")

def check_sample_data():
    """Get sample of actual data"""
    try:
        query = f"""
        SELECT 
          brand,
          creative_text,
          media_type,
          first_seen
        FROM `{PROJECT_ID}.{DATASET_ID}.ads_raw`
        WHERE brand IS NOT NULL
        ORDER BY first_seen DESC
        LIMIT 5
        """
        
        query_job = client.query(query)
        result = query_job.result()
        
        print(f"\nSample ads data:")
        for row in result:
            print(f"  {row.brand}: {row.creative_text[:50]}... ({row.media_type}, {row.first_seen})")
            
    except Exception as e:
        print(f"Error getting sample data: {e}")

if __name__ == "__main__":
    check_dataset_tables()
    sample_ads_raw()
    check_sample_data()