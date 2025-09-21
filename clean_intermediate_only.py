#!/usr/bin/env python3
"""
Clean intermediate tables while preserving ads_with_dates for accumulation testing
"""
import os
from src.utils.bigquery_client import get_bigquery_client, run_query

def clean_intermediate_tables():
    """Clean only intermediate tables, preserve ads_with_dates for accumulation testing"""
    
    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
    
    # Only clean intermediate tables, NOT ads_with_dates
    tables_to_clean = [
        "ads_raw",
        "ads_embeddings",
        "audience_intelligence_analysis",
        "channel_intelligence_analysis", 
        "competitive_intelligence_analysis",
        "creative_intelligence_analysis",
        "cta_intelligence_analysis",
        "visual_intelligence_analysis"
    ]
    
    print("🧹 CLEANING INTERMEDIATE TABLES ONLY")
    print("✅ PRESERVING ads_with_dates for accumulation testing")
    print("==========================================================")
    
    for table_name in tables_to_clean:
        try:
            # Check if table exists
            check_query = f"""
            SELECT table_name
            FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '{table_name}'
            """
            result = run_query(check_query)
            
            if result.empty:
                print(f"   ⏭️  Table {table_name} doesn't exist - skipping")
                continue
            
            # Get row count before deletion
            count_query = f"SELECT COUNT(*) as count FROM `{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"
            count_result = run_query(count_query)
            row_count = count_result.iloc[0]['count'] if not count_result.empty else 0
            
            if row_count == 0:
                print(f"   ✅ Table {table_name} already empty")
                continue
            
            print(f"   🗑️  Cleaning {table_name} ({row_count} rows)")
            
            # Drop table completely for clean start
            drop_query = f"DROP TABLE IF EXISTS `{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"
            run_query(drop_query)
            
            print(f"   ✅ Dropped {table_name}")
            
        except Exception as e:
            print(f"   ⚠️  Failed to clean {table_name}: {e}")
    
    print("\n✅ INTERMEDIATE CLEANUP COMPLETED")
    print("📊 ads_with_dates table preserved for accumulation testing")
    
if __name__ == "__main__":
    clean_intermediate_tables()
