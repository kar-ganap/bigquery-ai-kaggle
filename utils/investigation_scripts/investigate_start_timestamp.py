#!/usr/bin/env python3
"""
Deep investigation into start_timestamp vs start_date_string architecture.
Understanding the full impact and ensuring temporal intelligence works correctly.
"""
import os
from google.cloud import bigquery

def investigate_start_timestamp_architecture():
    """Comprehensive investigation of timestamp architecture"""
    
    client = bigquery.Client(project="bigquery-ai-kaggle-469620")
    
    print("🔍 COMPREHENSIVE START_TIMESTAMP ARCHITECTURE INVESTIGATION")
    print("="*70)
    
    # 1. Check if ads_with_dates table exists and its schema
    print("\n1️⃣ CHECKING ads_with_dates TABLE SCHEMA:")
    print("-" * 40)
    
    try:
        schema_query = """
        SELECT column_name, data_type, is_nullable
        FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.COLUMNS` 
        WHERE table_name = 'ads_with_dates'
        ORDER BY ordinal_position;
        """
        
        schema_results = client.query(schema_query)
        timestamp_cols = []
        for row in schema_results:
            print(f"  📋 {row.column_name} ({row.data_type}) - nullable: {row.is_nullable}")
            if 'timestamp' in row.column_name.lower() or 'date' in row.column_name.lower():
                timestamp_cols.append(row.column_name)
        
        print(f"\n  🕒 Timestamp/Date columns found: {timestamp_cols}")
        
    except Exception as e:
        print(f"  ❌ ads_with_dates table not found: {e}")
        print("  💡 This could be the root cause of the temporal intelligence issue")
        return False
    
    # 2. Check data quality in ads_with_dates  
    print("\n2️⃣ CHECKING TEMPORAL DATA QUALITY:")
    print("-" * 40)
    
    try:
        data_quality_query = """
        SELECT 
            brand,
            COUNT(*) as total_ads,
            COUNT(start_date_string) as has_start_date_string,
            COUNT(start_timestamp) as has_start_timestamp,
            COUNT(CASE WHEN start_timestamp IS NOT NULL AND start_date_string IS NOT NULL 
                      THEN 1 END) as both_populated,
            MIN(DATE(start_timestamp)) as earliest_timestamp,
            MAX(DATE(start_timestamp)) as latest_timestamp,
            -- Check for parsing failures
            COUNT(CASE WHEN start_date_string IS NOT NULL AND start_timestamp IS NULL 
                      THEN 1 END) as parse_failures
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
        WHERE brand = 'Warby Parker'
        GROUP BY brand
        """
        
        results = client.query(data_quality_query)
        for row in results:
            print(f"  📊 Brand: {row.brand}")
            print(f"     Total ads: {row.total_ads}")
            print(f"     Has start_date_string: {row.has_start_date_string}")
            print(f"     Has start_timestamp: {row.has_start_timestamp}")
            print(f"     Both populated: {row.both_populated}")
            print(f"     Parse failures: {row.parse_failures}")
            print(f"     Timestamp range: {row.earliest_timestamp} to {row.latest_timestamp}")
            
            if row.parse_failures > 0:
                print(f"  ⚠️  WARNING: {row.parse_failures} timestamp parsing failures!")
            if row.has_start_timestamp == 0:
                print(f"  🚨 CRITICAL: No start_timestamp values - this explains null velocity!")
                
    except Exception as e:
        print(f"  ❌ Data quality check failed: {e}")
    
    # 3. Test the exact temporal intelligence query  
    print("\n3️⃣ TESTING TEMPORAL INTELLIGENCE QUERY:")
    print("-" * 40)
    
    try:
        temporal_test_query = """
        SELECT 
            r.brand,
            -- Test the exact same logic from temporal_intelligence_module.py
            COUNT(CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_last_7d,
            COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_prior_7d,
            -- Calculate what velocity_change_7d should be
            SAFE_DIVIDE(
                COUNT(CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                          THEN 1 END) - 
                COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                          THEN 1 END), 
                NULLIF(COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                              THEN 1 END), 0)
            ) as calculated_velocity_change_7d
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r
        WHERE r.brand = 'Warby Parker'
        GROUP BY r.brand
        """
        
        temporal_results = client.query(temporal_test_query)
        for row in temporal_results:
            print(f"  🎯 TEMPORAL INTELLIGENCE TEST RESULTS:")
            print(f"     Brand: {row.brand}")
            print(f"     Ads last 7d: {row.ads_last_7d}")
            print(f"     Ads prior 7d: {row.ads_prior_7d}")
            print(f"     Calculated velocity_change_7d: {row.calculated_velocity_change_7d}")
            
            if row.calculated_velocity_change_7d is None:
                if row.ads_prior_7d == 0:
                    print(f"  ✅ NULL is expected: No ads in prior period")
                else:
                    print(f"  🐛 NULL is unexpected: There are ads in prior period!")
            else:
                print(f"  ✅ Velocity calculation successful: {row.calculated_velocity_change_7d:.3f}")
                
    except Exception as e:
        print(f"  ❌ Temporal intelligence test failed: {e}")
    
    # 4. Compare with ads_raw data
    print("\n4️⃣ COMPARING WITH LATEST ads_raw DATA:")
    print("-" * 40)
    
    try:
        # Find latest ads_raw table
        latest_raw_query = """
        SELECT table_name
        FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'ads_raw_warby_parker_%'
        ORDER BY table_name DESC
        LIMIT 1
        """
        
        latest_raw_result = client.query(latest_raw_query)
        latest_table = None
        for row in latest_raw_result:
            latest_table = row.table_name
            print(f"  📄 Latest ads_raw table: {latest_table}")
        
        if latest_table:
            raw_comparison_query = f"""
            SELECT 
                'ads_raw' as source,
                COUNT(*) as total_ads,
                COUNT(start_date_string) as has_date_string,
                MIN(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as earliest,
                MAX(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as latest
            FROM `bigquery-ai-kaggle-469620.ads_demo.{latest_table}`
            WHERE brand = 'Warby Parker'
            
            UNION ALL
            
            SELECT 
                'ads_with_dates' as source,
                COUNT(*) as total_ads,
                COUNT(start_timestamp) as has_date_string,
                MIN(DATE(start_timestamp)) as earliest,
                MAX(DATE(start_timestamp)) as latest
            FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
            WHERE brand = 'Warby Parker'
            ORDER BY source
            """
            
            comparison_results = client.query(raw_comparison_query)
            print(f"  📊 DATA COMPARISON:")
            for row in comparison_results:
                print(f"     {row.source}: {row.total_ads} ads, range: {row.earliest} to {row.latest}")
        
    except Exception as e:
        print(f"  ❌ Raw data comparison failed: {e}")
    
    return True

if __name__ == "__main__":
    # Set up environment
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-creds.json'
    investigate_start_timestamp_architecture()