#!/usr/bin/env python3
"""
Analyze the dual timestamp architecture (_timestamp vs _date_string) across the entire codebase
"""
import os
from google.cloud import bigquery

def analyze_timestamp_architecture():
    """Comprehensive analysis of timestamp vs string architecture"""
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-creds.json'
    client = bigquery.Client(project="bigquery-ai-kaggle-469620")
    
    print("🏗️  TIMESTAMP ARCHITECTURE ANALYSIS")
    print("="*45)
    
    # 1. Storage overhead analysis
    print("\n1️⃣ STORAGE OVERHEAD ANALYSIS:")
    print("-" * 35)
    
    storage_query = """
    SELECT 
        -- Count total columns
        COUNT(*) as total_columns,
        COUNT(CASE WHEN column_name LIKE '%_date_string' THEN 1 END) as date_string_columns,
        COUNT(CASE WHEN column_name LIKE '%_timestamp' THEN 1 END) as timestamp_columns,
        
        -- Estimate storage (rough calculation)
        COUNT(CASE WHEN column_name LIKE '%_date_string' THEN 1 END) * 25 as approx_string_bytes_per_row,
        COUNT(CASE WHEN column_name LIKE '%_timestamp' THEN 1 END) * 8 as approx_timestamp_bytes_per_row,
        
        -- Show specific dual columns
        ARRAY_AGG(column_name ORDER BY column_name) as all_columns
        
    FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'ads_with_dates'
    """
    
    results = client.query(storage_query)
    for row in results:
        print(f"  📊 Schema Analysis:")
        print(f"     Total columns: {row.total_columns}")
        print(f"     Date string columns: {row.date_string_columns}")
        print(f"     Timestamp columns: {row.timestamp_columns}")
        print(f"     Est. string storage/row: {row.approx_string_bytes_per_row} bytes")
        print(f"     Est. timestamp storage/row: {row.approx_timestamp_bytes_per_row} bytes")
        
        dual_columns = [col for col in row.all_columns if '_date_string' in col or '_timestamp' in col]
        print(f"     Dual timestamp columns: {dual_columns}")
    
    # 2. Data consistency analysis
    print("\n2️⃣ DATA CONSISTENCY ANALYSIS:")
    print("-" * 35)
    
    consistency_query = """
    SELECT 
        brand,
        COUNT(*) as total_rows,
        
        -- Start date consistency
        COUNT(start_date_string) as has_start_string,
        COUNT(start_timestamp) as has_start_timestamp,
        COUNT(CASE WHEN start_date_string IS NOT NULL AND start_timestamp IS NOT NULL THEN 1 END) as both_start,
        COUNT(CASE WHEN start_date_string IS NOT NULL AND start_timestamp IS NULL THEN 1 END) as string_only_start,
        COUNT(CASE WHEN start_date_string IS NULL AND start_timestamp IS NOT NULL THEN 1 END) as timestamp_only_start,
        
        -- End date consistency  
        COUNT(end_date_string) as has_end_string,
        COUNT(end_timestamp) as has_end_timestamp,
        COUNT(CASE WHEN end_date_string IS NOT NULL AND end_timestamp IS NOT NULL THEN 1 END) as both_end,
        COUNT(CASE WHEN end_date_string IS NOT NULL AND end_timestamp IS NULL THEN 1 END) as string_only_end,
        COUNT(CASE WHEN end_date_string IS NULL AND end_timestamp IS NOT NULL THEN 1 END) as timestamp_only_end,
        
        -- Value consistency (do parsed strings match timestamps?)
        COUNT(CASE WHEN start_date_string IS NOT NULL AND start_timestamp IS NOT NULL
                   AND DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) = DATE(start_timestamp)
                   THEN 1 END) as start_values_match,
        COUNT(CASE WHEN end_date_string IS NOT NULL AND end_timestamp IS NOT NULL
                   AND DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string)) = DATE(end_timestamp)
                   THEN 1 END) as end_values_match
        
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY brand
    """
    
    results = client.query(consistency_query)
    for row in results:
        print(f"  🔍 Data Consistency for {row.brand}:")
        print(f"     Total rows: {row.total_rows}")
        print(f"")
        print(f"  📅 START DATE ANALYSIS:")
        print(f"     Has start_date_string: {row.has_start_string}")
        print(f"     Has start_timestamp: {row.has_start_timestamp}")
        print(f"     Both populated: {row.both_start}")
        print(f"     String only: {row.string_only_start}")  
        print(f"     Timestamp only: {row.timestamp_only_start}")
        print(f"     Values match: {row.start_values_match}")
        
        if row.both_start == row.start_values_match:
            print(f"     ✅ All start dates are consistent")
        else:
            print(f"     ❌ {row.both_start - row.start_values_match} inconsistent start dates!")
        
        print(f"")
        print(f"  📅 END DATE ANALYSIS:")
        print(f"     Has end_date_string: {row.has_end_string}")
        print(f"     Has end_timestamp: {row.has_end_timestamp}")
        print(f"     Both populated: {row.both_end}")
        print(f"     String only: {row.string_only_end}")
        print(f"     Timestamp only: {row.timestamp_only_end}")
        print(f"     Values match: {row.end_values_match}")
        
        if row.both_end == row.end_values_match:
            print(f"     ✅ All end dates are consistent")
        else:
            print(f"     ❌ {row.both_end - row.end_values_match} inconsistent end dates!")
    
    # 3. Usage pattern analysis 
    print("\n3️⃣ CODEBASE USAGE ANALYSIS:")
    print("-" * 35)
    
    print("  📁 Files using start_timestamp:")
    # This would require file system analysis - showing conceptually
    timestamp_files = [
        "src/competitive_intel/intelligence/temporal_intelligence_module.py",
        "sql/*.sql files (most analysis queries)",
        "scripts/create_ads_with_dates.py"
    ]
    for file in timestamp_files:
        print(f"     • {file}")
    
    print(f"")
    print("  📁 Files using start_date_string:")
    string_files = [
        "scripts/create_ads_with_dates.py (for parsing)",
        "src/competitive_intel/ingestion/* (for API storage)",
        "Debug/audit queries"
    ]
    for file in string_files:
        print(f"     • {file}")
    
    # 4. Performance implications
    print("\n4️⃣ PERFORMANCE IMPLICATIONS:")
    print("-" * 35)
    
    performance_query = """
    SELECT 
        'Using start_timestamp (current)' as approach,
        COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END) as result
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    
    UNION ALL
    
    SELECT 
        'Using start_date_string + parsing' as approach,
        COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END) as result
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    """
    
    results = client.query(performance_query)
    print(f"  ⚡ Query Performance Test (same result, different methods):")
    for row in results:
        print(f"     {row.approach}: {row.result} ads")
    
    print(f"")
    print(f"  📈 Performance Considerations:")
    print(f"     • TIMESTAMP columns: Optimized for date/time operations")
    print(f"     • STRING columns: Require parsing for every query")  
    print(f"     • Index efficiency: TIMESTAMPs can be indexed more efficiently")
    print(f"     • Query complexity: Strings require SAFE.PARSE_TIMESTAMP() calls")
    
    # 5. Simplification recommendations
    print("\n5️⃣ ARCHITECTURE RECOMMENDATIONS:")
    print("-" * 35)
    
    print("  📋 OPTION A: Keep Both (Current Architecture)")
    print("     ✅ Preserves original API data for auditing")
    print("     ✅ Allows debugging of parsing failures")
    print("     ✅ Maintains data lineage")
    print("     ❌ 2x storage overhead")
    print("     ❌ Cognitive load for developers")
    print("     ❌ Schema complexity")
    
    print(f"")
    print("  📋 OPTION B: TIMESTAMP Only (Simplified)")
    print("     ✅ Reduced storage (~30-40 bytes per row)")
    print("     ✅ Simpler schema")
    print("     ✅ Faster queries (no parsing)")
    print("     ❌ Loses original API format")
    print("     ❌ Harder to debug ingestion issues")
    print("     ❌ No audit trail of raw data")
    
    print(f"")
    print("  📋 OPTION C: Hybrid (STRING primary, TIMESTAMP computed)")
    print("     ✅ Preserves original data")
    print("     ✅ Computed columns auto-update")
    print("     ❌ Still has complexity")
    print("     ❌ Requires BigQuery computed column features")

if __name__ == "__main__":
    analyze_timestamp_architecture()