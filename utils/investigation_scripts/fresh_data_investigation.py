#!/usr/bin/env python3
"""
Fresh investigation to check ALL data sources and time windows
The user says they see ads in the UI that should be in the prior period
"""
import os
from google.cloud import bigquery

def investigate_all_data_sources():
    """Check every possible data source and time window"""
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-creds.json'
    client = bigquery.Client(project="bigquery-ai-kaggle-469620")
    
    print("🕵️ FRESH DATA INVESTIGATION")
    print("="*40)
    print("User reports seeing ads in UI that should be in prior period")
    print("Let's check ALL data sources comprehensively")
    print("")
    
    # 1. Check ALL tables in the dataset
    print("1️⃣ CHECKING ALL TABLES IN DATASET:")
    print("-" * 35)
    
    tables_query = """
    SELECT table_name, 
           creation_time
    FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '%ads%' OR table_name LIKE '%warby%'
    ORDER BY creation_time DESC
    """
    
    results = client.query(tables_query)
    table_names = []
    for row in results:
        print(f"  📊 {row.table_name}")
        print(f"     Created: {row.creation_time}")
        table_names.append(row.table_name)
        print("")
    
    # 2. Check ads_with_dates in detail (current data)
    print("2️⃣ DETAILED CHECK OF ads_with_dates:")
    print("-" * 35)
    
    detailed_check_query = """
    SELECT 
        brand,
        COUNT(*) as total_ads,
        MIN(DATE(start_timestamp)) as earliest_date,
        MAX(DATE(start_timestamp)) as latest_date,
        
        -- Show every single date that has ads
        ARRAY_AGG(DISTINCT DATE(start_timestamp) ORDER BY DATE(start_timestamp)) as all_dates_with_ads,
        
        -- Count by specific date ranges that matter for temporal intelligence
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN '2025-08-31' AND '2025-09-05' THEN 1 END) as aug31_sep5,
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN '2025-09-06' AND '2025-09-12' THEN 1 END) as sep6_sep12,
        COUNT(CASE WHEN DATE(start_timestamp) >= '2025-09-13' THEN 1 END) as sep13_plus,
        
        -- Temporal intelligence windows (today = 2025-09-13)
        COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 1 END) as last_7d_from_today,
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                                     AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                   THEN 1 END) as prior_7d_from_today
        
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY brand
    """
    
    results = client.query(detailed_check_query)
    for row in results:
        print(f"  📊 CURRENT ads_with_dates table:")
        print(f"     Brand: {row.brand}")
        print(f"     Total ads: {row.total_ads}")
        print(f"     Date range: {row.earliest_date} to {row.latest_date}")
        print(f"     All dates with ads: {list(row.all_dates_with_ads)}")
        print(f"")
        print(f"  📅 PERIOD BREAKDOWN:")
        print(f"     Aug 31 - Sep 5: {row.aug31_sep5} ads")
        print(f"     Sep 6 - Sep 12: {row.sep6_sep12} ads") 
        print(f"     Sep 13+: {row.sep13_plus} ads")
        print(f"")
        print(f"  🎯 TEMPORAL INTELLIGENCE WINDOWS (from today {client.query('SELECT CURRENT_DATE()').to_dataframe().iloc[0,0]}):")
        print(f"     Last 7d: {row.last_7d_from_today} ads")
        print(f"     Prior 7d: {row.prior_7d_from_today} ads")
        print(f"")
        
        if row.prior_7d_from_today == 0:
            print(f"  ❌ CONFIRMED: 0 ads in prior 7d window - this explains null velocity")
        else:
            print(f"  🎯 FOUND: {row.prior_7d_from_today} ads in prior 7d - velocity should NOT be null!")
    
    # 3. Check all ads_raw tables for more data
    print("3️⃣ CHECKING ALL ads_raw TABLES:")
    print("-" * 35)
    
    raw_tables = [name for name in table_names if 'ads_raw' in name]
    
    for table_name in raw_tables[:5]:  # Check up to 5 most recent
        try:
            raw_query = f"""
            SELECT 
                brand,
                COUNT(*) as total_ads,
                MIN(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as earliest_date,
                MAX(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as latest_date,
                ARRAY_AGG(DISTINCT DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) 
                         ORDER BY DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as all_dates
            FROM `bigquery-ai-kaggle-469620.ads_demo.{table_name}`
            WHERE brand = 'Warby Parker'
            GROUP BY brand
            """
            
            results = client.query(raw_query)
            for row in results:
                print(f"  📊 {table_name}:")
                print(f"     Total ads: {row.total_ads}")
                print(f"     Date range: {row.earliest_date} to {row.latest_date}")
                print(f"     All dates: {list(row.all_dates)}")
                print("")
                
        except Exception as e:
            print(f"  ❌ {table_name}: Error - {str(e)}")
    
    # 4. Check if ads_with_dates is built from the wrong source
    print("4️⃣ CHECKING ads_with_dates SOURCE:")
    print("-" * 35)
    
    # Get the latest ads_raw table
    latest_raw_query = """
    SELECT table_name
    FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'ads_raw_warby_parker_%'
    ORDER BY creation_time DESC
    LIMIT 1
    """
    
    results = client.query(latest_raw_query)
    latest_raw_table = None
    for row in results:
        latest_raw_table = row.table_name
        print(f"  📄 Latest ads_raw table: {latest_raw_table}")
    
    if latest_raw_table:
        # Compare what's in latest raw vs what's in ads_with_dates
        comparison_query = f"""
        SELECT 
            'ads_with_dates' as source,
            COUNT(*) as ads_count,
            MIN(DATE(start_timestamp)) as earliest,
            MAX(DATE(start_timestamp)) as latest
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
        WHERE brand = 'Warby Parker'
        
        UNION ALL
        
        SELECT 
            '{latest_raw_table}' as source,
            COUNT(*) as ads_count,
            MIN(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as earliest,
            MAX(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as latest
        FROM `bigquery-ai-kaggle-469620.ads_demo.{latest_raw_table}`
        WHERE brand = 'Warby Parker'
        
        ORDER BY source
        """
        
        results = client.query(comparison_query)
        print(f"  🔍 SOURCE COMPARISON:")
        for row in results:
            print(f"     {row.source}: {row.ads_count} ads ({row.earliest} to {row.latest})")
    
    # 5. Final recommendation
    print("\n5️⃣ CONCLUSION & NEXT STEPS:")
    print("-" * 35)
    print("  Based on findings above:")
    print("  • If ads_with_dates has limited date range but raw tables have more data:")
    print("    → Need to rebuild ads_with_dates from a table with broader date range")
    print("  • If all tables show same limited range:")  
    print("    → Need fresh data ingestion from Meta Ads Library API")
    print("  • The user seeing ads in UI suggests data exists but isn't in our tables")

if __name__ == "__main__":
    investigate_all_data_sources()