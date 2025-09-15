#!/usr/bin/env python3
"""
Independent Ads Verification - Comprehensive check of actual ads presence
User believes there should be new ads but temporal intelligence shows null values
"""
import os
from google.cloud import bigquery
from datetime import datetime, timedelta

def independent_ads_verification():
    """Independently verify if new ads are actually present"""
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-creds.json'
    client = bigquery.Client(project="bigquery-ai-kaggle-469620")
    
    print("🕵️ INDEPENDENT ADS VERIFICATION")
    print("=" * 50)
    print("User believes there should be new ads but temporal intelligence shows nulls")
    print("Let's independently verify the actual situation")
    print()
    
    # 1. Check current ads_with_dates table structure and content
    print("1️⃣ CURRENT ads_with_dates TABLE ANALYSIS:")
    print("-" * 45)
    
    current_table_query = """
    SELECT 
        brand,
        COUNT(*) as total_ads,
        MIN(DATE(start_timestamp)) as earliest_date,
        MAX(DATE(start_timestamp)) as latest_date,
        
        -- Show daily breakdown for last 14 days
        COUNT(CASE WHEN DATE(start_timestamp) = CURRENT_DATE() THEN 1 END) as today,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN 1 END) as yesterday,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) THEN 1 END) as day_minus_2,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) THEN 1 END) as day_minus_3,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY) THEN 1 END) as day_minus_4,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 5 DAY) THEN 1 END) as day_minus_5,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) THEN 1 END) as day_minus_6,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 1 END) as day_minus_7,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) THEN 1 END) as day_minus_8,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 9 DAY) THEN 1 END) as day_minus_9,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 10 DAY) THEN 1 END) as day_minus_10,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 11 DAY) THEN 1 END) as day_minus_11,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 12 DAY) THEN 1 END) as day_minus_12,
        COUNT(CASE WHEN DATE(start_timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) THEN 1 END) as day_minus_13,
        
        -- Critical temporal intelligence windows
        COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
              THEN 1 END) as last_7d,
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
              THEN 1 END) as prior_7d,
              
        -- Show exact calculation that temporal intelligence uses
        SAFE_DIVIDE(
            COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END) - 
            COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END), 
            NULLIF(COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                            AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END), 0)
        ) as calculated_velocity_change_7d
        
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY brand
    """
    
    results = client.query(current_table_query)
    for row in results:
        print(f"📊 Current ads_with_dates Analysis:")
        print(f"   Brand: {row.brand}")
        print(f"   Total ads: {row.total_ads}")
        print(f"   Date range: {row.earliest_date} to {row.latest_date}")
        print()
        
        print(f"📅 DAILY BREAKDOWN (last 14 days):")
        daily_counts = [
            ("Today", row.today),
            ("Yesterday", row.yesterday),
            ("Day -2", row.day_minus_2),
            ("Day -3", row.day_minus_3),
            ("Day -4", row.day_minus_4),
            ("Day -5", row.day_minus_5),
            ("Day -6", row.day_minus_6),
            ("Day -7", row.day_minus_7),
            ("Day -8", row.day_minus_8),
            ("Day -9", row.day_minus_9),
            ("Day -10", row.day_minus_10),
            ("Day -11", row.day_minus_11),
            ("Day -12", row.day_minus_12),
            ("Day -13", row.day_minus_13),
        ]
        
        for day_name, count in daily_counts:
            print(f"   {day_name}: {count} ads")
        
        print()
        print(f"🎯 TEMPORAL INTELLIGENCE WINDOWS:")
        print(f"   Last 7d (days 0-6): {row.last_7d} ads")
        print(f"   Prior 7d (days 7-13): {row.prior_7d} ads")
        print(f"   Calculated velocity_change_7d: {row.calculated_velocity_change_7d}")
        
        if row.calculated_velocity_change_7d is None:
            if row.prior_7d == 0:
                print(f"   🔍 NULL EXPLANATION: Prior 7d = 0, causing SAFE_DIVIDE to return null")
                print(f"   📊 This means there are NO ads in the prior period (days 7-13)")
            else:
                print(f"   ❌ UNEXPECTED NULL: Prior 7d = {row.prior_7d} > 0 but velocity still null!")
        else:
            print(f"   ✅ VELOCITY CALCULATED: {row.calculated_velocity_change_7d}")
    
    # 2. Check the source raw table used to build ads_with_dates
    print(f"\n2️⃣ SOURCE RAW TABLE VERIFICATION:")
    print("-" * 35)
    
    # Find the raw table that was used in the latest refresh
    raw_source_query = """
    SELECT table_name, creation_time
    FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'ads_raw_warby_parker_%'
    ORDER BY creation_time DESC
    LIMIT 3
    """
    
    results = client.query(raw_source_query)
    raw_tables = []
    for row in results:
        raw_tables.append(row.table_name)
        print(f"   📊 {row.table_name} (created: {row.creation_time})")
    
    if raw_tables:
        latest_raw = raw_tables[0]
        print(f"\n   🔍 Analyzing latest raw table: {latest_raw}")
        
        raw_analysis_query = f"""
        SELECT 
            brand,
            COUNT(*) as total_ads,
            MIN(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as earliest_date,
            MAX(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as latest_date,
            
            -- Check if raw has more recent data than ads_with_dates
            COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) >= CURRENT_DATE() THEN 1 END) as today_in_raw,
            COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 1 END) as last_7d_in_raw,
            COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 1 END) as prior_7d_in_raw
            
        FROM `bigquery-ai-kaggle-469620.ads_demo.{latest_raw}`
        WHERE brand = 'Warby Parker'
        GROUP BY brand
        """
        
        results = client.query(raw_analysis_query)
        for row in results:
            print(f"     📊 Raw table analysis:")
            print(f"        Total ads: {row.total_ads}")
            print(f"        Date range: {row.earliest_date} to {row.latest_date}")
            print(f"        Today in raw: {row.today_in_raw}")
            print(f"        Last 7d in raw: {row.last_7d_in_raw}")
            print(f"        Prior 7d in raw: {row.prior_7d_in_raw}")
    
    # 3. Compare ads_with_dates vs raw table data coverage
    print(f"\n3️⃣ DATA COVERAGE COMPARISON:")
    print("-" * 35)
    
    if raw_tables:
        coverage_query = f"""
        SELECT 
            'ads_with_dates' as source,
            COUNT(*) as total_ads,
            MIN(DATE(start_timestamp)) as earliest,
            MAX(DATE(start_timestamp)) as latest,
            COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 1 END) as recent_ads
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
        WHERE brand = 'Warby Parker'
        
        UNION ALL
        
        SELECT 
            '{latest_raw}' as source,
            COUNT(*) as total_ads,
            MIN(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as earliest,
            MAX(DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))) as latest,
            COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 1 END) as recent_ads
        FROM `bigquery-ai-kaggle-469620.ads_demo.{latest_raw}`
        WHERE brand = 'Warby Parker'
        
        ORDER BY source
        """
        
        results = client.query(coverage_query)
        print(f"   📋 Coverage Comparison:")
        for row in results:
            print(f"     {row.source}:")
            print(f"       Total: {row.total_ads} ads")
            print(f"       Range: {row.earliest} to {row.latest}")
            print(f"       Recent (7d): {row.recent_ads} ads")
            print()
    
    # 4. Check for any newer raw tables not used in ads_with_dates
    print(f"4️⃣ CHECK FOR UNUSED NEWER DATA:")
    print("-" * 35)
    
    unused_data_query = """
    SELECT 
        table_name,
        creation_time,
        CASE 
            WHEN creation_time > (SELECT creation_time FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES` WHERE table_name = 'ads_with_dates')
            THEN '🚨 NEWER than ads_with_dates'
            ELSE '✅ Used in ads_with_dates'
        END as usage_status
    FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'ads_raw_warby_parker_%'
    ORDER BY creation_time DESC
    LIMIT 5
    """
    
    results = client.query(unused_data_query)
    print(f"   📊 Raw table timeline:")
    for row in results:
        print(f"     {row.table_name}: {row.creation_time} - {row.usage_status}")
    
    # 5. Direct API data freshness check (simulate)
    print(f"\n5️⃣ DATA FRESHNESS ASSESSMENT:")
    print("-" * 35)
    
    freshness_query = """
    SELECT 
        CURRENT_DATETIME() as current_time,
        MAX(start_timestamp) as latest_ad_timestamp,
        DATETIME_DIFF(CURRENT_DATETIME(), MAX(start_timestamp), HOUR) as hours_since_latest,
        COUNT(CASE WHEN DATE(start_timestamp) >= CURRENT_DATE() THEN 1 END) as todays_ads,
        COUNT(CASE WHEN start_timestamp >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 24 HOUR) THEN 1 END) as last_24h_ads
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    """
    
    results = client.query(freshness_query)
    for row in results:
        print(f"   📊 Data Freshness Analysis:")
        print(f"     Current time: {row.current_time}")
        print(f"     Latest ad timestamp: {row.latest_ad_timestamp}")
        print(f"     Hours since latest: {row.hours_since_latest}")
        print(f"     Today's ads: {row.todays_ads}")
        print(f"     Last 24h ads: {row.last_24h_ads}")
        
        if row.hours_since_latest > 24:
            print(f"     🚨 DATA STALENESS: Latest ad is {row.hours_since_latest} hours old!")
        else:
            print(f"     ✅ DATA FRESH: Latest ad is only {row.hours_since_latest} hours old")
    
    # 6. Final verdict
    print(f"\n6️⃣ INDEPENDENT VERIFICATION CONCLUSION:")
    print("-" * 45)
    
    # Re-run the core temporal intelligence query to give final verdict
    final_check_query = """
    SELECT 
        COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
              THEN 1 END) as last_7d_final,
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
              THEN 1 END) as prior_7d_final,
        COUNT(*) as total_ads_final
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    """
    
    results = client.query(final_check_query)
    for row in results:
        print(f"   📊 FINAL VERIFICATION RESULTS:")
        print(f"     Total ads in table: {row.total_ads_final}")
        print(f"     Last 7d ads: {row.last_7d_final}")
        print(f"     Prior 7d ads: {row.prior_7d_final}")
        print()
        
        if row.total_ads_final == 0:
            print(f"   ❌ CONFIRMED: NO ADS FOUND - Table is empty")
            print(f"   💡 User's belief incorrect - there are genuinely no ads")
        elif row.last_7d_final == 0 and row.prior_7d_final == 0:
            print(f"   🔍 FOUND: {row.total_ads_final} ads exist but none in relevant time windows")
            print(f"   💡 Ads exist but are outside the temporal intelligence windows")
        elif row.prior_7d_final == 0:
            print(f"   🎯 CONFIRMED: {row.last_7d_final} ads in last 7d, 0 in prior 7d")
            print(f"   💡 This explains null velocity - division by zero in prior period")
            print(f"   ✅ User is partially correct - ads exist but temporal math is accurate")
        else:
            print(f"   ❌ MYSTERY: Both periods have ads but velocity still null!")
            print(f"   🚨 This indicates a bug in the temporal intelligence calculation")

if __name__ == "__main__":
    independent_ads_verification()