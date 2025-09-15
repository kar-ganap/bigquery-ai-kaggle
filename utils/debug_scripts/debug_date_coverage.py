#!/usr/bin/env python3
"""
Debug script to verify actual date ranges and check for Aug 31 - Sep 5 data
"""
import os
from google.cloud import bigquery

def verify_date_coverage():
    """Comprehensive check of actual date coverage in the data"""
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-creds.json'
    client = bigquery.Client(project="bigquery-ai-kaggle-469620")
    
    print("📅 COMPREHENSIVE DATE COVERAGE ANALYSIS")
    print("="*50)
    
    # Test 1: Full date range analysis
    print("\n1️⃣ FULL DATE RANGE ANALYSIS:")
    print("-" * 35)
    
    range_query = """
    SELECT 
        brand,
        COUNT(*) as total_ads,
        MIN(DATE(start_timestamp)) as earliest_date,
        MAX(DATE(start_timestamp)) as latest_date,
        DATE_DIFF(MAX(DATE(start_timestamp)), MIN(DATE(start_timestamp)), DAY) + 1 as span_days,
        
        -- Check every single day for data
        ARRAY_AGG(DISTINCT DATE(start_timestamp) ORDER BY DATE(start_timestamp)) as all_dates
        
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY brand
    """
    
    results = client.query(range_query)
    for row in results:
        print(f"  📊 Brand: {row.brand}")
        print(f"     Total ads: {row.total_ads}")
        print(f"     Date range: {row.earliest_date} to {row.latest_date}")
        print(f"     Span: {row.span_days} days")
        print(f"     All dates: {list(row.all_dates)}")
    
    # Test 2: Day-by-day breakdown
    print("\n2️⃣ DAY-BY-DAY BREAKDOWN:")
    print("-" * 35)
    
    daily_query = """
    SELECT 
        DATE(start_timestamp) as ad_date,
        COUNT(*) as ads_count,
        -- Check which time period this falls into
        CASE 
            WHEN DATE(start_timestamp) <= '2025-09-05' THEN 'Aug 31 - Sep 5 (Prior Period)'
            WHEN DATE(start_timestamp) BETWEEN '2025-09-06' AND '2025-09-12' THEN 'Sep 6 - Sep 12 (Current Period)'  
            WHEN DATE(start_timestamp) >= '2025-09-13' THEN 'Sep 13+ (Future)'
            ELSE 'Other'
        END as period_classification,
        
        -- Show actual temporal intelligence periods
        CASE 
            WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 'Last 7d (Temporal Intelligence Current)'
            WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                              AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                 THEN 'Prior 7d (Temporal Intelligence Prior)'
            ELSE 'Outside temporal windows'
        END as temporal_window
        
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY DATE(start_timestamp)
    ORDER BY DATE(start_timestamp)
    """
    
    results = client.query(daily_query)
    for row in results:
        print(f"  📅 {row.ad_date}: {row.ads_count} ads ({row.period_classification}) -> {row.temporal_window}")
    
    # Test 3: Check specific periods mentioned in your finding
    print("\n3️⃣ CHECKING SPECIFIC PERIODS:")
    print("-" * 35)
    
    periods_query = """
    SELECT 
        'Aug 31 - Sep 5 (Prior Period)' as period_name,
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN '2025-08-31' AND '2025-09-05' THEN 1 END) as ads_count,
        MIN(CASE WHEN DATE(start_timestamp) BETWEEN '2025-08-31' AND '2025-09-05' 
               THEN DATE(start_timestamp) END) as earliest,
        MAX(CASE WHEN DATE(start_timestamp) BETWEEN '2025-08-31' AND '2025-09-05' 
               THEN DATE(start_timestamp) END) as latest
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    
    UNION ALL
    
    SELECT 
        'Sep 6 - Sep 12 (Current Period)' as period_name,
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN '2025-09-06' AND '2025-09-12' THEN 1 END) as ads_count,
        MIN(CASE WHEN DATE(start_timestamp) BETWEEN '2025-09-06' AND '2025-09-12' 
               THEN DATE(start_timestamp) END) as earliest,
        MAX(CASE WHEN DATE(start_timestamp) BETWEEN '2025-09-06' AND '2025-09-12' 
               THEN DATE(start_timestamp) END) as latest
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    
    UNION ALL
    
    SELECT 
        'Temporal Intelligence Prior (Current-13 to Current-7)' as period_name,
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                                     AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                   THEN 1 END) as ads_count,
        MIN(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                                   AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                 THEN DATE(start_timestamp) END) as earliest,
        MAX(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                                   AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                 THEN DATE(start_timestamp) END) as latest
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    
    ORDER BY period_name
    """
    
    results = client.query(periods_query)
    for row in results:
        print(f"  📊 {row.period_name}:")
        print(f"     Ads count: {row.ads_count}")
        if row.ads_count > 0:
            print(f"     Date range: {row.earliest} to {row.latest}")
        else:
            print(f"     ❌ NO ADS FOUND in this period")
    
    # Test 4: Show what today's date is for temporal calculations
    print("\n4️⃣ CURRENT DATE CONTEXT:")
    print("-" * 35)
    
    context_query = """
    SELECT 
        CURRENT_DATE() as today,
        DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) as seven_days_ago,
        DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) as thirteen_days_ago,
        DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) as prior_period_start,
        DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) as prior_period_end
    """
    
    results = client.query(context_query)
    for row in results:
        print(f"  🗓️  Today: {row.today}")
        print(f"     Last 7d period: {row.seven_days_ago} to {row.today}")  
        print(f"     Prior 7d period: {row.prior_period_start} to {row.prior_period_end}")

if __name__ == "__main__":
    verify_date_coverage()