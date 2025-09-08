#!/usr/bin/env python3
"""
Create time-series analysis tables using the comprehensive temporal data
"""

import os
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def create_ads_with_dates():
    """Create the ads_with_dates table from our comprehensive temporal data"""
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.ads_with_dates` AS
    SELECT 
      ad_id,
      brand,
      page_name,
      
      -- Core content
      creative_text,
      title,
      media_type,
      
      -- Temporal data
      start_timestamp,
      end_timestamp,
      start_date_string,
      end_date_string,
      active_days,
      is_active,
      
      -- Platform data  
      publisher_platforms,
      platforms_array,
      
      -- CTAs and format
      cta_type,
      cta_text,
      display_format,
      
      -- Additional context
      landing_url,
      snapshot_url,
      page_categories,
      
      -- Calculated fields for analysis
      DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS start_week,
      DATE_TRUNC(DATE(COALESCE(end_timestamp, CURRENT_TIMESTAMP())), WEEK(MONDAY)) AS end_week,
      
      -- Duration quality weighting using tanh function as specified
      GREATEST(0.2, TANH(COALESCE(active_days, 1) / 7.0)) AS duration_quality_weight,
      
      -- Influence tiers based on duration
      CASE 
        WHEN active_days >= 30 THEN 'HIGH_INFLUENCE'
        WHEN active_days >= 7 THEN 'MEDIUM_INFLUENCE' 
        WHEN active_days >= 2 THEN 'LOW_INFLUENCE'
        ELSE 'MINIMAL_INFLUENCE'
      END AS influence_tier
      
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_temporal_complete`
    WHERE 
      creative_text IS NOT NULL OR title IS NOT NULL  -- Has content
      AND start_timestamp IS NOT NULL  -- Has temporal data
      AND active_days >= 2  -- Meets 2+ day business rule
    """
    
    print("Creating ads_with_dates table...")
    job = client.query(query)
    job.result()
    print("✅ Created ads_with_dates")

def verify_data_quality():
    """Verify the quality of our time-series data"""
    query = f"""
    SELECT 
      'TIME_SERIES_DATA_QUALITY' AS test_name,
      COUNT(*) AS total_ads,
      COUNT(DISTINCT brand) AS unique_brands,
      COUNT(DISTINCT ad_id) AS unique_ad_ids,
      
      -- Temporal coverage
      COUNTIF(start_timestamp IS NOT NULL) AS ads_with_start_date,
      COUNTIF(end_timestamp IS NOT NULL) AS ads_with_end_date,
      COUNTIF(active_days >= 2) AS ads_meeting_duration_rule,
      
      -- Platform coverage
      COUNTIF(publisher_platforms IS NOT NULL) AS ads_with_platform_data,
      
      -- Date range
      MIN(DATE(start_timestamp)) AS earliest_date,
      MAX(DATE(COALESCE(end_timestamp, CURRENT_TIMESTAMP()))) AS latest_date,
      
      -- Duration analysis
      AVG(active_days) AS avg_duration_days,
      MIN(active_days) AS min_duration_days,
      MAX(active_days) AS max_duration_days,
      
      -- Influence distribution
      COUNTIF(influence_tier = 'HIGH_INFLUENCE') AS high_influence_ads,
      COUNTIF(influence_tier = 'MEDIUM_INFLUENCE') AS medium_influence_ads,
      COUNTIF(influence_tier = 'LOW_INFLUENCE') AS low_influence_ads
      
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    """
    
    print("\nVerifying data quality...")
    result = client.query(query).result()
    
    for row in result:
        print(f"📊 DATA QUALITY METRICS:")
        print(f"   Total ads: {row.total_ads}")
        print(f"   Unique brands: {row.unique_brands}")
        print(f"   Unique ad IDs: {row.unique_ad_ids}")
        print(f"   Temporal coverage: {row.ads_with_start_date}/{row.total_ads} start, {row.ads_with_end_date}/{row.total_ads} end")
        print(f"   Platform coverage: {row.ads_with_platform_data}/{row.total_ads}")
        print(f"   Date range: {row.earliest_date} to {row.latest_date}")
        print(f"   Duration: avg={row.avg_duration_days:.1f}, min={row.min_duration_days}, max={row.max_duration_days}")
        print(f"   Influence: {row.high_influence_ads} high, {row.medium_influence_ads} medium, {row.low_influence_ads} low")

def test_time_series_readiness():
    """Test if we're ready for comprehensive time-series analysis"""
    query = f"""
    SELECT 
      brand,
      COUNT(DISTINCT start_week) AS weeks_with_data,
      COUNT(*) AS total_ads,
      MIN(DATE(start_timestamp)) AS earliest_ad,
      MAX(DATE(COALESCE(end_timestamp, CURRENT_TIMESTAMP()))) AS latest_ad,
      COUNTIF(influence_tier IN ('HIGH_INFLUENCE', 'MEDIUM_INFLUENCE')) AS quality_ads
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    GROUP BY brand
    ORDER BY total_ads DESC
    """
    
    print(f"\n🔍 TIME-SERIES READINESS CHECK:")
    result = client.query(query).result()
    
    total_brands = 0
    ready_brands = 0
    
    for row in result:
        total_brands += 1
        is_ready = row.weeks_with_data >= 4 and row.quality_ads >= 5
        status = "✅ READY" if is_ready else "⚠️  LIMITED"
        
        print(f"   {status}: {row.brand}")
        print(f"     - {row.total_ads} ads across {row.weeks_with_data} weeks")
        print(f"     - {row.quality_ads} high/medium influence ads") 
        print(f"     - Date range: {row.earliest_ad} to {row.latest_ad}")
        
        if is_ready:
            ready_brands += 1
    
    print(f"\n📊 SUMMARY: {ready_brands}/{total_brands} brands ready for time-series analysis")
    
    if ready_brands >= 2:
        print("🎉 SUCCESS: Ready for comprehensive time-series testing!")
        return True
    else:
        print("⚠️  WARNING: Limited data for time-series analysis")
        return False

if __name__ == "__main__":
    print("🔧 CREATING TIME-SERIES ANALYSIS TABLES")
    print("="*50)
    
    try:
        create_ads_with_dates()
        verify_data_quality()
        ready = test_time_series_readiness()
        
        if ready:
            print("\n✅ Time-series infrastructure ready!")
            print("🚀 Can now run comprehensive Subgoal 6 tests")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)