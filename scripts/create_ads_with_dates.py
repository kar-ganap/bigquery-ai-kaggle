#!/usr/bin/env python3
"""
Create ads_with_dates table from real Meta ads data using start_date_string/end_date_string.
This restores the sophisticated temporal intelligence that was lost during modularization.
"""

import os
from scripts.utils.bigquery_client import get_bigquery_client, run_query

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

def create_ads_with_dates_from_run(run_id: str):
    """Create ads_with_dates table from a specific run's ads_raw data"""
    
    client = get_bigquery_client()
    
    # SQL to create ads_with_dates table from real Meta ads data
    create_ads_with_dates_sql = f"""
    CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates` AS
    
    WITH date_parsed AS (
      SELECT 
        ad_archive_id,
        brand,
        page_name,
        creative_text,
        title,
        media_type,
        start_date_string,
        end_date_string,
        snapshot_url,
        landing_url,
        display_format,
        publisher_platforms,
        card_index,
        image_url,
        video_url,
        cta_type,
        
        -- Parse ISO date strings to proper timestamps for time-series analysis
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string) AS start_timestamp,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string) AS end_timestamp,
        
        -- Create date fields for compatibility with legacy temporal intelligence
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) AS first_seen,
        DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string)) AS last_seen,
        
        -- Calculate active duration in days for influence scoring
        CASE 
          WHEN start_date_string IS NOT NULL AND end_date_string IS NOT NULL 
          THEN DATE_DIFF(
            DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string)),
            DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)),
            DAY
          ) + 1  -- Include both start and end days
          WHEN start_date_string IS NOT NULL AND end_date_string IS NULL
          THEN DATE_DIFF(CURRENT_DATE(), DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)), DAY) + 1
          ELSE NULL
        END AS active_days,
        
        -- Duration quality weight using tanh function as specified
        -- Weight ranges from 0.2 (2 days) to ~1.0 (30+ days)
        CASE 
          WHEN start_date_string IS NOT NULL AND end_date_string IS NOT NULL 
          THEN GREATEST(0.2, TANH(
            (DATE_DIFF(
              DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string)),
              DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)),
              DAY
            ) + 1) / 7.0
          ))
          WHEN start_date_string IS NOT NULL AND end_date_string IS NULL
          THEN GREATEST(0.2, TANH(
            (DATE_DIFF(CURRENT_DATE(), DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)), DAY) + 1) / 7.0
          ))
          ELSE 0.0
        END AS duration_quality_weight
        
      FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_raw_{run_id}`
      WHERE 
        -- Only include ads with valid date information
        start_date_string IS NOT NULL
        -- Apply 2+ day minimum as specified in business rules
        AND (
          end_date_string IS NULL OR  -- Still active
          DATE_DIFF(
            DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string)),
            DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)),
            DAY
          ) >= 1  -- At least 2 days (0-indexed difference = 1)
        )
    )
    
    SELECT 
      *,
      -- Weekly aggregation helpers for time-series analysis
      DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS start_week,
      DATE_TRUNC(DATE(COALESCE(end_timestamp, CURRENT_TIMESTAMP())), WEEK(MONDAY)) AS end_week,
      
      -- Extract platforms for analysis
      ARRAY(
        SELECT TRIM(platform) 
        FROM UNNEST(SPLIT(publisher_platforms, ',')) AS platform
        WHERE TRIM(platform) != ''
      ) AS platforms_array,
      
      -- Quality indicators
      CASE 
        WHEN active_days >= 30 THEN 'HIGH_INFLUENCE'
        WHEN active_days >= 7 THEN 'MEDIUM_INFLUENCE' 
        WHEN active_days >= 2 THEN 'LOW_INFLUENCE'
        ELSE 'MINIMAL_INFLUENCE'
      END AS influence_tier
    
    FROM date_parsed
    ORDER BY start_timestamp DESC, brand, ad_archive_id
    """
    
    print(f"🔧 Creating ads_with_dates table from ads_raw_{run_id}...")
    print("   📊 This restores sophisticated temporal intelligence using real Meta ads data")
    
    # Execute the query
    job = client.query(create_ads_with_dates_sql)
    job.result()  # Wait for completion
    
    print(f"   ✅ ads_with_dates table created successfully")
    
    # Verify the results
    verification_sql = f"""
    SELECT 
        brand,
        COUNT(*) as total_ads,
        COUNT(CASE WHEN start_timestamp IS NOT NULL THEN 1 END) as ads_with_start_time,
        COUNT(CASE WHEN end_timestamp IS NOT NULL THEN 1 END) as ads_with_end_time,
        MIN(DATE(start_timestamp)) as earliest_start,
        MAX(DATE(start_timestamp)) as latest_start,
        AVG(active_days) as avg_active_days
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    GROUP BY brand
    ORDER BY total_ads DESC
    """
    
    result = run_query(verification_sql)
    print("   📈 Temporal data summary:")
    for _, row in result.iterrows():
        print(f"      • {row['brand']}: {row['total_ads']} ads, {row['ads_with_start_time']} with timestamps")
        print(f"        Date range: {row['earliest_start']} to {row['latest_start']}")
        print(f"        Avg duration: {row['avg_active_days']:.1f} days")
    
    return True

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python create_ads_with_dates.py <run_id>")
        print("Example: python create_ads_with_dates.py warby_parker_20250911_145245")
        sys.exit(1)
    
    run_id = sys.argv[1]
    create_ads_with_dates_from_run(run_id)