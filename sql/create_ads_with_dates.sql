-- Create ads_with_dates view/table for time-series competitive intelligence
-- This leverages the updated ingestion script that captures start_date_string and end_date_string

CREATE OR REPLACE TABLE `your-project.ads_demo.ads_with_dates` AS
WITH date_parsed AS (
  SELECT 
    ad_id,
    brand,
    page_name,
    creative_text,
    title,
    media_type,
    first_seen,
    last_seen,
    start_date_string,
    end_date_string,
    snapshot_url,
    landing_url,
    display_format,
    publisher_platforms,
    
    -- Parse ISO date strings to proper timestamps for time-series analysis
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string) AS start_timestamp,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string) AS end_timestamp,
    
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
    
  FROM `your-project.ads_demo.ads_raw`
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
ORDER BY start_timestamp DESC, brand, ad_id;

-- Add helpful comment
COMMENT ON TABLE `your-project.ads_demo.ads_with_dates` AS 
'Enhanced ads table with temporal data for time-series competitive intelligence. 
Includes duration quality weighting, influence tiers, and weekly aggregation helpers.
Updated to use actual start_date_string/end_date_string from ScrapeCreators API.';