-- Enhanced Time-Series Weekly Aggregation for Competitive Intelligence
-- Using real temporal data from ads_with_dates table

CREATE OR REPLACE TABLE `your-project.ads_demo.weekly_strategy_metrics` AS
WITH filtered_ads AS (
  -- Use real ads_with_dates table with actual temporal data
  SELECT 
    ad_id,
    brand,
    creative_text,
    media_type,
    start_timestamp,
    end_timestamp,
    active_days,
    duration_quality_weight,
    influence_tier,
    platforms_array
  FROM `your-project.ads_demo.ads_with_dates`
  WHERE 
    creative_text IS NOT NULL
    AND brand IS NOT NULL
    -- Business rule: 2+ day minimum already applied in ads_with_dates
    AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)  -- Last 90 days for analysis
),

-- Generate daily time series points for each ad's active period
daily_points AS (
  SELECT 
    ad_id,
    brand,
    media_type,
    creative_text,
    active_days,
    duration_quality_weight,
    influence_tier,
    analysis_date,
    
    -- Weekly aggregation helper
    DATE_TRUNC(analysis_date, WEEK(MONDAY)) AS week_start,
    
    -- Recency weight: exponential decay with 7-day half-life
    EXP(-0.693 * DATE_DIFF(CURRENT_DATE(), analysis_date, DAY) / 7.0) AS recency_weight,
    
    -- Combined weight: duration quality × recency
    duration_quality_weight * EXP(-0.693 * DATE_DIFF(CURRENT_DATE(), analysis_date, DAY) / 7.0) AS combined_weight
    
  FROM filtered_ads
  CROSS JOIN UNNEST(
    GENERATE_DATE_ARRAY(
      DATE(start_timestamp), 
      DATE(COALESCE(end_timestamp, CURRENT_TIMESTAMP())), 
      INTERVAL 1 DAY
    )
  ) AS analysis_date
  WHERE analysis_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)  -- Performance optimization
),

-- Weekly aggregation with business intelligence metrics
weekly_base AS (
  SELECT 
    brand,
    week_start,
    
    -- Volume metrics
    COUNT(DISTINCT ad_id) AS unique_ads_active,
    COUNT(*) AS total_ad_days,
    SUM(duration_quality_weight) AS total_duration_weighted_days,
    SUM(combined_weight) AS total_combined_weight,
    
    -- Quality indicators
    AVG(active_days) AS avg_ad_duration_days,
    AVG(duration_quality_weight) AS avg_duration_quality,
    
    -- Media type distribution (duration weighted)
    SUM(CASE WHEN media_type = 'VIDEO' THEN duration_quality_weight ELSE 0 END) / 
      NULLIF(SUM(duration_quality_weight), 0) * 100 AS weighted_pct_video,
    SUM(CASE WHEN media_type = 'IMAGE' THEN duration_quality_weight ELSE 0 END) / 
      NULLIF(SUM(duration_quality_weight), 0) * 100 AS weighted_pct_image,
    SUM(CASE WHEN media_type = 'DCO' THEN duration_quality_weight ELSE 0 END) / 
      NULLIF(SUM(duration_quality_weight), 0) * 100 AS weighted_pct_dco,
    
    -- Influence tier distribution
    SUM(CASE WHEN influence_tier = 'HIGH_INFLUENCE' THEN duration_quality_weight ELSE 0 END) / 
      NULLIF(SUM(duration_quality_weight), 0) * 100 AS weighted_pct_high_influence,
    SUM(CASE WHEN influence_tier = 'MEDIUM_INFLUENCE' THEN duration_quality_weight ELSE 0 END) / 
      NULLIF(SUM(duration_quality_weight), 0) * 100 AS weighted_pct_medium_influence,
    SUM(CASE WHEN influence_tier = 'LOW_INFLUENCE' THEN duration_quality_weight ELSE 0 END) / 
      NULLIF(SUM(duration_quality_weight), 0) * 100 AS weighted_pct_low_influence
    
  FROM daily_points
  GROUP BY brand, week_start
),

-- Add week-over-week changes and trend analysis
weekly_with_trends AS (
  SELECT 
    *,
    
    -- Volume changes
    unique_ads_active - LAG(unique_ads_active) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS unique_ads_change_wow,
    
    total_duration_weighted_days - LAG(total_duration_weighted_days) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS duration_weighted_days_change_wow,
    
    -- Media strategy shifts
    weighted_pct_video - LAG(weighted_pct_video) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS video_pct_change_wow,
    
    -- Influence strategy changes
    weighted_pct_high_influence - LAG(weighted_pct_high_influence) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS high_influence_pct_change_wow,
    
    -- Strategy change detection
    CASE 
      WHEN ABS(unique_ads_active - LAG(unique_ads_active) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 5 THEN 'Major Volume Change'
      WHEN ABS(weighted_pct_video - LAG(weighted_pct_video) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 20 THEN 'Media Format Shift'
      WHEN ABS(weighted_pct_high_influence - LAG(weighted_pct_high_influence) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 25 THEN 'Campaign Duration Strategy Change'
      WHEN ABS(avg_ad_duration_days - LAG(avg_ad_duration_days) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 7 THEN 'Ad Longevity Strategy Change'
      ELSE 'Stable Strategy'
    END AS strategy_change_type
    
  FROM weekly_base
),

-- Competitive context: calculate relative positioning
competitive_context AS (
  SELECT 
    *,
    
    -- Rank within week across all brands
    RANK() OVER (PARTITION BY week_start ORDER BY unique_ads_active DESC) AS volume_rank_in_week,
    RANK() OVER (PARTITION BY week_start ORDER BY avg_ad_duration_days DESC) AS duration_rank_in_week,
    RANK() OVER (PARTITION BY week_start ORDER BY weighted_pct_high_influence DESC) AS influence_rank_in_week,
    
    -- Percentile positions
    PERCENT_RANK() OVER (PARTITION BY week_start ORDER BY unique_ads_active) AS volume_percentile_in_week,
    PERCENT_RANK() OVER (PARTITION BY week_start ORDER BY avg_ad_duration_days) AS duration_percentile_in_week,
    
    -- Market share (approximate)
    total_duration_weighted_days / SUM(total_duration_weighted_days) OVER (PARTITION BY week_start) * 100 AS market_share_approx
    
  FROM weekly_with_trends
)

SELECT 
  brand,
  week_start,
  
  -- Volume metrics
  unique_ads_active,
  total_ad_days,
  ROUND(total_duration_weighted_days, 2) AS total_duration_weighted_days,
  ROUND(total_combined_weight, 2) AS total_combined_weight,
  
  -- Quality indicators
  ROUND(avg_ad_duration_days, 1) AS avg_ad_duration_days,
  ROUND(avg_duration_quality, 3) AS avg_duration_quality,
  
  -- Media mix (rounded percentages)
  ROUND(weighted_pct_video, 1) AS pct_video,
  ROUND(weighted_pct_image, 1) AS pct_image,
  ROUND(weighted_pct_dco, 1) AS pct_dco,
  
  -- Influence distribution
  ROUND(weighted_pct_high_influence, 1) AS pct_high_influence,
  ROUND(weighted_pct_medium_influence, 1) AS pct_medium_influence,
  ROUND(weighted_pct_low_influence, 1) AS pct_low_influence,
  
  -- Week-over-week changes
  unique_ads_change_wow,
  ROUND(duration_weighted_days_change_wow, 2) AS duration_weighted_days_change_wow,
  ROUND(video_pct_change_wow, 1) AS video_pct_change_wow,
  ROUND(high_influence_pct_change_wow, 1) AS high_influence_pct_change_wow,
  
  -- Strategic insights
  strategy_change_type,
  
  -- Competitive positioning
  volume_rank_in_week,
  duration_rank_in_week,
  influence_rank_in_week,
  ROUND(volume_percentile_in_week * 100, 1) AS volume_percentile_in_week,
  ROUND(duration_percentile_in_week * 100, 1) AS duration_percentile_in_week,
  ROUND(market_share_approx, 2) AS market_share_approx,
  
  -- Dominant strategy classification
  CASE 
    WHEN weighted_pct_video >= 70 THEN 'Video-First Strategy'
    WHEN weighted_pct_high_influence >= 60 THEN 'Long-Term Campaign Strategy'
    WHEN avg_ad_duration_days >= 21 THEN 'Extended Campaign Strategy'
    WHEN unique_ads_active >= 10 THEN 'High-Volume Strategy'
    ELSE 'Balanced Strategy'
  END AS dominant_strategy_type
  
FROM competitive_context
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)  -- Last 12 weeks
ORDER BY brand, week_start DESC;

-- Performance optimization indexes
-- CREATE INDEX IF NOT EXISTS idx_ads_with_dates_brand_start ON `your-project.ads_demo.ads_with_dates` (brand, start_timestamp);
-- CREATE INDEX IF NOT EXISTS idx_weekly_metrics_brand_week ON `your-project.ads_demo.weekly_strategy_metrics` (brand, week_start);

-- Validation query to test the framework
SELECT 
  'WEEKLY_METRICS_VALIDATION' AS test_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT brand) AS unique_brands,
  COUNT(DISTINCT week_start) AS unique_weeks,
  MIN(week_start) AS earliest_week,
  MAX(week_start) AS latest_week,
  AVG(unique_ads_active) AS avg_ads_per_brand_week,
  SUM(CASE WHEN strategy_change_type != 'Stable Strategy' THEN 1 ELSE 0 END) AS strategy_changes_detected
FROM `your-project.ads_demo.weekly_strategy_metrics`;