-- Time-Series Construction with Weekly Aggregation
-- Implements business rules: 2+ day duration, duration weighting, weekly aggregation

-- Step 1: Create time-series eligible ads with all business rules
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_time_series_ads` AS

SELECT 
  ad_archive_id,
  brand,
  
  -- Date handling
  start_date,
  COALESCE(end_date, CURRENT_DATE()) as effective_end_date,
  
  -- Active duration calculation
  DATE_DIFF(COALESCE(end_date, CURRENT_DATE()), start_date, DAY) as active_days,
  
  -- Duration weight: tanh function saturating around 14-21 days
  GREATEST(0.2, TANH(DATE_DIFF(COALESCE(end_date, CURRENT_DATE()), start_date, DAY) / 7.0)) as duration_weight,
  
  -- Strategic dimensions
  funnel,
  persona,
  urgency_score,
  promotional_intensity,
  brand_voice_score,
  topics,
  classified_at
  
FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels` 
WHERE 
  -- Business rule: Must have start date
  start_date IS NOT NULL
  -- Business rule: Minimum 2 days active duration
  AND DATE_DIFF(COALESCE(end_date, CURRENT_DATE()), start_date, DAY) >= 2;


-- Step 2: Generate daily time series points for each ad's active period
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_daily_time_series_points` AS

SELECT 
  ad_archive_id,
  brand,
  analysis_date,
  funnel,
  persona,
  urgency_score,
  promotional_intensity,
  brand_voice_score,
  topics,
  duration_weight,
  active_days,
  
  -- Week assignment for aggregation
  DATE_TRUNC(analysis_date, WEEK) as week_start
  
FROM `bigquery-ai-kaggle-469620.ads_demo.v_time_series_ads`
CROSS JOIN UNNEST(
  -- Generate date array for each ad's active period
  GENERATE_DATE_ARRAY(start_date, effective_end_date, INTERVAL 1 DAY)
) as analysis_date;


-- Step 3: Weekly aggregation with duration weighting
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_weekly_strategy_timeseries` AS

WITH weekly_base AS (
  SELECT 
    brand,
    week_start,
    
    -- Raw counts for context
    COUNT(DISTINCT ad_archive_id) as unique_ads_active,
    COUNT(*) as total_ad_days,  -- Total ad-days in the week
    SUM(duration_weight) as total_weighted_days,
    
    -- Duration-weighted funnel distribution
    SUM(CASE WHEN funnel = 'Upper' THEN duration_weight ELSE 0 END) / 
      NULLIF(SUM(duration_weight), 0) * 100 as weighted_pct_upper_funnel,
    SUM(CASE WHEN funnel = 'Mid' THEN duration_weight ELSE 0 END) / 
      NULLIF(SUM(duration_weight), 0) * 100 as weighted_pct_mid_funnel,
    SUM(CASE WHEN funnel = 'Lower' THEN duration_weight ELSE 0 END) / 
      NULLIF(SUM(duration_weight), 0) * 100 as weighted_pct_lower_funnel,
    
    -- Duration-weighted persona distribution
    SUM(CASE WHEN persona = 'New Customer' THEN duration_weight ELSE 0 END) / 
      NULLIF(SUM(duration_weight), 0) * 100 as weighted_pct_new_customer,
    SUM(CASE WHEN persona = 'Existing Customer' THEN duration_weight ELSE 0 END) / 
      NULLIF(SUM(duration_weight), 0) * 100 as weighted_pct_existing_customer,
    SUM(CASE WHEN persona = 'General Market' THEN duration_weight ELSE 0 END) / 
      NULLIF(SUM(duration_weight), 0) * 100 as weighted_pct_general_market,
    
    -- Duration-weighted average scores
    SUM(urgency_score * duration_weight) / NULLIF(SUM(duration_weight), 0) as weighted_avg_urgency,
    SUM(promotional_intensity * duration_weight) / NULLIF(SUM(duration_weight), 0) as weighted_avg_promotional_intensity,
    SUM(brand_voice_score * duration_weight) / NULLIF(SUM(duration_weight), 0) as weighted_avg_brand_voice,
    
    -- Quality metrics
    AVG(active_days) as avg_ad_duration_days,
    MIN(active_days) as min_ad_duration_days,
    MAX(active_days) as max_ad_duration_days
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.v_daily_time_series_points`
  GROUP BY brand, week_start
),

-- Add week-over-week change calculations
weekly_with_changes AS (
  SELECT 
    *,
    
    -- Week-over-week funnel changes
    weighted_pct_upper_funnel - LAG(weighted_pct_upper_funnel) OVER (
      PARTITION BY brand ORDER BY week_start
    ) as upper_funnel_change_wow,
    
    weighted_pct_mid_funnel - LAG(weighted_pct_mid_funnel) OVER (
      PARTITION BY brand ORDER BY week_start  
    ) as mid_funnel_change_wow,
    
    weighted_pct_lower_funnel - LAG(weighted_pct_lower_funnel) OVER (
      PARTITION BY brand ORDER BY week_start
    ) as lower_funnel_change_wow,
    
    -- Score changes
    weighted_avg_urgency - LAG(weighted_avg_urgency) OVER (
      PARTITION BY brand ORDER BY week_start
    ) as urgency_change_wow,
    
    weighted_avg_promotional_intensity - LAG(weighted_avg_promotional_intensity) OVER (
      PARTITION BY brand ORDER BY week_start
    ) as promotional_intensity_change_wow,
    
    -- Strategy shift detection
    CASE 
      WHEN ABS(weighted_pct_upper_funnel - LAG(weighted_pct_upper_funnel) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 25 THEN 'Major Funnel Shift'
      WHEN ABS(weighted_avg_promotional_intensity - LAG(weighted_avg_promotional_intensity) OVER (
        PARTITION BY brand ORDER BY week_start  
      )) >= 0.3 THEN 'Promotional Strategy Change'
      WHEN ABS(weighted_avg_urgency - LAG(weighted_avg_urgency) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 0.3 THEN 'Urgency Strategy Change'
      WHEN unique_ads_active != LAG(unique_ads_active) OVER (
        PARTITION BY brand ORDER BY week_start
      ) THEN 'Ad Portfolio Change'
      ELSE 'Stable Strategy'
    END as strategy_change_type
    
  FROM weekly_base
)

SELECT 
  brand,
  week_start,
  unique_ads_active,
  total_ad_days,
  ROUND(total_weighted_days, 2) as total_weighted_days,
  
  -- Funnel distribution (rounded)
  ROUND(weighted_pct_upper_funnel, 1) as pct_upper_funnel,
  ROUND(weighted_pct_mid_funnel, 1) as pct_mid_funnel,
  ROUND(weighted_pct_lower_funnel, 1) as pct_lower_funnel,
  
  -- Persona distribution (rounded)
  ROUND(weighted_pct_new_customer, 1) as pct_new_customer,
  ROUND(weighted_pct_existing_customer, 1) as pct_existing_customer,
  ROUND(weighted_pct_general_market, 1) as pct_general_market,
  
  -- Strategic scores (rounded)
  ROUND(weighted_avg_urgency, 3) as avg_urgency_score,
  ROUND(weighted_avg_promotional_intensity, 3) as avg_promotional_intensity,
  ROUND(weighted_avg_brand_voice, 3) as avg_brand_voice_score,
  
  -- Quality context
  ROUND(avg_ad_duration_days, 1) as avg_ad_duration_days,
  min_ad_duration_days,
  max_ad_duration_days,
  
  -- Week-over-week changes (rounded)
  ROUND(upper_funnel_change_wow, 1) as upper_funnel_change_wow,
  ROUND(mid_funnel_change_wow, 1) as mid_funnel_change_wow,
  ROUND(lower_funnel_change_wow, 1) as lower_funnel_change_wow,
  ROUND(urgency_change_wow, 3) as urgency_change_wow,
  ROUND(promotional_intensity_change_wow, 3) as promotional_intensity_change_wow,
  
  -- Strategic insights
  strategy_change_type,
  
  -- Dominant strategy classification
  CASE 
    WHEN weighted_pct_upper_funnel >= 60 THEN 'Brand Awareness Focused'
    WHEN weighted_pct_lower_funnel >= 60 THEN 'Conversion Focused'
    WHEN weighted_avg_promotional_intensity >= 0.7 THEN 'Promotion Heavy'
    WHEN weighted_avg_urgency >= 0.6 THEN 'Urgency Driven'
    ELSE 'Balanced Strategy'
  END as dominant_strategy_type
  
FROM weekly_with_changes
ORDER BY brand, week_start DESC;


-- Step 4: Test the time series construction
-- This view validates our business rules and aggregation logic