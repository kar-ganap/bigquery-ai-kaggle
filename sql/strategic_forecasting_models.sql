-- Strategic Forecasting Models using BigQuery AI.FORECAST
-- Predicts competitive trends, promotional timing, and strategy shifts

-- Step 1: Prepare time-series data for forecasting
CREATE OR REPLACE TABLE `your-project.ads_demo.forecasting_base_data` AS
WITH weekly_competitive_metrics AS (
  SELECT 
    brand,
    week_start,
    
    -- Volume metrics for forecasting
    unique_ads_active,
    total_duration_weighted_days,
    
    -- Strategic metrics  
    avg_aggressiveness_score,
    pct_high_influence,
    pct_video,
    
    -- Cross-platform metrics
    pct_cross_platform,
    avg_platform_optimization_gap,
    
    -- Promotional metrics
    COUNT(*) FROM `your-project.ads_demo.promotional_calendar` p 
    WHERE p.brand = wm.brand 
      AND DATE_TRUNC(p.start_date, WEEK(MONDAY)) = wm.week_start
    ) AS promotional_campaigns_count,
    
    -- Lead the time series (shift back by 1 week for prediction target)
    LEAD(unique_ads_active, 1) OVER (PARTITION BY brand ORDER BY week_start) AS next_week_ads_active,
    LEAD(avg_aggressiveness_score, 1) OVER (PARTITION BY brand ORDER BY week_start) AS next_week_aggressiveness,
    LEAD(pct_cross_platform, 1) OVER (PARTITION BY brand ORDER BY week_start) AS next_week_cross_platform_pct

  FROM `your-project.ads_demo.weekly_strategy_metrics` wm
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 WEEK)  -- 6 months history
    AND week_start <= DATE_SUB(CURRENT_DATE(), INTERVAL 2 WEEK)   -- Exclude most recent weeks
),

-- Add external factors for better forecasting
enhanced_forecasting_data AS (
  SELECT 
    *,
    
    -- Seasonal indicators
    EXTRACT(MONTH FROM week_start) AS month_of_year,
    EXTRACT(WEEK FROM week_start) AS week_of_year,
    
    -- Holiday proximity (major promotional periods)
    CASE 
      WHEN EXTRACT(MONTH FROM week_start) = 11 AND EXTRACT(DAY FROM week_start) >= 15 THEN 1  -- Black Friday period
      WHEN EXTRACT(MONTH FROM week_start) = 12 AND EXTRACT(DAY FROM week_start) <= 31 THEN 1  -- Holiday season
      WHEN EXTRACT(MONTH FROM week_start) = 1 AND EXTRACT(DAY FROM week_start) <= 15 THEN 1   -- New Year
      WHEN EXTRACT(MONTH FROM week_start) = 9 AND EXTRACT(DAY FROM week_start) >= 15 THEN 1   -- Back to school
      ELSE 0
    END AS is_major_promotional_period,
    
    -- Competitive pressure indicators
    LAG(unique_ads_active, 1) OVER (PARTITION BY brand ORDER BY week_start) AS prev_week_ads,
    LAG(avg_aggressiveness_score, 1) OVER (PARTITION BY brand ORDER BY week_start) AS prev_week_aggressiveness,
    
    -- Market context (average across all brands for each week)
    AVG(unique_ads_active) OVER (PARTITION BY week_start) AS market_avg_ads_active,
    AVG(avg_aggressiveness_score) OVER (PARTITION BY week_start) AS market_avg_aggressiveness

  FROM weekly_competitive_metrics
  WHERE next_week_ads_active IS NOT NULL  -- Ensure we have target values
)

SELECT * FROM enhanced_forecasting_data
ORDER BY brand, week_start;

-- Step 2: Create AI.FORECAST models for different strategic metrics

-- Forecast 1: Ad Volume Forecasting
CREATE OR REPLACE MODEL `your-project.ads_demo.forecast_ad_volume`
OPTIONS(
  MODEL_TYPE='FORECAST',
  TIME_SERIES_TIMESTAMP_COL='week_start',
  TIME_SERIES_DATA_COL='unique_ads_active',
  TIME_SERIES_ID_COL='brand',
  HORIZON=4,  -- 4 weeks ahead
  AUTO_ARIMA=TRUE,
  DATA_FREQUENCY='WEEKLY'
) AS
SELECT 
  week_start,
  brand,
  unique_ads_active,
  -- Include external regressors for better predictions
  is_major_promotional_period,
  market_avg_ads_active,
  promotional_campaigns_count
FROM `your-project.ads_demo.forecasting_base_data`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 20 WEEK);

-- Forecast 2: Aggressiveness Score Forecasting  
CREATE OR REPLACE MODEL `your-project.ads_demo.forecast_aggressiveness`
OPTIONS(
  MODEL_TYPE='FORECAST',
  TIME_SERIES_TIMESTAMP_COL='week_start',
  TIME_SERIES_DATA_COL='avg_aggressiveness_score',
  TIME_SERIES_ID_COL='brand',
  HORIZON=4,
  AUTO_ARIMA=TRUE,
  DATA_FREQUENCY='WEEKLY'
) AS
SELECT 
  week_start,
  brand,
  avg_aggressiveness_score,
  is_major_promotional_period,
  market_avg_aggressiveness,
  promotional_campaigns_count
FROM `your-project.ads_demo.forecasting_base_data`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 20 WEEK)
  AND avg_aggressiveness_score IS NOT NULL;

-- Forecast 3: Cross-Platform Strategy Forecasting
CREATE OR REPLACE MODEL `your-project.ads_demo.forecast_cross_platform_strategy`
OPTIONS(
  MODEL_TYPE='FORECAST', 
  TIME_SERIES_TIMESTAMP_COL='week_start',
  TIME_SERIES_DATA_COL='pct_cross_platform',
  TIME_SERIES_ID_COL='brand',
  HORIZON=4,
  AUTO_ARIMA=TRUE,
  DATA_FREQUENCY='WEEKLY'
) AS
SELECT 
  week_start,
  brand,
  pct_cross_platform,
  is_major_promotional_period,
  total_duration_weighted_days  -- Campaign intensity affects platform strategy
FROM `your-project.ads_demo.forecasting_base_data`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 20 WEEK)
  AND pct_cross_platform IS NOT NULL;

-- Step 3: Generate forecasts and create comprehensive insights table
CREATE OR REPLACE TABLE `your-project.ads_demo.strategic_forecasts` AS
WITH volume_forecasts AS (
  SELECT 
    forecast_timestamp AS forecast_week,
    brand,
    forecast_value AS forecasted_ad_volume,
    standard_error AS volume_forecast_error,
    confidence_level,
    prediction_interval_lower_bound AS volume_lower_bound,
    prediction_interval_upper_bound AS volume_upper_bound
  FROM ML.FORECAST(
    MODEL `your-project.ads_demo.forecast_ad_volume`,
    STRUCT(4 AS horizon, 0.95 AS confidence_level)
  )
),

aggressiveness_forecasts AS (
  SELECT 
    forecast_timestamp AS forecast_week,
    brand,
    forecast_value AS forecasted_aggressiveness,
    standard_error AS aggressiveness_forecast_error,
    prediction_interval_lower_bound AS aggressiveness_lower_bound,
    prediction_interval_upper_bound AS aggressiveness_upper_bound
  FROM ML.FORECAST(
    MODEL `your-project.ads_demo.forecast_aggressiveness`,
    STRUCT(4 AS horizon, 0.95 AS confidence_level)
  )
),

platform_forecasts AS (
  SELECT 
    forecast_timestamp AS forecast_week,
    brand,
    forecast_value AS forecasted_cross_platform_pct,
    standard_error AS platform_forecast_error,
    prediction_interval_lower_bound AS platform_lower_bound,
    prediction_interval_upper_bound AS platform_upper_bound
  FROM ML.FORECAST(
    MODEL `your-project.ads_demo.forecast_cross_platform_strategy`,
    STRUCT(4 AS horizon, 0.95 AS confidence_level)
  )
),

-- Get current baseline for trend analysis
current_metrics AS (
  SELECT 
    brand,
    AVG(unique_ads_active) AS current_avg_ad_volume,
    AVG(avg_aggressiveness_score) AS current_avg_aggressiveness,
    AVG(pct_cross_platform) AS current_avg_cross_platform_pct
  FROM `your-project.ads_demo.forecasting_base_data`
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
  GROUP BY brand
)

-- Combine all forecasts with trend analysis
SELECT 
  vf.forecast_week,
  vf.brand,
  
  -- Volume predictions
  ROUND(vf.forecasted_ad_volume, 1) AS forecasted_ad_volume,
  ROUND(vf.volume_lower_bound, 1) AS volume_lower_bound,
  ROUND(vf.volume_upper_bound, 1) AS volume_upper_bound,
  ROUND(vf.volume_forecast_error, 2) AS volume_forecast_error,
  
  -- Aggressiveness predictions
  ROUND(af.forecasted_aggressiveness, 3) AS forecasted_aggressiveness,
  ROUND(af.aggressiveness_lower_bound, 3) AS aggressiveness_lower_bound,
  ROUND(af.aggressiveness_upper_bound, 3) AS aggressiveness_upper_bound,
  ROUND(af.aggressiveness_forecast_error, 3) AS aggressiveness_forecast_error,
  
  -- Platform strategy predictions
  ROUND(pf.forecasted_cross_platform_pct, 1) AS forecasted_cross_platform_pct,
  ROUND(pf.platform_lower_bound, 1) AS platform_lower_bound,
  ROUND(pf.platform_upper_bound, 1) AS platform_upper_bound,
  ROUND(pf.platform_forecast_error, 2) AS platform_forecast_error,
  
  -- Trend analysis vs current baseline
  ROUND((vf.forecasted_ad_volume - cm.current_avg_ad_volume) / cm.current_avg_ad_volume * 100, 1) AS volume_change_pct_vs_current,
  ROUND(af.forecasted_aggressiveness - cm.current_avg_aggressiveness, 3) AS aggressiveness_change_vs_current,
  ROUND(pf.forecasted_cross_platform_pct - cm.current_avg_cross_platform_pct, 1) AS platform_strategy_change_vs_current,
  
  -- Seasonal context
  EXTRACT(MONTH FROM vf.forecast_week) AS forecast_month,
  CASE 
    WHEN EXTRACT(MONTH FROM vf.forecast_week) IN (11, 12) THEN 'HOLIDAY_SEASON'
    WHEN EXTRACT(MONTH FROM vf.forecast_week) = 1 THEN 'NEW_YEAR_PERIOD'
    WHEN EXTRACT(MONTH FROM vf.forecast_week) IN (3, 4) THEN 'SPRING_SEASON'
    WHEN EXTRACT(MONTH FROM vf.forecast_week) IN (6, 7, 8) THEN 'SUMMER_SEASON'
    WHEN EXTRACT(MONTH FROM vf.forecast_week) = 9 THEN 'BACK_TO_SCHOOL'
    ELSE 'REGULAR_PERIOD'
  END AS seasonal_context,
  
  -- Strategic insights
  CASE 
    WHEN (vf.forecasted_ad_volume - cm.current_avg_ad_volume) / cm.current_avg_ad_volume > 0.25 
    THEN 'SIGNIFICANT_VOLUME_INCREASE_EXPECTED'
    WHEN (vf.forecasted_ad_volume - cm.current_avg_ad_volume) / cm.current_avg_ad_volume < -0.25 
    THEN 'SIGNIFICANT_VOLUME_DECREASE_EXPECTED'
    ELSE 'STABLE_VOLUME_EXPECTED'
  END AS volume_trend_signal,
  
  CASE 
    WHEN af.forecasted_aggressiveness - cm.current_avg_aggressiveness > 0.15 
    THEN 'INCREASING_AGGRESSIVENESS_EXPECTED'
    WHEN af.forecasted_aggressiveness - cm.current_avg_aggressiveness < -0.15 
    THEN 'DECREASING_AGGRESSIVENESS_EXPECTED'
    ELSE 'STABLE_AGGRESSIVENESS_EXPECTED'
  END AS aggressiveness_trend_signal,
  
  CASE 
    WHEN ABS(pf.forecasted_cross_platform_pct - cm.current_avg_cross_platform_pct) > 15 
    THEN 'PLATFORM_STRATEGY_SHIFT_EXPECTED'
    ELSE 'STABLE_PLATFORM_STRATEGY_EXPECTED'
  END AS platform_trend_signal

FROM volume_forecasts vf
LEFT JOIN aggressiveness_forecasts af 
  ON vf.forecast_week = af.forecast_week AND vf.brand = af.brand
LEFT JOIN platform_forecasts pf 
  ON vf.forecast_week = pf.forecast_week AND vf.brand = pf.brand
LEFT JOIN current_metrics cm ON vf.brand = cm.brand
ORDER BY vf.brand, vf.forecast_week;

-- Step 4: Create competitive forecast summary for strategic planning
CREATE OR REPLACE VIEW `your-project.ads_demo.v_competitive_forecast_summary` AS
SELECT 
  forecast_week,
  
  -- Market-level forecasts
  AVG(forecasted_ad_volume) AS market_avg_forecasted_volume,
  AVG(forecasted_aggressiveness) AS market_avg_forecasted_aggressiveness,
  AVG(forecasted_cross_platform_pct) AS market_avg_forecasted_cross_platform,
  
  -- Competitive intensity indicators
  MAX(forecasted_ad_volume) - MIN(forecasted_ad_volume) AS volume_competitive_spread,
  MAX(forecasted_aggressiveness) - MIN(forecasted_aggressiveness) AS aggressiveness_competitive_spread,
  
  -- Brands expected to increase activity
  COUNTIF(volume_trend_signal = 'SIGNIFICANT_VOLUME_INCREASE_EXPECTED') AS brands_increasing_volume,
  COUNTIF(aggressiveness_trend_signal = 'INCREASING_AGGRESSIVENESS_EXPECTED') AS brands_increasing_aggressiveness,
  
  -- Strategic insights
  STRING_AGG(
    CASE WHEN volume_trend_signal = 'SIGNIFICANT_VOLUME_INCREASE_EXPECTED' 
         THEN brand ELSE NULL END
  ) AS brands_ramping_up_volume,
  
  STRING_AGG(
    CASE WHEN aggressiveness_trend_signal = 'INCREASING_AGGRESSIVENESS_EXPECTED'
         THEN brand ELSE NULL END  
  ) AS brands_getting_more_aggressive,
  
  -- Forecast reliability
  AVG(volume_forecast_error) AS avg_volume_forecast_error,
  AVG(aggressiveness_forecast_error) AS avg_aggressiveness_forecast_error

FROM `your-project.ads_demo.strategic_forecasts`
GROUP BY forecast_week
ORDER BY forecast_week;

-- Validation query
SELECT 
  'STRATEGIC_FORECASTING_VALIDATION' AS test_name,
  COUNT(*) AS total_forecasts,
  COUNT(DISTINCT brand) AS brands_with_forecasts,
  COUNT(DISTINCT forecast_week) AS forecast_weeks,
  AVG(volume_forecast_error) AS avg_volume_error,
  AVG(aggressiveness_forecast_error) AS avg_aggressiveness_error,
  COUNTIF(volume_trend_signal != 'STABLE_VOLUME_EXPECTED') AS significant_volume_changes_predicted
FROM `your-project.ads_demo.strategic_forecasts`;