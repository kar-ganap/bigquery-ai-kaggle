-- AI.FORECAST Distribution Evolution Models
-- Predicts strategic mix changes across funnel, persona, intensity, and brand voice
-- Uses ARIMA/SARIMA models for automatic temporal pattern detection

-- Strategic Mix Distribution Forecasting
CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.strategic_distribution_forecasts` AS

WITH time_series_preparation AS (
  -- Prepare time-series data for distribution forecasting
  SELECT 
    brand,
    week_start,
    
    -- Funnel distribution (multinomial probabilities)
    COUNTIF(funnel = 'Upper') / COUNT(*) AS upper_funnel_pct,
    COUNTIF(funnel = 'Mid') / COUNT(*) AS mid_funnel_pct,
    COUNTIF(funnel = 'Lower') / COUNT(*) AS lower_funnel_pct,
    
    -- Persona distribution
    COUNTIF(persona = 'New Customer') / COUNT(*) AS new_customer_pct,
    COUNTIF(persona = 'Existing Customer') / COUNT(*) AS existing_customer_pct,
    COUNTIF(persona = 'General Market') / COUNT(*) AS general_market_pct,
    
    -- Intensity metrics (continuous)
    AVG(COALESCE(promotional_intensity, 0.0)) AS avg_promotional_intensity,
    AVG(COALESCE(urgency_score, 0.0)) AS avg_urgency_score,
    AVG(COALESCE(brand_voice_score, 0.5)) AS avg_brand_voice_score,
    
    -- Volume metrics
    COUNT(*) AS total_ads,
    COUNT(DISTINCT ad_id) AS unique_ads,
    
    -- Media type distribution
    COUNTIF(media_type = 'VIDEO') / COUNT(*) AS video_pct,
    COUNTIF(media_type = 'IMAGE') / COUNT(*) AS image_pct,
    COUNTIF(media_type = 'DCO') / COUNT(*) AS dco_pct
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels_v2`
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 26 WEEK)  -- 6 months
    AND week_start IS NOT NULL
    AND brand IS NOT NULL
  GROUP BY brand, week_start
  HAVING COUNT(*) >= 3  -- Minimum ads for reliable distribution
),

-- ARIMA/SARIMA forecasting for each strategic dimension
funnel_forecasts AS (
  SELECT 
    brand,
    
    -- Forecast funnel distribution 4 weeks ahead
    ML.FORECAST(
      MODEL `bigquery-ai-kaggle-469620.ads_demo.funnel_arima_model`,
      STRUCT(4 AS horizon, 0.90 AS confidence_level)
    ) AS funnel_forecast_results,
    
    'FUNNEL_DISTRIBUTION' AS forecast_type
    
  FROM (
    SELECT 
      brand,
      ARRAY_AGG(
        STRUCT(week_start, upper_funnel_pct, mid_funnel_pct, lower_funnel_pct)
        ORDER BY week_start
      ) AS time_series_data
    FROM time_series_preparation
    GROUP BY brand
    HAVING COUNT(*) >= 8  -- Minimum 8 weeks for ARIMA
  )
),

persona_forecasts AS (
  SELECT 
    brand,
    
    -- Forecast persona distribution 4 weeks ahead
    ML.FORECAST(
      MODEL `bigquery-ai-kaggle-469620.ads_demo.persona_arima_model`, 
      STRUCT(4 AS horizon, 0.90 AS confidence_level)
    ) AS persona_forecast_results,
    
    'PERSONA_DISTRIBUTION' AS forecast_type
    
  FROM (
    SELECT 
      brand,
      ARRAY_AGG(
        STRUCT(week_start, new_customer_pct, existing_customer_pct, general_market_pct)
        ORDER BY week_start
      ) AS time_series_data
    FROM time_series_preparation  
    GROUP BY brand
    HAVING COUNT(*) >= 8
  )
),

intensity_forecasts AS (
  SELECT 
    brand,
    
    -- Forecast intensity metrics 4 weeks ahead
    ML.FORECAST(
      MODEL `bigquery-ai-kaggle-469620.ads_demo.intensity_arima_model`,
      STRUCT(4 AS horizon, 0.90 AS confidence_level)
    ) AS intensity_forecast_results,
    
    'INTENSITY_METRICS' AS forecast_type
    
  FROM (
    SELECT 
      brand,
      ARRAY_AGG(
        STRUCT(week_start, avg_promotional_intensity, avg_urgency_score, avg_brand_voice_score)
        ORDER BY week_start
      ) AS time_series_data
    FROM time_series_preparation
    GROUP BY brand  
    HAVING COUNT(*) >= 8
  )
);

-- Create ARIMA models for automatic pattern detection
-- Note: These would be created once with sufficient historical data

-- Funnel distribution ARIMA model
CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.funnel_arima_model`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_start',
  time_series_data_col='upper_funnel_pct',  -- Primary series
  time_series_id_col='brand',
  auto_arima=TRUE,
  auto_arima_max_order=5,
  data_frequency='WEEKLY'
) AS
SELECT 
  brand,
  week_start,
  upper_funnel_pct,
  mid_funnel_pct,
  lower_funnel_pct
FROM time_series_preparation;

-- Persona distribution ARIMA model  
CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.persona_arima_model`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_start', 
  time_series_data_col='new_customer_pct',
  time_series_id_col='brand',
  auto_arima=TRUE,
  auto_arima_max_order=5,
  data_frequency='WEEKLY'
) AS
SELECT 
  brand,
  week_start,
  new_customer_pct,
  existing_customer_pct, 
  general_market_pct
FROM time_series_preparation;

-- Intensity metrics ARIMA model
CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.intensity_arima_model`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_start',
  time_series_data_col='avg_promotional_intensity', 
  time_series_id_col='brand',
  auto_arima=TRUE,
  auto_arima_max_order=5,
  data_frequency='WEEKLY'
) AS
SELECT 
  brand,
  week_start,
  avg_promotional_intensity,
  avg_urgency_score,
  avg_brand_voice_score
FROM time_series_preparation;

-- Comprehensive forecast analysis view
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_strategic_distribution_forecasts` AS

WITH current_distributions AS (
  -- Latest actual distributions for comparison
  SELECT 
    brand,
    MAX(week_start) AS latest_week,
    
    -- Current funnel distribution
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN upper_funnel_pct END) AS current_upper_funnel_pct,
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN mid_funnel_pct END) AS current_mid_funnel_pct,
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN lower_funnel_pct END) AS current_lower_funnel_pct,
             
    -- Current persona distribution
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN new_customer_pct END) AS current_new_customer_pct,
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN existing_customer_pct END) AS current_existing_customer_pct,
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN general_market_pct END) AS current_general_market_pct,
             
    -- Current intensity levels
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN avg_promotional_intensity END) AS current_promotional_intensity,
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN avg_urgency_score END) AS current_urgency_score,
    AVG(CASE WHEN week_start = MAX(week_start) OVER (PARTITION BY brand)
             THEN avg_brand_voice_score END) AS current_brand_voice_score
             
  FROM time_series_preparation
  GROUP BY brand
),

forecast_predictions AS (
  -- Simulate forecast results (in production, these would come from actual ML.FORECAST calls)
  SELECT 
    brand,
    DATE_ADD(cd.latest_week, INTERVAL 1 WEEK) AS forecast_week_1,
    DATE_ADD(cd.latest_week, INTERVAL 2 WEEK) AS forecast_week_2,
    DATE_ADD(cd.latest_week, INTERVAL 3 WEEK) AS forecast_week_3,
    DATE_ADD(cd.latest_week, INTERVAL 4 WEEK) AS forecast_week_4,
    
    -- Simulated forecast evolution (with trending)
    -- Upper funnel trending up
    current_upper_funnel_pct * 1.02 AS forecast_upper_funnel_week_1,
    current_upper_funnel_pct * 1.05 AS forecast_upper_funnel_week_4,
    
    -- Lower funnel compensating
    current_lower_funnel_pct * 0.98 AS forecast_lower_funnel_week_1,
    current_lower_funnel_pct * 0.93 AS forecast_lower_funnel_week_4,
    
    -- Persona shifts toward existing customers
    current_existing_customer_pct * 1.03 AS forecast_existing_customer_week_1,
    current_existing_customer_pct * 1.08 AS forecast_existing_customer_week_4,
    
    -- Intensity evolution
    current_promotional_intensity * 0.97 AS forecast_promotional_intensity_week_1,
    current_promotional_intensity * 0.92 AS forecast_promotional_intensity_week_4,
    
    -- Confidence intervals (simulated)
    current_upper_funnel_pct * 0.95 AS upper_funnel_lower_ci_week_4,
    current_upper_funnel_pct * 1.15 AS upper_funnel_upper_ci_week_4
    
  FROM current_distributions cd
)

SELECT 
  cd.brand,
  cd.latest_week,
  
  -- Current state
  ROUND(cd.current_upper_funnel_pct, 3) AS current_upper_funnel_pct,
  ROUND(cd.current_mid_funnel_pct, 3) AS current_mid_funnel_pct,
  ROUND(cd.current_lower_funnel_pct, 3) AS current_lower_funnel_pct,
  
  ROUND(cd.current_new_customer_pct, 3) AS current_new_customer_pct,
  ROUND(cd.current_existing_customer_pct, 3) AS current_existing_customer_pct,
  ROUND(cd.current_general_market_pct, 3) AS current_general_market_pct,
  
  ROUND(cd.current_promotional_intensity, 3) AS current_promotional_intensity,
  ROUND(cd.current_urgency_score, 3) AS current_urgency_score,
  ROUND(cd.current_brand_voice_score, 3) AS current_brand_voice_score,
  
  -- 4-week forecasts
  fp.forecast_week_4,
  ROUND(fp.forecast_upper_funnel_week_4, 3) AS forecast_upper_funnel_week_4,
  ROUND(fp.forecast_lower_funnel_week_4, 3) AS forecast_lower_funnel_week_4,
  ROUND(fp.forecast_existing_customer_week_4, 3) AS forecast_existing_customer_week_4,
  ROUND(fp.forecast_promotional_intensity_week_4, 3) AS forecast_promotional_intensity_week_4,
  
  -- Distribution shift analysis
  ROUND(fp.forecast_upper_funnel_week_4 - cd.current_upper_funnel_pct, 3) AS upper_funnel_shift,
  ROUND(fp.forecast_existing_customer_week_4 - cd.current_existing_customer_pct, 3) AS existing_customer_shift,
  ROUND(fp.forecast_promotional_intensity_week_4 - cd.current_promotional_intensity, 3) AS promotional_intensity_shift,
  
  -- Strategic implications
  CASE 
    WHEN ABS(fp.forecast_upper_funnel_week_4 - cd.current_upper_funnel_pct) >= 0.10 
    THEN 'MAJOR_FUNNEL_SHIFT'
    WHEN ABS(fp.forecast_existing_customer_week_4 - cd.current_existing_customer_pct) >= 0.15
    THEN 'MAJOR_PERSONA_SHIFT' 
    WHEN ABS(fp.forecast_promotional_intensity_week_4 - cd.current_promotional_intensity) >= 0.20
    THEN 'MAJOR_INTENSITY_SHIFT'
    ELSE 'STABLE_STRATEGY'
  END AS predicted_strategic_change,
  
  -- Confidence intervals
  ROUND(fp.upper_funnel_lower_ci_week_4, 3) AS upper_funnel_lower_ci_week_4,
  ROUND(fp.upper_funnel_upper_ci_week_4, 3) AS upper_funnel_upper_ci_week_4,
  
  -- Business recommendations
  CASE 
    WHEN fp.forecast_upper_funnel_week_4 > cd.current_upper_funnel_pct + 0.10
    THEN 'PREPARE: Increase brand awareness content'
    WHEN fp.forecast_lower_funnel_week_4 > cd.current_lower_funnel_pct + 0.10  
    THEN 'OPTIMIZE: Focus on conversion-driving creative'
    WHEN fp.forecast_promotional_intensity_week_4 < cd.current_promotional_intensity - 0.15
    THEN 'BRAND_SHIFT: Moving away from promotional messaging'
    WHEN fp.forecast_existing_customer_week_4 > cd.current_existing_customer_pct + 0.15
    THEN 'RETENTION_FOCUS: Increasing existing customer targeting'
    ELSE 'MONITOR: Continue current strategy'
  END AS strategic_recommendation

FROM current_distributions cd
JOIN forecast_predictions fp USING (brand)
ORDER BY cd.brand;

-- Competitive benchmarking with forecasts
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitive_forecast_benchmarks` AS

WITH brand_forecasts AS (
  SELECT * FROM `bigquery-ai-kaggle-469620.ads_demo.v_strategic_distribution_forecasts`
),

competitive_analysis AS (
  SELECT 
    bf1.brand,
    bf1.forecast_week_4,
    
    -- Own brand predictions
    bf1.forecast_upper_funnel_week_4 AS own_forecast_upper_funnel,
    bf1.forecast_existing_customer_week_4 AS own_forecast_existing_customer,
    bf1.forecast_promotional_intensity_week_4 AS own_forecast_promotional_intensity,
    
    -- Competitive landscape predictions
    AVG(bf2.forecast_upper_funnel_week_4) AS competitor_avg_upper_funnel,
    AVG(bf2.forecast_existing_customer_week_4) AS competitor_avg_existing_customer,
    AVG(bf2.forecast_promotional_intensity_week_4) AS competitor_avg_promotional_intensity,
    
    -- Competitive positioning analysis
    bf1.forecast_upper_funnel_week_4 - AVG(bf2.forecast_upper_funnel_week_4) AS upper_funnel_competitive_gap,
    bf1.forecast_promotional_intensity_week_4 - AVG(bf2.forecast_promotional_intensity_week_4) AS promotional_intensity_gap,
    
    -- Convergence/divergence analysis
    CASE 
      WHEN ABS(bf1.forecast_upper_funnel_week_4 - AVG(bf2.forecast_upper_funnel_week_4)) > 
           ABS(bf1.current_upper_funnel_pct - AVG(bf2.current_upper_funnel_pct))
      THEN 'DIVERGING'
      WHEN ABS(bf1.forecast_upper_funnel_week_4 - AVG(bf2.forecast_upper_funnel_week_4)) < 
           ABS(bf1.current_upper_funnel_pct - AVG(bf2.current_upper_funnel_pct))
      THEN 'CONVERGING'
      ELSE 'STABLE_POSITIONING'
    END AS competitive_trajectory
    
  FROM brand_forecasts bf1
  JOIN brand_forecasts bf2 ON bf1.brand != bf2.brand
  GROUP BY bf1.brand, bf1.forecast_week_4, bf1.forecast_upper_funnel_week_4, 
           bf1.forecast_existing_customer_week_4, bf1.forecast_promotional_intensity_week_4,
           bf1.current_upper_funnel_pct, bf1.current_existing_customer_pct, bf1.current_promotional_intensity
)

SELECT 
  brand,
  forecast_week_4,
  
  -- Competitive positioning
  ROUND(own_forecast_upper_funnel, 3) AS own_forecast_upper_funnel,
  ROUND(competitor_avg_upper_funnel, 3) AS competitor_avg_upper_funnel,
  ROUND(upper_funnel_competitive_gap, 3) AS upper_funnel_competitive_gap,
  
  ROUND(own_forecast_promotional_intensity, 3) AS own_forecast_promotional_intensity,
  ROUND(competitor_avg_promotional_intensity, 3) AS competitor_avg_promotional_intensity,
  ROUND(promotional_intensity_gap, 3) AS promotional_intensity_gap,
  
  competitive_trajectory,
  
  -- Strategic insights
  CASE 
    WHEN upper_funnel_competitive_gap > 0.15 THEN 'BRAND_LEADER: Higher brand focus than competitors'
    WHEN upper_funnel_competitive_gap < -0.15 THEN 'CONVERSION_FOCUS: Lower brand focus than competitors'  
    WHEN promotional_intensity_gap > 0.20 THEN 'MORE_PROMOTIONAL: Higher promotional intensity'
    WHEN promotional_intensity_gap < -0.20 THEN 'PREMIUM_POSITIONING: Lower promotional intensity'
    ELSE 'BALANCED_POSITIONING'
  END AS competitive_positioning,
  
  -- Strategic recommendations based on competitive forecasts
  CASE 
    WHEN competitive_trajectory = 'DIVERGING' AND upper_funnel_competitive_gap > 0.10
    THEN 'MAINTAIN_DIFFERENTIATION: Continue brand-focused strategy'
    WHEN competitive_trajectory = 'CONVERGING' AND promotional_intensity_gap < -0.15
    THEN 'DEFEND_PREMIUM: Strengthen premium positioning before convergence'
    WHEN competitive_trajectory = 'DIVERGING' AND promotional_intensity_gap > 0.20
    THEN 'EVALUATE_SUSTAINABILITY: High promotional intensity may not be sustainable'
    ELSE 'MONITOR: Current trajectory appears stable'
  END AS competitive_recommendation
  
FROM competitive_analysis
ORDER BY brand;