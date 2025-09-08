-- Strategic Goldmine Forecasting: Tier 1 High-ROI Intelligence
-- Predicts competitor promotional intensity, messaging pivots, and urgency spikes
-- Business Impact: Pricing strategy, counter-positioning, launch timing

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.strategic_goldmine_forecasts` AS

WITH strategic_time_series AS (
  -- Prepare weekly strategic intelligence metrics
  SELECT 
    brand,
    week_start,
    
    -- Tier 1 Strategic Goldmine Metrics
    AVG(COALESCE(promotional_intensity, 0.0)) AS avg_promotional_intensity,
    AVG(COALESCE(urgency_score, 0.0)) AS avg_urgency_score,
    
    -- Primary angle distribution for pivot detection
    MODE(primary_angle) AS dominant_angle,
    COUNT(DISTINCT primary_angle) AS angle_diversity,
    
    -- Angle shift indicators
    COUNTIF(primary_angle = 'PROMOTIONAL') / COUNT(*) AS promotional_angle_pct,
    COUNTIF(primary_angle = 'EMOTIONAL') / COUNT(*) AS emotional_angle_pct,
    COUNTIF(primary_angle = 'FEATURE_FOCUSED') / COUNT(*) AS feature_focused_angle_pct,
    COUNTIF(primary_angle = 'URGENCY') / COUNT(*) AS urgency_angle_pct,
    
    -- Supporting tactical metrics
    COUNTIF(media_type = 'VIDEO') / COUNT(*) AS video_pct,
    COUNTIF(REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                           r'(\d+% OFF|SALE|DISCOUNT|BLACK FRIDAY|CYBER)')) / COUNT(*) AS explicit_promo_signals_pct,
    AVG(COALESCE(brand_voice_score, 0.5)) AS avg_brand_voice_score,
    
    -- Volume and activity
    COUNT(*) AS weekly_ad_count,
    COUNT(DISTINCT ad_id) AS unique_ads
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels_v2`  
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 20 WEEK)
    AND week_start IS NOT NULL
    AND brand IS NOT NULL
  GROUP BY brand, week_start
  HAVING COUNT(*) >= 2  -- Minimum ads for meaningful metrics
),

-- ARIMA-style trend and seasonal pattern detection
trend_analysis AS (
  SELECT 
    brand,
    week_start,
    avg_promotional_intensity,
    avg_urgency_score,
    promotional_angle_pct,
    emotional_angle_pct,
    
    -- Trend detection using linear correlation with time
    CORR(UNIX_SECONDS(CAST(week_start AS TIMESTAMP)), avg_promotional_intensity) OVER (
      PARTITION BY brand 
      ORDER BY week_start 
      ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS promotional_intensity_trend_8week,
    
    CORR(UNIX_SECONDS(CAST(week_start AS TIMESTAMP)), avg_urgency_score) OVER (
      PARTITION BY brand
      ORDER BY week_start
      ROWS BETWEEN 7 PRECEDING AND CURRENT ROW  
    ) AS urgency_trend_8week,
    
    -- Moving averages for smoothing
    AVG(avg_promotional_intensity) OVER (
      PARTITION BY brand ORDER BY week_start ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS promotional_intensity_ma3,
    
    AVG(avg_urgency_score) OVER (
      PARTITION BY brand ORDER BY week_start ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS urgency_score_ma3,
    
    -- Seasonal pattern detection (simplified)
    EXTRACT(WEEK FROM week_start) AS week_of_year,
    CASE 
      WHEN EXTRACT(WEEK FROM week_start) BETWEEN 46 AND 50 THEN 'BLACK_FRIDAY_SEASON'
      WHEN EXTRACT(WEEK FROM week_start) BETWEEN 51 AND 2 THEN 'HOLIDAY_SEASON'
      WHEN EXTRACT(WEEK FROM week_start) BETWEEN 35 AND 40 THEN 'BACK_TO_SCHOOL'
      ELSE 'REGULAR_SEASON'
    END AS seasonal_context,
    
    -- Angle shift detection
    LAG(promotional_angle_pct, 1) OVER (PARTITION BY brand ORDER BY week_start) AS prev_promotional_angle_pct,
    LAG(emotional_angle_pct, 1) OVER (PARTITION BY brand ORDER BY week_start) AS prev_emotional_angle_pct
    
  FROM strategic_time_series
),

-- Strategic Goldmine Forecasting Engine
goldmine_forecasts AS (
  SELECT 
    brand,
    week_start,
    seasonal_context,
    
    -- Current state
    avg_promotional_intensity,
    avg_urgency_score,
    promotional_angle_pct,
    emotional_angle_pct,
    
    -- Trend signals
    promotional_intensity_trend_8week,
    urgency_trend_8week,
    
    -- 4-week forecasts using trend + seasonal adjustments
    LEAST(1.0, promotional_intensity_ma3 + 
      CASE seasonal_context
        WHEN 'BLACK_FRIDAY_SEASON' THEN 0.25  -- Major promotional boost
        WHEN 'HOLIDAY_SEASON' THEN 0.15       -- Sustained promotional pressure
        WHEN 'BACK_TO_SCHOOL' THEN 0.10       -- Moderate promotional increase
        ELSE COALESCE(promotional_intensity_trend_8week * 0.1, 0.0)  -- Trend-based
      END
    ) AS forecast_promotional_intensity_4week,
    
    LEAST(1.0, urgency_score_ma3 + 
      CASE seasonal_context
        WHEN 'BLACK_FRIDAY_SEASON' THEN 0.30  -- Extreme urgency
        WHEN 'HOLIDAY_SEASON' THEN 0.20       -- High urgency  
        ELSE COALESCE(urgency_trend_8week * 0.08, 0.0)
      END
    ) AS forecast_urgency_score_4week,
    
    -- Primary angle pivot prediction
    CASE 
      WHEN promotional_angle_pct - COALESCE(prev_promotional_angle_pct, promotional_angle_pct) >= 0.20
      THEN 'PROMOTIONAL_PIVOT_DETECTED'
      WHEN emotional_angle_pct - COALESCE(prev_emotional_angle_pct, emotional_angle_pct) >= 0.25  
      THEN 'EMOTIONAL_PIVOT_DETECTED'
      WHEN seasonal_context = 'BLACK_FRIDAY_SEASON' AND promotional_angle_pct < 0.60
      THEN 'PROMOTIONAL_PIVOT_IMMINENT'
      WHEN promotional_intensity_trend_8week > 0.3 AND promotional_angle_pct < 0.50
      THEN 'PROMOTIONAL_PIVOT_LIKELY'
      ELSE 'STABLE_MESSAGING_STRATEGY'
    END AS predicted_angle_pivot_4week,
    
    -- Strategic timing predictions
    DATE_ADD(week_start, INTERVAL 4 WEEK) AS forecast_target_week,
    
    -- Confidence scoring
    CASE 
      WHEN ABS(COALESCE(promotional_intensity_trend_8week, 0)) > 0.3 OR seasonal_context != 'REGULAR_SEASON'
      THEN 'HIGH_CONFIDENCE'
      WHEN ABS(COALESCE(promotional_intensity_trend_8week, 0)) > 0.15
      THEN 'MEDIUM_CONFIDENCE'
      ELSE 'LOW_CONFIDENCE' 
    END AS forecast_confidence,
    
    -- Change magnitude predictions
    ABS(forecast_promotional_intensity_4week - avg_promotional_intensity) AS predicted_promotional_intensity_change,
    ABS(forecast_urgency_score_4week - avg_urgency_score) AS predicted_urgency_change
    
  FROM trend_analysis
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK)  -- Recent data for forecasting
),

-- Business Impact Assessment
strategic_intelligence AS (
  SELECT 
    *,
    
    -- Tier 1 Strategic Goldmine Classifications
    CASE 
      WHEN forecast_promotional_intensity_4week >= 0.80 AND predicted_promotional_intensity_change >= 0.20
      THEN 'DEEP_DISCOUNT_IMMINENT - Major promotional push predicted'
      WHEN forecast_promotional_intensity_4week >= 0.65 AND seasonal_context = 'BLACK_FRIDAY_SEASON'
      THEN 'SEASONAL_PROMOTIONAL_SPIKE - Black Friday intensity building'
      WHEN predicted_promotional_intensity_change >= 0.15 AND forecast_confidence = 'HIGH_CONFIDENCE'
      THEN 'PROMOTIONAL_ESCALATION - Significant promotional increase predicted'
      WHEN predicted_promotional_intensity_change <= -0.15
      THEN 'PROMOTIONAL_PULLBACK - Reducing promotional pressure'
      ELSE 'STABLE_PROMOTIONAL_STRATEGY'
    END AS promotional_intensity_intelligence,
    
    CASE 
      WHEN forecast_urgency_score_4week >= 0.75 AND predicted_urgency_change >= 0.20  
      THEN 'URGENCY_SPIKE_IMMINENT - Time pressure campaign likely'
      WHEN seasonal_context = 'BLACK_FRIDAY_SEASON' AND forecast_urgency_score_4week >= 0.60
      THEN 'SEASONAL_URGENCY_BUILD - Holiday urgency escalating'  
      WHEN predicted_urgency_change >= 0.15
      THEN 'URGENCY_ESCALATION - Increasing time pressure tactics'
      ELSE 'STABLE_URGENCY_LEVELS'
    END AS urgency_spike_intelligence,
    
    -- Business impact scoring
    CASE 
      WHEN predicted_angle_pivot_4week LIKE '%PROMOTIONAL_PIVOT%' AND predicted_promotional_intensity_change >= 0.20
      THEN 'CRITICAL_COMPETITIVE_THREAT - Major promotional offensive predicted'
      WHEN predicted_angle_pivot_4week LIKE '%EMOTIONAL_PIVOT%'
      THEN 'STRATEGIC_REPOSITIONING - Brand shifting to emotional messaging'
      WHEN forecast_promotional_intensity_4week >= 0.70 AND seasonal_context != 'BLACK_FRIDAY_SEASON'
      THEN 'UNEXPECTED_PROMOTIONAL_AGGRESSION - Non-seasonal promotional spike'
      ELSE 'ROUTINE_COMPETITIVE_ACTIVITY'
    END AS business_impact_assessment,
    
    -- Recommended competitive responses
    CASE 
      WHEN promotional_intensity_intelligence LIKE 'DEEP_DISCOUNT_IMMINENT%'
      THEN 'PRICING_STRATEGY: Prepare defensive pricing or differentiated positioning'
      WHEN predicted_angle_pivot_4week = 'EMOTIONAL_PIVOT_DETECTED'
      THEN 'COUNTER_MESSAGING: Strengthen rational/feature-focused messaging'
      WHEN urgency_spike_intelligence LIKE 'URGENCY_SPIKE_IMMINENT%'
      THEN 'TIMING_STRATEGY: Accelerate launch timing or extend current campaigns'
      WHEN forecast_promotional_intensity_4week <= 0.30
      THEN 'PREMIUM_OPPORTUNITY: Capitalize on competitor premium positioning'
      ELSE 'MONITOR: Continue current competitive strategy'
    END AS recommended_competitive_response
    
  FROM goldmine_forecasts
)

SELECT 
  brand,
  week_start,
  forecast_target_week,
  seasonal_context,
  
  -- Current intelligence baseline  
  ROUND(avg_promotional_intensity, 3) AS current_promotional_intensity,
  ROUND(avg_urgency_score, 3) AS current_urgency_score,
  ROUND(promotional_angle_pct, 3) AS current_promotional_angle_pct,
  
  -- Strategic Goldmine Forecasts
  ROUND(forecast_promotional_intensity_4week, 3) AS forecast_promotional_intensity_4week,
  ROUND(forecast_urgency_score_4week, 3) AS forecast_urgency_score_4week,
  predicted_angle_pivot_4week,
  
  -- Change predictions
  ROUND(predicted_promotional_intensity_change, 3) AS predicted_promotional_intensity_change,
  ROUND(predicted_urgency_change, 3) AS predicted_urgency_change,
  
  -- Intelligence assessments
  promotional_intensity_intelligence,
  urgency_spike_intelligence,
  business_impact_assessment,
  
  -- Competitive response recommendations
  recommended_competitive_response,
  forecast_confidence,
  
  -- Success metrics for validation
  CASE 
    WHEN predicted_promotional_intensity_change >= 0.15 AND forecast_confidence = 'HIGH_CONFIDENCE'
    THEN 'BLACK_FRIDAY_EARLY_PREDICTION_CANDIDATE'
    WHEN predicted_angle_pivot_4week LIKE '%PIVOT_DETECTED%'  
    THEN 'MESSAGING_STRATEGY_CHANGE_PREDICTED'
    WHEN forecast_urgency_score_4week >= 0.70
    THEN 'URGENCY_CAMPAIGN_LAUNCH_PREDICTED'
    ELSE 'BASELINE_MONITORING'
  END AS forecast_validation_category
  
FROM strategic_intelligence
WHERE brand IS NOT NULL
ORDER BY 
  CASE business_impact_assessment
    WHEN 'CRITICAL_COMPETITIVE_THREAT - Major promotional offensive predicted' THEN 1
    WHEN 'STRATEGIC_REPOSITIONING - Brand shifting to emotional messaging' THEN 2
    WHEN 'UNEXPECTED_PROMOTIONAL_AGGRESSION - Non-seasonal promotional spike' THEN 3
    ELSE 4
  END,
  predicted_promotional_intensity_change DESC,
  brand, week_start DESC;

-- Create tactical intelligence forecasting view (Tier 2)
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_tactical_intelligence_forecasts` AS

WITH tactical_metrics AS (
  SELECT 
    brand,
    week_start,
    
    -- Platform strategy distribution
    COUNTIF(REGEXP_CONTAINS(publisher_platforms, r'FACEBOOK.*INSTAGRAM|INSTAGRAM.*FACEBOOK')) / COUNT(*) AS cross_platform_pct,
    COUNTIF(publisher_platforms LIKE '%INSTAGRAM%' AND NOT REGEXP_CONTAINS(publisher_platforms, r'FACEBOOK')) / COUNT(*) AS instagram_only_pct,
    COUNTIF(publisher_platforms LIKE '%FACEBOOK%' AND NOT REGEXP_CONTAINS(publisher_platforms, r'INSTAGRAM')) / COUNT(*) AS facebook_only_pct,
    
    -- Media type evolution
    COUNTIF(media_type = 'VIDEO') / COUNT(*) AS video_pct,
    COUNTIF(media_type = 'IMAGE') / COUNT(*) AS image_pct,
    COUNTIF(media_type = 'DCO') / COUNT(*) AS dco_pct,
    
    -- Discount depth analysis
    AVG(CASE 
      WHEN REGEXP_EXTRACT(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), r'(\d+)% OFF') IS NOT NULL
      THEN SAFE_CAST(REGEXP_EXTRACT(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), r'(\d+)% OFF') AS INT64)
      ELSE NULL
    END) AS avg_discount_percentage,
    
    COUNTIF(REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), r'\d+% OFF')) / COUNT(*) AS explicit_discount_pct,
    
    COUNT(*) AS weekly_ad_count
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
  WHERE DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 WEEK)
    AND brand IS NOT NULL
    AND start_timestamp IS NOT NULL
  GROUP BY brand, week_start
  HAVING COUNT(*) >= 2
),

tactical_forecasts AS (
  SELECT 
    brand,
    week_start,
    
    -- Current tactical metrics
    cross_platform_pct,
    video_pct,
    avg_discount_percentage,
    explicit_discount_pct,
    
    -- 4-week tactical forecasts using simple trend + seasonal
    LEAST(1.0, GREATEST(0.0, 
      cross_platform_pct + 
      COALESCE((cross_platform_pct - LAG(cross_platform_pct, 1) OVER (PARTITION BY brand ORDER BY week_start)) * 2, 0)
    )) AS forecast_cross_platform_pct_4week,
    
    LEAST(1.0, GREATEST(0.0,
      video_pct + 
      CASE EXTRACT(WEEK FROM week_start)
        WHEN 46 THEN 0.15  -- Video increase for Black Friday
        WHEN 47 THEN 0.20  
        WHEN 51 THEN 0.10  -- Holiday video content
        ELSE COALESCE((video_pct - LAG(video_pct, 1) OVER (PARTITION BY brand ORDER BY week_start)) * 1.5, 0)
      END
    )) AS forecast_video_pct_4week,
    
    COALESCE(avg_discount_percentage + 
      CASE EXTRACT(WEEK FROM week_start)
        WHEN 46 THEN 15.0  -- Black Friday discount deepening
        WHEN 47 THEN 20.0
        ELSE 0.0
      END, 0) AS forecast_avg_discount_4week
    
  FROM tactical_metrics
)

SELECT 
  brand,
  week_start,
  DATE_ADD(week_start, INTERVAL 4 WEEK) AS forecast_target_week,
  
  -- Current state
  ROUND(cross_platform_pct, 3) AS current_cross_platform_pct,
  ROUND(video_pct, 3) AS current_video_pct,
  ROUND(COALESCE(avg_discount_percentage, 0), 1) AS current_avg_discount_pct,
  
  -- Tactical forecasts
  ROUND(forecast_cross_platform_pct_4week, 3) AS forecast_cross_platform_pct_4week,
  ROUND(forecast_video_pct_4week, 3) AS forecast_video_pct_4week,  
  ROUND(forecast_avg_discount_4week, 1) AS forecast_avg_discount_4week,
  
  -- Platform strategy intelligence
  CASE 
    WHEN ABS(forecast_cross_platform_pct_4week - cross_platform_pct) >= 0.20
    THEN 'PLATFORM_STRATEGY_SHIFT - Major platform allocation change'
    WHEN forecast_cross_platform_pct_4week <= 0.30 AND cross_platform_pct >= 0.60
    THEN 'PLATFORM_CONSOLIDATION - Moving toward single-platform focus'
    ELSE 'STABLE_PLATFORM_STRATEGY'
  END AS platform_strategy_intelligence,
  
  -- Media type intelligence  
  CASE 
    WHEN forecast_video_pct_4week - video_pct >= 0.25
    THEN 'VIDEO_CONTENT_SURGE - Major increase in video creative'
    WHEN video_pct >= 0.70 AND forecast_video_pct_4week >= 0.80
    THEN 'VIDEO_DOMINANT_STRATEGY - Sustained high video usage'
    ELSE 'BALANCED_MEDIA_STRATEGY'
  END AS media_type_intelligence,
  
  -- Discount intelligence
  CASE 
    WHEN forecast_avg_discount_4week - COALESCE(avg_discount_percentage, 0) >= 15.0
    THEN 'DISCOUNT_DEEPENING - Major promotional discount increase'
    WHEN forecast_avg_discount_4week >= 40.0  
    THEN 'DEEP_DISCOUNT_STRATEGY - High discount promotional period'
    ELSE 'STABLE_DISCOUNT_STRATEGY'
  END AS discount_intelligence
  
FROM tactical_forecasts
ORDER BY brand, week_start DESC;