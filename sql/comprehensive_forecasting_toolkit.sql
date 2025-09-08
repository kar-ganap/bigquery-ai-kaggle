-- Comprehensive Forecasting Toolkit with Signal Prioritization
-- Casts wide net across ALL distribution evolutions, surfaces only meaningful signals above noise threshold
-- Intelligent summarization prioritizes by magnitude, significance, business impact, competitive uniqueness

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.comprehensive_forecasting_signals` AS

WITH comprehensive_time_series AS (
  -- Cast wide net: Extract ALL possible distribution metrics for forecasting
  SELECT 
    brand,
    week_start,
    COUNT(*) AS weekly_ad_count,
    
    -- TIER 1: Core Strategic Distributions
    COUNTIF(funnel = 'Upper') / COUNT(*) AS upper_funnel_pct,
    COUNTIF(funnel = 'Mid') / COUNT(*) AS mid_funnel_pct,
    COUNTIF(funnel = 'Lower') / COUNT(*) AS lower_funnel_pct,
    
    COUNTIF(persona = 'New Customer') / COUNT(*) AS new_customer_pct,
    COUNTIF(persona = 'Existing Customer') / COUNT(*) AS existing_customer_pct,
    COUNTIF(persona = 'General Market') / COUNT(*) AS general_market_pct,
    
    -- Intensity metrics
    AVG(COALESCE(promotional_intensity, 0.0)) AS avg_promotional_intensity,
    AVG(COALESCE(urgency_score, 0.0)) AS avg_urgency_score,
    AVG(COALESCE(brand_voice_score, 0.5)) AS avg_brand_voice_score,
    
    -- TIER 2: Message Angle Distributions (10+ angles)
    COUNTIF('PROMOTIONAL' IN UNNEST(angles)) / COUNT(*) AS promotional_angle_pct,
    COUNTIF('EMOTIONAL' IN UNNEST(angles)) / COUNT(*) AS emotional_angle_pct,
    COUNTIF('RATIONAL' IN UNNEST(angles)) / COUNT(*) AS rational_angle_pct,
    COUNTIF('URGENCY' IN UNNEST(angles)) / COUNT(*) AS urgency_angle_pct,
    COUNTIF('TRUST' IN UNNEST(angles)) / COUNT(*) AS trust_angle_pct,
    COUNTIF('SOCIAL_PROOF' IN UNNEST(angles)) / COUNT(*) AS social_proof_angle_pct,
    COUNTIF('SCARCITY' IN UNNEST(angles)) / COUNT(*) AS scarcity_angle_pct,
    COUNTIF('BENEFIT_FOCUSED' IN UNNEST(angles)) / COUNT(*) AS benefit_focused_angle_pct,
    COUNTIF('FEATURE_FOCUSED' IN UNNEST(angles)) / COUNT(*) AS feature_focused_angle_pct,
    COUNTIF('ASPIRATIONAL' IN UNNEST(angles)) / COUNT(*) AS aspirational_angle_pct,
    
    -- TIER 3: Tactical Distribution Metrics
    COUNTIF(media_type = 'VIDEO') / COUNT(*) AS video_pct,
    COUNTIF(media_type = 'IMAGE') / COUNT(*) AS image_pct,
    COUNTIF(media_type = 'DCO') / COUNT(*) AS dco_pct,
    
    -- Platform strategy sophistication
    COUNTIF(REGEXP_CONTAINS(publisher_platforms, r'FACEBOOK.*INSTAGRAM|INSTAGRAM.*FACEBOOK')) / COUNT(*) AS cross_platform_pct,
    COUNTIF(publisher_platforms LIKE '%INSTAGRAM%' AND NOT REGEXP_CONTAINS(publisher_platforms, r'FACEBOOK')) / COUNT(*) AS instagram_only_pct,
    COUNTIF(publisher_platforms LIKE '%FACEBOOK%' AND NOT REGEXP_CONTAINS(publisher_platforms, r'INSTAGRAM')) / COUNT(*) AS facebook_only_pct,
    
    -- TIER 4: Advanced Intelligence Metrics
    -- Message complexity evolution
    AVG(ARRAY_LENGTH(angles)) AS avg_angles_per_ad,
    AVG(ARRAY_LENGTH(topics)) AS avg_topics_per_ad,
    
    -- Discount depth analysis
    AVG(CASE 
      WHEN REGEXP_EXTRACT(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), r'(\d+)% OFF') IS NOT NULL
      THEN SAFE_CAST(REGEXP_EXTRACT(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), r'(\d+)% OFF') AS INT64)
      ELSE NULL
    END) AS avg_discount_percentage,
    
    COUNTIF(REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), r'\d+% OFF')) / COUNT(*) AS explicit_discount_pct,
    
    -- Audience sophistication indicators
    (COUNTIF('RATIONAL' IN UNNEST(angles)) + COUNTIF('FEATURE_FOCUSED' IN UNNEST(angles)) + COUNTIF('TRUST' IN UNNEST(angles))) / COUNT(*) AS rational_sophistication_pct,
    (COUNTIF('EMOTIONAL' IN UNNEST(angles)) + COUNTIF('ASPIRATIONAL' IN UNNEST(angles))) / COUNT(*) AS emotional_appeal_pct,
    
    -- Seasonal context for adjustments
    EXTRACT(WEEK FROM week_start) AS week_of_year,
    CASE 
      WHEN EXTRACT(WEEK FROM week_start) BETWEEN 46 AND 50 THEN 'BLACK_FRIDAY_SEASON'
      WHEN EXTRACT(WEEK FROM week_start) BETWEEN 51 AND 2 THEN 'HOLIDAY_SEASON'
      WHEN EXTRACT(WEEK FROM week_start) BETWEEN 35 AND 40 THEN 'BACK_TO_SCHOOL'
      ELSE 'REGULAR_SEASON'
    END AS seasonal_context
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels_v2` 
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 WEEK)  -- 6 months of data
    AND week_start IS NOT NULL
    AND brand IS NOT NULL
    AND ARRAY_LENGTH(angles) > 0  -- Valid angle data
  GROUP BY brand, week_start
  HAVING COUNT(*) >= 2  -- Minimum ads for reliable distributions
),

-- Generate forecasts for ALL metrics using trend analysis + seasonal adjustments
comprehensive_forecasts AS (
  SELECT 
    brand,
    week_start,
    seasonal_context,
    
    -- Current state (baseline)
    upper_funnel_pct,
    avg_promotional_intensity,
    avg_urgency_score,
    avg_brand_voice_score,
    promotional_angle_pct,
    emotional_angle_pct,
    video_pct,
    cross_platform_pct,
    avg_angles_per_ad,
    rational_sophistication_pct,
    avg_discount_percentage,
    
    -- Trend calculations (8-week linear trend)
    COALESCE(
      (upper_funnel_pct - LAG(upper_funnel_pct, 4) OVER (PARTITION BY brand ORDER BY week_start)) / 4.0,
      0.0
    ) AS upper_funnel_trend_weekly,
    
    COALESCE(
      (avg_promotional_intensity - LAG(avg_promotional_intensity, 4) OVER (PARTITION BY brand ORDER BY week_start)) / 4.0,
      0.0
    ) AS promotional_intensity_trend_weekly,
    
    COALESCE(
      (avg_brand_voice_score - LAG(avg_brand_voice_score, 4) OVER (PARTITION BY brand ORDER BY week_start)) / 4.0,
      0.0
    ) AS brand_voice_trend_weekly,
    
    COALESCE(
      (promotional_angle_pct - LAG(promotional_angle_pct, 4) OVER (PARTITION BY brand ORDER BY week_start)) / 4.0,
      0.0
    ) AS promotional_angle_trend_weekly,
    
    COALESCE(
      (video_pct - LAG(video_pct, 4) OVER (PARTITION BY brand ORDER BY week_start)) / 4.0,
      0.0
    ) AS video_trend_weekly,
    
    COALESCE(
      (rational_sophistication_pct - LAG(rational_sophistication_pct, 4) OVER (PARTITION BY brand ORDER BY week_start)) / 4.0,
      0.0
    ) AS rational_sophistication_trend_weekly,
    
    -- 4-week forecasts with seasonal adjustments
    DATE_ADD(week_start, INTERVAL 4 WEEK) AS forecast_target_week
    
  FROM comprehensive_time_series
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)  -- Recent data for forecasting
),

-- Calculate actual forecasted values with seasonal intelligence
forecast_calculations AS (
  SELECT 
    *,
    
    -- Apply trends + seasonal adjustments for 4-week forecasts
    LEAST(1.0, GREATEST(0.0, upper_funnel_pct + 4 * upper_funnel_trend_weekly + 
      CASE seasonal_context
        WHEN 'HOLIDAY_SEASON' THEN 0.05  -- Upper funnel boost for brand awareness
        ELSE 0.0
      END
    )) AS forecast_upper_funnel_pct_4week,
    
    LEAST(1.0, GREATEST(0.0, avg_promotional_intensity + 4 * promotional_intensity_trend_weekly +
      CASE seasonal_context  
        WHEN 'BLACK_FRIDAY_SEASON' THEN 0.25
        WHEN 'HOLIDAY_SEASON' THEN 0.15
        WHEN 'BACK_TO_SCHOOL' THEN 0.10
        ELSE 0.0
      END
    )) AS forecast_promotional_intensity_4week,
    
    LEAST(1.0, GREATEST(0.0, avg_brand_voice_score + 4 * brand_voice_trend_weekly)) AS forecast_brand_voice_score_4week,
    
    LEAST(1.0, GREATEST(0.0, promotional_angle_pct + 4 * promotional_angle_trend_weekly +
      CASE seasonal_context
        WHEN 'BLACK_FRIDAY_SEASON' THEN 0.20
        WHEN 'HOLIDAY_SEASON' THEN 0.10  
        ELSE 0.0
      END
    )) AS forecast_promotional_angle_pct_4week,
    
    LEAST(1.0, GREATEST(0.0, video_pct + 4 * video_trend_weekly +
      CASE seasonal_context
        WHEN 'BLACK_FRIDAY_SEASON' THEN 0.15  -- Video surge for seasonal campaigns
        WHEN 'HOLIDAY_SEASON' THEN 0.10
        ELSE 0.0
      END  
    )) AS forecast_video_pct_4week,
    
    LEAST(1.0, GREATEST(0.0, rational_sophistication_pct + 4 * rational_sophistication_trend_weekly)) AS forecast_rational_sophistication_pct_4week
    
  FROM comprehensive_forecasts
),

-- Signal detection: Calculate change magnitudes and statistical significance
signal_detection AS (
  SELECT 
    brand,
    week_start,
    forecast_target_week,
    seasonal_context,
    
    -- Calculate all change magnitudes
    ABS(forecast_upper_funnel_pct_4week - upper_funnel_pct) AS upper_funnel_change_magnitude,
    ABS(forecast_promotional_intensity_4week - avg_promotional_intensity) AS promotional_intensity_change_magnitude,  
    ABS(forecast_brand_voice_score_4week - avg_brand_voice_score) AS brand_voice_change_magnitude,
    ABS(forecast_promotional_angle_pct_4week - promotional_angle_pct) AS promotional_angle_change_magnitude,
    ABS(forecast_video_pct_4week - video_pct) AS video_change_magnitude,
    ABS(forecast_rational_sophistication_pct_4week - rational_sophistication_pct) AS rational_sophistication_change_magnitude,
    
    -- Direction indicators (+ = increase, - = decrease)
    SIGN(forecast_upper_funnel_pct_4week - upper_funnel_pct) AS upper_funnel_direction,
    SIGN(forecast_promotional_intensity_4week - avg_promotional_intensity) AS promotional_intensity_direction,
    SIGN(forecast_brand_voice_score_4week - avg_brand_voice_score) AS brand_voice_direction,
    SIGN(forecast_promotional_angle_pct_4week - promotional_angle_pct) AS promotional_angle_direction,
    SIGN(forecast_video_pct_4week - video_pct) AS video_direction,
    SIGN(forecast_rational_sophistication_pct_4week - rational_sophistication_pct) AS rational_sophistication_direction,
    
    -- Trend confidence (based on consistency of trend)
    CASE 
      WHEN ABS(upper_funnel_trend_weekly) >= 0.02 THEN 'HIGH_CONFIDENCE'
      WHEN ABS(upper_funnel_trend_weekly) >= 0.01 THEN 'MEDIUM_CONFIDENCE'  
      ELSE 'LOW_CONFIDENCE'
    END AS upper_funnel_trend_confidence,
    
    CASE
      WHEN ABS(promotional_intensity_trend_weekly) >= 0.03 THEN 'HIGH_CONFIDENCE'
      WHEN ABS(promotional_intensity_trend_weekly) >= 0.015 THEN 'MEDIUM_CONFIDENCE'
      ELSE 'LOW_CONFIDENCE'  
    END AS promotional_intensity_trend_confidence,
    
    CASE
      WHEN ABS(brand_voice_trend_weekly) >= 0.025 THEN 'HIGH_CONFIDENCE'  
      WHEN ABS(brand_voice_trend_weekly) >= 0.01 THEN 'MEDIUM_CONFIDENCE'
      ELSE 'LOW_CONFIDENCE'
    END AS brand_voice_trend_confidence,
    
    -- Store forecasted values for business logic
    forecast_upper_funnel_pct_4week,
    forecast_promotional_intensity_4week,  
    forecast_brand_voice_score_4week,
    forecast_promotional_angle_pct_4week,
    forecast_video_pct_4week,
    forecast_rational_sophistication_pct_4week
    
  FROM forecast_calculations
),

-- Business impact weighting and competitive uniqueness analysis
signal_prioritization AS (
  SELECT 
    sd.*,
    
    -- Business impact weights (1-5 scale, 5 = highest impact)
    CASE 
      WHEN promotional_intensity_change_magnitude >= 0.20 AND promotional_intensity_trend_confidence = 'HIGH_CONFIDENCE' THEN 5
      WHEN promotional_intensity_change_magnitude >= 0.15 THEN 4  
      WHEN promotional_intensity_change_magnitude >= 0.10 THEN 3
      ELSE 1
    END AS promotional_intensity_business_impact_weight,
    
    CASE
      WHEN brand_voice_change_magnitude >= 0.20 AND brand_voice_trend_confidence = 'HIGH_CONFIDENCE' THEN 5
      WHEN brand_voice_change_magnitude >= 0.15 THEN 4
      WHEN brand_voice_change_magnitude >= 0.10 THEN 3  
      ELSE 1
    END AS brand_voice_business_impact_weight,
    
    CASE
      WHEN upper_funnel_change_magnitude >= 0.15 AND upper_funnel_trend_confidence = 'HIGH_CONFIDENCE' THEN 4
      WHEN upper_funnel_change_magnitude >= 0.10 THEN 3
      WHEN upper_funnel_change_magnitude >= 0.08 THEN 2
      ELSE 1
    END AS upper_funnel_business_impact_weight,
    
    CASE
      WHEN promotional_angle_change_magnitude >= 0.25 THEN 4
      WHEN promotional_angle_change_magnitude >= 0.15 THEN 3
      WHEN promotional_angle_change_magnitude >= 0.10 THEN 2
      ELSE 1  
    END AS promotional_angle_business_impact_weight,
    
    CASE  
      WHEN video_change_magnitude >= 0.30 THEN 3
      WHEN video_change_magnitude >= 0.20 THEN 2
      ELSE 1
    END AS video_business_impact_weight,
    
    CASE
      WHEN rational_sophistication_change_magnitude >= 0.20 THEN 3
      WHEN rational_sophistication_change_magnitude >= 0.15 THEN 2  
      ELSE 1
    END AS rational_sophistication_business_impact_weight,
    
    -- Competitive uniqueness scoring (compare to average competitor change)
    promotional_intensity_change_magnitude - AVG(promotional_intensity_change_magnitude) OVER (PARTITION BY week_start) AS promotional_intensity_competitive_uniqueness,
    brand_voice_change_magnitude - AVG(brand_voice_change_magnitude) OVER (PARTITION BY week_start) AS brand_voice_competitive_uniqueness,
    upper_funnel_change_magnitude - AVG(upper_funnel_change_magnitude) OVER (PARTITION BY week_start) AS upper_funnel_competitive_uniqueness
    
  FROM signal_detection sd
),

-- Apply noise thresholds and create prioritized signal summary
filtered_signals AS (
  SELECT 
    brand,
    week_start,
    forecast_target_week,
    seasonal_context,
    
    -- NOISE THRESHOLD FILTERING: Only surface signals above meaningful thresholds
    ARRAY(
      SELECT AS STRUCT 
        signal_type,
        change_magnitude,
        direction,
        business_impact_weight,
        trend_confidence,
        competitive_uniqueness,
        forecast_value,
        strategic_interpretation
      FROM UNNEST([
        -- Promotional Intensity Signal
        STRUCT(
          'PROMOTIONAL_INTENSITY' AS signal_type,
          promotional_intensity_change_magnitude AS change_magnitude,
          promotional_intensity_direction AS direction,
          promotional_intensity_business_impact_weight AS business_impact_weight,
          promotional_intensity_trend_confidence AS trend_confidence,
          promotional_intensity_competitive_uniqueness AS competitive_uniqueness,
          forecast_promotional_intensity_4week AS forecast_value,
          CASE 
            WHEN forecast_promotional_intensity_4week >= 0.80 AND promotional_intensity_direction = 1 
            THEN 'DEEP_DISCOUNT_OFFENSIVE - Major promotional push imminent'
            WHEN promotional_intensity_change_magnitude >= 0.20 AND promotional_intensity_direction = 1
            THEN 'PROMOTIONAL_ESCALATION - Significant promotional increase'  
            WHEN promotional_intensity_change_magnitude >= 0.15 AND promotional_intensity_direction = -1
            THEN 'PROMOTIONAL_PULLBACK - Reducing promotional pressure'
            ELSE 'PROMOTIONAL_ADJUSTMENT'
          END AS strategic_interpretation
        ),
        -- Brand Voice Signal  
        STRUCT(
          'BRAND_VOICE_POSITIONING' AS signal_type,
          brand_voice_change_magnitude AS change_magnitude,
          brand_voice_direction AS direction,
          brand_voice_business_impact_weight AS business_impact_weight,
          brand_voice_trend_confidence AS trend_confidence,
          brand_voice_competitive_uniqueness AS competitive_uniqueness,
          forecast_brand_voice_score_4week AS forecast_value,
          CASE
            WHEN forecast_brand_voice_score_4week >= 0.80 AND brand_voice_direction = 1
            THEN 'PREMIUM_POSITIONING_SHIFT - Moving toward premium brand voice'
            WHEN forecast_brand_voice_score_4week <= 0.30 AND brand_voice_direction = -1  
            THEN 'MASS_MARKET_PIVOT - Brand voice trending mass market'
            WHEN brand_voice_change_magnitude >= 0.15
            THEN 'BRAND_VOICE_REPOSITIONING - Significant brand voice evolution'
            ELSE 'BRAND_VOICE_ADJUSTMENT'
          END AS strategic_interpretation
        ),
        -- Funnel Strategy Signal
        STRUCT(
          'FUNNEL_STRATEGY' AS signal_type, 
          upper_funnel_change_magnitude AS change_magnitude,
          upper_funnel_direction AS direction,
          upper_funnel_business_impact_weight AS business_impact_weight,
          upper_funnel_trend_confidence AS trend_confidence,
          upper_funnel_competitive_uniqueness AS competitive_uniqueness,
          forecast_upper_funnel_pct_4week AS forecast_value,
          CASE
            WHEN forecast_upper_funnel_pct_4week >= 0.70 AND upper_funnel_direction = 1
            THEN 'BRAND_BUILDING_PHASE - Major upper funnel focus increase'
            WHEN forecast_upper_funnel_pct_4week <= 0.30 AND upper_funnel_direction = -1
            THEN 'CONVERSION_FOCUS_SHIFT - Moving toward lower funnel emphasis' 
            WHEN upper_funnel_change_magnitude >= 0.12
            THEN 'FUNNEL_STRATEGY_PIVOT - Significant funnel allocation change'
            ELSE 'FUNNEL_OPTIMIZATION'
          END AS strategic_interpretation
        ),
        -- Promotional Angle Signal
        STRUCT(
          'MESSAGING_ANGLE_STRATEGY' AS signal_type,
          promotional_angle_change_magnitude AS change_magnitude, 
          promotional_angle_direction AS direction,
          promotional_angle_business_impact_weight AS business_impact_weight,
          'MEDIUM_CONFIDENCE' AS trend_confidence,  -- Default for angles
          0.0 AS competitive_uniqueness,  -- Placeholder
          forecast_promotional_angle_pct_4week AS forecast_value,
          CASE
            WHEN forecast_promotional_angle_pct_4week >= 0.70 AND promotional_angle_direction = 1
            THEN 'PROMOTIONAL_MESSAGING_DOMINANCE - Heavy promotional angle emphasis'
            WHEN promotional_angle_change_magnitude >= 0.20 AND promotional_angle_direction = 1
            THEN 'PROMOTIONAL_ANGLE_PIVOT - Shifting toward promotional messaging'
            WHEN promotional_angle_change_magnitude >= 0.20 AND promotional_angle_direction = -1  
            THEN 'BRAND_MESSAGING_PIVOT - Moving away from promotional angles'
            ELSE 'MESSAGING_ANGLE_ADJUSTMENT'
          END AS strategic_interpretation  
        ),
        -- Video Strategy Signal
        STRUCT(
          'MEDIA_TYPE_STRATEGY' AS signal_type,
          video_change_magnitude AS change_magnitude,
          video_direction AS direction, 
          video_business_impact_weight AS business_impact_weight,
          'MEDIUM_CONFIDENCE' AS trend_confidence,
          0.0 AS competitive_uniqueness,
          forecast_video_pct_4week AS forecast_value,
          CASE
            WHEN forecast_video_pct_4week >= 0.80 AND video_direction = 1
            THEN 'VIDEO_DOMINANT_STRATEGY - Major shift toward video content'
            WHEN video_change_magnitude >= 0.25 
            THEN 'MEDIA_TYPE_STRATEGY_SHIFT - Significant video allocation change'
            ELSE 'MEDIA_TYPE_OPTIMIZATION'
          END AS strategic_interpretation
        ),
        -- Audience Sophistication Signal
        STRUCT(
          'AUDIENCE_SOPHISTICATION' AS signal_type,
          rational_sophistication_change_magnitude AS change_magnitude,
          rational_sophistication_direction AS direction,
          rational_sophistication_business_impact_weight AS business_impact_weight, 
          'MEDIUM_CONFIDENCE' AS trend_confidence,
          0.0 AS competitive_uniqueness,
          forecast_rational_sophistication_pct_4week AS forecast_value,
          CASE
            WHEN rational_sophistication_change_magnitude >= 0.20 AND rational_sophistication_direction = 1
            THEN 'AUDIENCE_MATURATION - Increasing rational/feature-focused messaging'  
            WHEN rational_sophistication_change_magnitude >= 0.20 AND rational_sophistication_direction = -1
            THEN 'EMOTIONAL_STRATEGY_SHIFT - Moving toward emotional appeal'
            ELSE 'AUDIENCE_STRATEGY_ADJUSTMENT'
          END AS strategic_interpretation
        )
      ]) AS signal
      -- NOISE THRESHOLD: Only include signals above minimum thresholds
      WHERE signal.change_magnitude >= CASE signal.signal_type
        WHEN 'PROMOTIONAL_INTENSITY' THEN 0.10        -- 10%+ promotional intensity change  
        WHEN 'BRAND_VOICE_POSITIONING' THEN 0.10      -- 10%+ brand voice change
        WHEN 'FUNNEL_STRATEGY' THEN 0.08              -- 8%+ funnel allocation change
        WHEN 'MESSAGING_ANGLE_STRATEGY' THEN 0.15     -- 15%+ messaging angle change
        WHEN 'MEDIA_TYPE_STRATEGY' THEN 0.20          -- 20%+ media type change
        WHEN 'AUDIENCE_SOPHISTICATION' THEN 0.15      -- 15%+ sophistication change
        ELSE 0.10
      END
      AND signal.business_impact_weight >= 2  -- Minimum business impact threshold
      ORDER BY signal.business_impact_weight DESC, signal.change_magnitude DESC
      LIMIT 5  -- Top 5 signals per brand to avoid noise
    ) AS prioritized_signals
    
  FROM signal_prioritization
  WHERE ARRAY_LENGTH([
    promotional_intensity_change_magnitude,
    brand_voice_change_magnitude, 
    upper_funnel_change_magnitude,
    promotional_angle_change_magnitude,
    video_change_magnitude,
    rational_sophistication_change_magnitude
  ]) > 0  -- Ensure we have actual signal data
)

SELECT 
  brand,
  week_start,
  forecast_target_week,
  seasonal_context,
  prioritized_signals,
  ARRAY_LENGTH(prioritized_signals) AS signal_count,
  
  -- Extract top signal for summary
  CASE WHEN ARRAY_LENGTH(prioritized_signals) > 0 
       THEN prioritized_signals[ORDINAL(1)].signal_type 
       ELSE NULL END AS top_signal_type,
       
  CASE WHEN ARRAY_LENGTH(prioritized_signals) > 0
       THEN prioritized_signals[ORDINAL(1)].strategic_interpretation
       ELSE NULL END AS top_strategic_intelligence,
       
  CASE WHEN ARRAY_LENGTH(prioritized_signals) > 0  
       THEN prioritized_signals[ORDINAL(1)].business_impact_weight
       ELSE NULL END AS top_signal_business_impact,
       
  -- Customer-facing executive summary (only above-noise signals)
  CASE 
    WHEN ARRAY_LENGTH(prioritized_signals) = 0 
    THEN 'STABLE_STRATEGY - No significant distribution changes predicted'
    WHEN ARRAY_LENGTH(prioritized_signals) = 1 AND prioritized_signals[ORDINAL(1)].business_impact_weight >= 4
    THEN CONCAT('🚨 CRITICAL: ', prioritized_signals[ORDINAL(1)].strategic_interpretation)
    WHEN ARRAY_LENGTH(prioritized_signals) >= 3 
    THEN CONCAT('⚠️  MULTIPLE_SHIFTS: ', CAST(ARRAY_LENGTH(prioritized_signals) AS STRING), ' strategic changes predicted')
    WHEN prioritized_signals[ORDINAL(1)].business_impact_weight >= 3
    THEN CONCAT('📊 MODERATE: ', prioritized_signals[ORDINAL(1)].strategic_interpretation)  
    ELSE CONCAT('📈 MINOR: ', prioritized_signals[ORDINAL(1)].strategic_interpretation)
  END AS executive_summary,
  
  -- Analysis timestamp
  CURRENT_TIMESTAMP() AS analysis_timestamp

FROM filtered_signals
WHERE ARRAY_LENGTH(prioritized_signals) > 0  -- Only brands with above-threshold signals
ORDER BY 
  CASE WHEN ARRAY_LENGTH(prioritized_signals) > 0 
       THEN prioritized_signals[ORDINAL(1)].business_impact_weight 
       ELSE 0 END DESC,
  signal_count DESC,
  brand, week_start DESC;