-- Complete Required Views Integration
-- Creates all missing views specified in CRAWL_SUBGOALS.md
-- Integrates promotional patterns into time-series framework

-- 3. v_promotional_calendar - CTA and promotional patterns (rename existing to match spec)
CREATE OR REPLACE VIEW `your-project.ads_demo.v_promotional_calendar` AS
SELECT 
  brand,
  promotional_event_type,
  promotional_intensity_tier,
  start_date,
  end_date,
  campaign_duration_days,
  final_aggressiveness_score,
  discount_percentage,
  week_start,
  month_start,
  
  -- Competitive context
  competing_brands_count,
  competing_brands_list,
  competition_level,
  market_avg_aggressiveness,
  
  -- Strategic insights
  competitive_positioning,
  duration_vs_brand_pattern,
  
  -- Integration fields for time-series
  DATE_TRUNC(start_date, WEEK(MONDAY)) AS promotional_week_start,
  
  -- Promotional intensity for time-series integration
  CASE promotional_intensity_tier
    WHEN 'MEGA_SALE' THEN 1.0
    WHEN 'MAJOR_SALE' THEN 0.8  
    WHEN 'MODERATE_SALE' THEN 0.6
    WHEN 'LIGHT_PROMOTION' THEN 0.4
    WHEN 'SOFT_PROMOTION' THEN 0.2
    ELSE 0.0
  END AS promotional_intensity_score

FROM `your-project.ads_demo.promotional_calendar`
ORDER BY brand, start_date DESC;

-- 4. v_brand_voice_consistency - Voice drift detection over time
CREATE OR REPLACE VIEW `your-project.ads_demo.v_brand_voice_consistency` AS
WITH brand_voice_weekly AS (
  SELECT 
    brand,
    week_start,
    
    -- Voice consistency metrics
    AVG(brand_voice_score) AS avg_brand_voice_score,
    STDDEV(brand_voice_score) AS brand_voice_score_stddev,
    MIN(brand_voice_score) AS min_brand_voice_score,
    MAX(brand_voice_score) AS max_brand_voice_score,
    
    -- Voice characteristics analysis
    AVG(ARRAY_LENGTH(angles)) AS avg_angle_complexity,
    AVG(promotional_intensity) AS avg_promotional_intensity,
    AVG(urgency_score) AS avg_urgency_score,
    
    -- Tone consistency indicators
    COUNT(*) AS total_ads,
    COUNT(DISTINCT primary_angle) AS unique_primary_angles,
    COUNT(DISTINCT persona) AS unique_personas_targeted,
    
    -- Most common voice characteristics
    MODE() OVER (PARTITION BY brand, week_start ORDER BY primary_angle) AS dominant_angle,
    MODE() OVER (PARTITION BY brand, week_start ORDER BY persona) AS dominant_persona,
    
    -- Voice diversity score (higher = less consistent)
    COUNT(DISTINCT primary_angle) / COUNT(*) AS voice_diversity_score

  FROM `your-project.ads_demo.ads_strategic_labels_v2`
  WHERE brand_voice_score IS NOT NULL
  GROUP BY brand, week_start
),

voice_drift_detection AS (
  SELECT 
    *,
    
    -- Voice drift detection over time
    avg_brand_voice_score - LAG(avg_brand_voice_score, 1) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS voice_score_change_wow,
    
    avg_brand_voice_score - LAG(avg_brand_voice_score, 4) OVER (
      PARTITION BY brand ORDER BY week_start  
    ) AS voice_score_change_4w,
    
    -- Consistency changes
    brand_voice_score_stddev - LAG(brand_voice_score_stddev, 1) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS consistency_change_wow,
    
    voice_diversity_score - LAG(voice_diversity_score, 1) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS diversity_change_wow,
    
    -- Positioning change detection
    CASE 
      WHEN dominant_angle != LAG(dominant_angle) OVER (
        PARTITION BY brand ORDER BY week_start
      ) THEN 'ANGLE_POSITIONING_SHIFT'
      WHEN dominant_persona != LAG(dominant_persona) OVER (
        PARTITION BY brand ORDER BY week_start
      ) THEN 'PERSONA_POSITIONING_SHIFT'
      ELSE 'STABLE_POSITIONING'  
    END AS positioning_change_type

  FROM brand_voice_weekly
)

SELECT 
  brand,
  week_start,
  ROUND(avg_brand_voice_score, 3) AS avg_brand_voice_score,
  ROUND(brand_voice_score_stddev, 3) AS brand_voice_consistency,
  ROUND(voice_diversity_score, 3) AS voice_diversity_score,
  
  total_ads,
  unique_primary_angles,
  unique_personas_targeted,
  dominant_angle,
  dominant_persona,
  
  -- Change indicators
  ROUND(voice_score_change_wow, 3) AS voice_score_change_wow,
  ROUND(voice_score_change_4w, 3) AS voice_score_change_4w,
  ROUND(consistency_change_wow, 3) AS consistency_change_wow,
  ROUND(diversity_change_wow, 3) AS diversity_change_wow,
  
  positioning_change_type,
  
  -- Voice drift assessment
  CASE 
    WHEN ABS(voice_score_change_4w) >= 0.2 THEN 'SIGNIFICANT_VOICE_DRIFT'
    WHEN ABS(voice_score_change_wow) >= 0.15 THEN 'MODERATE_VOICE_DRIFT'
    WHEN consistency_change_wow >= 0.1 THEN 'DECREASING_CONSISTENCY'
    WHEN diversity_change_wow >= 0.2 THEN 'INCREASING_VOICE_FRAGMENTATION'
    ELSE 'STABLE_BRAND_VOICE'
  END AS voice_drift_assessment,
  
  -- Strategic recommendations
  CASE 
    WHEN voice_drift_assessment = 'SIGNIFICANT_VOICE_DRIFT' 
    THEN 'URGENT: Review brand guidelines and messaging consistency'
    WHEN positioning_change_type != 'STABLE_POSITIONING'
    THEN 'MODERATE: Validate intentional positioning shift vs drift'
    WHEN voice_diversity_score >= 0.7
    THEN 'LOW: Consider consolidating messaging themes'
    ELSE 'HEALTHY: Maintain current brand voice consistency'
  END AS voice_consistency_recommendation

FROM voice_drift_detection
ORDER BY brand, week_start DESC;

-- 5. Integrated Time-Series with Promotional Patterns
CREATE OR REPLACE VIEW `your-project.ads_demo.v_integrated_strategy_timeseries` AS
WITH weekly_base_metrics AS (
  -- Base strategic metrics from ads_strategic_labels_v2
  SELECT 
    brand,
    week_start,
    
    -- Volume
    COUNT(*) AS total_ads,
    COUNT(DISTINCT ad_id) AS unique_ads,
    
    -- Strategic mix
    COUNTIF(funnel = 'Upper') / COUNT(*) * 100 AS upper_funnel_pct,
    COUNTIF(funnel = 'Mid') / COUNT(*) * 100 AS mid_funnel_pct,
    COUNTIF(funnel = 'Lower') / COUNT(*) * 100 AS lower_funnel_pct,
    
    -- Angle distribution
    COUNTIF('PROMOTIONAL' IN UNNEST(angles)) / COUNT(*) * 100 AS promotional_angle_pct,
    COUNTIF('EMOTIONAL' IN UNNEST(angles)) / COUNT(*) * 100 AS emotional_angle_pct,
    COUNTIF('URGENCY' IN UNNEST(angles)) / COUNT(*) * 100 AS urgency_angle_pct,
    
    -- Strategic scores
    AVG(promotional_intensity) AS avg_promotional_intensity,
    AVG(urgency_score) AS avg_urgency_score,
    AVG(brand_voice_score) AS avg_brand_voice_score

  FROM `your-project.ads_demo.ads_strategic_labels_v2`
  GROUP BY brand, week_start
),

weekly_promotional_overlay AS (
  -- Promotional calendar integration
  SELECT 
    brand,
    promotional_week_start AS week_start,
    
    -- Promotional metrics
    COUNT(*) AS promotional_campaigns,
    AVG(promotional_intensity_score) AS avg_promotional_intensity_score,
    AVG(campaign_duration_days) AS avg_campaign_duration,
    
    -- Promotional event types this week
    STRING_AGG(DISTINCT promotional_event_type) AS promotional_events_this_week,
    
    -- Competitive promotional context
    AVG(competing_brands_count) AS avg_competitors_promoting,
    MAX(competing_brands_count) AS max_competitors_promoting

  FROM `your-project.ads_demo.v_promotional_calendar`
  WHERE promotional_week_start IS NOT NULL
  GROUP BY brand, promotional_week_start
),

weekly_platform_overlay AS (
  -- Platform strategy integration
  SELECT 
    brand,
    week_start,
    
    AVG(avg_platform_optimization_gap) AS avg_platform_consistency,
    AVG(pct_cross_platform) AS avg_cross_platform_pct,
    MODE() OVER (PARTITION BY brand, week_start ORDER BY platform_strategy_classification) AS dominant_platform_strategy

  FROM `your-project.ads_demo.platform_consistency_analysis`  
  GROUP BY brand, week_start
),

integrated_weekly_metrics AS (
  SELECT 
    bm.*,
    
    -- Promotional overlay
    COALESCE(po.promotional_campaigns, 0) AS promotional_campaigns,
    COALESCE(po.avg_promotional_intensity_score, 0) AS promotional_intensity_score,
    po.promotional_events_this_week,
    COALESCE(po.avg_competitors_promoting, 0) AS competitors_promoting_count,
    
    -- Platform overlay
    pl.avg_platform_consistency,
    pl.avg_cross_platform_pct,
    pl.dominant_platform_strategy,
    
    -- Combined promotional signal (from both angles and calendar)
    GREATEST(
      bm.avg_promotional_intensity,
      COALESCE(po.avg_promotional_intensity_score, 0)
    ) AS combined_promotional_signal,
    
    -- Strategic complexity score
    (bm.promotional_angle_pct + bm.emotional_angle_pct + bm.urgency_angle_pct) / 100 AS strategic_complexity

  FROM weekly_base_metrics bm
  LEFT JOIN weekly_promotional_overlay po USING (brand, week_start)
  LEFT JOIN weekly_platform_overlay pl USING (brand, week_start)
),

trend_analysis AS (
  SELECT 
    *,
    
    -- Multi-dimensional trend detection
    upper_funnel_pct - LAG(upper_funnel_pct) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS funnel_trend_wow,
    
    combined_promotional_signal - LAG(combined_promotional_signal) OVER (
      PARTITION BY brand ORDER BY week_start  
    ) AS promotional_trend_wow,
    
    avg_cross_platform_pct - LAG(avg_cross_platform_pct) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS platform_strategy_trend_wow,
    
    -- Integrated strategy shift detection
    CASE 
      WHEN ABS(upper_funnel_pct - LAG(upper_funnel_pct) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 20 THEN 'FUNNEL_STRATEGY_SHIFT'
      WHEN ABS(combined_promotional_signal - LAG(combined_promotional_signal) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 0.3 THEN 'PROMOTIONAL_STRATEGY_SHIFT'  
      WHEN promotional_campaigns > 0 AND LAG(promotional_campaigns) OVER (
        PARTITION BY brand ORDER BY week_start
      ) = 0 THEN 'PROMOTIONAL_CAMPAIGN_LAUNCH'
      WHEN ABS(avg_cross_platform_pct - LAG(avg_cross_platform_pct) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 15 THEN 'PLATFORM_STRATEGY_SHIFT'
      ELSE 'STABLE_INTEGRATED_STRATEGY'
    END AS integrated_strategy_change_type

  FROM integrated_weekly_metrics
)

SELECT 
  brand,
  week_start,
  
  -- Core metrics
  total_ads,
  unique_ads,
  
  -- Strategic distribution
  ROUND(upper_funnel_pct, 1) AS upper_funnel_pct,
  ROUND(mid_funnel_pct, 1) AS mid_funnel_pct, 
  ROUND(lower_funnel_pct, 1) AS lower_funnel_pct,
  
  ROUND(promotional_angle_pct, 1) AS promotional_angle_pct,
  ROUND(emotional_angle_pct, 1) AS emotional_angle_pct,
  ROUND(urgency_angle_pct, 1) AS urgency_angle_pct,
  
  -- Integrated promotional intelligence
  promotional_campaigns,
  ROUND(combined_promotional_signal, 3) AS combined_promotional_signal,
  promotional_events_this_week,
  competitors_promoting_count,
  
  -- Platform strategy
  ROUND(avg_platform_consistency, 3) AS platform_consistency,
  ROUND(avg_cross_platform_pct, 1) AS cross_platform_pct,
  dominant_platform_strategy,
  
  -- Strategic complexity and trends
  ROUND(strategic_complexity, 3) AS strategic_complexity,
  ROUND(funnel_trend_wow, 1) AS funnel_trend_wow,
  ROUND(promotional_trend_wow, 3) AS promotional_trend_wow,
  ROUND(platform_strategy_trend_wow, 1) AS platform_strategy_trend_wow,
  
  integrated_strategy_change_type,
  
  -- Strategic insights
  CASE 
    WHEN promotional_campaigns > 0 AND competitors_promoting_count >= 3 
    THEN 'HIGH_COMPETITIVE_PROMOTIONAL_PERIOD'
    WHEN integrated_strategy_change_type != 'STABLE_INTEGRATED_STRATEGY'
    THEN 'STRATEGY_EVOLUTION_DETECTED'
    WHEN strategic_complexity >= 0.8
    THEN 'COMPLEX_MULTI_ANGLE_STRATEGY'  
    WHEN upper_funnel_pct >= 70
    THEN 'BRAND_AWARENESS_FOCUSED_WEEK'
    WHEN lower_funnel_pct >= 70 AND promotional_campaigns > 0
    THEN 'CONVERSION_PROMOTIONAL_WEEK'
    ELSE 'BALANCED_STRATEGY_WEEK'
  END AS strategic_profile

FROM trend_analysis
ORDER BY brand, week_start DESC;

-- Validation queries for schema compliance
SELECT 
  'REQUIRED_VIEWS_VALIDATION' AS test_name,
  
  -- Check all required views exist
  (SELECT COUNT(*) FROM `INFORMATION_SCHEMA.VIEWS` 
   WHERE table_name = 'v_strategy_evolution') AS v_strategy_evolution_exists,
  (SELECT COUNT(*) FROM `INFORMATION_SCHEMA.VIEWS` 
   WHERE table_name = 'v_competitive_responses') AS v_competitive_responses_exists,
  (SELECT COUNT(*) FROM `INFORMATION_SCHEMA.VIEWS` 
   WHERE table_name = 'v_creative_fatigue') AS v_creative_fatigue_exists,
  (SELECT COUNT(*) FROM `INFORMATION_SCHEMA.VIEWS` 
   WHERE table_name = 'v_promotional_calendar') AS v_promotional_calendar_exists,
  (SELECT COUNT(*) FROM `INFORMATION_SCHEMA.VIEWS`
   WHERE table_name = 'v_brand_voice_consistency') AS v_brand_voice_consistency_exists;

SELECT 
  'SCHEMA_COMPLIANCE_VALIDATION' AS test_name,
  COUNT(*) AS ads_with_angle_arrays,
  AVG(ARRAY_LENGTH(angles)) AS avg_angles_per_ad,
  COUNTIF(ARRAY_LENGTH(angles) > 1) AS ads_with_multiple_angles,
  COUNTIF(ARRAY_LENGTH(topics) > 0) AS ads_with_topics,
  COUNT(DISTINCT primary_angle) AS unique_primary_angles
FROM `your-project.ads_demo.ads_strategic_labels_v2`;