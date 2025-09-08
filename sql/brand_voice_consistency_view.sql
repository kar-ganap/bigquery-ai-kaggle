-- Brand Voice Consistency View - Missing from required_views_integration.sql
-- Tracks voice consistency and positioning changes over time

CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_brand_voice_consistency` AS
WITH brand_voice_weekly AS (
  SELECT 
    brand,
    week_start,
    AVG(brand_voice_score) AS weekly_brand_voice,
    STDDEV(brand_voice_score) AS weekly_voice_variation,
    COUNT(*) AS weekly_ad_count,
    
    -- Dominant messaging themes
    STRING_AGG(DISTINCT primary_angle ORDER BY primary_angle) AS weekly_angles,
    COUNTIF(primary_angle = 'PROMOTIONAL') / COUNT(*) AS promo_messaging_pct,
    COUNTIF(primary_angle = 'EMOTIONAL') / COUNT(*) AS emotional_messaging_pct,
    COUNTIF(funnel = 'Upper') / COUNT(*) AS upper_funnel_pct
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels_mock`
  GROUP BY brand, week_start
),

voice_consistency AS (
  SELECT 
    brand,
    week_start,
    weekly_brand_voice,
    weekly_voice_variation,
    
    -- Week-over-week voice change
    ABS(weekly_brand_voice - LAG(weekly_brand_voice) OVER (
      PARTITION BY brand ORDER BY week_start
    )) AS voice_change_magnitude,
    
    -- Voice consistency score (0-1, higher = more consistent)
    EXP(-2 * ABS(weekly_brand_voice - LAG(weekly_brand_voice) OVER (
      PARTITION BY brand ORDER BY week_start
    ))) AS voice_consistency_score,
    
    -- Messaging strategy shifts
    CASE
      WHEN ABS(promo_messaging_pct - LAG(promo_messaging_pct) OVER (
        PARTITION BY brand ORDER BY week_start
      )) > 0.3 THEN 'PROMOTIONAL_SHIFT'
      WHEN ABS(emotional_messaging_pct - LAG(emotional_messaging_pct) OVER (
        PARTITION BY brand ORDER BY week_start  
      )) > 0.3 THEN 'EMOTIONAL_SHIFT'
      WHEN ABS(upper_funnel_pct - LAG(upper_funnel_pct) OVER (
        PARTITION BY brand ORDER BY week_start
      )) > 0.3 THEN 'FUNNEL_SHIFT'
      ELSE 'CONSISTENT_MESSAGING'
    END AS messaging_shift_type,
    
    -- Brand positioning detection
    CASE
      WHEN weekly_brand_voice > 0.8 THEN 'PREMIUM_POSITIONING'
      WHEN weekly_brand_voice < 0.3 THEN 'VALUE_POSITIONING'
      WHEN promo_messaging_pct > 0.6 THEN 'PRICE_FOCUSED'
      WHEN emotional_messaging_pct > 0.6 THEN 'EMOTION_FOCUSED'
      ELSE 'BALANCED_POSITIONING'
    END AS brand_positioning
    
  FROM brand_voice_weekly
)

SELECT 
  brand,
  week_start,
  weekly_brand_voice,
  voice_consistency_score,
  voice_change_magnitude,
  messaging_shift_type,
  brand_positioning,
  
  -- Rolling consistency metrics
  AVG(voice_consistency_score) OVER (
    PARTITION BY brand ORDER BY week_start 
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ) AS consistency_trend_4week,
  
  -- Brand drift detection (significant positioning changes)
  CASE
    WHEN voice_change_magnitude > 0.2 THEN 'SIGNIFICANT_DRIFT'
    WHEN voice_change_magnitude > 0.1 THEN 'MODERATE_DRIFT'
    ELSE 'STABLE_VOICE'
  END AS voice_drift_level

FROM voice_consistency
ORDER BY brand, week_start;