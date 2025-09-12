-- Competitive Influence View
-- Measures how much each ad is influenced by competitor ads that came before it
-- Used by Creative Fatigue Detection to identify original vs derivative content

CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitive_influence` AS
WITH competitor_prior_ads AS (
  -- Find all competitor ads that came before each current ad
  SELECT 
    current_ad.ad_archive_id AS current_ad_archive_id,
    current_ad.brand AS current_brand,
    current_ad.start_timestamp AS current_start,
    current_ad.primary_angle AS current_angle,
    current_ad.funnel AS current_funnel,
    current_ad.promotional_intensity AS current_promo,
    
    prior_ad.ad_archive_id AS prior_ad_archive_id,
    prior_ad.brand AS prior_brand,
    prior_ad.start_timestamp AS prior_start,
    prior_ad.primary_angle AS prior_angle,
    prior_ad.funnel AS prior_funnel,
    prior_ad.promotional_intensity AS prior_promo,
    
    -- Time gap between ads
    DATETIME_DIFF(current_ad.start_timestamp, prior_ad.start_timestamp, DAY) AS days_before,
    
    -- Duration overlap factor (longer running ads have more influence)
    LEAST(30, COALESCE(prior_ad.active_days, 7)) / 30.0 AS duration_weight
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels_mock` current_ad
  CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels_mock` prior_ad
  WHERE current_ad.brand != prior_ad.brand  -- Only look at competitor ads
    AND prior_ad.start_timestamp < current_ad.start_timestamp  -- Prior ads only
    AND DATETIME_DIFF(current_ad.start_timestamp, prior_ad.start_timestamp, DAY) <= 30  -- Within 30 days
),

influence_scoring AS (
  SELECT 
    current_ad_archive_id,
    current_brand,
    prior_ad_archive_id,
    prior_brand,
    days_before,
    duration_weight,
    
    -- Calculate influence score based on similarity and timing
    CASE 
      -- High influence: Same angle, funnel, and high promotional similarity
      WHEN current_angle = prior_angle 
           AND current_funnel = prior_funnel
           AND ABS(current_promo - prior_promo) < 0.2
           AND days_before <= 7
      THEN 0.9 * duration_weight * (1 - days_before/30.0)
      
      -- Medium influence: Same angle or similar promotional intensity
      WHEN (current_angle = prior_angle OR ABS(current_promo - prior_promo) < 0.3)
           AND days_before <= 14
      THEN 0.6 * duration_weight * (1 - days_before/30.0)
      
      -- Low influence: Some similarity
      WHEN current_funnel = prior_funnel
           AND days_before <= 21
      THEN 0.3 * duration_weight * (1 - days_before/30.0)
      
      -- Minimal influence
      ELSE 0.1 * duration_weight * (1 - days_before/30.0)
    END AS influence_score,
    
    -- Confidence in the influence assessment
    CASE 
      WHEN current_angle = prior_angle AND current_funnel = prior_funnel
      THEN 'High Confidence'
      WHEN current_angle = prior_angle OR current_funnel = prior_funnel
      THEN 'Medium Confidence'
      ELSE 'Low Confidence'
    END AS influence_confidence
    
  FROM competitor_prior_ads
)

SELECT 
  current_ad_archive_id,
  current_brand,
  prior_ad_archive_id,
  prior_brand,
  influence_score,
  influence_confidence,
  days_before,
  duration_weight,
  
  -- Influence classification
  CASE 
    WHEN influence_score >= 0.7 THEN 'Strong Influence'
    WHEN influence_score >= 0.4 THEN 'Moderate Influence'
    WHEN influence_score >= 0.2 THEN 'Weak Influence'
    ELSE 'Minimal Influence'
  END AS influence_level
  
FROM influence_scoring
WHERE influence_score > 0.05  -- Filter out negligible influences
ORDER BY current_ad_archive_id, influence_score DESC;