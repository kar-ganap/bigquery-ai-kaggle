-- Competitive Influence Scoring with Duration Weighting
-- Incorporates active days as quality signal - longer-running ads more likely to influence

-- Step 1: Calculate influence scores with duration weighting
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitive_influence` AS

WITH ad_timing AS (
  -- Prepare ads with timing and duration information
  SELECT 
    a.ad_archive_id,
    a.brand,
    a.start_date,
    COALESCE(a.end_date, CURRENT_DATE()) as effective_end_date,
    
    -- Calculate active duration
    DATE_DIFF(COALESCE(a.end_date, CURRENT_DATE()), a.start_date, DAY) as active_days,
    
    -- Quality signal: tanh-based weight (saturates around 14-21 days)
    -- Logic: Ads running 2+ weeks are likely successful
    GREATEST(0.2, TANH(DATE_DIFF(COALESCE(a.end_date, CURRENT_DATE()), a.start_date, DAY) / 7.0)) as duration_quality_weight,
    
    a.funnel,
    a.persona,
    a.page_category,
    e.content_embedding,
    e.context_embedding,
    a.urgency_score,
    a.promotional_intensity
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` a
  JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings` e
    ON a.ad_archive_id = e.ad_archive_id
  WHERE a.start_date IS NOT NULL
    -- Only include ads with minimum 2-day duration
    AND DATE_DIFF(COALESCE(a.end_date, CURRENT_DATE()), a.start_date, DAY) >= 2
),

-- Calculate pairwise influence candidates
influence_candidates AS (
  SELECT 
    current.ad_archive_id as current_ad_id,
    current.brand as current_brand,
    current.start_date as current_start_date,
    current.active_days as current_active_days,
    
    prior.ad_archive_id as potential_influencer_id,
    prior.brand as influencer_brand,
    prior.start_date as influencer_start_date,
    prior.active_days as influencer_active_days,
    prior.duration_quality_weight as influencer_quality_weight,
    
    -- Temporal lag (positive = prior ad started earlier)
    DATE_DIFF(current.start_date, prior.start_date, DAY) as lag_days,
    
    -- Check if influencer was still active when current ad launched
    CASE 
      WHEN current.start_date <= prior.effective_end_date THEN 1.0
      ELSE 0.5  -- Lower weight if influencer had already ended
    END as overlap_factor,
    
    -- Content similarity (messaging)
    (1 - ML.DISTANCE(current.content_embedding, prior.content_embedding, 'COSINE')) as content_similarity,
    
    -- Context similarity (business positioning) 
    (1 - ML.DISTANCE(current.context_embedding, prior.context_embedding, 'COSINE')) as context_similarity,
    
    -- Strategic alignment
    CASE 
      WHEN current.funnel = prior.funnel AND current.persona = prior.persona THEN 1.0
      WHEN current.funnel = prior.funnel OR current.persona = prior.persona THEN 0.5
      ELSE 0.0
    END as strategic_alignment,
    
    -- Category overlap
    CASE 
      WHEN current.page_category = prior.page_category THEN 1.0
      ELSE 0.0
    END as audience_overlap,
    
    -- Promotional similarity
    1 - ABS(current.urgency_score - prior.urgency_score) as urgency_alignment,
    1 - ABS(current.promotional_intensity - prior.promotional_intensity) as promo_alignment
    
  FROM ad_timing current
  CROSS JOIN ad_timing prior
  WHERE 
    -- Different brands only
    current.brand != prior.brand
    -- Prior ad started 1-14 days before current
    AND prior.start_date BETWEEN DATE_SUB(current.start_date, INTERVAL 14 DAY) 
                              AND DATE_SUB(current.start_date, INTERVAL 1 DAY)
    -- Minimum content similarity threshold
    AND (1 - ML.DISTANCE(current.content_embedding, prior.content_embedding, 'COSINE')) >= 0.6
),

-- Calculate composite influence scores with duration weighting
influence_scores AS (
  SELECT 
    *,
    
    -- Recency weight: exponential decay with 7-day half-life
    EXP(-0.693 * lag_days / 7.0) as recency_weight,
    
    -- Composite influence score WITH duration quality weighting
    (
      content_similarity * 0.35 +           -- Message similarity (highest weight)
      strategic_alignment * 0.25 +          -- Same funnel/persona
      audience_overlap * 0.15 +             -- Same category
      (urgency_alignment + promo_alignment) / 2 * 0.15 +  -- Promotional similarity
      context_similarity * 0.10              -- Business context
    ) * EXP(-0.693 * lag_days / 7.0)         -- Recency decay
      * influencer_quality_weight             -- Duration quality signal
      * overlap_factor                        -- Was influencer still active?
    as influence_score,
    
    -- Classification considering duration
    CASE 
      WHEN content_similarity >= 0.9 AND lag_days <= 3 AND influencer_active_days >= 7 
        THEN 'Direct Copy'
      WHEN content_similarity >= 0.85 AND strategic_alignment = 1.0 AND lag_days <= 7 AND influencer_active_days >= 5
        THEN 'Strategic Copy'
      WHEN content_similarity >= 0.75 AND lag_days <= 14 AND influencer_quality_weight >= 0.5
        THEN 'Influenced'
      WHEN influencer_active_days < 3
        THEN 'Unlikely Influence (Short-lived source)'
      ELSE 'Coincidental Similarity'
    END as influence_type,
    
    -- Confidence in influence assessment
    CASE 
      WHEN influencer_active_days >= 14 THEN 'High Confidence'
      WHEN influencer_active_days >= 7 THEN 'Medium Confidence'
      WHEN influencer_active_days >= 3 THEN 'Low Confidence'
      ELSE 'Very Low Confidence'
    END as influence_confidence
    
  FROM influence_candidates
)

SELECT 
  current_ad_id,
  current_brand,
  current_start_date,
  current_active_days,
  potential_influencer_id,
  influencer_brand,
  influencer_start_date,
  influencer_active_days,
  lag_days,
  ROUND(content_similarity, 3) as content_similarity,
  ROUND(strategic_alignment, 2) as strategic_alignment,
  ROUND(audience_overlap, 2) as audience_overlap,
  ROUND(influencer_quality_weight, 3) as influencer_quality_weight,
  ROUND(recency_weight, 3) as recency_weight,
  ROUND(influence_score, 3) as influence_score,
  influence_type,
  influence_confidence,
  
  -- Influence classification with threshold
  CASE 
    WHEN influence_score >= 0.75 THEN 'High Influence'
    WHEN influence_score >= 0.60 THEN 'Moderate Influence'
    WHEN influence_score >= 0.45 THEN 'Low Influence'
    ELSE 'No Clear Influence'
  END as influence_level
  
FROM influence_scores
WHERE influence_score >= 0.3  -- Lower threshold but quality-weighted


-- Step 2: Aggregate with duration-aware metrics
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_brand_influence_patterns` AS

WITH influence_metrics AS (
  SELECT 
    influencer_brand,
    current_brand,
    
    -- Quality-weighted influence metrics
    COUNT(*) as total_interactions,
    COUNT(CASE WHEN influence_confidence IN ('High Confidence', 'Medium Confidence') THEN 1 END) as quality_interactions,
    
    -- Influence given (weighted by duration quality)
    SUM(influence_score) as total_influence_given,
    AVG(influence_score) as avg_influence_given,
    MAX(influence_score) as max_influence_given,
    
    -- Copy detection with confidence
    COUNT(CASE WHEN influence_type IN ('Direct Copy', 'Strategic Copy') 
               AND influence_confidence IN ('High Confidence', 'Medium Confidence') 
          THEN 1 END) as confident_copies_inspired,
    
    -- Average quality of influencing ads
    AVG(influencer_quality_weight) as avg_influencer_quality,
    AVG(influencer_active_days) as avg_influencer_duration
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.v_competitive_influence`
  GROUP BY influencer_brand, current_brand
)

SELECT 
  influencer_brand as brand,
  current_brand as influenced_brand,
  total_interactions,
  quality_interactions,
  ROUND(avg_influence_given, 3) as avg_influence_score,
  ROUND(max_influence_given, 3) as max_influence_score,
  confident_copies_inspired,
  ROUND(avg_influencer_quality, 3) as avg_ad_quality,
  ROUND(avg_influencer_duration, 1) as avg_ad_duration_days,
  
  -- Strategic assessment
  CASE 
    WHEN avg_influence_given >= 0.7 AND avg_influencer_duration >= 14 
      THEN 'Proven Trendsetter'
    WHEN avg_influence_given >= 0.6 AND quality_interactions >= 3
      THEN 'Market Influencer'
    WHEN avg_influence_given >= 0.5
      THEN 'Occasional Leader'
    WHEN avg_influencer_duration < 5
      THEN 'Test-Heavy Strategy'
    ELSE 'Independent Player'
  END as influence_role,
  
  -- Recommendation
  CASE 
    WHEN confident_copies_inspired >= 3
      THEN 'Monitor closely - competitors copying your successful ads'
    WHEN avg_influence_given >= 0.6 AND avg_influencer_duration >= 10
      THEN 'Your long-running ads are setting market trends'
    WHEN avg_influencer_duration < 5
      THEN 'Consider longer campaign durations to establish market presence'
    ELSE 'Maintain current strategy'
  END as strategic_insight
  
FROM influence_metrics
ORDER BY brand, avg_influence_given DESC;