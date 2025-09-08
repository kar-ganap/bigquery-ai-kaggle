-- Competitive Influence Scoring with Lagged Autocorrelation
-- Detects bidirectional influence between brands using temporal and similarity analysis

-- Step 1: Calculate influence scores for ad pairs
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitive_influence` AS

WITH ad_timing AS (
  -- Prepare ads with timing information
  SELECT 
    a.ad_archive_id,
    a.brand,
    a.start_date,
    a.end_date,
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
),

-- Calculate pairwise influence candidates
influence_candidates AS (
  SELECT 
    current.ad_archive_id as current_ad_id,
    current.brand as current_brand,
    current.start_date as current_start_date,
    prior.ad_archive_id as potential_influencer_id,
    prior.brand as influencer_brand,
    prior.start_date as influencer_start_date,
    
    -- Temporal lag (positive = prior ad started earlier)
    DATE_DIFF(current.start_date, prior.start_date, DAY) as lag_days,
    
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

-- Calculate composite influence scores
influence_scores AS (
  SELECT 
    *,
    
    -- Recency weight: exponential decay with 7-day half-life
    EXP(-0.693 * lag_days / 7.0) as recency_weight,
    
    -- Composite influence score
    (
      content_similarity * 0.35 +           -- Message similarity (highest weight)
      strategic_alignment * 0.25 +          -- Same funnel/persona
      audience_overlap * 0.15 +             -- Same category
      (urgency_alignment + promo_alignment) / 2 * 0.15 +  -- Promotional similarity
      context_similarity * 0.10              -- Business context
    ) * EXP(-0.693 * lag_days / 7.0) as influence_score,
    
    -- Classification
    CASE 
      WHEN content_similarity >= 0.9 AND lag_days <= 3 THEN 'Direct Copy'
      WHEN content_similarity >= 0.85 AND strategic_alignment = 1.0 AND lag_days <= 7 THEN 'Strategic Copy'
      WHEN content_similarity >= 0.75 AND lag_days <= 14 THEN 'Influenced'
      ELSE 'Coincidental Similarity'
    END as influence_type
    
  FROM influence_candidates
)

SELECT 
  current_ad_id,
  current_brand,
  current_start_date,
  potential_influencer_id,
  influencer_brand,
  influencer_start_date,
  lag_days,
  ROUND(content_similarity, 3) as content_similarity,
  ROUND(strategic_alignment, 2) as strategic_alignment,
  ROUND(audience_overlap, 2) as audience_overlap,
  ROUND(recency_weight, 3) as recency_weight,
  ROUND(influence_score, 3) as influence_score,
  influence_type,
  
  -- Influence classification with threshold
  CASE 
    WHEN influence_score >= 0.75 THEN 'High Influence'
    WHEN influence_score >= 0.60 THEN 'Moderate Influence'
    WHEN influence_score >= 0.45 THEN 'Low Influence'
    ELSE 'No Clear Influence'
  END as influence_level
  
FROM influence_scores
WHERE influence_score >= 0.45;  -- Minimum threshold for meaningful influence


-- Step 2: Aggregate bidirectional influence patterns
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_brand_influence_summary` AS

WITH influence_given AS (
  -- How much each brand influences others
  SELECT 
    influencer_brand as brand,
    current_brand as influenced_brand,
    COUNT(*) as influence_count,
    AVG(influence_score) as avg_influence_given,
    MAX(influence_score) as max_influence_given,
    COUNT(CASE WHEN influence_type IN ('Direct Copy', 'Strategic Copy') THEN 1 END) as copies_inspired
  FROM `bigquery-ai-kaggle-469620.ads_demo.v_competitive_influence`
  GROUP BY influencer_brand, current_brand
),

influence_received AS (
  -- How much each brand is influenced by others
  SELECT 
    current_brand as brand,
    influencer_brand as influencing_brand,
    COUNT(*) as influenced_count,
    AVG(influence_score) as avg_influence_received,
    MAX(influence_score) as max_influence_received,
    COUNT(CASE WHEN influence_type IN ('Direct Copy', 'Strategic Copy') THEN 1 END) as times_copied
  FROM `bigquery-ai-kaggle-469620.ads_demo.v_competitive_influence`
  GROUP BY current_brand, influencer_brand
)

SELECT 
  COALESCE(g.brand, r.brand) as brand,
  COALESCE(g.influenced_brand, r.influencing_brand) as other_brand,
  
  -- Influence given (this brand → other)
  COALESCE(g.influence_count, 0) as influence_given_count,
  ROUND(COALESCE(g.avg_influence_given, 0), 3) as avg_influence_given,
  COALESCE(g.copies_inspired, 0) as copies_inspired,
  
  -- Influence received (other → this brand)
  COALESCE(r.influenced_count, 0) as influence_received_count,
  ROUND(COALESCE(r.avg_influence_received, 0), 3) as avg_influence_received,
  COALESCE(r.times_copied, 0) as times_copied_from,
  
  -- Net influence (positive = influencer, negative = follower)
  ROUND(COALESCE(g.avg_influence_given, 0) - COALESCE(r.avg_influence_received, 0), 3) as net_influence,
  
  -- Role classification
  CASE 
    WHEN COALESCE(g.avg_influence_given, 0) > COALESCE(r.avg_influence_received, 0) * 1.5 THEN 'Market Leader'
    WHEN COALESCE(r.avg_influence_received, 0) > COALESCE(g.avg_influence_given, 0) * 1.5 THEN 'Fast Follower'
    ELSE 'Balanced Player'
  END as competitive_role

FROM influence_given g
FULL OUTER JOIN influence_received r 
  ON g.brand = r.brand AND g.influenced_brand = r.influencing_brand
ORDER BY brand, net_influence DESC;