-- Create semantic similarity search functions for competitive intelligence
-- These functions enable "find similar ads" capabilities

-- Function 1: Find similar ads for a given ad
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_ad_similarity_search` AS

WITH ad_pairs AS (
  SELECT 
    base.ad_archive_id as base_ad_id,
    base.brand as base_brand,
    base.creative_text as base_creative_text,
    
    comp.ad_archive_id as similar_ad_id,
    comp.brand as similar_brand,
    comp.creative_text as similar_creative_text,
    
    -- Calculate semantic similarity
    (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) as similarity_score,
    ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE') as cosine_distance,
    
    -- Categorize similarity level
    CASE 
      WHEN (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) >= 0.9 THEN 'Very High'
      WHEN (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) >= 0.8 THEN 'High' 
      WHEN (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) >= 0.7 THEN 'Medium'
      WHEN (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) >= 0.6 THEN 'Low'
      ELSE 'Very Low'
    END as similarity_level,
    
    -- Cross-competitor analysis
    base.brand != comp.brand as is_cross_competitor,
    
    -- Content quality indicators
    base.has_title as base_has_title,
    base.has_body as base_has_body,
    base.has_cta as base_has_cta,
    comp.has_title as similar_has_title,
    comp.has_body as similar_has_body,
    comp.has_cta as similar_has_cta
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings` base
  CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings` comp
  WHERE base.ad_archive_id != comp.ad_archive_id  -- Exclude self-comparison
)

SELECT *
FROM ad_pairs
WHERE similarity_score >= 0.5  -- Only show meaningful similarities
ORDER BY base_ad_id, similarity_score DESC;


-- Function 2: Top competitor insights
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitor_insights` AS

WITH similarity_stats AS (
  SELECT 
    base_brand,
    similar_brand,
    COUNT(*) as total_similar_pairs,
    AVG(similarity_score) as avg_similarity,
    MAX(similarity_score) as max_similarity,
    COUNT(CASE WHEN similarity_level IN ('High', 'Very High') THEN 1 END) as high_similarity_count,
    
    -- Message overlap analysis
    AVG(CASE WHEN base_has_title AND similar_has_title THEN similarity_score END) as title_similarity_avg,
    AVG(CASE WHEN base_has_body AND similar_has_body THEN similarity_score END) as body_similarity_avg,
    AVG(CASE WHEN base_has_cta AND similar_has_cta THEN similarity_score END) as cta_similarity_avg
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.v_ad_similarity_search`
  WHERE is_cross_competitor = TRUE  -- Only cross-competitor comparisons
  GROUP BY base_brand, similar_brand
)

SELECT 
  base_brand,
  similar_brand,
  total_similar_pairs,
  ROUND(avg_similarity, 3) as avg_similarity_score,
  ROUND(max_similarity, 3) as max_similarity_score,
  high_similarity_count,
  ROUND(high_similarity_count / total_similar_pairs * 100, 1) as pct_high_similarity,
  
  -- Strategic insights
  CASE 
    WHEN avg_similarity >= 0.8 THEN 'Very High Overlap - Potential direct competition'
    WHEN avg_similarity >= 0.7 THEN 'High Overlap - Similar messaging strategies' 
    WHEN avg_similarity >= 0.6 THEN 'Medium Overlap - Some common themes'
    ELSE 'Low Overlap - Distinct positioning'
  END as competitive_assessment,
  
  ROUND(title_similarity_avg, 3) as avg_title_similarity,
  ROUND(body_similarity_avg, 3) as avg_body_similarity,
  ROUND(cta_similarity_avg, 3) as avg_cta_similarity

FROM similarity_stats
ORDER BY avg_similarity DESC;