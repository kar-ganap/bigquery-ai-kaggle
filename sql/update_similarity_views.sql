-- Update similarity search views to work with enhanced embeddings table

CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_ad_similarity_search` AS

WITH ad_pairs AS (
  SELECT 
    base.ad_archive_id as base_ad_id,
    base.brand as base_brand,
    base.creative_text as base_creative_text,
    '' as base_category,  -- page_category not available in embeddings table
    '' as base_cta,       -- cta_text not available in embeddings table
    
    comp.ad_archive_id as similar_ad_id,
    comp.brand as similar_brand,
    comp.creative_text as similar_creative_text,
    '' as similar_category,  -- page_category not available in embeddings table
    '' as similar_cta,       -- cta_text not available in embeddings table
    
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
    
    -- Cross-competitor and category analysis
    base.brand != comp.brand as is_cross_competitor,
    base.page_category = comp.page_category as same_category,
    base.cta_text = comp.cta_text as same_cta,
    
    -- Enhanced quality indicators
    base.has_title as base_has_title,
    base.has_body as base_has_body,
    CASE WHEN base.cta_text IS NOT NULL AND LENGTH(base.cta_text) > 0 THEN TRUE ELSE FALSE END as base_has_cta,
    CASE WHEN base.page_category IS NOT NULL AND LENGTH(base.page_category) > 0 THEN TRUE ELSE FALSE END as base_has_category,
    comp.has_title as similar_has_title,
    comp.has_body as similar_has_body,
    CASE WHEN comp.cta_text IS NOT NULL AND LENGTH(comp.cta_text) > 0 THEN TRUE ELSE FALSE END as similar_has_cta,
    CASE WHEN comp.page_category IS NOT NULL AND LENGTH(comp.page_category) > 0 THEN TRUE ELSE FALSE END as similar_has_category,
    
    -- Text quality scores
    base.text_richness_score as base_quality,
    comp.text_richness_score as similar_quality
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings` base
  CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings` comp
  WHERE base.ad_archive_id != comp.ad_archive_id  -- Exclude self-comparison
)

SELECT *
FROM ad_pairs
WHERE similarity_score >= 0.5  -- Only show meaningful similarities
ORDER BY base_ad_id, similarity_score DESC;


-- Enhanced competitor insights with category and CTA analysis
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitor_insights_enhanced` AS

WITH similarity_stats AS (
  SELECT 
    base_brand,
    similar_brand,
    COUNT(*) as total_similar_pairs,
    AVG(similarity_score) as avg_similarity,
    MAX(similarity_score) as max_similarity,
    COUNT(CASE WHEN similarity_level IN ('High', 'Very High') THEN 1 END) as high_similarity_count,
    
    -- Enhanced analysis by content components
    AVG(CASE WHEN base_has_title AND similar_has_title THEN similarity_score END) as title_similarity_avg,
    AVG(CASE WHEN base_has_body AND similar_has_body THEN similarity_score END) as body_similarity_avg,
    AVG(CASE WHEN base_has_cta AND similar_has_cta THEN similarity_score END) as cta_similarity_avg,
    
    -- Category and CTA overlap analysis
    AVG(CASE WHEN same_category THEN similarity_score END) as same_category_similarity,
    AVG(CASE WHEN same_cta THEN similarity_score END) as same_cta_similarity,
    COUNT(CASE WHEN same_category THEN 1 END) as same_category_count,
    COUNT(CASE WHEN same_cta THEN 1 END) as same_cta_count,
    
    -- Quality analysis
    AVG((base_quality + similar_quality) / 2) as avg_content_quality
    
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
  
  -- Enhanced strategic insights
  CASE 
    WHEN avg_similarity >= 0.8 THEN 'Very High Overlap - Direct competition likely'
    WHEN avg_similarity >= 0.7 THEN 'High Overlap - Similar messaging strategies' 
    WHEN avg_similarity >= 0.6 THEN 'Medium Overlap - Some common themes'
    ELSE 'Low Overlap - Distinct positioning'
  END as competitive_assessment,
  
  -- Component analysis
  ROUND(title_similarity_avg, 3) as avg_title_similarity,
  ROUND(body_similarity_avg, 3) as avg_body_similarity,
  ROUND(cta_similarity_avg, 3) as avg_cta_similarity,
  
  -- Category and CTA insights
  ROUND(same_category_similarity, 3) as same_category_similarity_avg,
  same_category_count,
  ROUND(same_cta_similarity, 3) as same_cta_similarity_avg,
  same_cta_count,
  ROUND(avg_content_quality, 3) as avg_content_quality_score

FROM similarity_stats
ORDER BY avg_similarity DESC;