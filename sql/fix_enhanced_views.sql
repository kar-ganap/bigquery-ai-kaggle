-- Fixed enhanced competitor insights view that works with actual schema
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitor_insights_enhanced` AS

WITH similarity_stats AS (
  SELECT 
    base_brand,
    similar_brand,
    COUNT(*) as total_similar_pairs,
    AVG(similarity_score) as avg_similarity,
    MAX(similarity_score) as max_similarity,
    COUNT(CASE WHEN similarity_level IN ('High', 'Very High') THEN 1 END) as high_similarity_count,
    
    -- Available analysis by content components only
    AVG(CASE WHEN base_has_title AND similar_has_title THEN similarity_score END) as title_similarity_avg,
    AVG(CASE WHEN base_has_body AND similar_has_body THEN similarity_score END) as body_similarity_avg,
    -- cta_similarity_avg not available due to missing cta_text in embeddings table
    
    -- Category and CTA analysis not available due to missing columns in embeddings table
    -- Set defaults for compatibility
    0.0 as same_category_similarity,
    0.0 as same_cta_similarity, 
    0 as same_category_count,
    0 as same_cta_count,
    
    -- Quality analysis not available due to missing quality scores
    0.5 as avg_content_quality
    
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
  ROUND(high_similarity_count / NULLIF(total_similar_pairs, 0) * 100, 1) as pct_high_similarity,
  
  -- Enhanced strategic insights
  CASE 
    WHEN avg_similarity >= 0.8 THEN 'Very High Overlap - Direct competition likely'
    WHEN avg_similarity >= 0.7 THEN 'High Overlap - Similar messaging strategies' 
    WHEN avg_similarity >= 0.6 THEN 'Medium Overlap - Some common themes'
    ELSE 'Low Overlap - Distinct positioning'
  END as competitive_assessment,
  
  -- Available component analysis
  ROUND(title_similarity_avg, 3) as avg_title_similarity,
  ROUND(body_similarity_avg, 3) as avg_body_similarity,
  -- CTA analysis not available 
  NULL as avg_cta_similarity,
  
  -- Category and CTA insights (defaults due to missing data)
  ROUND(same_category_similarity, 3) as same_category_similarity_avg,
  same_category_count,
  ROUND(same_cta_similarity, 3) as same_cta_similarity_avg,
  same_cta_count,
  ROUND(avg_content_quality, 3) as avg_content_quality_score

FROM similarity_stats
WHERE total_similar_pairs > 0  -- Only include brands with actual comparisons
ORDER BY avg_similarity DESC;