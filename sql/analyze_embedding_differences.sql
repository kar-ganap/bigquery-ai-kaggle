-- Comprehensive analysis of basic vs enhanced embedding differences
-- Tests specific hypotheses about when enhanced embeddings should perform better

WITH 
-- Calculate similarities for basic embeddings
basic_similarities AS (
  SELECT 
    base.brand as base_brand,
    base.ad_archive_id as base_ad,
    comp.brand as comp_brand,
    comp.ad_archive_id as comp_ad,
    base.page_category as base_category,
    comp.page_category as comp_category,
    base.cta_text as base_cta,
    comp.cta_text as comp_cta,
    (1 - ML.DISTANCE(base.ml_generate_embedding_result, comp.ml_generate_embedding_result, 'COSINE')) as similarity,
    'basic' as method
  FROM `bigquery-ai-kaggle-469620.ads_demo.test_basic_embeddings` base
  CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.test_basic_embeddings` comp
  WHERE base.ad_archive_id != comp.ad_archive_id
),

-- Calculate similarities for enhanced embeddings  
enhanced_similarities AS (
  SELECT 
    base.brand as base_brand,
    base.ad_archive_id as base_ad,
    comp.brand as comp_brand,
    comp.ad_archive_id as comp_ad,
    base.page_category as base_category,
    comp.page_category as comp_category,
    base.cta_text as base_cta,
    comp.cta_text as comp_cta,
    (1 - ML.DISTANCE(base.ml_generate_embedding_result, comp.ml_generate_embedding_result, 'COSINE')) as similarity,
    'enhanced' as method
  FROM `bigquery-ai-kaggle-469620.ads_demo.test_enhanced_embeddings` base
  CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.test_enhanced_embeddings` comp
  WHERE base.ad_archive_id != comp.ad_archive_id
),

-- Combine and pivot for comparison
comparison AS (
  SELECT 
    base_ad,
    comp_ad,
    base_brand,
    comp_brand,
    base_category,
    comp_category,
    base_category = comp_category as same_category,
    base_cta,
    comp_cta,
    base_cta = comp_cta as same_cta,
    MAX(CASE WHEN method = 'basic' THEN similarity END) as basic_similarity,
    MAX(CASE WHEN method = 'enhanced' THEN similarity END) as enhanced_similarity
  FROM (
    SELECT * FROM basic_similarities
    UNION ALL
    SELECT * FROM enhanced_similarities
  )
  GROUP BY base_ad, comp_ad, base_brand, comp_brand, base_category, comp_category, base_cta, comp_cta
)

-- Analyze specific test cases
SELECT 
  CONCAT(base_brand, ' → ', comp_brand) as comparison,
  base_ad,
  comp_ad,
  
  -- Context
  CASE 
    WHEN same_category AND same_cta THEN 'Same Category + CTA'
    WHEN same_category THEN 'Same Category'
    WHEN same_cta THEN 'Same CTA'
    ELSE 'Different'
  END as context_match,
  
  -- Similarities
  ROUND(basic_similarity, 4) as basic_sim,
  ROUND(enhanced_similarity, 4) as enhanced_sim,
  ROUND(enhanced_similarity - basic_similarity, 4) as difference,
  
  -- Percentage change
  ROUND((enhanced_similarity - basic_similarity) / basic_similarity * 100, 2) as pct_change,
  
  -- Analysis
  CASE 
    WHEN ABS(enhanced_similarity - basic_similarity) < 0.01 THEN 'No Change'
    WHEN enhanced_similarity > basic_similarity THEN 'Enhanced Better'
    ELSE 'Basic Better'
  END as winner,
  
  -- Expected vs Actual
  CASE
    -- Chase ads (same brand, similar content) - should be similar in both
    WHEN base_ad IN ('chase_001', 'chase_002') AND comp_ad IN ('chase_001', 'chase_002') 
      THEN 'Similar content, same context'
    
    -- Capital One vs Expedia (similar text, different categories) - enhanced should differentiate
    WHEN (base_ad = 'cap_001' AND comp_ad = 'exp_001') OR (base_ad = 'exp_001' AND comp_ad = 'cap_001')
      THEN 'Similar text, different categories'
    
    -- Nike vs Adidas (same category/CTA, different content) - enhanced should show context
    WHEN (base_ad = 'nike_001' AND comp_ad = 'adidas_001') OR (base_ad = 'adidas_001' AND comp_ad = 'nike_001')
      THEN 'Same category/CTA, different content'
    
    -- Spotify vs YouTube (same category, similar offering)
    WHEN (base_ad = 'spotify_001' AND comp_ad = 'youtube_001') OR (base_ad = 'youtube_001' AND comp_ad = 'spotify_001')
      THEN 'Same category, competing services'
    
    -- Netflix vs Geico (completely different)
    WHEN (base_ad = 'netflix_001' AND comp_ad = 'geico_001') OR (base_ad = 'geico_001' AND comp_ad = 'netflix_001')
      THEN 'Completely different domains'
    
    ELSE 'Other comparison'
  END as test_case_type

FROM comparison
WHERE base_ad < comp_ad  -- Avoid duplicate pairs
ORDER BY ABS(enhanced_similarity - basic_similarity) DESC;