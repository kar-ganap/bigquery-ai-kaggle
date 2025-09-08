-- Compare embedding quality: basic vs enhanced text content
-- This will show if additional fields improve semantic similarity accuracy

WITH 
-- Get basic embeddings (old approach)
basic_embeddings AS (
  SELECT 
    brand,
    ad_archive_id,
    creative_text as basic_content,
    content_embedding as basic_embedding,
    'basic' as embedding_type
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings`
),

-- Get enhanced embeddings (new approach) 
enhanced_embeddings AS (
  SELECT 
    brand,
    ad_archive_id,
    basic_creative_text as basic_content,
    content as enhanced_content,
    ml_generate_embedding_result as enhanced_embedding,
    text_richness_score,
    'enhanced' as embedding_type
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_enhanced`
),

-- Calculate similarities for basic embeddings
basic_similarities AS (
  SELECT 
    base.brand as base_brand,
    base.ad_archive_id as base_ad_id,
    comp.brand as similar_brand,
    comp.ad_archive_id as similar_ad_id,
    (1 - ML.DISTANCE(base.basic_embedding, comp.basic_embedding, 'COSINE')) as similarity_score,
    'basic' as method,
    LEFT(base.basic_content, 60) as base_preview,
    LEFT(comp.basic_content, 60) as similar_preview
  FROM basic_embeddings base
  CROSS JOIN basic_embeddings comp
  WHERE base.ad_archive_id != comp.ad_archive_id
),

-- Calculate similarities for enhanced embeddings
enhanced_similarities AS (
  SELECT 
    base.brand as base_brand,
    base.ad_archive_id as base_ad_id,
    comp.brand as similar_brand, 
    comp.ad_archive_id as similar_ad_id,
    (1 - ML.DISTANCE(base.enhanced_embedding, comp.enhanced_embedding, 'COSINE')) as similarity_score,
    'enhanced' as method,
    LEFT(base.enhanced_content, 60) as base_preview,
    LEFT(comp.enhanced_content, 60) as similar_preview
  FROM enhanced_embeddings base
  CROSS JOIN enhanced_embeddings comp
  WHERE base.ad_archive_id != comp.ad_archive_id
),

-- Combine and compare
all_similarities AS (
  SELECT * FROM basic_similarities
  UNION ALL
  SELECT * FROM enhanced_similarities
)

-- Show side-by-side comparison for key ad pairs
SELECT 
  base_brand,
  similar_brand,
  base_ad_id,
  similar_ad_id,
  
  -- Basic vs enhanced similarity scores
  MAX(CASE WHEN method = 'basic' THEN similarity_score END) as basic_similarity,
  MAX(CASE WHEN method = 'enhanced' THEN similarity_score END) as enhanced_similarity,
  
  -- Improvement calculation
  ROUND(
    MAX(CASE WHEN method = 'enhanced' THEN similarity_score END) - 
    MAX(CASE WHEN method = 'basic' THEN similarity_score END), 
    4
  ) as improvement,
  
  -- Percentage improvement
  ROUND(
    (MAX(CASE WHEN method = 'enhanced' THEN similarity_score END) - 
     MAX(CASE WHEN method = 'basic' THEN similarity_score END)) /
    MAX(CASE WHEN method = 'basic' THEN similarity_score END) * 100,
    1
  ) as pct_improvement,
  
  -- Content previews
  MAX(CASE WHEN method = 'basic' THEN base_preview END) as content_preview

FROM all_similarities
WHERE base_brand != similar_brand  -- Focus on cross-competitor comparisons
GROUP BY base_brand, similar_brand, base_ad_id, similar_ad_id
ORDER BY ABS(improvement) DESC;