-- Dual-vector similarity views for nuanced competitive analysis
-- Separates content similarity (messaging) from context similarity (business)

CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_dual_vector_similarity` AS

WITH ad_comparisons AS (
  SELECT 
    base.ad_archive_id as base_ad_id,
    base.brand as base_brand,
    base.page_category as base_category,
    base.content_text as base_content,
    base.context_text as base_context,
    
    comp.ad_archive_id as similar_ad_id,
    comp.brand as similar_brand,
    comp.page_category as similar_category,
    comp.content_text as similar_content,
    comp.context_text as similar_context,
    
    -- Content similarity (messaging, value props, offers)
    (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) as content_similarity,
    
    -- Context similarity (business identity, industry, model)
    (1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE')) as context_similarity,
    
    -- Weighted combinations for different use cases
    (0.7 * (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) + 
     0.3 * (1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE'))) as content_weighted_similarity,
     
    (0.3 * (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) + 
     0.7 * (1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE'))) as context_weighted_similarity,
    
    -- Equal weighting
    (0.5 * (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) + 
     0.5 * (1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE'))) as balanced_similarity,
    
    -- Analysis flags
    base.brand != comp.brand as is_cross_brand,
    base.page_category = comp.page_category as same_industry,
    
    -- Quality metrics
    base.text_richness_score as base_quality,
    comp.text_richness_score as similar_quality
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_dual_vector` base
  CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_dual_vector` comp
  WHERE base.ad_archive_id != comp.ad_archive_id  -- Exclude self-comparison
)

SELECT 
  base_ad_id,
  similar_ad_id,
  base_brand,
  similar_brand,
  base_category,
  similar_category,
  
  -- Similarity scores
  ROUND(content_similarity, 4) as content_similarity,
  ROUND(context_similarity, 4) as context_similarity,
  ROUND(content_weighted_similarity, 4) as content_weighted_similarity,
  ROUND(context_weighted_similarity, 4) as context_weighted_similarity,
  ROUND(balanced_similarity, 4) as balanced_similarity,
  
  -- Competitive analysis categories
  CASE 
    WHEN content_similarity >= 0.8 AND context_similarity >= 0.8 THEN 'Direct Competitor'
    WHEN content_similarity >= 0.8 AND context_similarity < 0.6 THEN 'Message Overlap'
    WHEN content_similarity < 0.6 AND context_similarity >= 0.8 THEN 'Industry Peer'
    WHEN content_similarity >= 0.7 AND context_similarity >= 0.6 THEN 'Competitive Threat'
    ELSE 'Distinct'
  END as competitive_relationship,
  
  -- Strategic insights
  CASE
    WHEN content_similarity > context_similarity + 0.2 THEN 'Similar messaging, different context'
    WHEN context_similarity > content_similarity + 0.2 THEN 'Same industry, different messaging'
    WHEN ABS(content_similarity - context_similarity) <= 0.2 THEN 'Balanced similarity'
    ELSE 'Other'
  END as similarity_pattern,
  
  -- Flags
  is_cross_brand,
  same_industry,
  
  -- Content previews (truncated for readability)
  LEFT(base_content, 100) as base_content_preview,
  LEFT(similar_content, 100) as similar_content_preview,
  base_context,
  similar_context,
  
  -- Quality scores
  ROUND((base_quality + similar_quality) / 2, 3) as avg_quality

FROM ad_comparisons
WHERE content_similarity >= 0.5 OR context_similarity >= 0.5  -- Filter for meaningful similarities
ORDER BY base_ad_id, content_similarity DESC;


-- Strategic insights view focused on competitive intelligence
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitive_intelligence_dual` AS

WITH brand_comparisons AS (
  SELECT 
    base_brand,
    similar_brand,
    COUNT(*) as total_ad_pairs,
    
    -- Content analysis
    AVG(content_similarity) as avg_content_similarity,
    MAX(content_similarity) as max_content_similarity,
    COUNT(CASE WHEN content_similarity >= 0.8 THEN 1 END) as high_content_similarity_count,
    
    -- Context analysis  
    AVG(context_similarity) as avg_context_similarity,
    MAX(context_similarity) as max_context_similarity,
    COUNT(CASE WHEN context_similarity >= 0.8 THEN 1 END) as high_context_similarity_count,
    
    -- Relationship patterns
    COUNT(CASE WHEN competitive_relationship = 'Direct Competitor' THEN 1 END) as direct_competitor_pairs,
    COUNT(CASE WHEN competitive_relationship = 'Message Overlap' THEN 1 END) as message_overlap_pairs,
    COUNT(CASE WHEN competitive_relationship = 'Industry Peer' THEN 1 END) as industry_peer_pairs,
    COUNT(CASE WHEN competitive_relationship = 'Competitive Threat' THEN 1 END) as competitive_threat_pairs,
    
    -- Quality metrics
    AVG(avg_quality) as avg_content_quality
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.v_dual_vector_similarity`
  WHERE is_cross_brand = TRUE  -- Focus on cross-brand comparisons
  GROUP BY base_brand, similar_brand
)

SELECT 
  base_brand,
  similar_brand,
  total_ad_pairs,
  
  -- Content competitive assessment
  ROUND(avg_content_similarity, 3) as avg_content_similarity,
  ROUND(max_content_similarity, 3) as max_content_similarity,
  high_content_similarity_count,
  ROUND(high_content_similarity_count / total_ad_pairs * 100, 1) as pct_high_content_similarity,
  
  -- Context competitive assessment
  ROUND(avg_context_similarity, 3) as avg_context_similarity,
  ROUND(max_context_similarity, 3) as max_context_similarity,  
  high_context_similarity_count,
  ROUND(high_context_similarity_count / total_ad_pairs * 100, 1) as pct_high_context_similarity,
  
  -- Strategic insights
  CASE 
    WHEN avg_content_similarity >= 0.8 AND avg_context_similarity >= 0.8 THEN 'Direct Competition'
    WHEN avg_content_similarity >= 0.8 AND avg_context_similarity < 0.6 THEN 'Message Competition (Cross-Industry)'
    WHEN avg_content_similarity < 0.6 AND avg_context_similarity >= 0.8 THEN 'Industry Competition (Different Messaging)'
    WHEN avg_content_similarity >= 0.7 OR avg_context_similarity >= 0.7 THEN 'Moderate Competition'
    ELSE 'Low Competition'
  END as overall_competitive_assessment,
  
  -- Relationship breakdown
  direct_competitor_pairs,
  message_overlap_pairs,
  industry_peer_pairs,
  competitive_threat_pairs,
  
  -- Content quality
  ROUND(avg_content_quality, 3) as avg_content_quality_score

FROM brand_comparisons
ORDER BY 
  GREATEST(avg_content_similarity, avg_context_similarity) DESC,
  avg_content_similarity DESC;