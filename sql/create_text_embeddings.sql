-- Create Text Embeddings for Similarity Detection
-- Adds semantic similarity capabilities to our existing system

-- Step 1: Create the text embedding model
CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`
OPTIONS(
  MODEL_TYPE='TEXTEMBEDDING_GECKO@001'  -- Latest text embedding model
) AS
SELECT 1 AS dummy;  -- Dummy data for model creation

-- Step 2: Create ads table with embeddings
CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_with_embeddings` AS
SELECT 
  *,
  -- Generate embeddings for combined creative text and title
  ML.GENERATE_EMBEDDING(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
    COALESCE(creative_text, '') || 
    CASE WHEN title IS NOT NULL THEN ' ' || title ELSE '' END
  ) AS content_embedding

FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
WHERE 
  (creative_text IS NOT NULL OR title IS NOT NULL)
  AND LENGTH(COALESCE(creative_text, '') || COALESCE(title, '')) >= 10;

-- Step 3: Create enhanced similarity spike detection view
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_similarity_spikes_enhanced` AS
WITH pairwise_similarity AS (
  SELECT 
    a1.ad_archive_id AS ad_id_a,
    a1.brand AS brand_a,
    a1.creative_text AS text_a,
    a1.start_date AS start_date_a,
    
    a2.ad_archive_id AS ad_id_b,
    a2.brand AS brand_b,
    a2.creative_text AS text_b,
    a2.start_date AS start_date_b,
    
    -- Calculate semantic similarity using embeddings
    ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) AS semantic_similarity,
    
    -- Time difference analysis
    DATE_DIFF(a2.start_date, a1.start_date, DAY) AS days_difference,
    
    -- Strategic context similarity
    CASE 
      WHEN a1.funnel = a2.funnel AND a1.persona = a2.persona THEN 0.3
      WHEN a1.funnel = a2.funnel THEN 0.2
      WHEN a1.persona = a2.persona THEN 0.1
      ELSE 0.0
    END AS strategic_context_bonus
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_embeddings` a1
  CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_with_embeddings` a2
  WHERE a1.brand != a2.brand  -- Only cross-brand comparisons
    AND a1.ad_archive_id != a2.ad_archive_id
    AND a2.start_date > a1.start_date  -- Only future ads
    AND DATE_DIFF(a2.start_date, a1.start_date, DAY) BETWEEN 1 AND 14  -- Within 2 weeks
),

similarity_spikes AS (
  SELECT 
    brand_a,
    brand_b,
    days_difference,
    COUNT(*) AS similar_pairs,
    AVG(semantic_similarity) AS avg_similarity,
    MAX(semantic_similarity) AS max_similarity,
    AVG(strategic_context_bonus) AS avg_strategic_alignment,
    
    -- Enhanced spike classification
    CASE 
      WHEN MAX(semantic_similarity) >= 0.85 AND COUNT(*) >= 2 THEN 'HIGH_SEMANTIC_SPIKE'
      WHEN MAX(semantic_similarity) >= 0.75 AND COUNT(*) >= 3 THEN 'MODERATE_SEMANTIC_SPIKE'
      WHEN MAX(semantic_similarity) >= 0.65 AND COUNT(*) >= 5 THEN 'PATTERN_SIMILARITY_SPIKE'
      ELSE 'LOW_SIMILARITY'
    END AS spike_classification,
    
    -- Confidence scoring
    CASE 
      WHEN MAX(semantic_similarity) >= 0.85 AND AVG(strategic_context_bonus) > 0.1 THEN 'HIGH_CONFIDENCE'
      WHEN MAX(semantic_similarity) >= 0.75 THEN 'MEDIUM_CONFIDENCE'  
      ELSE 'LOW_CONFIDENCE'
    END AS confidence_level
    
  FROM pairwise_similarity
  WHERE semantic_similarity >= 0.6  -- Focus on meaningful similarities
  GROUP BY brand_a, brand_b, days_difference
)

SELECT 
  spike_classification,
  confidence_level,
  COUNT(*) AS spike_occurrences,
  AVG(avg_similarity) AS overall_avg_similarity,
  AVG(days_difference) AS avg_days_between_similar_content,
  SUM(similar_pairs) AS total_similar_ad_pairs,
  
  -- Top brand pairs for each spike type
  STRING_AGG(
    CONCAT(brand_a, ' → ', brand_b, ' (', CAST(ROUND(avg_similarity, 3) AS STRING), ')') 
    ORDER BY avg_similarity DESC 
    LIMIT 3
  ) AS top_brand_pairs

FROM similarity_spikes
WHERE spike_classification != 'LOW_SIMILARITY'
GROUP BY spike_classification, confidence_level
ORDER BY 
  CASE spike_classification
    WHEN 'HIGH_SEMANTIC_SPIKE' THEN 1
    WHEN 'MODERATE_SEMANTIC_SPIKE' THEN 2  
    WHEN 'PATTERN_SIMILARITY_SPIKE' THEN 3
    ELSE 4
  END,
  overall_avg_similarity DESC;

-- Step 4: Create enhanced complex copying detection view  
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_complex_copying_detection` AS
WITH semantic_copying_analysis AS (
  SELECT 
    a1.ad_archive_id AS original_ad_id,
    a1.brand AS original_brand,
    a1.creative_text AS original_text,
    a1.start_date AS original_date,
    
    a2.ad_archive_id AS follower_ad_id,
    a2.brand AS follower_brand, 
    a2.creative_text AS follower_text,
    a2.start_date AS follower_date,
    
    -- Semantic similarity score
    ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) AS semantic_similarity,
    
    -- Time lag
    DATE_DIFF(a2.start_date, a1.start_date, DAY) AS copy_lag_days,
    
    -- Enhanced copying type classification
    CASE 
      -- High semantic similarity with promotional keywords
      WHEN ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) >= 0.8
           AND (REGEXP_CONTAINS(UPPER(a1.creative_text), r'SALE|DISCOUNT|OFF|SAVE') 
                OR REGEXP_CONTAINS(UPPER(a2.creative_text), r'SALE|DISCOUNT|OFF|SAVE'))
      THEN 'SEMANTIC_PROMOTIONAL_COPYING'
      
      -- High semantic similarity with urgency language
      WHEN ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) >= 0.8
           AND (REGEXP_CONTAINS(UPPER(a1.creative_text), r'LIMITED|HURRY|NOW|TODAY')
                OR REGEXP_CONTAINS(UPPER(a2.creative_text), r'LIMITED|HURRY|NOW|TODAY'))
      THEN 'SEMANTIC_URGENCY_COPYING'
      
      -- Moderate semantic similarity with strategic alignment
      WHEN ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) >= 0.7
           AND a1.funnel = a2.funnel AND a1.persona = a2.persona
      THEN 'STRATEGIC_MESSAGE_COPYING'
      
      -- Lower semantic similarity but conceptual overlap
      WHEN ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) >= 0.65
      THEN 'CONCEPTUAL_INSPIRATION'
      
      ELSE 'NO_SEMANTIC_COPYING'
    END AS copying_type,
    
    -- Confidence based on similarity + timing + strategic context
    (ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) * 0.6 +
     (1 - DATE_DIFF(a2.start_date, a1.start_date, DAY) / 14.0) * 0.3 +
     CASE WHEN a1.funnel = a2.funnel AND a1.persona = a2.persona THEN 0.1 ELSE 0.0 END
    ) AS copying_confidence_score
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_embeddings` a1
  CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_with_embeddings` a2
  WHERE a1.brand != a2.brand
    AND a2.start_date > a1.start_date
    AND DATE_DIFF(a2.start_date, a1.start_date, DAY) BETWEEN 1 AND 21
    AND ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) >= 0.6
)

SELECT 
  copying_type,
  COUNT(*) AS copying_instances,
  AVG(semantic_similarity) AS avg_semantic_similarity,
  AVG(copy_lag_days) AS avg_copy_lag_days,
  AVG(copying_confidence_score) AS avg_confidence,
  
  -- Severity assessment
  CASE 
    WHEN COUNT(*) >= 15 AND AVG(copying_confidence_score) >= 0.8 THEN 'SYSTEMATIC_SEMANTIC_COPYING'
    WHEN COUNT(*) >= 8 AND AVG(copying_confidence_score) >= 0.7 THEN 'REGULAR_SEMANTIC_COPYING'
    WHEN COUNT(*) >= 3 THEN 'OCCASIONAL_SEMANTIC_COPYING'
    ELSE 'ISOLATED_SEMANTIC_COPYING'
  END AS copying_severity,
  
  -- Top examples
  STRING_AGG(
    CONCAT(original_brand, ' → ', follower_brand, ' (', CAST(ROUND(semantic_similarity, 3) AS STRING), ')')
    ORDER BY semantic_similarity DESC
    LIMIT 2
  ) AS top_copying_examples

FROM semantic_copying_analysis  
WHERE copying_type != 'NO_SEMANTIC_COPYING'
GROUP BY copying_type
ORDER BY avg_semantic_similarity DESC;