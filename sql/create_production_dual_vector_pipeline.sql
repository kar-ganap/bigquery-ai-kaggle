-- Production dual-vector embedding pipeline
-- Processes real ingested ads data and generates content + context embeddings

-- First, update the raw ads table schema to include enhanced fields
-- (This would be done during table creation, but showing the schema here)
/*
ALTER TABLE `bigquery-ai-kaggle-469620.ads_demo.fb_ads_raw`
ADD COLUMN enhanced_page_name STRING,
ADD COLUMN enhanced_page_category STRING,
ADD COLUMN enhanced_title STRING,
ADD COLUMN enhanced_body_text STRING,
ADD COLUMN enhanced_cta_text STRING,
ADD COLUMN enhanced_cta_type STRING;
*/

-- Production embedding generation from real ads data
CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_production` AS

WITH enhanced_ads_data AS (
  SELECT 
    ad_archive_id,
    page_name,
    enhanced_page_name,
    enhanced_page_category,
    enhanced_title,
    enhanced_body_text,
    enhanced_cta_text,
    enhanced_cta_type,
    publisher_platform,
    ad_creation_time,
    ingestion_timestamp,
    
    -- Use enhanced fields if available, fallback to legacy extraction if needed
    COALESCE(
      NULLIF(enhanced_page_name, ''), 
      page_name,
      ''
    ) as final_page_name,
    
    COALESCE(
      NULLIF(enhanced_page_category, ''), 
      'Unknown'
    ) as final_page_category,
    
    COALESCE(
      NULLIF(enhanced_title, ''), 
      ''
    ) as final_title,
    
    COALESCE(
      NULLIF(enhanced_body_text, ''), 
      ''
    ) as final_body_text,
    
    COALESCE(
      NULLIF(enhanced_cta_text, ''), 
      ''
    ) as final_cta_text,
    
    COALESCE(
      NULLIF(enhanced_cta_type, ''), 
      'UNKNOWN'
    ) as final_cta_type
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.fb_ads_raw`
  WHERE ad_archive_id IS NOT NULL
    AND ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)  -- Only recent data
),

structured_content AS (
  SELECT 
    ad_archive_id,
    final_page_name as brand,
    final_page_category as page_category,
    final_title as title,
    final_body_text as body_text,
    final_cta_text as cta_text,
    final_cta_type as cta_type,
    publisher_platform,
    ad_creation_time,
    ingestion_timestamp,
    
    -- CONTENT: What the ad message is about (customer-facing semantics)
    CONCAT(
      'Headline: ', COALESCE(final_title, ''),
      ' Message: ', COALESCE(final_body_text, ''),
      ' Action: ', COALESCE(final_cta_text, '')
    ) as content_text,
    
    -- CONTEXT: Who is advertising and business context
    CONCAT(
      'Company: ', COALESCE(final_page_name, ''),
      ' Industry: ', COALESCE(final_page_category, ''),
      ' Model: ', COALESCE(final_cta_type, '')
    ) as context_text,
    
    -- Quality indicators (computed in SQL, not Python)
    final_title IS NOT NULL AND LENGTH(TRIM(final_title)) > 0 as has_title,
    final_body_text IS NOT NULL AND LENGTH(TRIM(final_body_text)) > 0 as has_body,
    final_cta_text IS NOT NULL AND LENGTH(TRIM(final_cta_text)) > 0 as has_cta,
    final_page_category IS NOT NULL AND LENGTH(TRIM(final_page_category)) > 0 as has_category,
    
    -- Content metrics
    LENGTH(CONCAT(
      COALESCE(final_title, ''), ' ',
      COALESCE(final_body_text, ''), ' ', 
      COALESCE(final_cta_text, '')
    )) as content_length_chars,
    
    -- Text richness score (computed in SQL)
    (
      CASE WHEN final_title IS NOT NULL AND LENGTH(TRIM(final_title)) > 5 THEN 0.2 ELSE 0.1 END +
      CASE 
        WHEN final_body_text IS NOT NULL AND LENGTH(TRIM(final_body_text)) > 50 THEN 0.5
        WHEN final_body_text IS NOT NULL AND LENGTH(TRIM(final_body_text)) > 20 THEN 0.3
        ELSE 0.1
      END +
      CASE WHEN final_cta_text IS NOT NULL AND LENGTH(TRIM(final_cta_text)) > 2 THEN 0.2 ELSE 0.1 END +
      CASE WHEN final_page_category IS NOT NULL AND final_page_category != 'Unknown' THEN 0.1 ELSE 0.0 END
    ) as text_richness_score
    
  FROM enhanced_ads_data
  WHERE LENGTH(CONCAT(
    COALESCE(final_title, ''), 
    COALESCE(final_body_text, ''), 
    COALESCE(final_cta_text, '')
  )) > 10  -- Only ads with meaningful text content
),

-- Generate content embeddings
content_embeddings AS (
  SELECT *
  FROM ML.GENERATE_EMBEDDING(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
    (
      SELECT 
        ad_archive_id,
        brand,
        page_category,
        title,
        body_text,
        cta_text,
        cta_type,
        publisher_platform,
        ad_creation_time,
        ingestion_timestamp,
        content_text as content,
        context_text,
        has_title,
        has_body,
        has_cta,
        has_category,
        content_length_chars,
        text_richness_score
      FROM structured_content
    ),
    STRUCT('SEMANTIC_SIMILARITY' as task_type)
  )
),

-- Generate context embeddings  
context_embeddings AS (
  SELECT *
  FROM ML.GENERATE_EMBEDDING(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
    (
      SELECT 
        ad_archive_id,
        brand,
        page_category,
        title,
        body_text,
        cta_text,
        cta_type,
        publisher_platform,
        ad_creation_time,
        ingestion_timestamp,
        content_text,
        context_text as content,  -- This gets embedded as context
        has_title,
        has_body,
        has_cta,
        has_category,
        content_length_chars,
        text_richness_score
      FROM structured_content
    ),
    STRUCT('SEMANTIC_SIMILARITY' as task_type)
  )
)

-- Combine both embeddings into production table
SELECT 
  ce.ad_archive_id,
  ce.brand,
  ce.page_category,
  ce.title,
  ce.body_text,
  ce.cta_text,
  ce.cta_type,
  ce.publisher_platform,
  ce.ad_creation_time,
  ce.ingestion_timestamp,
  sc.content_text,
  sc.context_text,
  ce.has_title,
  ce.has_body,
  ce.has_cta,
  ce.has_category,
  ce.content_length_chars,
  ce.text_richness_score,
  
  -- Dual embeddings
  ce.ml_generate_embedding_result as content_embedding,
  cte.ml_generate_embedding_result as context_embedding,
  
  -- Metadata
  CURRENT_TIMESTAMP() as embedding_generation_time,
  'text-embedding-004' as embedding_model,
  'dual_vector_production_v1' as embedding_version

FROM content_embeddings ce
JOIN context_embeddings cte
  ON ce.ad_archive_id = cte.ad_archive_id
JOIN structured_content sc
  ON ce.ad_archive_id = sc.ad_archive_id;

-- Create production similarity views that use the real data
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_production_similarity` AS
SELECT 
  base.ad_archive_id as base_ad_id,
  base.brand as base_brand,
  comp.ad_archive_id as similar_ad_id,
  comp.brand as similar_brand,
  
  ROUND((1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')), 4) as content_similarity,
  ROUND((1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE')), 4) as context_similarity,
  
  CASE 
    WHEN (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) >= 0.8 
     AND (1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE')) >= 0.8 
    THEN 'Direct Competitor'
    WHEN (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) >= 0.8 
     AND (1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE')) < 0.6 
    THEN 'Message Overlap'
    WHEN (1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) < 0.6 
     AND (1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE')) >= 0.8 
    THEN 'Industry Peer'
    ELSE 'Distinct'
  END as competitive_relationship,
  
  base.content_text as base_content_preview,
  comp.content_text as similar_content_preview
  
FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_production` base
CROSS JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_production` comp
WHERE base.ad_archive_id != comp.ad_archive_id
  AND base.brand != comp.brand  -- Cross-brand comparisons only
  AND ((1 - ML.DISTANCE(base.content_embedding, comp.content_embedding, 'COSINE')) >= 0.6 
    OR (1 - ML.DISTANCE(base.context_embedding, comp.context_embedding, 'COSINE')) >= 0.6)
ORDER BY base.ad_archive_id, content_similarity DESC;