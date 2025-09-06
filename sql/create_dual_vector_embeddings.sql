-- Dual-vector approach: separate embeddings for content and context
-- This allows for more nuanced competitive analysis

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_dual_vector` AS

WITH enhanced_ad_data AS (
  -- Enhanced real ad data with all meaningful text fields
  SELECT 'PayPal' as brand, '784934174032998' as ad_archive_id, 
    'PayPal' as page_name,
    'Financial Service' as page_category,
    'Search like a Pro on us' as title,
    'Get a year of Perplexity Pro free ($200 value) & early access to their AI-powered browser Comet if you subscribe with PayPal.' as body_text,
    'Download' as cta_text,
    'DOWNLOAD' as cta_type
  UNION ALL
  SELECT 'PayPal', '2440772946322747',
    'PayPal',
    'Financial Service',
    'Free year Pro subscription',
    'Get a year of Perplexity Pro free ($200 value) when you subscribe with PayPal. Easily manage subscriptions in the PayPal app.',
    'Subscribe',
    'LEARN_MORE'
  UNION ALL  
  SELECT 'Venmo', '1461478151850095',
    'Venmo',
    'Financial Service',
    'NEWS ALERT',
    'YOU CAN CHECK OUT AT SEPHORA WITH VENMO! No more running around for your credit card to find the number. Venmo and done.',
    'Check out',
    'SHOP_NOW'
  UNION ALL
  SELECT 'Venmo', '642109495224976', 
    'Venmo',
    'Financial Service',
    'Enter your beauty era',
    'Check out with Venmo when you get juicy drops from Rare Beauty at Sephora',
    'Shop Now',
    'SHOP_NOW'
  UNION ALL
  -- Add some diverse examples for better testing
  SELECT 'Nike', 'nike_001',
    'Nike',
    'Retail & Shopping',
    'Just Do It',
    'New Air Max collection available. Experience ultimate comfort and style with innovative cushioning technology.',
    'Shop Now',
    'SHOP_NOW'
  UNION ALL
  SELECT 'Adidas', 'adidas_001', 
    'Adidas',
    'Retail & Shopping',
    'Impossible is Nothing',
    'Latest Ultraboost technology helps you run faster and go further. Discover the perfect fit for your running style.',
    'Shop Now',
    'SHOP_NOW'
),

structured_content AS (
  SELECT 
    brand,
    ad_archive_id,
    page_name,
    page_category,
    title,
    body_text,
    cta_text,
    cta_type,
    
    -- CONTENT: What the ad message is about (customer-facing semantics)
    CONCAT(
      'Headline: ', COALESCE(title, ''),
      ' Message: ', COALESCE(body_text, ''),
      ' Action: ', COALESCE(cta_text, '')
    ) as content_text,
    
    -- CONTEXT: Who is advertising and business context
    CONCAT(
      'Company: ', COALESCE(page_name, ''),
      ' Industry: ', COALESCE(page_category, ''),
      ' Model: ', COALESCE(cta_type, '')
    ) as context_text,
    
    -- Quality indicators  
    title IS NOT NULL AND LENGTH(TRIM(title)) > 0 as has_title,
    body_text IS NOT NULL AND LENGTH(TRIM(body_text)) > 0 as has_body,
    cta_text IS NOT NULL AND LENGTH(TRIM(cta_text)) > 0 as has_cta,
    page_category IS NOT NULL AND LENGTH(TRIM(page_category)) > 0 as has_category,
    
    -- Content metrics
    LENGTH(CONCAT(
      COALESCE(title, ''), ' ',
      COALESCE(body_text, ''), ' ', 
      COALESCE(cta_text, '')
    )) as content_length_chars,
    
    -- Text richness score
    (
      CASE WHEN title IS NOT NULL AND LENGTH(TRIM(title)) > 5 THEN 0.2 ELSE 0.1 END +
      CASE 
        WHEN body_text IS NOT NULL AND LENGTH(TRIM(body_text)) > 50 THEN 0.5
        WHEN body_text IS NOT NULL AND LENGTH(TRIM(body_text)) > 20 THEN 0.3
        ELSE 0.1
      END +
      CASE WHEN cta_text IS NOT NULL AND LENGTH(TRIM(cta_text)) > 2 THEN 0.2 ELSE 0.1 END +
      CASE WHEN page_category IS NOT NULL THEN 0.1 ELSE 0.0 END
    ) as text_richness_score
    
  FROM enhanced_ad_data
),

-- Generate content embeddings
content_embeddings AS (
  SELECT *
  FROM ML.GENERATE_EMBEDDING(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
    (
      SELECT 
        brand,
        ad_archive_id,
        page_name,
        page_category,
        title,
        body_text,
        cta_text,
        cta_type,
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
        brand,
        ad_archive_id,
        page_name,
        page_category,
        title,
        body_text,
        cta_text,
        cta_type,
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

-- Combine both embeddings into final table
SELECT 
  ce.brand,
  ce.ad_archive_id,
  ce.page_name,
  ce.page_category,
  ce.title,
  ce.body_text,
  ce.cta_text,
  ce.cta_type,
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
  CURRENT_TIMESTAMP() as embedding_date,
  'text-embedding-004' as embedding_model,
  'dual_vector_v1' as embedding_version

FROM content_embeddings ce
JOIN context_embeddings cte
  ON ce.ad_archive_id = cte.ad_archive_id
JOIN structured_content sc
  ON ce.ad_archive_id = sc.ad_archive_id;