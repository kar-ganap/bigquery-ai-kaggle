-- Step 1: Generate embeddings for real ad data
-- Step 2: Insert into ads_embeddings table

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_temp` AS

WITH real_ad_data AS (
  -- Real ad data from our competitor validation testing
  SELECT 'PayPal' as brand, '784934174032998' as ad_archive_id, 
    'Search like a Pro on us' as title,
    'Get a year of Perplexity Pro free ($200 value) & early access to their AI-powered browser Comet if you subscribe with PayPal.' as body_text,
    'Subscribe with PayPal' as cta_text
  UNION ALL
  SELECT 'PayPal', '2440772946322747',
    'Free year Pro subscription',
    'Get a year of Perplexity Pro free ($200 value) when you subscribe with PayPal. Easily manage subscriptions in the PayPal app.',
    'Subscribe Now'
  UNION ALL  
  SELECT 'Venmo', '1461478151850095',
    'NEWS ALERT',
    'YOU CAN CHECK OUT AT SEPHORA WITH VENMO! No more running around for your credit card to find the number. Venmo and done.',
    'Check out with Venmo'
  UNION ALL
  SELECT 'Venmo', '642109495224976', 
    'Enter your beauty era',
    'Check out with Venmo when you get juicy drops from Rare Beauty at Sephora',
    'Shop Now'
),

structured_content AS (
  SELECT 
    brand,
    ad_archive_id,
    
    -- Combine all text components
    CONCAT(
      COALESCE(title, ''), ' ',
      COALESCE(body_text, ''), ' ', 
      COALESCE(cta_text, '')
    ) as creative_text,
    
    -- Structured content for embedding
    CONCAT(
      'Brand: ', brand,
      ' Title: ', COALESCE(title, ''), 
      ' Content: ', COALESCE(body_text, ''),
      ' Action: ', COALESCE(cta_text, '')
    ) as structured_text,
    
    -- Quality indicators  
    title IS NOT NULL AND LENGTH(TRIM(title)) > 0 as has_title,
    body_text IS NOT NULL AND LENGTH(TRIM(body_text)) > 0 as has_body,
    cta_text IS NOT NULL AND LENGTH(TRIM(cta_text)) > 0 as has_cta,
    
    -- Content metrics
    LENGTH(CONCAT(COALESCE(title, ''), COALESCE(body_text, ''), COALESCE(cta_text, ''))) as content_length_chars
    
  FROM real_ad_data
)

-- Generate embeddings and structure for final table
SELECT *
FROM ML.GENERATE_EMBEDDING(
  MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
  (
    SELECT 
      brand,
      ad_archive_id,
      creative_text,
      structured_text as content,
      has_title,
      has_body,
      has_cta,
      content_length_chars
    FROM structured_content
  ),
  STRUCT('SEMANTIC_SIMILARITY' as task_type)
);