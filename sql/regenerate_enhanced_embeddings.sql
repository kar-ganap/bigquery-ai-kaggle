-- Regenerate embeddings with enhanced text content structure
-- Includes brand, category, title, content, and CTA for richer semantic understanding

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_enhanced` AS

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
    
    -- Basic creative text (old approach)
    CONCAT(
      COALESCE(title, ''), ' ',
      COALESCE(body_text, '')
    ) as basic_creative_text,
    
    -- Enhanced structured content (new approach)
    CONCAT(
      'Brand: ', COALESCE(page_name, ''),
      ' Category: ', COALESCE(page_category, ''),
      ' Title: ', COALESCE(title, ''), 
      ' Content: ', COALESCE(body_text, ''),
      ' Action: ', COALESCE(cta_text, '')
    ) as enhanced_structured_text,
    
    -- Quality indicators  
    title IS NOT NULL AND LENGTH(TRIM(title)) > 0 as has_title,
    body_text IS NOT NULL AND LENGTH(TRIM(body_text)) > 0 as has_body,
    cta_text IS NOT NULL AND LENGTH(TRIM(cta_text)) > 0 as has_cta,
    page_category IS NOT NULL AND LENGTH(TRIM(page_category)) > 0 as has_category,
    
    -- Content metrics
    LENGTH(CONCAT(
      COALESCE(page_name, ''), ' ',
      COALESCE(page_category, ''), ' ',
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
)

-- Generate embeddings with enhanced content and store comprehensive data
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
      basic_creative_text,
      enhanced_structured_text as content,  -- This gets embedded
      has_title,
      has_body,
      has_cta,
      has_category,
      content_length_chars,
      text_richness_score
    FROM structured_content
  ),
  STRUCT('SEMANTIC_SIMILARITY' as task_type)
);