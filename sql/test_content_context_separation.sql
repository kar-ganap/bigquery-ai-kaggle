-- Test content vs context similarity separation
-- This approach creates separate embeddings for content and business context

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.test_separated_embeddings` AS

WITH diverse_ad_data AS (
  -- Same test data as before
  SELECT 'Capital One' as brand, 'cap_001' as ad_archive_id,
    'Banking & Credit' as page_category,
    'Travel more, worry less' as title,
    'Book your dream vacation with no foreign transaction fees. Earn miles on every purchase.' as body_text,
    'Learn More' as cta_text
  UNION ALL
  SELECT 'Expedia', 'exp_001',
    'Travel & Tourism',
    'Travel more, save more',
    'Book your perfect vacation with exclusive member discounts. Earn rewards on every booking.',
    'Book Now'
  UNION ALL
  SELECT 'Chase', 'chase_001',
    'Banking & Credit',
    'Earn 5% cash back',
    'Get 5% cash back on travel and dining with the Chase Sapphire card. No annual fee for the first year.',
    'Apply Now'
  UNION ALL
  SELECT 'Netflix', 'netflix_001',
    'Entertainment',
    'Stream unlimited',
    'Watch thousands of movies and shows. First month free for new members.',
    'Start Watching'
),

-- Generate content-only embeddings (what the ad is actually about)
content_embeddings AS (
  SELECT *
  FROM ML.GENERATE_EMBEDDING(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
    (
      SELECT 
        brand,
        ad_archive_id,
        page_category,
        title,
        body_text,
        cta_text,
        -- Pure content - what the message is about
        CONCAT(
          COALESCE(title, ''), ' ',
          COALESCE(body_text, '')
        ) as content
      FROM diverse_ad_data
    ),
    STRUCT('SEMANTIC_SIMILARITY' as task_type)
  )
),

-- Generate context-only embeddings (business context)  
context_embeddings AS (
  SELECT *
  FROM ML.GENERATE_EMBEDDING(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
    (
      SELECT 
        brand,
        ad_archive_id,
        page_category,
        title,
        body_text,
        cta_text,
        -- Business context - who is advertising and what type of business
        CONCAT(
          'Company: ', COALESCE(brand, ''),
          ' Industry: ', COALESCE(page_category, ''),
          ' Business Type: ', COALESCE(cta_text, '')
        ) as content
      FROM diverse_ad_data
    ),
    STRUCT('SEMANTIC_SIMILARITY' as task_type)
  )
)

-- Combine both embeddings in one table for analysis
SELECT 
  ce.brand,
  ce.ad_archive_id,
  ce.page_category,
  ce.title,
  ce.body_text,
  ce.cta_text,
  ce.ml_generate_embedding_result as content_embedding,
  cte.ml_generate_embedding_result as context_embedding
FROM content_embeddings ce
JOIN context_embeddings cte
  ON ce.ad_archive_id = cte.ad_archive_id;