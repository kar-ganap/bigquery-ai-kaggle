-- Test text embeddings with sample ad creative content
-- Uses our collected PayPal and Venmo ads as test data

WITH sample_ads AS (
  -- Sample ad creative content from our pipeline testing
  SELECT 'PayPal' as company, '784934174032998' as ad_id, 
    'Search like a Pro on us Get a year of Perplexity Pro free ($200 value) & early access to their AI-powered browser Comet if you subscribe with PayPal.' as creative_text
  UNION ALL
  SELECT 'PayPal', '2440772946322747',
    'Free year Pro subscription Get a year of Perplexity Pro free ($200 value) when you subscribe with PayPal. Easily manage subscriptions in the PayPal app.'
  UNION ALL  
  SELECT 'Venmo', '1461478151850095',
    'NEWS ALERT YOU CAN CHECK OUT AT SEPHORA WITH VENMO! No more running around for your credit card to find the number. Venmo and done. @soleilssofia'
  UNION ALL
  SELECT 'Venmo', '642109495224976', 
    'Enter your beauty era with Venmo Check out with Venmo when you get juicy drops from Rare Beauty at Sephora'
),

structured_content AS (
  SELECT 
    company,
    ad_id,
    creative_text,
    -- Create semantic structure for embedding
    CONCAT(
      'Company: ', company, 
      ' Content: ', creative_text
    ) as structured_text
  FROM sample_ads
)

-- Generate embeddings and analyze
SELECT *
FROM ML.GENERATE_EMBEDDING(
  MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
  (
    SELECT 
      company,
      ad_id,
      LEFT(creative_text, 80) as text_preview,
      structured_text as content
    FROM structured_content
  ),
  STRUCT(
    'SEMANTIC_SIMILARITY' as task_type  -- Optimize for similarity search
  )
)
ORDER BY company, ad_id;