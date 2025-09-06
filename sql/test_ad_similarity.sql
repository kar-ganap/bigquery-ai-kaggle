-- Test semantic similarity between ads using embeddings
-- This demonstrates the core "find similar ads" functionality

WITH sample_ads AS (
  SELECT 'PayPal' as company, '784934174032998' as ad_id, 
    'Search like a Pro on us Get a year of Perplexity Pro free ($200 value) & early access to their AI-powered browser Comet if you subscribe with PayPal.' as creative_text
  UNION ALL
  SELECT 'PayPal', '2440772946322747',
    'Free year Pro subscription Get a year of Perplexity Pro free ($200 value) when you subscribe with PayPal. Easily manage subscriptions in the PayPal app.'
  UNION ALL  
  SELECT 'Venmo', '1461478151850095',
    'NEWS ALERT YOU CAN CHECK OUT AT SEPHORA WITH VENMO! No more running around for your credit card to find the number. Venmo and done.'
  UNION ALL
  SELECT 'Venmo', '642109495224976', 
    'Enter your beauty era with Venmo Check out with Venmo when you get juicy drops from Rare Beauty at Sephora'
),

-- Generate embeddings for all ads
embeddings AS (
  SELECT *
  FROM ML.GENERATE_EMBEDDING(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
    (
      SELECT 
        company,
        ad_id,
        creative_text,
        CONCAT('Company: ', company, ' Content: ', creative_text) as content
      FROM sample_ads
    ),
    STRUCT('SEMANTIC_SIMILARITY' as task_type)
  )
)

-- Test similarity search: Find ads similar to the first PayPal ad
SELECT 
  base.company as base_company,
  base.ad_id as base_ad_id,
  comp.company as similar_company,
  comp.ad_id as similar_ad_id,
  
  -- Calculate cosine similarity
  ML.DISTANCE(base.ml_generate_embedding_result, comp.ml_generate_embedding_result, 'COSINE') as cosine_distance,
  (1 - ML.DISTANCE(base.ml_generate_embedding_result, comp.ml_generate_embedding_result, 'COSINE')) as cosine_similarity,
  
  -- Show preview of content
  LEFT(base.creative_text, 60) as base_text_preview,
  LEFT(comp.creative_text, 60) as similar_text_preview

FROM embeddings base
CROSS JOIN embeddings comp
WHERE base.ad_id = '784934174032998'  -- First PayPal ad as base
  AND base.ad_id != comp.ad_id        -- Exclude self-comparison

ORDER BY cosine_similarity DESC;