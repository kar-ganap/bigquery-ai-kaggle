-- Simple comparison between Gemini 2.5 Pro and Flash

WITH test_prompt AS (
  SELECT 
    'Classify this Facebook ad funnel stage. Return only: Upper, Mid, or Lower' as prompt,
    ad_archive_id,
    brand,
    creative_text
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings`
  WHERE creative_text IS NOT NULL
  LIMIT 1
),

flash_test AS (
  SELECT 
    'Flash_2.5' as model_type,
    ml_generate_text_result,
    JSON_VALUE(ml_generate_text_result, '$.usage_metadata.total_token_count') as total_tokens,
    JSON_VALUE(ml_generate_text_result, '$.candidates[0].finish_reason') as finish_reason,
    brand,
    SUBSTR(creative_text, 1, 50) as creative_sample
  FROM ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_flash_model`,
    (SELECT prompt, ad_archive_id, brand, creative_text FROM test_prompt),
    STRUCT(
      0.1 AS temperature,
      100 AS max_output_tokens
    )
  )
),

pro_test AS (
  SELECT 
    'Pro_2.5' as model_type,
    ml_generate_text_result,
    JSON_VALUE(ml_generate_text_result, '$.usage_metadata.total_token_count') as total_tokens,
    JSON_VALUE(ml_generate_text_result, '$.candidates[0].finish_reason') as finish_reason,
    brand,
    SUBSTR(creative_text, 1, 50) as creative_sample
  FROM ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_pro_model`,
    (SELECT prompt, ad_archive_id, brand, creative_text FROM test_prompt),
    STRUCT(
      0.1 AS temperature,
      100 AS max_output_tokens
    )
  )
)

SELECT 
  model_type,
  brand,
  creative_sample,
  total_tokens,
  finish_reason,
  -- Try to extract response text
  COALESCE(
    JSON_VALUE(ml_generate_text_result, '$.candidates[0].content.parts[0].text'),
    'No text response'
  ) as response_text,
  
  -- Cost analysis (approximate)
  CASE 
    WHEN model_type = 'Flash_2.5' THEN CAST(total_tokens AS INT64) * 0.000075  -- Flash pricing estimate
    WHEN model_type = 'Pro_2.5' THEN CAST(total_tokens AS INT64) * 0.00125     -- Pro pricing estimate  
  END as estimated_cost_usd
  
FROM (
  SELECT * FROM flash_test
  UNION ALL  
  SELECT * FROM pro_test
)
ORDER BY model_type;