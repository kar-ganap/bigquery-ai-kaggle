-- Compare Gemini 2.5 Pro vs 2.5 Flash for strategic classification
-- Test: Quality, consistency, cost, and speed

WITH test_ads AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title,
    cta_text,
    CONCAT(
      'Analyze this Facebook ad and provide strategic classification in JSON format:\n\n',
      'Ad Content:\n',
      'Headline: ', COALESCE(title, 'None'), '\n',
      'Body: ', COALESCE(creative_text, 'None'), '\n', 
      'CTA: ', COALESCE(cta_text, 'None'), '\n\n',
      
      'Classify across these dimensions:\n\n',
      '1. FUNNEL STAGE: "Upper", "Mid", or "Lower"\n',
      '2. PERSONA TARGET: "New Customer", "Existing Customer", "Competitor Switch", or "General Market"\n',
      '3. URGENCY SCORE: 0.0-1.0 (0=no urgency, 1=high urgency)\n',
      '4. PROMOTIONAL INTENSITY: 0.0-1.0 (0=no promotion, 1=heavy promotion)\n\n',
      
      'Return ONLY valid JSON:\n',
      '{"funnel": "Upper|Mid|Lower", "persona": "...", "urgency_score": 0.0, "promotional_intensity": 0.0}'
    ) as prompt
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings`
  WHERE creative_text IS NOT NULL 
  LIMIT 3  -- Test on same 3 ads for direct comparison
),

-- Test Gemini 2.5 Flash
flash_results AS (
  SELECT 
    ad_archive_id,
    brand,
    'Flash' as model_type,
    ml_generate_text_result,
    JSON_VALUE(ml_generate_text_result, '$.usage_metadata.total_token_count') as total_tokens,
    JSON_VALUE(ml_generate_text_result, '$.create_time') as create_time
  FROM ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_flash_model`,
    (SELECT prompt, ad_archive_id, brand FROM test_ads),
    STRUCT(
      0.1 AS temperature,
      500 AS max_output_tokens
    )
  )
),

-- Test Gemini 2.5 Pro  
pro_results AS (
  SELECT 
    ad_archive_id,
    brand,
    'Pro' as model_type,
    ml_generate_text_result,
    JSON_VALUE(ml_generate_text_result, '$.usage_metadata.total_token_count') as total_tokens,
    JSON_VALUE(ml_generate_text_result, '$.create_time') as create_time
  FROM ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_pro_model`,
    (SELECT prompt, ad_archive_id, brand FROM test_ads),
    STRUCT(
      0.1 AS temperature,
      500 AS max_output_tokens
    )
  )
),

-- Combine results
combined_results AS (
  SELECT * FROM flash_results
  UNION ALL
  SELECT * FROM pro_results
),

-- Extract and parse responses
parsed_results AS (
  SELECT 
    *,
    JSON_VALUE(ml_generate_text_result, '$.candidates[0].content.parts[0].text') as generated_text,
    
    -- Extract JSON (handle markdown)
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        JSON_VALUE(ml_generate_text_result, '$.candidates[0].content.parts[0].text'),
        r'```json\n', ''
      ),
      r'\n```', ''
    ) as clean_json,
    
    CAST(total_tokens AS INT64) as token_count
  FROM combined_results
)

-- Final comparison
SELECT 
  ad_archive_id,
  brand,
  model_type,
  
  -- Quality metrics
  JSON_VALUE(clean_json, '$.funnel') as funnel_classification,
  JSON_VALUE(clean_json, '$.persona') as persona_classification,
  SAFE_CAST(JSON_VALUE(clean_json, '$.urgency_score') AS FLOAT64) as urgency_score,
  SAFE_CAST(JSON_VALUE(clean_json, '$.promotional_intensity') AS FLOAT64) as promotional_intensity,
  
  -- Cost/efficiency metrics
  token_count,
  
  -- JSON validity
  CASE 
    WHEN JSON_VALUE(clean_json, '$.funnel') IS NOT NULL THEN 'Valid JSON'
    ELSE 'Invalid JSON'
  END as json_validity,
  
  -- Response sample
  SUBSTR(generated_text, 1, 200) as response_sample
  
FROM parsed_results
ORDER BY ad_archive_id, model_type;