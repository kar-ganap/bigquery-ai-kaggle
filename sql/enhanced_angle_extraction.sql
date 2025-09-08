-- Enhanced Message Angle Extraction with Confidence Scoring
-- Uses Gemini 2.5 Flash to extract angles with confidence levels

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_angles_with_confidence` AS

WITH angle_prompts AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title,
    cta_text,
    CONCAT(
      'Analyze this Facebook ad and identify message angles with confidence scores.\n\n',
      'Ad Content:\n',
      'Headline: ', COALESCE(title, 'None'), '\n',
      'Body: ', COALESCE(creative_text, 'None'), '\n',
      'CTA: ', COALESCE(cta_text, 'None'), '\n\n',
      
      'Identify up to 3 message angles from this list:\n',
      'PROMOTIONAL: discount_offer, flash_sale, limited_time, free_trial, bundle_deal\n',
      'EMOTIONAL: social_proof, lifestyle_aspiration, brand_story, testimonial, community\n',
      'RATIONAL: benefits_focused, feature_highlight, comparison, educational, problem_solution\n',
      'URGENCY: scarcity, deadline, trending, exclusive, last_chance\n',
      'TRUST: guarantee, certification, awards, security, transparency\n\n',
      
      'Return JSON with confidence scores (0.0-1.0):\n',
      '{\n',
      '  "angles": [\n',
      '    {"angle": "angle_name", "confidence": 0.0, "category": "CATEGORY"},\n',
      '    ...\n',
      '  ],\n',
      '  "primary_angle": "most_confident_angle",\n',
      '  "angle_diversity": 0.0  // 0=single focus, 1=very diverse\n',
      '}'
    ) as prompt
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings`
  WHERE creative_text IS NOT NULL OR title IS NOT NULL
),

angle_analysis AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title,
    cta_text,
    ml_generate_text_result,
    -- Extract the generated text
    JSON_VALUE(ml_generate_text_result, '$.candidates[0].content.parts[0].text') as generated_text
  FROM ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
    (SELECT prompt, ad_archive_id, brand, creative_text, title, cta_text FROM angle_prompts),
    STRUCT(
      0.1 AS temperature,
      1000 AS max_output_tokens
    )
  )
)

SELECT 
  ad_archive_id,
  brand,
  creative_text,
  title,
  cta_text,
  
  -- Clean and parse JSON
  REGEXP_REPLACE(
    REGEXP_REPLACE(generated_text, r'```json\n', ''),
    r'\n```', ''
  ) as angles_json,
  
  -- Extract primary angle
  JSON_VALUE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(generated_text, r'```json\n', ''),
      r'\n```', ''
    ), 
    '$.primary_angle'
  ) as primary_angle,
  
  -- Extract angle diversity score
  SAFE_CAST(
    JSON_VALUE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(generated_text, r'```json\n', ''),
        r'\n```', ''
      ), 
      '$.angle_diversity'
    ) AS FLOAT64
  ) as angle_diversity,
  
  -- Extract angles array
  JSON_EXTRACT_ARRAY(
    REGEXP_REPLACE(
      REGEXP_REPLACE(generated_text, r'```json\n', ''),
      r'\n```', ''
    ),
    '$.angles'
  ) as angles_with_confidence,
  
  CURRENT_TIMESTAMP() as analyzed_at
  
FROM angle_analysis;