-- Fix strategic labels extraction using proper multiline regex

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels` AS

WITH extracted_text AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title,
    cta_text,
    page_category,
    JSON_VALUE(classification_json, '$.candidates[0].content.parts[0].text') as generated_text
  FROM `bigquery-ai-kaggle-469620.ads_demo.strategic_raw_temp`
),

cleaned_json AS (
  SELECT 
    *,
    -- Remove markdown code blocks and extract just the JSON
    REGEXP_REPLACE(
      REGEXP_REPLACE(generated_text, r'```json\n', ''),
      r'\n```', ''
    ) as clean_json_text
  FROM extracted_text
)

SELECT 
  ad_archive_id,
  brand,
  creative_text,
  title,
  cta_text,
  page_category,
  clean_json_text as classification_json,
  
  -- Parse JSON fields from cleaned JSON
  JSON_VALUE(clean_json_text, '$.funnel') AS funnel,
  JSON_VALUE(clean_json_text, '$.persona') AS persona,
  JSON_EXTRACT_ARRAY(clean_json_text, '$.topics') AS topics,
  SAFE_CAST(JSON_VALUE(clean_json_text, '$.urgency_score') AS FLOAT64) AS urgency_score,
  SAFE_CAST(JSON_VALUE(clean_json_text, '$.promotional_intensity') AS FLOAT64) AS promotional_intensity,
  SAFE_CAST(JSON_VALUE(clean_json_text, '$.brand_voice_score') AS FLOAT64) AS brand_voice_score,
  
  -- Add timestamp for time-series analysis
  CURRENT_TIMESTAMP() AS classified_at
  
FROM cleaned_json
WHERE JSON_VALUE(clean_json_text, '$.funnel') IS NOT NULL;  -- Only include valid classifications