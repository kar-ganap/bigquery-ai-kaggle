-- Create final strategic labels table from raw classifications
-- Extract JSON from Gemini 2.0 Flash responses (handles markdown code blocks)

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels` AS

WITH cleaned_json AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title,
    cta_text,
    page_category,
    classification_json,
    
    -- Extract the actual text from the ML.GENERATE_TEXT result structure
    JSON_VALUE(classification_json, '$.candidates[0].content.parts[0].text') as generated_text
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.strategic_raw_temp`
),

parsed_classifications AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title,
    cta_text,
    page_category,
    generated_text,
    
    -- Extract JSON from markdown code blocks in the generated text
    REGEXP_EXTRACT(generated_text, r'```json\n(.*?)\n```') as extracted_json_text,
    
    -- Parse JSON fields from the cleaned JSON
    JSON_VALUE(REGEXP_EXTRACT(generated_text, r'```json\n(.*?)\n```'), '$.funnel') AS funnel,
    JSON_VALUE(REGEXP_EXTRACT(generated_text, r'```json\n(.*?)\n```'), '$.persona') AS persona,
    JSON_EXTRACT_ARRAY(REGEXP_EXTRACT(generated_text, r'```json\n(.*?)\n```'), '$.topics') AS topics,
    SAFE_CAST(JSON_VALUE(REGEXP_EXTRACT(generated_text, r'```json\n(.*?)\n```'), '$.urgency_score') AS FLOAT64) AS urgency_score,
    SAFE_CAST(JSON_VALUE(REGEXP_EXTRACT(generated_text, r'```json\n(.*?)\n```'), '$.promotional_intensity') AS FLOAT64) AS promotional_intensity,
    SAFE_CAST(JSON_VALUE(REGEXP_EXTRACT(generated_text, r'```json\n(.*?)\n```'), '$.brand_voice_score') AS FLOAT64) AS brand_voice_score
    
  FROM cleaned_json
)

SELECT 
  ad_archive_id,
  brand,
  creative_text,
  title,
  cta_text,
  page_category,
  extracted_json_text as classification_json,
  funnel,
  persona,
  topics,
  urgency_score,
  promotional_intensity,
  brand_voice_score,
  
  -- Add timestamp for time-series analysis
  CURRENT_TIMESTAMP() AS classified_at
  
FROM parsed_classifications
WHERE funnel IS NOT NULL;  -- Only include successfully parsed classifications