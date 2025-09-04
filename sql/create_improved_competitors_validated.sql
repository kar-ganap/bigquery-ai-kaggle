-- Create improved competitors_validated view using BigQuery AI validation
-- This replaces the old view with AI-powered filtering

CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.competitors_validated` AS

WITH competitor_prompts AS (
  SELECT 
    *,
    CONCAT(
      'Is "', company_name, '" a real company name? ',
      'Answer: YES if it is a specific business/corporation with employees and products/services. ',
      'Answer: NO if it is a generic business term, page title fragment, or descriptive phrase. ',
      'Format: YES/NO - Brief reason'
    ) AS prompt
  FROM `bigquery-ai-kaggle-469620.ads_demo.competitors_curated`
  WHERE company_name IS NOT NULL
    AND LENGTH(TRIM(company_name)) > 1  -- Basic length filter
),

ai_validation AS (
  SELECT *
  FROM ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.company_validator_model`,
    (SELECT prompt, * EXCEPT(prompt) FROM competitor_prompts),
    STRUCT(
      100 AS max_output_tokens,
      0.1 AS temperature,
      TRUE AS flatten_json_output
    )
  )
),

final_validation AS (
  SELECT 
    -- Original fields
    target_brand,
    target_vertical,
    company_name,
    source_url,
    source_title,
    query_used,
    raw_score,
    found_in,
    discovery_method,
    discovered_at,
    is_competitor,
    tier,
    market_overlap_pct,
    customer_substitution_ease,
    confidence,
    reasoning,
    evidence_sources,
    curated_at,
    quality_score,
    
    -- AI validation fields
    ml_generate_text_llm_result as ai_validation_response,
    CASE 
      -- Handle "YES - reason" format
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'^YES\s*-') THEN TRUE
      -- Handle "NO - reason" format
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'^NO\s*-') THEN FALSE
      -- Handle mixed responses like "YES/NO - Brief reason: NO - ..." 
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r':\s*NO\s*-') THEN FALSE
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r':\s*YES\s*-') THEN TRUE
      -- Fallback: look for decisive NO anywhere in response
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'\bNO\b.*generic|generic.*\bNO\b') THEN FALSE
      -- Default cases
      WHEN UPPER(ml_generate_text_llm_result) LIKE 'YES %' THEN TRUE
      WHEN UPPER(ml_generate_text_llm_result) LIKE 'NO %' THEN FALSE
      ELSE NULL
    END as ai_approved,
    
    -- Enhanced confidence combining original + AI
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'^YES\s*-') THEN GREATEST(confidence, 0.85)
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r':\s*YES\s*-') THEN GREATEST(confidence, 0.75)
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'^NO\s*-') THEN LEAST(confidence, 0.2)
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r':\s*NO\s*-') THEN LEAST(confidence, 0.1)
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'\bNO\b.*generic') THEN LEAST(confidence, 0.1)
      ELSE confidence * 0.5  -- Unclear responses get penalized
    END as enhanced_confidence,
    
    -- Enhanced quality score
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'^YES\s*-') THEN GREATEST(quality_score, 0.8)
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r':\s*YES\s*-') THEN GREATEST(quality_score, 0.7)
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'^NO\s*-') THEN LEAST(quality_score, 0.3)
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r':\s*NO\s*-') THEN LEAST(quality_score, 0.2)
      WHEN REGEXP_CONTAINS(UPPER(ml_generate_text_llm_result), r'\bNO\b.*generic') THEN LEAST(quality_score, 0.2)
      ELSE quality_score * 0.5  -- Unclear responses get penalized
    END as enhanced_quality_score
    
  FROM ai_validation
)

-- Final filtering for high-quality competitors
SELECT *
FROM final_validation
WHERE ai_approved = TRUE
  AND enhanced_confidence >= 0.7
  AND enhanced_quality_score >= 0.6
  -- Additional safety filters
  AND company_name NOT IN ('competitors', 'analysis', 'insights', 'overview', 'management')
  AND LENGTH(TRIM(company_name)) >= 2
  AND LENGTH(TRIM(company_name)) <= 50
  -- Exclude obvious patterns
  AND NOT REGEXP_CONTAINS(LOWER(company_name), r'^(top \d+|best \d+|leading \w+)')

ORDER BY enhanced_confidence DESC, enhanced_quality_score DESC;