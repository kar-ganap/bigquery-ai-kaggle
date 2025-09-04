-- Apply ML.GENERATE_TEXT validation to all competitor names
-- This will use Gemini 2.0 Flash to intelligently filter garbage company names

WITH competitor_prompts AS (
  SELECT 
    company_name,
    target_brand,
    confidence as original_ai_confidence,
    quality_score as original_quality_score,
    reasoning as original_ai_reasoning,
    CONCAT(
      'Is "', company_name, '" a real company name? ',
      'Answer: YES if it is a specific business/corporation with employees and products/services. ',
      'Answer: NO if it is a generic business term, page title fragment, or descriptive phrase. ',
      'Examples: YES for "PayPal", "Stripe", "Microsoft". NO for "Market Share", "Payment Management", "Competitor Analysis". ',
      'Format: YES/NO - Brief reason (max 20 words)'
    ) AS prompt
  FROM `bigquery-ai-kaggle-469620.ads_demo.competitors_curated`
  WHERE company_name IS NOT NULL
),

ai_validation AS (
  SELECT *
  FROM ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.company_validator_model`,
    (SELECT prompt, company_name, target_brand, original_ai_confidence, original_quality_score, original_ai_reasoning FROM competitor_prompts),
    STRUCT(
      150 AS max_output_tokens,
      0.1 AS temperature,  -- Low temperature for consistent responses
      TRUE AS flatten_json_output
    )
  )
)

-- Parse and analyze results
SELECT 
  company_name,
  target_brand,
  
  -- Parse AI response
  ml_generate_text_llm_result as ai_response,
  CASE 
    WHEN UPPER(ml_generate_text_llm_result) LIKE 'YES%' THEN TRUE
    WHEN UPPER(ml_generate_text_llm_result) LIKE 'NO%' THEN FALSE
    ELSE NULL
  END as is_real_company,
  
  -- Extract reasoning
  REGEXP_EXTRACT(ml_generate_text_llm_result, r'^(?:YES|NO)\s*-\s*(.+)$') as ai_reasoning,
  
  -- Calculate AI confidence based on response clarity
  CASE 
    WHEN UPPER(ml_generate_text_llm_result) LIKE 'YES - %' THEN 0.9
    WHEN UPPER(ml_generate_text_llm_result) LIKE 'NO - %' THEN 0.9
    WHEN UPPER(ml_generate_text_llm_result) LIKE 'YES%' THEN 0.7
    WHEN UPPER(ml_generate_text_llm_result) LIKE 'NO%' THEN 0.7
    ELSE 0.3  -- Unclear response
  END as ai_confidence,
  
  -- Original curation data for comparison
  original_ai_confidence,
  original_quality_score,
  original_ai_reasoning,
  
  -- Final recommendation
  CASE 
    WHEN UPPER(ml_generate_text_llm_result) LIKE 'YES%' THEN 'APPROVE'
    WHEN UPPER(ml_generate_text_llm_result) LIKE 'NO%' THEN 'REJECT'
    ELSE 'MANUAL_REVIEW'
  END as final_recommendation

FROM ai_validation
ORDER BY 
  final_recommendation,
  ai_confidence DESC,
  company_name;