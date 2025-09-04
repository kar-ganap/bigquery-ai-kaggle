-- Test ML.GENERATE_TEXT with our remote model
-- Validate problematic competitor names using Gemini 2.0 Flash

WITH test_companies AS (
  SELECT 'Payment Management' as prompt_text
  UNION ALL SELECT 'PayPal'
  UNION ALL SELECT 'Market Share'  
  UNION ALL SELECT 'Stripe'
  UNION ALL SELECT 'Competitor Insights'
),

prompts AS (
  SELECT 
    prompt_text as company_name,
    CONCAT(
      'Is "', prompt_text, '" a real company name? Answer: YES if it is a specific business/corporation, NO if it is a generic term, page title, or description. ',
      'Format: YES/NO - Brief reason'
    ) AS prompt
  FROM test_companies
)

SELECT *
FROM ML.GENERATE_TEXT(
  MODEL `bigquery-ai-kaggle-469620.ads_demo.company_validator_model`,
  (SELECT prompt, company_name FROM prompts),
  STRUCT(
    100 AS max_output_tokens,
    0.1 AS temperature,
    TRUE AS flatten_json_output
  )
)
ORDER BY company_name;