-- Validate competitor names using BigQuery AI (ML.GENERATE_TEXT)
-- This query uses Gemini Pro to intelligently assess if names are real companies

-- First, let's test on our problematic company names
WITH test_companies AS (
  SELECT company_name
  FROM `bigquery-ai-kaggle-469620.ads_demo.competitors_curated`
  WHERE company_name IN ('Payment Management', 'Market Share', 'Competitor Insights', 'PayPal', 'Venmo')
),

company_validation AS (
  SELECT 
    company_name,
    ML.GENERATE_TEXT(
      MODEL `bigquery-ai-kaggle-469620.ads_demo.company_validator_model`,
      STRUCT(
        CONCAT(
          'Analyze if "', company_name, '" is a real company name. ',
          'Consider: Is this a specific business entity or a generic term? ',
          'Does it sound like a company name or page title fragment? ',
          'Respond in JSON format: ',
          '{"is_company": true/false, "confidence": 0.0-1.0, "category": "real_company|generic_term|page_fragment|unclear", "reasoning": "brief explanation"}'
        ) AS prompt,
        0.1 AS temperature,  -- Low temperature for consistent, factual responses
        1024 AS max_output_tokens
      )
    ) AS ai_validation_response
  FROM test_companies
)

SELECT 
  company_name,
  ai_validation_response,
  
  -- Extract JSON fields (will need to parse this properly)
  REGEXP_EXTRACT(ai_validation_response, r'"is_company":\s*(true|false)') AS is_company_raw,
  REGEXP_EXTRACT(ai_validation_response, r'"confidence":\s*([0-9.]+)') AS confidence_raw,
  REGEXP_EXTRACT(ai_validation_response, r'"category":\s*"([^"]+)"') AS category,
  REGEXP_EXTRACT(ai_validation_response, r'"reasoning":\s*"([^"]+)"') AS reasoning

FROM company_validation
ORDER BY company_name;