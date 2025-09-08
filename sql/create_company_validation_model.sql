-- Create BigQuery AI remote model for company name validation
-- Using Gemini Pro for reliable and cost-effective text generation

CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.company_validator_model`
REMOTE WITH CONNECTION `bigquery-ai-kaggle-469620.us.vertex-ai` 
OPTIONS (
  ENDPOINT = 'gemini-2.5-flash'  -- Using Gemini 2.5 Flash for company validation (reliable & cost-effective)
  -- Previous: 'gemini-2.0-flash-001' (deprecated)
);