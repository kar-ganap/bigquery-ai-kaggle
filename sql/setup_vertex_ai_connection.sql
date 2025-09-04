-- Create Vertex AI connection for BigQuery ML remote models
-- This enables us to use Gemini and other AI models in BigQuery

CREATE OR REPLACE EXTERNAL CONNECTION `bigquery-ai-kaggle-469620.us.vertex-ai`
CONNECTION_TYPE = 'CLOUD_RESOURCE'
LOCATION = 'us'
PROPERTIES = (
  "serviceAccountId" = "bigquery-connection-service-account@bigquery-ai-kaggle-469620.iam.gserviceaccount.com"
);