-- Create BigQuery AI text embedding model for ad creative content
-- Uses text-embedding-004 for high-quality semantic embeddings

CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`
REMOTE WITH CONNECTION `bigquery-ai-kaggle-469620.us.vertex-ai` 
OPTIONS (
  ENDPOINT = 'text-embedding-004'  -- Google's latest text embedding model
  -- Alternative: 'textembedding-gecko@003' (older), 'text-multilingual-embedding-002' (multilingual)
);