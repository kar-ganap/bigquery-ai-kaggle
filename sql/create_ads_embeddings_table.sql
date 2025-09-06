-- Create ads_embeddings table for storing semantic vectors
-- Links to existing ads_raw table for complete ad metadata

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings` (
  -- Primary key linking to ads_raw
  ad_archive_id STRING NOT NULL,
  brand STRING,
  
  -- Text content used for embedding
  creative_text STRING,
  structured_content STRING,  -- The actual text sent to embedding model
  
  -- Embedding vectors
  content_embedding ARRAY<FLOAT64>,
  
  -- Metadata
  embedding_model STRING DEFAULT 'text-embedding-004',
  embedding_generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  content_length_chars INT64,
  content_length_tokens INT64,  -- Future: track token usage for cost
  
  -- Quality indicators
  has_title BOOL DEFAULT FALSE,
  has_body BOOL DEFAULT FALSE, 
  has_cta BOOL DEFAULT FALSE,
  text_quality_score FLOAT64,  -- Future: content quality assessment
  
  -- Partitioning and clustering for performance
  embedding_date DATE DEFAULT CURRENT_DATE()
)
PARTITION BY embedding_date
CLUSTER BY brand, embedding_model
OPTIONS (
  description = "Semantic embeddings for ad creative content - supports similarity search and competitive analysis",
  labels = [("purpose", "embeddings"), ("subgoal", "4")]
);

-- Create indexes for efficient similarity search (future enhancement)
-- Note: BigQuery doesn't have vector indexes yet, but clustering helps

-- Add some sample documentation
INSERT INTO `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings` 
(ad_archive_id, brand, creative_text, structured_content, content_embedding, has_title, has_body)
VALUES 
('SAMPLE_001', 'DOCUMENTATION', 
 'Sample ad creative content for testing semantic similarity search', 
 'Company: SAMPLE Content: Sample ad creative content for testing semantic similarity search',
 [0.1, 0.2, 0.3], -- Dummy embedding for schema testing
 TRUE, TRUE
);

-- Clean up the sample row
DELETE FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings` 
WHERE ad_archive_id = 'SAMPLE_001';