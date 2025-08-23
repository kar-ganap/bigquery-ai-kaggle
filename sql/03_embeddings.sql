-- OPTIONAL: Embeddings + Vector Index
-- One-time setup (requires Vertex AI connection in the same region):
-- CREATE OR REPLACE MODEL `yourproj.ads_demo.embed_model`
-- REMOTE WITH CONNECTION `us.vertex-conn`
-- OPTIONS (endpoint = 'text-embedding-004');

CREATE OR REPLACE TABLE `yourproj.ads_demo.ads_emb` AS
SELECT ad_id,
       ML.GENERATE_EMBEDDING(MODEL `yourproj.ads_demo.embed_model`,
         CONCAT(IFNULL(title,''),' ',IFNULL(creative_text,''))) AS emb
FROM `yourproj.ads_demo.ads_raw`;

CREATE VECTOR INDEX IF NOT EXISTS `yourproj.ads_demo.ads_emb_idx`
ON `yourproj.ads_demo.ads_emb` (emb);
