CREATE SCHEMA IF NOT EXISTS `yourproj.ads_demo`;

CREATE OR REPLACE TABLE `yourproj.ads_demo.ads_raw` (
  ad_id STRING,
  brand STRING,
  page_name STRING,
  creative_text STRING,
  title STRING,
  media_type STRING,
  first_seen DATE,
  last_seen DATE,
  snapshot_url STRING,
  landing_url STRING,
  card_index INT64,
  image_url STRING,
  video_url STRING,
  display_format STRING,
  publisher_platforms STRING
)
PARTITION BY first_seen
CLUSTER BY brand, media_type;
