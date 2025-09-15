-- BATCH OPTIMIZED VERSION: Process all ads in one AI.GENERATE_TABLE call
-- This reduces from 150 individual AI calls to 1 batch call
CREATE OR REPLACE TABLE `yourproj.ads_demo.ads_with_dates` AS

WITH base_ads AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title,
    media_type,
    start_date_string,
    end_date_string,
    publisher_platforms
  FROM `yourproj.ads_demo.ads_raw`
  WHERE (creative_text IS NOT NULL OR title IS NOT NULL)
),

-- Generate temporal data from timestamps  
temporal_data AS (
  SELECT *,
    -- Parse ISO timestamp strings to proper timestamps
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string) AS start_timestamp,
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string) AS end_timestamp,
    
    -- Create date fields for legacy compatibility
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string)) AS first_seen,
    DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string)) AS last_seen
  FROM base_ads
),

-- Add duration calculations
duration_enriched AS (
  SELECT *,
    CASE 
      WHEN start_timestamp IS NOT NULL AND end_timestamp IS NOT NULL 
      THEN DATE_DIFF(DATE(end_timestamp), DATE(start_timestamp), DAY) + 1
      ELSE NULL
    END AS active_days
  FROM temporal_data
),

-- BATCH PROCESSING: Generate ALL labels in one AI.GENERATE_TABLE call
ai_batch_results AS (
  SELECT 
    ad_archive_id,
    funnel,
    angles,
    promotional_intensity,
    urgency_score,
    brand_voice_score
  FROM AI.GENERATE_TABLE(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
    (
      SELECT 
        ad_archive_id,  -- CRITICAL: Preserve ID for joining back
        CONCAT(
          'Analyze this ad content and classify:\n',
          'Ad Text: ', COALESCE(creative_text, ''), '\n', 
          'Headline: ', COALESCE(title, ''), '\n',
          'Brand: ', brand, '\n\n',
          'Return structured analysis with:\n',
          '1. funnel: Upper (brand/storytelling) | Mid (consideration/benefits) | Lower (offers/urgency)\n',
          '2. angles: up to 3 from [discount,benefits,UGC,social proof,launch,brand story,urgency,feature,sustainability,guarantee,testimonial]\n',
          '3. promotional_intensity: 0.0-1.0 (0=brand-focused, 1=heavy promotion)\n',
          '4. urgency_score: 0.0-1.0 (0=no urgency, 1=very urgent)\n', 
          '5. brand_voice_score: 0.0-1.0 (0=very promotional, 1=very brand-focused)'
        ) AS prompt
      FROM duration_enriched
      WHERE creative_text IS NOT NULL OR title IS NOT NULL
    ),
    STRUCT(
      "funnel STRING, angles ARRAY<STRING>, promotional_intensity FLOAT64, urgency_score FLOAT64, brand_voice_score FLOAT64" AS output_schema,
      "SHARED" AS request_type
    )
  )
)

-- Join AI results back to original data using ad_archive_id
SELECT 
  de.ad_archive_id,
  de.brand,
  de.creative_text, 
  de.title,
  de.media_type,
  de.start_date_string,
  de.end_date_string,
  de.start_timestamp,
  de.end_timestamp,
  de.first_seen,
  de.last_seen,
  de.active_days,
  de.publisher_platforms,
  -- AI-generated intelligence fields
  ai.funnel,
  ai.angles,
  ai.promotional_intensity,
  ai.urgency_score,
  ai.brand_voice_score
FROM duration_enriched de
LEFT JOIN ai_batch_results ai
  ON de.ad_archive_id = ai.ad_archive_id;