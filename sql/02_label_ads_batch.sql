-- BATCH OPTIMIZED VERSION WITH INTELLIGENT DEDUPLICATION
-- Handles API variability by merging new ads with existing ads_with_dates
-- Prefers new data while preserving strategic labels from existing data
CREATE OR REPLACE TABLE `yourproj.ads_demo.ads_with_dates` AS

WITH all_raw_ads AS (
  -- New ads from current ingestion run - PRESERVE ALL CORE INVIOLABLE FIELDS
  SELECT
    -- Core identification fields
    ad_archive_id,
    brand,

    -- Content fields (inviolable - from API)
    creative_text,
    title,
    cta_text,  -- CRITICAL: CTA text for CTA analysis

    -- Media fields (inviolable - from API)
    -- Use new computed media type (with fallback to old field)
    COALESCE(computed_media_type, media_type, 'unknown') AS media_type,
    media_storage_path,

    -- Temporal fields (inviolable - from API)
    start_date_string,
    end_date_string,

    -- Platform and metadata fields (inviolable - from API)
    publisher_platforms,  -- CRITICAL: Used in channel intelligence
    page_name,           -- PRESERVE: Err on side of caution
    snapshot_url,        -- PRESERVE: Err on side of caution
    -- Keep legacy fields for backwards compatibility with existing logic
    CASE
      WHEN computed_media_type IN ('image', 'video') AND media_storage_path IS NOT NULL
      THEN [media_storage_path]
      ELSE []
    END AS image_urls,
    CASE
      WHEN computed_media_type = 'video' AND media_storage_path IS NOT NULL
      THEN [media_storage_path]
      ELSE []
    END AS video_urls,
    'current' AS source_type
  FROM `yourproj.ads_demo.ads_raw`
  WHERE (creative_text IS NOT NULL OR title IS NOT NULL)

  -- START_UNION_SECTION_FOR_EXISTING_DATA
  UNION ALL

  -- Existing ads with strategic labels (if table exists) - PRESERVE ALL CORE FIELDS
  SELECT
    -- Core identification fields
    ad_archive_id,
    brand,

    -- Content fields (inviolable - from API)
    creative_text,
    title,
    cta_text,  -- CRITICAL: CTA text for CTA analysis

    -- Media fields (inviolable - from API)
    media_type,
    media_storage_path,

    -- Temporal fields (inviolable - from API)
    start_date_string,
    end_date_string,

    -- Platform and metadata fields (inviolable - from API)
    publisher_platforms,  -- CRITICAL: Used in channel intelligence
    page_name,           -- PRESERVE: Err on side of caution
    snapshot_url,        -- PRESERVE: Err on side of caution
    image_urls,
    video_urls,
    'existing' AS source_type
  FROM `yourproj.ads_demo.ads_with_dates`
  WHERE 1=1  -- This will fail gracefully if table doesn't exist
  -- END_UNION_SECTION_FOR_EXISTING_DATA
),

deduplicated_ads AS (
  SELECT * EXCEPT(source_type, row_rank)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY ad_archive_id
        ORDER BY
          -- Prefer current run data (newest from API)
          CASE WHEN source_type = 'current' THEN 1 ELSE 2 END,
          -- Secondary sort by brand for consistency
          brand
      ) AS row_rank
    FROM all_raw_ads
    WHERE ad_archive_id IS NOT NULL
  )
  WHERE row_rank = 1
),

base_ads AS (
  SELECT * FROM deduplicated_ads
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
          '1. funnel: EXACTLY one of these three values (case-sensitive): "Upper", "Mid", or "Lower"\n',
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
  de.cta_text,              -- CRITICAL: CTA text for CTA analysis
  de.media_type,
  de.media_storage_path,
  de.start_date_string,
  de.end_date_string,
  de.start_timestamp,
  de.end_timestamp,
  de.first_seen,
  de.last_seen,
  de.active_days,
  de.publisher_platforms,
  de.page_name,             -- CRITICAL: Page name field
  de.snapshot_url,          -- CRITICAL: Snapshot URL field
  -- Multimodal fields for visual intelligence
  de.image_urls,
  de.video_urls,
  -- AI-generated intelligence fields (with normalization)
  CASE
    WHEN UPPER(ai.funnel) LIKE 'UPPER%' THEN 'Upper'
    WHEN UPPER(ai.funnel) LIKE 'MID%' THEN 'Mid'
    WHEN UPPER(ai.funnel) LIKE 'LOWER%' THEN 'Lower'
    ELSE ai.funnel
  END AS funnel,
  ai.angles,
  ai.promotional_intensity,
  ai.urgency_score,
  ai.brand_voice_score
FROM duration_enriched de
LEFT JOIN ai_batch_results ai
  ON de.ad_archive_id = ai.ad_archive_id;