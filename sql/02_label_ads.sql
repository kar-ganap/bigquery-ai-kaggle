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

-- Generate BOTH original funnel/angles AND new strategic labels using AI  
comprehensive_labels AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title, 
    media_type,
    start_date_string,
    end_date_string,
    start_timestamp,
    end_timestamp,
    first_seen,
    last_seen,
    active_days,
    publisher_platforms,
    -- Extract structured fields from AI analysis
    ai_analysis.funnel,
    ai_analysis.angles,
    ai_analysis.promotional_intensity,
    ai_analysis.urgency_score,
    ai_analysis.brand_voice_score
  FROM (
    SELECT 
      *,
      -- Generate structured AI analysis using AI.GENERATE_TABLE
      (
        SELECT AS STRUCT 
          funnel,
          angles, 
          promotional_intensity,
          urgency_score,
          brand_voice_score
        FROM AI.GENERATE_TABLE(
          MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
          (
            SELECT 
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
          ),
          STRUCT(
            "funnel STRING, angles ARRAY<STRING>, promotional_intensity FLOAT64, urgency_score FLOAT64, brand_voice_score FLOAT64" AS output_schema,
            "SHARED" AS request_type
          )
        )
        LIMIT 1
      ) AS ai_analysis
    FROM duration_enriched
    WHERE creative_text IS NOT NULL OR title IS NOT NULL
  )
)

SELECT 
  ad_archive_id,
  brand,
  creative_text, 
  title,
  media_type,
  start_date_string,
  end_date_string,
  start_timestamp,
  end_timestamp,
  first_seen,
  last_seen,
  active_days,
  publisher_platforms,
  -- Original intelligence fields (for "where are we" analysis)
  funnel,
  angles,
  -- New temporal intelligence fields (for sophisticated analysis)
  promotional_intensity,
  urgency_score,
  brand_voice_score
FROM comprehensive_labels;
