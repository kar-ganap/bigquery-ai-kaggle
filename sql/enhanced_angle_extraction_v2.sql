-- Enhanced Message Angle Extraction v2
-- Updated to match CRAWL_SUBGOALS.md spec: ARRAY<STRING> angles with confidence scoring
-- Replaces single angle approach with multi-angle classification

CREATE OR REPLACE TABLE `your-project.ads_demo.ads_strategic_labels_v2` AS
WITH angle_extraction AS (
  SELECT 
    ad_id,
    brand,
    creative_text,
    title,
    media_type,
    start_timestamp,
    
    -- Use ML.GENERATE_TEXT for enhanced multi-angle extraction
    ML.GENERATE_TEXT(
      MODEL `your-project.ads_demo.gemini-2.5-flash`,
      STRUCT(
        CONCAT(
          'Analyze this ad and extract ALL applicable message angles with confidence scores.\n\n',
          'Ad Text: "', COALESCE(creative_text, ''), ' ', COALESCE(title, ''), '"\n\n',
          'Extract multiple angles that apply (ads often have multiple angles).\n',
          'Return ONLY valid JSON in this exact format:\n',
          '{\n',
          '  "angles": [\n',
          '    {"angle": "PROMOTIONAL", "confidence": 0.95},\n',
          '    {"angle": "EMOTIONAL", "confidence": 0.72},\n',
          '    {"angle": "URGENCY", "confidence": 0.88}\n',
          '  ],\n',
          '  "primary_angle": "PROMOTIONAL",\n',
          '  "funnel_stage": "Lower",\n',
          '  "persona": "General Market",\n',
          '  "topics": ["discount", "sale", "limited time"],\n',
          '  "urgency_score": 0.88,\n',
          '  "promotional_intensity": 0.92,\n',
          '  "brand_voice_score": 0.65\n',
          '}\n\n',
          'Valid angles: PROMOTIONAL, EMOTIONAL, RATIONAL, URGENCY, TRUST, SOCIAL_PROOF, SCARCITY, BENEFIT_FOCUSED, FEATURE_FOCUSED, ASPIRATIONAL\n',
          'Funnel stages: Upper, Mid, Lower\n',
          'Personas: New Customer, Existing Customer, General Market\n',
          'All scores 0.0-1.0. Topics as array of key themes.'
        ) AS prompt
      )
    ) AS raw_response

  FROM `your-project.ads_demo.ads_with_dates`
  WHERE (creative_text IS NOT NULL OR title IS NOT NULL)
    AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
),

parsed_angles AS (
  SELECT 
    ad_id,
    brand,
    creative_text,
    title,
    media_type,
    start_timestamp,
    raw_response,
    
    -- Clean and parse JSON response
    SAFE.PARSE_JSON(
      REGEXP_REPLACE(
        REGEXP_REPLACE(raw_response, r'^```json\s*', ''),
        r'\s*```$', ''
      )
    ) AS parsed_json

  FROM angle_extraction
),

structured_angles AS (
  SELECT 
    ad_id,
    brand,
    creative_text,
    title,
    media_type,
    start_timestamp,
    
    -- Extract angles array as required by spec
    ARRAY(
      SELECT angle_obj.angle
      FROM UNNEST(JSON_EXTRACT_ARRAY(parsed_json, '$.angles')) AS angle_data
      CROSS JOIN UNNEST([STRUCT(
        JSON_UNQUOTE(JSON_EXTRACT(angle_data, '$.angle')) AS angle,
        SAFE_CAST(JSON_UNQUOTE(JSON_EXTRACT(angle_data, '$.confidence')) AS FLOAT64) AS confidence
      )]) AS angle_obj
      WHERE angle_obj.confidence >= 0.5  -- Only include angles with decent confidence
    ) AS angles,
    
    -- Primary angle for backwards compatibility
    JSON_UNQUOTE(JSON_EXTRACT(parsed_json, '$.primary_angle')) AS primary_angle,
    
    -- Strategic classifications as per spec
    JSON_UNQUOTE(JSON_EXTRACT(parsed_json, '$.funnel_stage')) AS funnel,
    JSON_UNQUOTE(JSON_EXTRACT(parsed_json, '$.persona')) AS persona,
    
    -- Topics array as required by spec
    ARRAY(
      SELECT JSON_UNQUOTE(topic)
      FROM UNNEST(JSON_EXTRACT_ARRAY(parsed_json, '$.topics')) AS topic
    ) AS topics,
    
    -- Scoring metrics as per spec schema
    SAFE_CAST(JSON_UNQUOTE(JSON_EXTRACT(parsed_json, '$.urgency_score')) AS FLOAT64) AS urgency_score,
    SAFE_CAST(JSON_UNQUOTE(JSON_EXTRACT(parsed_json, '$.promotional_intensity')) AS FLOAT64) AS promotional_intensity,
    SAFE_CAST(JSON_UNQUOTE(JSON_EXTRACT(parsed_json, '$.brand_voice_score')) AS FLOAT64) AS brand_voice_score,
    
    -- Additional angle confidence metrics
    ARRAY(
      SELECT STRUCT(
        angle_obj.angle AS angle,
        angle_obj.confidence AS confidence
      )
      FROM UNNEST(JSON_EXTRACT_ARRAY(parsed_json, '$.angles')) AS angle_data
      CROSS JOIN UNNEST([STRUCT(
        JSON_UNQUOTE(JSON_EXTRACT(angle_data, '$.angle')) AS angle,
        SAFE_CAST(JSON_UNQUOTE(JSON_EXTRACT(angle_data, '$.confidence')) AS FLOAT64) AS confidence
      )]) AS angle_obj
      WHERE angle_obj.confidence >= 0.3  -- Include all angles for analysis
    ) AS angle_confidence_details

  FROM parsed_angles
  WHERE parsed_json IS NOT NULL
)

SELECT 
  ad_id,
  brand,
  creative_text,
  title,
  media_type,
  start_timestamp,
  
  -- Core classification matching CRAWL_SUBGOALS.md schema exactly
  funnel,
  angles,  -- ARRAY<STRING> as specified
  persona,
  topics,  -- ARRAY<STRING> as specified
  urgency_score,
  promotional_intensity,
  brand_voice_score,
  
  -- Additional fields for analysis
  primary_angle,
  angle_confidence_details,
  ARRAY_LENGTH(angles) AS angle_count,
  
  -- Quality indicators
  CASE 
    WHEN ARRAY_LENGTH(angles) = 0 THEN 'NO_ANGLES_DETECTED'
    WHEN ARRAY_LENGTH(angles) = 1 THEN 'SINGLE_ANGLE'  
    WHEN ARRAY_LENGTH(angles) <= 3 THEN 'FOCUSED_MULTI_ANGLE'
    ELSE 'COMPLEX_MULTI_ANGLE'
  END AS angle_complexity,
  
  -- Week for time-series integration
  DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week_start,
  
  -- Analysis timestamp
  CURRENT_TIMESTAMP() AS classified_at

FROM structured_angles
WHERE 
  funnel IS NOT NULL 
  AND angles IS NOT NULL 
  AND ARRAY_LENGTH(angles) > 0
ORDER BY brand, start_timestamp DESC;

-- Create required views matching CRAWL_SUBGOALS.md specification exactly

-- 1. v_strategy_evolution - Time-series strategy tracking
CREATE OR REPLACE VIEW `your-project.ads_demo.v_strategy_evolution` AS
WITH weekly_strategy_evolution AS (
  SELECT 
    brand,
    week_start,
    
    -- Volume metrics
    COUNT(*) AS total_ads,
    COUNT(DISTINCT ad_id) AS unique_ads,
    
    -- Funnel evolution tracking as specified
    COUNTIF(funnel = 'Upper') / COUNT(*) * 100 AS upper_funnel_pct,
    COUNTIF(funnel = 'Mid') / COUNT(*) * 100 AS mid_funnel_pct,  
    COUNTIF(funnel = 'Lower') / COUNT(*) * 100 AS lower_funnel_pct,
    
    -- Message angle trend detection as specified
    -- Most common angles this week
    ARRAY_AGG(DISTINCT angle ORDER BY angle) AS all_angles_this_week,
    
    -- Count frequency of each angle type
    COUNTIF('PROMOTIONAL' IN UNNEST(angles)) / COUNT(*) * 100 AS promotional_angle_pct,
    COUNTIF('EMOTIONAL' IN UNNEST(angles)) / COUNT(*) * 100 AS emotional_angle_pct,
    COUNTIF('RATIONAL' IN UNNEST(angles)) / COUNT(*) * 100 AS rational_angle_pct,
    COUNTIF('URGENCY' IN UNNEST(angles)) / COUNT(*) * 100 AS urgency_angle_pct,
    COUNTIF('TRUST' IN UNNEST(angles)) / COUNT(*) * 100 AS trust_angle_pct,
    
    -- Promotional intensity cycles as specified
    AVG(promotional_intensity) AS avg_promotional_intensity,
    AVG(urgency_score) AS avg_urgency_score,
    AVG(brand_voice_score) AS avg_brand_voice_score,
    
    -- Persona evolution
    COUNTIF(persona = 'New Customer') / COUNT(*) * 100 AS new_customer_pct,
    COUNTIF(persona = 'Existing Customer') / COUNT(*) * 100 AS existing_customer_pct,
    COUNTIF(persona = 'General Market') / COUNT(*) * 100 AS general_market_pct

  FROM `your-project.ads_demo.ads_strategic_labels_v2`
  CROSS JOIN UNNEST(angles) AS angle  -- Expand angles array for analysis
  GROUP BY brand, week_start
),

evolution_with_changes AS (
  SELECT 
    *,
    
    -- Week-over-week changes for strategy shift detection
    upper_funnel_pct - LAG(upper_funnel_pct) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS upper_funnel_change_wow,
    
    promotional_angle_pct - LAG(promotional_angle_pct) OVER (
      PARTITION BY brand ORDER BY week_start
    ) AS promotional_angle_change_wow,
    
    avg_promotional_intensity - LAG(avg_promotional_intensity) OVER (
      PARTITION BY brand ORDER BY week_start  
    ) AS promotional_intensity_change_wow,
    
    -- Detect significant strategy shifts as specified
    CASE 
      WHEN ABS(upper_funnel_pct - LAG(upper_funnel_pct) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 20 THEN 'MAJOR_FUNNEL_SHIFT'
      WHEN ABS(promotional_angle_pct - LAG(promotional_angle_pct) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 25 THEN 'MAJOR_ANGLE_SHIFT'  
      WHEN ABS(avg_promotional_intensity - LAG(avg_promotional_intensity) OVER (
        PARTITION BY brand ORDER BY week_start
      )) >= 0.3 THEN 'PROMOTIONAL_INTENSITY_SHIFT'
      ELSE 'STABLE_STRATEGY'
    END AS strategy_change_type

  FROM weekly_strategy_evolution
)

SELECT * FROM evolution_with_changes
ORDER BY brand, week_start DESC;

-- 2. v_creative_fatigue - Content repetition analysis  
CREATE OR REPLACE VIEW `your-project.ads_demo.v_creative_fatigue` AS
WITH angle_repetition_analysis AS (
  SELECT 
    brand,
    week_start,
    angle,
    
    -- Repetition metrics
    COUNT(*) AS angle_usage_count,
    COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY brand, week_start) * 100 AS angle_usage_pct,
    
    -- Historical usage for fatigue detection
    AVG(COUNT(*)) OVER (
      PARTITION BY brand, angle 
      ORDER BY week_start 
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS avg_historical_usage,
    
    -- Fatigue scoring
    CASE 
      WHEN COUNT(*) > 2 * AVG(COUNT(*)) OVER (
        PARTITION BY brand, angle 
        ORDER BY week_start 
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
      ) THEN 'HIGH_FATIGUE_RISK'
      WHEN COUNT(*) > 1.5 * AVG(COUNT(*)) OVER (
        PARTITION BY brand, angle 
        ORDER BY week_start 
        ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING  
      ) THEN 'MODERATE_FATIGUE_RISK'
      ELSE 'LOW_FATIGUE_RISK'
    END AS fatigue_risk_level

  FROM `your-project.ads_demo.ads_strategic_labels_v2`
  CROSS JOIN UNNEST(angles) AS angle
  GROUP BY brand, week_start, angle
),

creative_refresh_recommendations AS (
  SELECT 
    brand,
    week_start,
    
    -- Overall fatigue assessment
    COUNTIF(fatigue_risk_level = 'HIGH_FATIGUE_RISK') AS high_fatigue_angles,
    COUNTIF(fatigue_risk_level = 'MODERATE_FATIGUE_RISK') AS moderate_fatigue_angles,
    
    -- Most overused angles
    ARRAY_AGG(
      CASE WHEN fatigue_risk_level = 'HIGH_FATIGUE_RISK' THEN angle ELSE NULL END 
      IGNORE NULLS
    ) AS overused_angles,
    
    -- Recommendations
    CASE 
      WHEN COUNTIF(fatigue_risk_level = 'HIGH_FATIGUE_RISK') >= 2 
      THEN 'URGENT: Diversify creative angles - multiple angles showing fatigue'
      WHEN COUNTIF(fatigue_risk_level = 'HIGH_FATIGUE_RISK') = 1
      THEN 'MODERATE: Consider refreshing overused angle'  
      WHEN COUNTIF(fatigue_risk_level = 'MODERATE_FATIGUE_RISK') >= 3
      THEN 'LOW: Monitor angle usage to prevent fatigue'
      ELSE 'HEALTHY: Good angle diversity'
    END AS creative_refresh_recommendation

  FROM angle_repetition_analysis
  GROUP BY brand, week_start
)

SELECT 
  ar.*,
  crr.high_fatigue_angles,
  crr.moderate_fatigue_angles,
  crr.overused_angles,
  crr.creative_refresh_recommendation
FROM angle_repetition_analysis ar
LEFT JOIN creative_refresh_recommendations crr 
  USING (brand, week_start)
ORDER BY brand, week_start DESC, fatigue_risk_level DESC;

-- Validation query for new schema
SELECT 
  'ENHANCED_SCHEMA_VALIDATION' AS test_name,
  COUNT(*) AS total_ads_with_angles,
  COUNT(DISTINCT brand) AS unique_brands,
  AVG(ARRAY_LENGTH(angles)) AS avg_angles_per_ad,
  AVG(ARRAY_LENGTH(topics)) AS avg_topics_per_ad,
  COUNTIF(ARRAY_LENGTH(angles) > 1) AS ads_with_multiple_angles,
  COUNTIF(funnel IS NOT NULL) AS ads_with_funnel_classification,
  COUNTIF(persona IS NOT NULL) AS ads_with_persona_classification
FROM `your-project.ads_demo.ads_strategic_labels_v2`;