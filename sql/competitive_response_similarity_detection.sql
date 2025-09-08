-- Competitive Response System: Content Similarity Spike Detection
-- Detects when competitors copy each other using embedding similarity >0.85 within 2 weeks
-- Meets CRAWL_SUBGOALS.md checkpoint requirements

-- Step 1: Generate embeddings for all ad creative content if not exists
CREATE OR REPLACE TABLE `your-project.ads_demo.ads_with_embeddings` AS
SELECT 
  ad_id,
  brand,
  creative_text,
  title,
  start_timestamp,
  end_timestamp,
  active_days,
  
  -- Combine text for embedding
  COALESCE(creative_text, '') || ' ' || COALESCE(title, '') AS combined_text,
  
  -- Generate embeddings using ML.GENERATE_EMBEDDING
  ML.GENERATE_EMBEDDING(
    MODEL `your-project.ads_demo.text_embedding_model`,
    COALESCE(creative_text, '') || ' ' || COALESCE(title, '')
  ) AS content_embedding

FROM `your-project.ads_demo.ads_with_dates`
WHERE 
  (creative_text IS NOT NULL OR title IS NOT NULL)
  AND LENGTH(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) >= 10
  AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 MONTH);

-- Step 2: Calculate pairwise similarity between all ads across different brands
CREATE OR REPLACE TABLE `your-project.ads_demo.cross_brand_similarity_analysis` AS
WITH brand_pairs AS (
  -- Generate all possible brand pairs for comparison
  SELECT DISTINCT 
    a1.brand AS brand_a,
    a2.brand AS brand_b
  FROM `your-project.ads_demo.ads_with_embeddings` a1
  CROSS JOIN `your-project.ads_demo.ads_with_embeddings` a2
  WHERE a1.brand != a2.brand
),

pairwise_similarity AS (
  SELECT 
    a1.ad_id AS ad_id_a,
    a1.brand AS brand_a,
    a1.combined_text AS text_a,
    a1.start_timestamp AS start_timestamp_a,
    a1.active_days AS active_days_a,
    
    a2.ad_id AS ad_id_b,
    a2.brand AS brand_b, 
    a2.combined_text AS text_b,
    a2.start_timestamp AS start_timestamp_b,
    a2.active_days AS active_days_b,
    
    -- Calculate cosine similarity between embeddings
    ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) AS cosine_similarity,
    
    -- Time difference analysis
    TIMESTAMP_DIFF(a2.start_timestamp, a1.start_timestamp, DAY) AS days_between_launches,
    ABS(TIMESTAMP_DIFF(a2.start_timestamp, a1.start_timestamp, DAY)) AS abs_days_between,
    
    -- Determine potential copying direction based on timing
    CASE 
      WHEN TIMESTAMP_DIFF(a2.start_timestamp, a1.start_timestamp, DAY) > 0 
      THEN 'B_AFTER_A'  -- Brand B launched after Brand A
      WHEN TIMESTAMP_DIFF(a2.start_timestamp, a1.start_timestamp, DAY) < 0 
      THEN 'A_AFTER_B'  -- Brand A launched after Brand B
      ELSE 'SIMULTANEOUS'
    END AS timing_relationship

  FROM `your-project.ads_demo.ads_with_embeddings` a1
  JOIN `your-project.ads_demo.ads_with_embeddings` a2 
    ON a1.brand != a2.brand  -- Only cross-brand comparisons
  WHERE 
    -- Performance optimization: only compare ads within reasonable time windows
    ABS(TIMESTAMP_DIFF(a2.start_timestamp, a1.start_timestamp, DAY)) <= 60
)

SELECT 
  *,
  
  -- Copying likelihood scoring
  CASE 
    WHEN cosine_similarity >= 0.90 AND abs_days_between <= 7 THEN 'HIGHLY_LIKELY_COPYING'
    WHEN cosine_similarity >= 0.85 AND abs_days_between <= 14 THEN 'LIKELY_COPYING'
    WHEN cosine_similarity >= 0.80 AND abs_days_between <= 21 THEN 'POSSIBLE_COPYING'
    WHEN cosine_similarity >= 0.75 AND abs_days_between <= 30 THEN 'WEAK_COPYING_SIGNAL'
    ELSE 'NO_COPYING_DETECTED'
  END AS copying_likelihood,
  
  -- Quality weighting based on campaign duration (longer campaigns more likely to be copied)
  CASE 
    WHEN active_days_a >= 14 AND active_days_b >= 7 THEN 'HIGH_QUALITY_SIGNAL'
    WHEN active_days_a >= 7 AND active_days_b >= 3 THEN 'MEDIUM_QUALITY_SIGNAL'
    ELSE 'LOW_QUALITY_SIGNAL'
  END AS signal_quality,
  
  -- Strategic context
  CASE 
    WHEN timing_relationship = 'B_AFTER_A' THEN CONCAT(brand_b, ' potentially copied ', brand_a)
    WHEN timing_relationship = 'A_AFTER_B' THEN CONCAT(brand_a, ' potentially copied ', brand_b) 
    ELSE 'Simultaneous launch - no clear copying direction'
  END AS copying_hypothesis

FROM pairwise_similarity
WHERE cosine_similarity >= 0.75  -- Only keep potential copying cases
ORDER BY cosine_similarity DESC, abs_days_between ASC;

-- Step 3: Similarity spike detection - identify sudden increases in similarity
CREATE OR REPLACE TABLE `your-project.ads_demo.similarity_spike_detection` AS
WITH weekly_similarity_baseline AS (
  -- Calculate baseline similarity levels between brand pairs
  SELECT 
    brand_a,
    brand_b,
    DATE_TRUNC(DATE(start_timestamp_a), WEEK(MONDAY)) AS week_start,
    
    AVG(cosine_similarity) AS avg_weekly_similarity,
    MAX(cosine_similarity) AS max_weekly_similarity,
    COUNT(*) AS comparisons_count,
    
    -- Calculate rolling 4-week baseline
    AVG(AVG(cosine_similarity)) OVER (
      PARTITION BY brand_a, brand_b 
      ORDER BY DATE_TRUNC(DATE(start_timestamp_a), WEEK(MONDAY))
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS rolling_baseline_similarity

  FROM `your-project.ads_demo.cross_brand_similarity_analysis`
  GROUP BY brand_a, brand_b, week_start
),

spike_detection AS (
  SELECT 
    *,
    
    -- Spike detection: current week similarity significantly above baseline
    CASE 
      WHEN max_weekly_similarity >= 0.85 
           AND rolling_baseline_similarity IS NOT NULL
           AND max_weekly_similarity > rolling_baseline_similarity + 0.20 
      THEN 'SIGNIFICANT_SIMILARITY_SPIKE'
      WHEN max_weekly_similarity >= 0.80 
           AND rolling_baseline_similarity IS NOT NULL
           AND max_weekly_similarity > rolling_baseline_similarity + 0.15 
      THEN 'MODERATE_SIMILARITY_SPIKE'
      WHEN max_weekly_similarity >= 0.85 
           AND rolling_baseline_similarity IS NULL  -- New brand pair relationship
      THEN 'HIGH_SIMILARITY_NEW_RELATIONSHIP'
      ELSE 'NO_SPIKE_DETECTED'
    END AS spike_type,
    
    -- Spike magnitude
    CASE 
      WHEN rolling_baseline_similarity IS NOT NULL 
      THEN max_weekly_similarity - rolling_baseline_similarity
      ELSE NULL
    END AS spike_magnitude

  FROM weekly_similarity_baseline
)

SELECT 
  *,
  
  -- Alert prioritization
  CASE 
    WHEN spike_type = 'SIGNIFICANT_SIMILARITY_SPIKE' AND spike_magnitude >= 0.25 
    THEN 'HIGH_PRIORITY_ALERT'
    WHEN spike_type IN ('SIGNIFICANT_SIMILARITY_SPIKE', 'HIGH_SIMILARITY_NEW_RELATIONSHIP') 
    THEN 'MEDIUM_PRIORITY_ALERT'
    WHEN spike_type = 'MODERATE_SIMILARITY_SPIKE' 
    THEN 'LOW_PRIORITY_ALERT'
    ELSE 'NO_ALERT'
  END AS alert_priority

FROM spike_detection
WHERE spike_type != 'NO_SPIKE_DETECTED'
ORDER BY week_start DESC, spike_magnitude DESC NULLS LAST;

-- Step 4: Strategic response recommendations based on competitor moves
CREATE OR REPLACE VIEW `your-project.ads_demo.v_competitive_responses` AS
WITH recent_copying_events AS (
  SELECT 
    brand_a,
    brand_b,
    copying_likelihood,
    copying_hypothesis,
    cosine_similarity,
    abs_days_between,
    start_timestamp_a,
    start_timestamp_b,
    text_a,
    text_b
  FROM `your-project.ads_demo.cross_brand_similarity_analysis`
  WHERE copying_likelihood IN ('HIGHLY_LIKELY_COPYING', 'LIKELY_COPYING')
    AND start_timestamp_b >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)  -- Recent events
),

copying_patterns AS (
  SELECT 
    brand_a AS original_brand,
    brand_b AS copying_brand,
    COUNT(*) AS copying_incidents_30d,
    AVG(cosine_similarity) AS avg_similarity_score,
    AVG(abs_days_between) AS avg_response_time_days,
    
    -- Most recent copying event details
    ARRAY_AGG(
      STRUCT(
        copying_hypothesis,
        cosine_similarity,
        abs_days_between,
        text_a AS original_text,
        text_b AS copied_text
      ) 
      ORDER BY start_timestamp_b DESC 
      LIMIT 3
    ) AS recent_copying_examples

  FROM recent_copying_events
  GROUP BY brand_a, brand_b
),

strategic_recommendations AS (
  SELECT 
    original_brand,
    copying_brand,
    copying_incidents_30d,
    avg_similarity_score,
    avg_response_time_days,
    recent_copying_examples,
    
    -- Strategic response recommendations
    CASE 
      WHEN copying_incidents_30d >= 3 AND avg_similarity_score >= 0.88 
      THEN 'AGGRESSIVE_COPYCAT_DETECTED: Consider legal review and differentiation strategy'
      WHEN copying_incidents_30d >= 2 AND avg_response_time_days <= 7 
      THEN 'FAST_FOLLOWER_DETECTED: Accelerate creative refresh cycles'
      WHEN avg_similarity_score >= 0.90 
      THEN 'HIGH_SIMILARITY_COPYING: Review creative uniqueness and IP protection'
      WHEN copying_incidents_30d >= 2 
      THEN 'PATTERN_COPYING: Monitor closely and prepare counter-strategies'
      ELSE 'STANDARD_MONITORING: Continue regular competitive surveillance'
    END AS strategic_recommendation,
    
    -- Competitive response priority
    CASE 
      WHEN copying_incidents_30d >= 3 THEN 'HIGH_PRIORITY'
      WHEN copying_incidents_30d >= 2 OR avg_similarity_score >= 0.90 THEN 'MEDIUM_PRIORITY'
      ELSE 'LOW_PRIORITY'
    END AS response_priority

  FROM copying_patterns
)

SELECT 
  original_brand AS your_brand,
  copying_brand AS competitor_brand,
  copying_incidents_30d,
  ROUND(avg_similarity_score, 3) AS avg_similarity_score,
  ROUND(avg_response_time_days, 1) AS avg_competitor_response_time_days,
  recent_copying_examples,
  strategic_recommendation,
  response_priority,
  
  -- Actionable insights
  CASE 
    WHEN avg_response_time_days <= 5 
    THEN 'RECOMMENDATION: Implement rapid creative testing to stay ahead'
    WHEN copying_incidents_30d >= 3 
    THEN 'RECOMMENDATION: Diversify creative themes to reduce copying effectiveness'  
    WHEN avg_similarity_score >= 0.90 
    THEN 'RECOMMENDATION: Strengthen unique brand positioning and differentiation'
    ELSE 'RECOMMENDATION: Continue monitoring and maintain creative innovation'
  END AS actionable_insight

FROM strategic_recommendations
ORDER BY response_priority DESC, copying_incidents_30d DESC;

-- Validation and testing queries
SELECT 
  'SIMILARITY_DETECTION_VALIDATION' AS test_name,
  COUNT(*) AS total_similarity_comparisons,
  COUNTIF(cosine_similarity >= 0.85) AS high_similarity_cases,
  COUNTIF(copying_likelihood IN ('HIGHLY_LIKELY_COPYING', 'LIKELY_COPYING')) AS likely_copying_cases,
  COUNT(DISTINCT brand_a || '-' || brand_b) AS unique_brand_pairs,
  AVG(cosine_similarity) AS avg_similarity_score,
  MAX(cosine_similarity) AS max_similarity_found
FROM `your-project.ads_demo.cross_brand_similarity_analysis`;

SELECT 
  'SPIKE_DETECTION_VALIDATION' AS test_name,
  COUNT(*) AS total_weeks_analyzed,
  COUNTIF(spike_type != 'NO_SPIKE_DETECTED') AS weeks_with_spikes,
  COUNTIF(alert_priority = 'HIGH_PRIORITY_ALERT') AS high_priority_alerts,
  AVG(spike_magnitude) AS avg_spike_magnitude,
  MAX(spike_magnitude) AS max_spike_magnitude
FROM `your-project.ads_demo.similarity_spike_detection`;