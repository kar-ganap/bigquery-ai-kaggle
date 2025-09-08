-- Hierarchical Similarity Framework with Sparsity Diagnostics
-- Addresses curse of dimensionality while maintaining strategic relevance

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.hierarchical_similarity_analysis` AS

WITH feature_space_definition AS (
  SELECT 
    ad_id,
    brand,
    creative_text,
    start_timestamp,
    
    -- Tier 1: Core Strategic Features (Required Match)
    funnel,                    -- Upper/Mid/Lower
    persona,                  -- New Customer/Existing Customer/General Market
    
    -- Tier 2: Contextual Features (Weighted Similarity)
    media_type,               -- VIDEO/IMAGE/DCO
    CASE 
      WHEN publisher_platforms LIKE '%FACEBOOK%' AND publisher_platforms LIKE '%INSTAGRAM%' THEN 'Cross-Platform'
      WHEN publisher_platforms LIKE '%INSTAGRAM%' THEN 'Instagram-Only'
      WHEN publisher_platforms LIKE '%FACEBOOK%' THEN 'Facebook-Only'
      ELSE 'Other'
    END AS platform_strategy,
    
    -- Tier 3: Behavioral Features (Distance-based Similarity)
    COALESCE(promotional_intensity, 0.0) AS promotional_intensity,
    COALESCE(urgency_score, 0.0) AS urgency_score,
    COALESCE(brand_voice_score, 0.5) AS brand_voice_score,
    
    -- Tier 4: Temporal Context
    active_days,
    DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week_start
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` ads
  -- Join with strategic labels (using simplified version since we don't have full table)
  WHERE start_timestamp IS NOT NULL
    AND brand IS NOT NULL
    AND creative_text IS NOT NULL
),

-- Step 1: Calculate similarity scores for all ad pairs
pairwise_similarity AS (
  SELECT 
    base.ad_id AS base_ad_id,
    base.brand AS base_brand,
    base.funnel AS base_funnel,
    base.persona AS base_persona,
    
    comp.ad_id AS comp_ad_id,
    comp.brand AS comp_brand,
    comp.funnel AS comp_funnel,
    comp.persona AS comp_persona,
    
    -- Temporal relationship
    DATE_DIFF(DATE(comp.start_timestamp), DATE(base.start_timestamp), DAY) AS time_lag_days,
    
    -- Tier 1: Core Strategic Match (binary: 0 or 1)
    CASE 
      WHEN base.funnel = comp.funnel AND base.persona = comp.persona THEN 1.0 
      ELSE 0.0 
    END AS core_strategic_match,
    
    -- Tier 2: Contextual Similarity (0.0-1.0)
    (
      0.4 * CASE WHEN base.media_type = comp.media_type THEN 1.0 ELSE 0.0 END +
      0.3 * CASE WHEN base.platform_strategy = comp.platform_strategy THEN 1.0 ELSE 0.0 END +
      0.3 * CASE 
              WHEN ABS(base.active_days - comp.active_days) <= 7 THEN 1.0
              WHEN ABS(base.active_days - comp.active_days) <= 14 THEN 0.7
              WHEN ABS(base.active_days - comp.active_days) <= 30 THEN 0.4
              ELSE 0.0
            END
    ) AS contextual_similarity,
    
    -- Tier 3: Behavioral Similarity (0.0-1.0)  
    (
      0.4 * (1.0 - ABS(base.promotional_intensity - comp.promotional_intensity)) +
      0.3 * (1.0 - ABS(base.urgency_score - comp.urgency_score)) +
      0.3 * (1.0 - ABS(base.brand_voice_score - comp.brand_voice_score))
    ) AS behavioral_similarity,
    
    -- Combined Similarity Score
    CASE 
      WHEN base.funnel = comp.funnel AND base.persona = comp.persona
      THEN (
        0.5 * (
          0.4 * CASE WHEN base.media_type = comp.media_type THEN 1.0 ELSE 0.0 END +
          0.3 * CASE WHEN base.platform_strategy = comp.platform_strategy THEN 1.0 ELSE 0.0 END +
          0.3 * CASE 
                  WHEN ABS(base.active_days - comp.active_days) <= 7 THEN 1.0
                  WHEN ABS(base.active_days - comp.active_days) <= 14 THEN 0.7
                  ELSE 0.4
                END
        ) +
        0.5 * (
          0.4 * (1.0 - ABS(base.promotional_intensity - comp.promotional_intensity)) +
          0.3 * (1.0 - ABS(base.urgency_score - comp.urgency_score)) +
          0.3 * (1.0 - ABS(base.brand_voice_score - comp.brand_voice_score))
        )
      )
      ELSE 0.0
    END AS hierarchical_similarity_score
    
  FROM feature_space_definition base
  CROSS JOIN feature_space_definition comp
  WHERE base.ad_id != comp.ad_id  -- Exclude self-comparison
    AND base.brand != comp.brand  -- Cross-brand comparison only
    AND ABS(DATE_DIFF(DATE(comp.start_timestamp), DATE(base.start_timestamp), DAY)) <= 90  -- 90-day window
),

-- Step 2: Sparsity Diagnostics - Analyze data density at different similarity thresholds
sparsity_analysis AS (
  SELECT 
    'SPARSITY_ANALYSIS' AS analysis_type,
    
    -- Total potential comparisons
    COUNT(*) AS total_comparisons,
    COUNT(DISTINCT base_brand) AS unique_base_brands,
    COUNT(DISTINCT comp_brand) AS unique_comp_brands,
    
    -- Core strategic matches
    COUNTIF(core_strategic_match = 1.0) AS core_matches_available,
    COUNTIF(core_strategic_match = 1.0) / COUNT(*) * 100 AS pct_core_matches,
    
    -- Similarity threshold analysis
    COUNTIF(hierarchical_similarity_score >= 0.8) AS high_similarity_pairs,
    COUNTIF(hierarchical_similarity_score >= 0.6) AS medium_similarity_pairs,  
    COUNTIF(hierarchical_similarity_score >= 0.4) AS low_similarity_pairs,
    COUNTIF(hierarchical_similarity_score >= 0.2) AS any_similarity_pairs,
    
    -- Average comparisons per ad
    COUNTIF(hierarchical_similarity_score >= 0.6) / 
      NULLIF(COUNT(DISTINCT base_ad_id), 0) AS avg_medium_similarity_matches_per_ad,
    
    -- Temporal distribution
    AVG(ABS(time_lag_days)) AS avg_time_lag_days,
    
    -- By funnel/persona combination
    STRING_AGG(DISTINCT 
      CASE WHEN core_strategic_match = 1.0 
      THEN CONCAT(base_funnel, '+', base_persona, ' vs ', comp_funnel, '+', comp_persona)
      END, ', ' LIMIT 10
    ) AS active_strategic_combinations
    
  FROM pairwise_similarity
),

-- Step 3: Similarity-based groupings for fatigue/influence analysis
similarity_groups AS (
  SELECT 
    base_ad_id,
    base_brand,
    base_funnel,
    base_persona,
    
    -- Count of similar ads by similarity tier
    COUNT(CASE WHEN hierarchical_similarity_score >= 0.8 THEN 1 END) AS high_similarity_count,
    COUNT(CASE WHEN hierarchical_similarity_score >= 0.6 THEN 1 END) AS medium_similarity_count,
    COUNT(CASE WHEN hierarchical_similarity_score >= 0.4 THEN 1 END) AS low_similarity_count,
    
    -- Most similar competitor ads (for influence analysis)
    ARRAY_AGG(
      STRUCT(
        comp_ad_id,
        comp_brand, 
        hierarchical_similarity_score,
        time_lag_days
      ) 
      ORDER BY hierarchical_similarity_score DESC 
      LIMIT 10
    ) AS most_similar_competitor_ads,
    
    -- Time-weighted influence potential
    AVG(
      CASE WHEN hierarchical_similarity_score >= 0.6
      THEN hierarchical_similarity_score * EXP(-0.693 * ABS(time_lag_days) / 14.0)  -- 2-week half-life
      END
    ) AS avg_time_weighted_similarity,
    
    -- Sparsity flag for this specific ad
    CASE 
      WHEN COUNT(CASE WHEN hierarchical_similarity_score >= 0.6 THEN 1 END) >= 5 THEN 'SUFFICIENT_DATA'
      WHEN COUNT(CASE WHEN hierarchical_similarity_score >= 0.4 THEN 1 END) >= 3 THEN 'MARGINAL_DATA'
      ELSE 'INSUFFICIENT_DATA'
    END AS data_sufficiency_flag
    
  FROM pairwise_similarity
  WHERE core_strategic_match = 1.0  -- Only strategically relevant comparisons
  GROUP BY base_ad_id, base_brand, base_funnel, base_persona
)

-- Final output: Hierarchical similarity results with sparsity diagnostics
SELECT 
  sg.*,
  sa.total_comparisons,
  sa.pct_core_matches,
  sa.avg_medium_similarity_matches_per_ad,
  sa.avg_time_lag_days
FROM similarity_groups sg
CROSS JOIN sparsity_analysis sa
ORDER BY sg.avg_time_weighted_similarity DESC;

-- Diagnostic query: Sparsity by strategic combination
SELECT 
  base_funnel,
  base_persona,
  COUNT(DISTINCT base_ad_id) AS ads_in_category,
  AVG(medium_similarity_count) AS avg_similar_competitors,
  COUNTIF(data_sufficiency_flag = 'INSUFFICIENT_DATA') AS ads_with_insufficient_data,
  COUNTIF(data_sufficiency_flag = 'INSUFFICIENT_DATA') / COUNT(*) * 100 AS pct_insufficient_data
FROM similarity_groups
GROUP BY base_funnel, base_persona
ORDER BY pct_insufficient_data DESC;

-- Success metrics
SELECT 
  'HIERARCHICAL_SIMILARITY_SUCCESS_METRICS' AS test_name,
  
  -- Data sufficiency
  COUNTIF(data_sufficiency_flag = 'SUFFICIENT_DATA') / COUNT(*) * 100 AS pct_sufficient_data,
  COUNTIF(data_sufficiency_flag != 'INSUFFICIENT_DATA') / COUNT(*) * 100 AS pct_usable_data,
  
  -- Comparison richness  
  AVG(medium_similarity_count) AS avg_medium_similarity_matches,
  PERCENTILE_CONT(medium_similarity_count, 0.5) OVER() AS median_similarity_matches,
  
  -- Coverage
  COUNT(DISTINCT base_brand) AS brands_covered,
  COUNT(DISTINCT CONCAT(base_funnel, '_', base_persona)) AS strategic_segments_covered,
  
  -- Sparsity resolution assessment
  CASE 
    WHEN AVG(medium_similarity_count) >= 5.0 AND 
         COUNTIF(data_sufficiency_flag = 'SUFFICIENT_DATA') / COUNT(*) >= 0.70
    THEN 'SPARSITY_RESOLVED - Hierarchical similarity provides sufficient data'
    WHEN AVG(medium_similarity_count) >= 3.0 AND 
         COUNTIF(data_sufficiency_flag != 'INSUFFICIENT_DATA') / COUNT(*) >= 0.60  
    THEN 'SPARSITY_IMPROVED - Significant improvement over exact matching'
    ELSE 'SPARSITY_PERSISTS - May need further threshold adjustments'
  END AS sparsity_resolution_status
  
FROM similarity_groups;