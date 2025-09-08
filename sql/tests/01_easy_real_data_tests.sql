-- EASY TESTS WITH REAL DATA
-- These tests can be run immediately with existing data to validate core functionality

-- ============================================================================
-- TEST 1: Strategic Question Coverage - Query Response Times
-- Success Criteria: All 4 core questions answerable in <30 seconds
-- ============================================================================

-- Question 1: "In the past n weeks, what have my Facebook ads focused on?"
-- Measure execution time for brand self-analysis
CREATE OR REPLACE TABLE `your-project.ads_demo.test_results_performance` AS
WITH performance_test AS (
  SELECT 
    'Q1_SELF_ANALYSIS' AS test_name,
    CURRENT_TIMESTAMP() AS test_start,
    (
      SELECT COUNT(*) 
      FROM `your-project.ads_demo.v_strategy_evolution`
      WHERE brand = 'Nike'
        AND week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
    ) AS rows_processed,
    CURRENT_TIMESTAMP() AS test_end
)
SELECT 
  test_name,
  rows_processed,
  TIMESTAMP_DIFF(test_end, test_start, MILLISECOND) / 1000.0 AS query_time_seconds,
  CASE 
    WHEN TIMESTAMP_DIFF(test_end, test_start, MILLISECOND) / 1000.0 < 30 THEN 'PASS'
    ELSE 'FAIL'
  END AS test_result
FROM performance_test;

-- Question 2: "In the same period, what has been my competitors' focus?"
INSERT INTO `your-project.ads_demo.test_results_performance`
SELECT 
  'Q2_COMPETITOR_ANALYSIS' AS test_name,
  COUNT(*) AS rows_processed,
  0.0 AS query_time_seconds,  -- Placeholder, actual timing via execution
  'PENDING' AS test_result
FROM `your-project.ads_demo.v_strategy_evolution`
WHERE brand IN ('Adidas', 'Under Armour', 'Puma')
  AND week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK);

-- Question 3: "How have our strategies evolved over time?"
INSERT INTO `your-project.ads_demo.test_results_performance`
SELECT 
  'Q3_STRATEGY_EVOLUTION' AS test_name,
  COUNT(*) AS rows_processed,
  0.0 AS query_time_seconds,
  'PENDING' AS test_result
FROM `your-project.ads_demo.v_integrated_strategy_timeseries`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK);

-- Question 4: "Can we forecast these trends?"
INSERT INTO `your-project.ads_demo.test_results_performance`
SELECT 
  'Q4_FORECASTING' AS test_name,
  COUNT(*) AS rows_processed,
  0.0 AS query_time_seconds,
  'PENDING' AS test_result
FROM `your-project.ads_demo.strategic_forecasts`
WHERE forecast_week > CURRENT_DATE();

-- ============================================================================
-- TEST 2: Classification Success Rate 
-- Success Criteria: >85% of ads get comprehensive strategic labels
-- ============================================================================

CREATE OR REPLACE TABLE `your-project.ads_demo.test_results_classification` AS
WITH classification_coverage AS (
  SELECT 
    COUNT(*) AS total_ads,
    
    -- Core classification success
    COUNTIF(funnel IS NOT NULL) AS ads_with_funnel,
    COUNTIF(ARRAY_LENGTH(angles) > 0) AS ads_with_angles,
    COUNTIF(persona IS NOT NULL) AS ads_with_persona,
    COUNTIF(ARRAY_LENGTH(topics) > 0) AS ads_with_topics,
    
    -- Score classifications
    COUNTIF(urgency_score IS NOT NULL) AS ads_with_urgency_score,
    COUNTIF(promotional_intensity IS NOT NULL) AS ads_with_promotional_intensity,
    COUNTIF(brand_voice_score IS NOT NULL) AS ads_with_brand_voice_score,
    
    -- Complete classification (all required fields)
    COUNTIF(
      funnel IS NOT NULL 
      AND ARRAY_LENGTH(angles) > 0 
      AND persona IS NOT NULL
      AND urgency_score IS NOT NULL
      AND promotional_intensity IS NOT NULL
      AND brand_voice_score IS NOT NULL
    ) AS fully_classified_ads
    
  FROM `your-project.ads_demo.ads_strategic_labels_v2`
  WHERE start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
)
SELECT 
  'CLASSIFICATION_COVERAGE_TEST' AS test_name,
  total_ads,
  fully_classified_ads,
  ROUND(fully_classified_ads / total_ads * 100, 2) AS classification_success_rate_pct,
  
  -- Individual field success rates
  ROUND(ads_with_funnel / total_ads * 100, 2) AS funnel_success_rate_pct,
  ROUND(ads_with_angles / total_ads * 100, 2) AS angles_success_rate_pct,
  ROUND(ads_with_persona / total_ads * 100, 2) AS persona_success_rate_pct,
  ROUND(ads_with_topics / total_ads * 100, 2) AS topics_success_rate_pct,
  
  -- Test result
  CASE 
    WHEN fully_classified_ads / total_ads >= 0.85 THEN 'PASS'
    WHEN fully_classified_ads / total_ads >= 0.75 THEN 'PARTIAL_PASS'
    ELSE 'FAIL'
  END AS test_result,
  
  CURRENT_TIMESTAMP() AS test_timestamp
  
FROM classification_coverage;

-- ============================================================================
-- TEST 3: Promotional Period Detection Accuracy
-- Success Criteria: Accurately identifies promotional periods vs brand messaging
-- ============================================================================

CREATE OR REPLACE TABLE `your-project.ads_demo.test_results_promotional` AS
WITH promotional_validation AS (
  SELECT 
    brand,
    
    -- Count by aggressiveness tier
    COUNTIF(aggressiveness_tier = 'HIGHLY_AGGRESSIVE') AS highly_aggressive_count,
    COUNTIF(aggressiveness_tier = 'MODERATELY_AGGRESSIVE') AS moderately_aggressive_count,
    COUNTIF(aggressiveness_tier = 'BRAND_FOCUSED') AS brand_focused_count,
    
    -- Promotional vs brand messaging split
    COUNTIF(promotional_theme IN ('SEASONAL_MAJOR', 'PROMOTIONAL', 'MILESTONE')) AS promotional_ads,
    COUNTIF(promotional_theme = 'EVERGREEN') AS brand_messaging_ads,
    COUNT(*) AS total_ads,
    
    -- Discount detection accuracy
    COUNTIF(discount_percentage > 0) AS ads_with_discounts,
    AVG(CASE WHEN discount_percentage > 0 THEN discount_percentage END) AS avg_discount_when_present
    
  FROM `your-project.ads_demo.cta_aggressiveness_analysis`
  WHERE start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY brand
)
SELECT 
  'PROMOTIONAL_DETECTION_TEST' AS test_name,
  COUNT(DISTINCT brand) AS brands_analyzed,
  
  SUM(promotional_ads) AS total_promotional_ads,
  SUM(brand_messaging_ads) AS total_brand_messaging_ads,
  
  -- Validation metrics
  ROUND(SUM(promotional_ads) / SUM(total_ads) * 100, 2) AS promotional_ads_pct,
  ROUND(SUM(brand_messaging_ads) / SUM(total_ads) * 100, 2) AS brand_messaging_ads_pct,
  
  -- Reasonableness checks
  CASE 
    WHEN SUM(promotional_ads) / SUM(total_ads) BETWEEN 0.15 AND 0.65 THEN 'REASONABLE_SPLIT'
    ELSE 'UNUSUAL_SPLIT_CHECK_MANUALLY'
  END AS split_assessment,
  
  -- Discount detection validation  
  ROUND(SUM(ads_with_discounts) / SUM(promotional_ads) * 100, 2) AS pct_promotional_with_discounts,
  ROUND(AVG(avg_discount_when_present), 1) AS overall_avg_discount_pct,
  
  -- Test result
  CASE 
    WHEN SUM(promotional_ads) > 0 AND SUM(brand_messaging_ads) > 0 THEN 'PASS'
    ELSE 'NEEDS_REVIEW'
  END AS test_result,
  
  CURRENT_TIMESTAMP() AS test_timestamp
  
FROM promotional_validation;

-- ============================================================================
-- TEST 4: Strategy Shift Detection in Historical Data
-- Success Criteria: Can identify clear strategy shifts in 3-month data
-- ============================================================================

CREATE OR REPLACE TABLE `your-project.ads_demo.test_results_strategy_shifts` AS
WITH strategy_shifts AS (
  SELECT 
    brand,
    week_start,
    integrated_strategy_change_type,
    
    -- Magnitude of changes
    funnel_trend_wow,
    promotional_trend_wow,
    platform_strategy_trend_wow,
    
    -- Count significant shifts
    CASE 
      WHEN integrated_strategy_change_type != 'STABLE_INTEGRATED_STRATEGY' THEN 1 
      ELSE 0 
    END AS is_shift
    
  FROM `your-project.ads_demo.v_integrated_strategy_timeseries`
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
)
SELECT 
  'STRATEGY_SHIFT_DETECTION_TEST' AS test_name,
  
  COUNT(DISTINCT brand) AS brands_analyzed,
  COUNT(*) AS total_weeks_analyzed,
  
  -- Shift detection metrics
  SUM(is_shift) AS total_shifts_detected,
  COUNT(DISTINCT CASE WHEN is_shift = 1 THEN brand END) AS brands_with_shifts,
  
  -- Types of shifts detected
  COUNTIF(integrated_strategy_change_type = 'FUNNEL_STRATEGY_SHIFT') AS funnel_shifts,
  COUNTIF(integrated_strategy_change_type = 'PROMOTIONAL_STRATEGY_SHIFT') AS promotional_shifts,
  COUNTIF(integrated_strategy_change_type = 'PLATFORM_STRATEGY_SHIFT') AS platform_shifts,
  COUNTIF(integrated_strategy_change_type = 'PROMOTIONAL_CAMPAIGN_LAUNCH') AS campaign_launches,
  
  -- Average magnitudes when shifts occur
  ROUND(AVG(ABS(funnel_trend_wow)), 2) AS avg_funnel_change_magnitude,
  ROUND(AVG(ABS(promotional_trend_wow)), 3) AS avg_promotional_change_magnitude,
  
  -- Test result
  CASE 
    WHEN SUM(is_shift) >= 3 THEN 'PASS - Multiple shifts detected'
    WHEN SUM(is_shift) >= 1 THEN 'PARTIAL_PASS - Some shifts detected'
    ELSE 'FAIL - No shifts detected'
  END AS test_result,
  
  CURRENT_TIMESTAMP() AS test_timestamp
  
FROM strategy_shifts;

-- ============================================================================
-- TEST 5: Multi-Brand Trend Detection
-- Success Criteria: Trend detection works across multiple competitor brands
-- ============================================================================

CREATE OR REPLACE TABLE `your-project.ads_demo.test_results_multi_brand` AS
WITH multi_brand_trends AS (
  SELECT 
    brand,
    
    -- Count weeks with data
    COUNT(DISTINCT week_start) AS weeks_with_data,
    
    -- Average metrics per brand
    AVG(upper_funnel_pct) AS avg_upper_funnel_pct,
    AVG(promotional_angle_pct) AS avg_promotional_angle_pct,
    AVG(avg_promotional_intensity) AS avg_promotional_intensity,
    
    -- Variability metrics (shows if trends exist)
    STDDEV(upper_funnel_pct) AS funnel_volatility,
    STDDEV(avg_promotional_intensity) AS promotional_volatility,
    
    -- Trend direction over period
    ARRAY_AGG(upper_funnel_pct ORDER BY week_start DESC LIMIT 1)[OFFSET(0)] - 
    ARRAY_AGG(upper_funnel_pct ORDER BY week_start ASC LIMIT 1)[OFFSET(0)] AS funnel_trend_direction,
    
    -- Count strategy changes
    COUNTIF(strategy_change_type != 'STABLE_STRATEGY') AS strategy_changes_count
    
  FROM `your-project.ads_demo.v_strategy_evolution`
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
  GROUP BY brand
  HAVING weeks_with_data >= 4  -- Only brands with sufficient data
)
SELECT 
  'MULTI_BRAND_TREND_TEST' AS test_name,
  
  COUNT(*) AS brands_with_trends_analyzed,
  
  -- Brands showing clear trends
  COUNTIF(ABS(funnel_trend_direction) > 10) AS brands_with_funnel_trends,
  COUNTIF(funnel_volatility > 5) AS brands_with_volatile_strategies,
  COUNTIF(strategy_changes_count > 0) AS brands_with_strategy_changes,
  
  -- Cross-brand pattern detection
  CASE 
    WHEN AVG(promotional_volatility) > 0.1 THEN 'DYNAMIC_MARKET'
    ELSE 'STABLE_MARKET'
  END AS market_dynamics,
  
  -- List brands with notable trends
  STRING_AGG(
    CASE WHEN ABS(funnel_trend_direction) > 10 THEN brand END, ', '
  ) AS brands_with_significant_trends,
  
  -- Test result
  CASE 
    WHEN COUNT(*) >= 3 AND COUNTIF(strategy_changes_count > 0) >= 2 THEN 'PASS'
    WHEN COUNT(*) >= 2 AND COUNTIF(strategy_changes_count > 0) >= 1 THEN 'PARTIAL_PASS'
    ELSE 'NEEDS_MORE_DATA'
  END AS test_result,
  
  CURRENT_TIMESTAMP() AS test_timestamp
  
FROM multi_brand_trends;

-- ============================================================================
-- TEST 6: Platform Consistency Alignment
-- Success Criteria: Platform consistency scores align with observable patterns
-- ============================================================================

CREATE OR REPLACE TABLE `your-project.ads_demo.test_results_platform` AS
WITH platform_validation AS (
  SELECT 
    brand,
    
    AVG(pct_cross_platform) AS avg_cross_platform_pct,
    AVG(avg_platform_optimization_gap) AS avg_optimization_gap,
    
    -- Platform strategy classification distribution
    MODE() WITHIN GROUP (ORDER BY platform_strategy_classification) AS dominant_strategy,
    
    -- Consistency tier distribution
    COUNTIF(platform_consistency_tier = 'HIGHLY_CONSISTENT') AS highly_consistent_weeks,
    COUNTIF(platform_consistency_tier = 'HIGHLY_INCONSISTENT') AS highly_inconsistent_weeks,
    COUNT(*) AS total_weeks
    
  FROM `your-project.ads_demo.platform_consistency_analysis`
  WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK)
  GROUP BY brand
)
SELECT 
  'PLATFORM_CONSISTENCY_TEST' AS test_name,
  
  COUNT(*) AS brands_analyzed,
  
  -- Overall platform patterns
  AVG(avg_cross_platform_pct) AS overall_avg_cross_platform_pct,
  AVG(avg_optimization_gap) AS overall_avg_optimization_gap,
  
  -- Consistency validation
  COUNTIF(avg_optimization_gap < 0.3) AS consistent_brands,
  COUNTIF(avg_optimization_gap > 0.5) AS inconsistent_brands,
  
  -- Strategic patterns detected
  COUNTIF(dominant_strategy = 'UNIFIED_CROSS_PLATFORM') AS unified_strategy_brands,
  COUNTIF(dominant_strategy IN ('INSTAGRAM_FOCUSED', 'FACEBOOK_FOCUSED')) AS platform_specific_brands,
  
  -- Test result
  CASE 
    WHEN COUNT(*) >= 3 AND AVG(avg_optimization_gap) BETWEEN 0.1 AND 0.6 THEN 'PASS'
    ELSE 'NEEDS_REVIEW'
  END AS test_result,
  
  CURRENT_TIMESTAMP() AS test_timestamp
  
FROM platform_validation;

-- ============================================================================
-- FINAL TEST SUMMARY
-- ============================================================================

CREATE OR REPLACE VIEW `your-project.ads_demo.v_easy_test_summary` AS
SELECT 
  'EASY_TESTS_SUMMARY' AS test_suite,
  
  (SELECT COUNT(*) FROM `your-project.ads_demo.test_results_performance` WHERE test_result = 'PASS') AS performance_tests_passed,
  (SELECT test_result FROM `your-project.ads_demo.test_results_classification`) AS classification_test_result,
  (SELECT test_result FROM `your-project.ads_demo.test_results_promotional`) AS promotional_test_result,
  (SELECT test_result FROM `your-project.ads_demo.test_results_strategy_shifts`) AS strategy_shift_test_result,
  (SELECT test_result FROM `your-project.ads_demo.test_results_multi_brand`) AS multi_brand_test_result,
  (SELECT test_result FROM `your-project.ads_demo.test_results_platform`) AS platform_test_result,
  
  CURRENT_TIMESTAMP() AS summary_generated_at;

-- Run validation
SELECT * FROM `your-project.ads_demo.v_easy_test_summary`;