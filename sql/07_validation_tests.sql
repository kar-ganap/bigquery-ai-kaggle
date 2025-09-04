-- Validation Tests for BigQuery AI Competitor Curation
-- These queries test the quality and accuracy of AI-generated competitor analysis

-- Test 1: Ground Truth Validation
-- Test against known competitor relationships
WITH ground_truth_pairs AS (
  SELECT * FROM UNNEST([
    STRUCT('Nike' as brand, 'Adidas' as competitor, 'Direct-Rival' as expected_tier, 85 as expected_overlap),
    STRUCT('Nike', 'Under Armour', 'Direct-Rival', 70),
    STRUCT('Nike', 'Puma', 'Direct-Rival', 75),
    STRUCT('Stripe', 'PayPal', 'Market-Leader', 80),
    STRUCT('Stripe', 'Square', 'Direct-Rival', 85),
    STRUCT('Airbnb', 'Booking.com', 'Market-Leader', 60),
    STRUCT('Airbnb', 'VRBO', 'Direct-Rival', 90)
  ])
),
validation_results AS (
  SELECT 
    gt.brand,
    gt.competitor,
    gt.expected_tier,
    gt.expected_overlap,
    c.tier as actual_tier,
    c.market_overlap_pct as actual_overlap,
    c.confidence,
    c.quality_score,
    -- Tier accuracy
    CASE WHEN c.tier = gt.expected_tier THEN 1.0 ELSE 0.0 END as tier_accuracy,
    -- Overlap accuracy (within 20% tolerance)
    CASE WHEN ABS(c.market_overlap_pct - gt.expected_overlap) <= 20 THEN 1.0 ELSE 0.0 END as overlap_accuracy,
    -- Overall match
    CASE WHEN c.company_name IS NOT NULL THEN 1.0 ELSE 0.0 END as found_competitor
  FROM ground_truth_pairs gt
  LEFT JOIN `${BQ_PROJECT}.${BQ_DATASET}.competitors_validated` c
    ON LOWER(gt.brand) = LOWER(c.target_brand) 
    AND LOWER(gt.competitor) = LOWER(c.company_name)
)
SELECT
  'Ground Truth Validation' as test_name,
  COUNT(*) as total_test_cases,
  AVG(found_competitor) * 100 as competitor_detection_rate_pct,
  AVG(tier_accuracy) * 100 as tier_accuracy_pct,
  AVG(overlap_accuracy) * 100 as overlap_accuracy_pct,
  AVG(confidence) as avg_confidence,
  AVG(quality_score) as avg_quality_score
FROM validation_results;

-- Test 2: Confidence Calibration
-- Check if AI confidence correlates with actual accuracy
SELECT
  'Confidence Calibration' as test_name,
  CASE 
    WHEN confidence >= 0.9 THEN '0.90-1.00'
    WHEN confidence >= 0.8 THEN '0.80-0.89'
    WHEN confidence >= 0.7 THEN '0.70-0.79'
    WHEN confidence >= 0.6 THEN '0.60-0.69'
    ELSE '0.00-0.59'
  END as confidence_bucket,
  COUNT(*) as competitors_in_bucket,
  AVG(quality_score) as avg_quality_score,
  AVG(market_overlap_pct) as avg_market_overlap,
  -- Validation metrics would need manual review for full accuracy
  COUNTIF(tier IN ('Direct-Rival', 'Market-Leader')) / COUNT(*) * 100 as high_relevance_pct
FROM `${BQ_PROJECT}.${BQ_DATASET}.competitors_validated`
GROUP BY confidence_bucket
ORDER BY confidence_bucket DESC;

-- Test 3: Tier Distribution Analysis
-- Ensure reasonable distribution across competitor tiers
SELECT
  'Tier Distribution' as test_name,
  tier,
  COUNT(*) as competitor_count,
  AVG(market_overlap_pct) as avg_market_overlap,
  AVG(confidence) as avg_confidence,
  AVG(quality_score) as avg_quality_score,
  -- Expected characteristics per tier
  CASE tier
    WHEN 'Direct-Rival' THEN CASE WHEN AVG(market_overlap_pct) >= 70 THEN 'PASS' ELSE 'FAIL' END
    WHEN 'Market-Leader' THEN CASE WHEN AVG(confidence) >= 0.8 THEN 'PASS' ELSE 'FAIL' END
    WHEN 'Disruptor' THEN CASE WHEN AVG(market_overlap_pct) <= 60 THEN 'PASS' ELSE 'FAIL' END
    WHEN 'Niche-Player' THEN CASE WHEN AVG(market_overlap_pct) <= 50 THEN 'PASS' ELSE 'FAIL' END
    ELSE 'REVIEW'
  END as validation_status
FROM `${BQ_PROJECT}.${BQ_DATASET}.competitors_validated`
GROUP BY tier
ORDER BY competitor_count DESC;

-- Test 4: Data Quality Checks
-- Identify potential issues in AI-generated data
WITH quality_issues AS (
  SELECT 
    target_brand,
    company_name,
    -- Flag potential issues
    CASE WHEN confidence < 0.6 THEN 'Low Confidence' END as issue_confidence,
    CASE WHEN market_overlap_pct > 100 OR market_overlap_pct < 0 THEN 'Invalid Overlap %' END as issue_overlap,
    CASE WHEN tier NOT IN ('Direct-Rival', 'Market-Leader', 'Disruptor', 'Niche-Player', 'Adjacent') THEN 'Invalid Tier' END as issue_tier,
    CASE WHEN customer_substitution_ease NOT IN ('Easy', 'Medium', 'Hard') THEN 'Invalid Substitution' END as issue_substitution,
    CASE WHEN LENGTH(reasoning) < 20 THEN 'Short Reasoning' END as issue_reasoning,
    CASE WHEN evidence_sources IS NULL OR LENGTH(evidence_sources) < 10 THEN 'Missing Evidence' END as issue_evidence
  FROM `${BQ_PROJECT}.${BQ_DATASET}.competitors_validated`
)
SELECT
  'Data Quality Issues' as test_name,
  COUNTIF(issue_confidence IS NOT NULL) as low_confidence_count,
  COUNTIF(issue_overlap IS NOT NULL) as invalid_overlap_count,
  COUNTIF(issue_tier IS NOT NULL) as invalid_tier_count,
  COUNTIF(issue_substitution IS NOT NULL) as invalid_substitution_count,
  COUNTIF(issue_reasoning IS NOT NULL) as short_reasoning_count,
  COUNTIF(issue_evidence IS NOT NULL) as missing_evidence_count,
  COUNT(*) as total_competitors,
  -- Overall quality percentage
  (COUNT(*) - COUNTIF(
    issue_confidence IS NOT NULL OR 
    issue_overlap IS NOT NULL OR 
    issue_tier IS NOT NULL OR 
    issue_substitution IS NOT NULL OR 
    issue_reasoning IS NOT NULL OR 
    issue_evidence IS NOT NULL
  )) / COUNT(*) * 100 as clean_data_pct
FROM quality_issues;

-- Test 5: Brand Coverage Analysis  
-- Ensure all target brands have reasonable competitor discovery
SELECT
  'Brand Coverage' as test_name,
  target_brand,
  target_vertical,
  COUNT(*) as competitors_found,
  AVG(quality_score) as avg_quality_score,
  AVG(market_overlap_pct) as avg_market_overlap,
  MAX(confidence) as max_confidence,
  -- Coverage assessment
  CASE 
    WHEN COUNT(*) >= 5 AND AVG(quality_score) >= 0.5 THEN 'Good Coverage'
    WHEN COUNT(*) >= 3 AND AVG(quality_score) >= 0.4 THEN 'Moderate Coverage'
    WHEN COUNT(*) >= 1 THEN 'Limited Coverage'
    ELSE 'No Coverage'
  END as coverage_status
FROM `${BQ_PROJECT}.${BQ_DATASET}.competitors_validated`
GROUP BY target_brand, target_vertical
ORDER BY competitors_found DESC;

-- Test 6: Performance Metrics Summary
-- Overall system performance summary
SELECT
  'System Performance Summary' as test_name,
  COUNT(DISTINCT target_brand) as brands_analyzed,
  COUNT(*) as total_competitors_validated,
  AVG(confidence) as avg_confidence,
  AVG(quality_score) as avg_quality_score,
  AVG(market_overlap_pct) as avg_market_overlap,
  COUNTIF(tier = 'Direct-Rival') / COUNT(*) * 100 as direct_rival_pct,
  COUNTIF(customer_substitution_ease = 'Easy') / COUNT(*) * 100 as easy_substitution_pct,
  -- Cost efficiency (estimated)
  COUNT(*) * 0.002 as estimated_cost_usd,  -- Rough estimate for AI.GENERATE_TABLE calls
  COUNT(*) / COUNT(DISTINCT target_brand) as avg_competitors_per_brand
FROM `${BQ_PROJECT}.${BQ_DATASET}.competitors_validated`;