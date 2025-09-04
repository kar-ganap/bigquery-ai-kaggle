-- BigQuery AI Competitor Curation using AI.GENERATE_TABLE
-- This script validates raw competitor candidates and enriches them with AI-powered analysis

-- Step 1: Create curated competitors table with enhanced schema
CREATE OR REPLACE TABLE `${BQ_PROJECT}.${BQ_DATASET}.competitors_curated` AS
SELECT 
  target_brand,
  target_vertical,
  company_name,
  source_url,
  source_title,
  query_used,
  raw_score,
  found_in,
  discovery_method,
  discovered_at,
  -- AI-generated fields will be added in next step
  CAST(NULL AS BOOL) as is_competitor,
  CAST(NULL AS STRING) as tier,
  CAST(NULL AS INT64) as market_overlap_pct,
  CAST(NULL AS STRING) as customer_substitution_ease,
  CAST(NULL AS FLOAT64) as confidence,
  CAST(NULL AS STRING) as reasoning,
  CAST(NULL AS STRING) as evidence_sources,
  CURRENT_TIMESTAMP() as curated_at
FROM `${BQ_PROJECT}.${BQ_DATASET}.competitors_raw`
WHERE discovered_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)  -- Only recent discoveries
;

-- Step 2: AI-powered competitor validation and curation
-- This uses BigQuery AI to analyze each candidate and determine if it's truly a competitor
CREATE OR REPLACE TABLE `${BQ_PROJECT}.${BQ_DATASET}.competitors_curated` AS
SELECT * FROM ML.GENERATE_TABLE(
  MODEL `${BQ_PROJECT}.${BQ_DATASET}.gemini_model`,
  TABLE `${BQ_PROJECT}.${BQ_DATASET}.competitors_raw`,
  STRUCT(
    CONCAT(
      'Analyze if "', company_name, '" is a legitimate competitor of "', target_brand, 
      '" in the ', COALESCE(target_vertical, 'unknown'), ' industry. ',
      
      'Context: This candidate was found through the query "', query_used, 
      '" in a web search result titled "', source_title, '". ',
      'Discovery method: ', discovery_method, '. ',
      'Search relevance score: ', CAST(raw_score as STRING), '. ',
      
      'Instructions:',
      '1. is_competitor: TRUE if this is a real company that competes with the target brand, FALSE otherwise',
      '2. tier: Categorize as "Direct-Rival" (same market/products), "Market-Leader" (dominant player), ',
      '"Disruptor" (innovative challenger), "Niche-Player" (specialized segment), or "Adjacent" (related but different focus)',
      '3. market_overlap_pct: Estimate 0-100% how much their target markets overlap',
      '4. customer_substitution_ease: "Easy" (customers switch readily), "Medium" (some friction), or "Hard" (significant barriers)',
      '5. confidence: 0.0-1.0 confidence in your assessment',
      '6. reasoning: Brief explanation of your analysis (max 200 chars)',
      '7. evidence_sources: What information you used to make this decision (max 150 chars)',
      
      'Be conservative - only mark as competitor if you are confident they actually compete.',
      'Consider: Do they serve similar customers? Similar products/services? Geographic overlap?'
    ) AS prompt
  ) AS generation_config,
  STRUCT(
    'is_competitor BOOL, tier STRING, market_overlap_pct INT64, customer_substitution_ease STRING, confidence FLOAT64, reasoning STRING, evidence_sources STRING'
  ) AS output_schema_config
);

-- Step 3: Add metadata and quality scores
UPDATE `${BQ_PROJECT}.${BQ_DATASET}.competitors_curated`
SET 
  curated_at = CURRENT_TIMESTAMP(),
  -- Calculate composite quality score
  quality_score = CASE 
    WHEN is_competitor THEN
      (confidence * 0.4) +  -- AI confidence weight
      (LEAST(raw_score / 5.0, 1.0) * 0.3) +  -- Search relevance weight  
      (market_overlap_pct / 100.0 * 0.2) +  -- Market overlap weight
      (CASE discovery_method WHEN 'standard' THEN 0.1 ELSE 0.05 END)  -- Discovery method bonus
    ELSE 0.0
  END
WHERE curated_at IS NULL;

-- Step 4: Create final validated competitors view
CREATE OR REPLACE VIEW `${BQ_PROJECT}.${BQ_DATASET}.competitors_validated` AS
SELECT 
  target_brand,
  target_vertical,
  company_name,
  tier,
  market_overlap_pct,
  customer_substitution_ease,
  confidence,
  reasoning,
  evidence_sources,
  quality_score,
  source_url,
  source_title,
  raw_score,
  discovery_method,
  discovered_at,
  curated_at
FROM `${BQ_PROJECT}.${BQ_DATASET}.competitors_curated`
WHERE is_competitor = TRUE
  AND confidence >= 0.6  -- High confidence threshold
  AND quality_score >= 0.4  -- Minimum quality threshold
ORDER BY target_brand, quality_score DESC, market_overlap_pct DESC;