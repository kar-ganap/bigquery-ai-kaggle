# Competitor Name Validation Strategy

## Problem Statement

The competitor discovery pipeline (Subgoals 1-3) is generating poor quality company names that break the ad fetching pipeline (Subgoal 4). Examples of invalid names being classified as legitimate competitors:

- "Payment Management" (page title fragment)
- "Market Share" (generic business term)  
- "Competitor Insights" (meta-analysis term)

**Root Cause**: Web scraper extracts page title fragments as company names, and AI curation gives identical confidence scores (0.7) to both garbage and real companies.

## Multi-Layered Validation Approach

### Phase 1: Pattern-Based Filtering (Python)
**File**: `scripts/competitor_name_validator.py`

**Strategy**: Multi-tier blacklists with false negative protection
- **Tier 1**: Safe generic terms (never companies): "Analysis", "Market Share", "2024"
- **Tier 2**: Suspicious patterns: "Top 10", "Leading SaaS", "vs"  
- **Tier 3**: Protected companies: Fortune 500, major tech brands, acronyms

**Results**: 
- ✅ Correctly filters garbage: "Market Share", "Analysis", "Top 10"
- ✅ Protects real companies: PayPal, Stripe, IBM, 3M
- ⚠️ Flags suspicious for review: "Payment Management Solutions"

### Phase 2: BigQuery AI Validation (SQL-Native)

#### Option A: ML.GENERATE_TEXT Smart Validation
```sql
SELECT 
  company_name,
  ML.GENERATE_TEXT(
    MODEL `project.dataset.text_model`,
    STRUCT(
      CONCAT('Is "', company_name, '" a real company name? Answer with JSON: {"is_company": true/false, "confidence": 0.0-1.0, "reasoning": "brief explanation"}') AS prompt
    )
  ) AS validation_result
FROM competitors_raw
```

**Advantages**:
- ✅ Pure SQL - No external dependencies
- ✅ Leverages existing BigQuery AI capabilities  
- ✅ Flexible prompting for fine-tuned validation
- ✅ Batch processing for thousands of names
- ✅ Structured JSON output for easy parsing

#### Option B: ML.GENERATE_EMBEDDING + Vector Search
```sql
-- Step 1: Create embeddings for known companies
CREATE OR REPLACE TABLE known_companies_embeddings AS
SELECT 
  company_name,
  ML.GENERATE_EMBEDDING(
    MODEL `project.dataset.embedding_model`,
    STRUCT(company_name AS content)
  ) AS embedding
FROM fortune_500_companies;

-- Step 2: Find similar names (catch variations/misspellings)
SELECT candidate_name, best_match, similarity_score
FROM ML.DISTANCE(
  MODEL `project.dataset.embedding_model`,
  candidate_embeddings,
  known_company_embeddings
) 
WHERE similarity_score > 0.85
ORDER BY similarity_score DESC;
```

**Advantages**:
- ✅ Catches name variations: "Apple Inc" → "Apple"
- ✅ Detects misspellings: "Microsft" → "Microsoft"  
- ✅ Fuzzy matching for real-world data messiness
- ✅ Scalable similarity search

#### Option C: External API Integration
```sql
-- Via Cloud Functions
SELECT 
  company_name,
  `project.dataset.validate_company_clearbit`(company_name) AS is_real_company,
  `project.dataset.get_company_domain`(company_name) AS website_exists
FROM competitors_raw;
```

**External APIs**:
- **Clearbit Company API**: Verify if real company
- **Domain validation**: Check if `companyname.com` exists
- **Crunchbase API**: Startup/private company database
- **SEC EDGAR**: Public company filings
- **Fortune Lists**: Fortune 500/1000 companies

#### Option D: BigQuery Public Datasets
```sql
-- Use existing public company data
WITH public_companies AS (
  SELECT DISTINCT UPPER(company_name) as company_name
  FROM `bigquery-public-data.sec_quarterly_financials.submission`
  WHERE company_name IS NOT NULL
  
  UNION ALL
  
  SELECT DISTINCT UPPER(name) as company_name  
  FROM `bigquery-public-data.austin_311.311_service_requests`
  WHERE name LIKE '%Inc%' OR name LIKE '%LLC%' OR name LIKE '%Corp%'
)
SELECT 
  cr.company_name,
  CASE WHEN pc.company_name IS NOT NULL THEN true ELSE false END as is_known_company
FROM competitors_raw cr
LEFT JOIN public_companies pc ON UPPER(cr.company_name) = pc.company_name;
```

## Implementation Plan

### Phase 1: Quick Wins (Immediate)
1. **Deploy pattern-based validator** (`competitor_name_validator.py`)
2. **Apply to existing data** to clean up current competitors_curated table
3. **Validate results** with manual spot-checking

### Phase 2: BigQuery AI Integration (This Week)  
1. **Start with ML.GENERATE_TEXT** approach for intelligent validation
2. **Create company validation model** in BigQuery
3. **Batch validate** all existing competitor names
4. **Update competitors_validated view** with improved filtering

### Phase 3: Comprehensive Validation (Next Week)
1. **Add embedding-based matching** for name variations
2. **Integrate external APIs** for real-time validation
3. **Build confidence scoring** combining multiple signals
4. **Create monitoring** for ongoing data quality

### Phase 4: Production Pipeline (Ongoing)
1. **Add validation step** to discovery pipeline
2. **Automated quality monitoring** and alerts
3. **Feedback loop** to improve validation rules
4. **A/B testing** of different validation approaches

## Success Metrics

**Data Quality Targets**:
- **Precision**: >95% of validated names are real companies
- **Recall**: >90% of real companies pass validation  
- **Coverage**: Handle all major business verticals
- **Speed**: <1 second per name for real-time validation

**Pipeline Impact**:
- **Ad Retrieval Success Rate**: >80% (vs current ~25%)
- **Competitor Coverage**: 20+ validated competitors (vs current 1-2)
- **False Positive Rate**: <5% garbage names getting through

## Risk Mitigation

**False Negatives** (Real companies filtered out):
- Maintain protected company lists
- Manual review process for borderline cases  
- Feedback mechanism to improve validation rules

**External API Dependencies**:
- Fallback to pattern-based validation
- Caching and rate limiting
- Cost monitoring for API usage

**Model Drift**:
- Regular validation of ML.GENERATE_TEXT prompts
- A/B testing of different validation approaches
- Monitoring for changing business naming patterns

## Next Steps

1. **Implement ML.GENERATE_TEXT validation** in BigQuery
2. **Test on current competitor data** to validate approach  
3. **Deploy improved filtering** to competitors_validated view
4. **Validate impact** on ad fetching pipeline success rates