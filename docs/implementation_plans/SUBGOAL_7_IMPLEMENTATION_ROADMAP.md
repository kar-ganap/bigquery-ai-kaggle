# Subgoal 7: End-to-End Integration & Demo - Complete Implementation Roadmap

## Executive Summary
Transform 6 isolated components (Subgoals 1-6) into a unified competitive intelligence pipeline that delivers enterprise-grade insights in <10 minutes through a 4-level progressive disclosure framework.

## Core Command
```bash
python compete_intel_pipeline.py --brand "Allbirds" --vertical "Sustainable Footwear"
```

---

## 🎯 Success Criteria

### Primary Deliverables
1. **`scripts/compete_intel_pipeline.py`** - One-shot orchestration script
2. **4-Level Intelligence Output** - Progressive disclosure from executive summary to deep analytics
3. **SQL Dashboard Suite** - Ready-to-run BigQuery queries for continued analysis
4. **Demo Capability** - "Warby Parker" end-to-end demonstration
5. **Performance Target** - Full pipeline <10 minutes

### Technical Requirements
- ✅ Zero manual intervention after launch
- ✅ Comprehensive error handling with rollback
- ✅ Progress tracking with ETA
- ✅ Configurable output formats (terminal, JSON, CSV)
- ✅ Reproducible results with logging

---

## 🏗️ System Architecture

### Pipeline Flow (6 Stages)
```
[User Input] → [Discovery] → [Curation] → [Ingestion] → [Embeddings] → [Analysis] → [4-Level Output]
```

### Data Flow
```python
Input: brand="Allbirds", vertical="Sustainable Footwear"
  ↓
Stage 1: Discover 40-60 competitor candidates via Google CSE
  ↓
Stage 2: AI validates to 10-15 true competitors
  ↓
Stage 3: Collect 60-100 ads from top 5 competitors
  ↓
Stage 4: Generate 768-dim embeddings for all ads
  ↓
Stage 5: Run strategic analysis (current state, influence, evolution, forecasting)
  ↓
Output: 4-Level Intelligence Report + SQL Dashboards
```

---

## 📊 4-Level Intelligence Output Structure

### Level 1: Executive Summary (30 seconds to consume)
```
- Pipeline metrics (duration, competitors found, ads analyzed)
- 3 priority alerts (copying detected, market shifts, opportunities)
- Strategic position assessment (offensive/defensive/stable)
- Single recommendation
```

### Level 2: Strategic Intelligence Dashboard (5 minutes to consume)
```
1. Current State Analysis - Strategic mix distributions
2. Influence Detection - Who's copying whom
3. Temporal Evolution - 90-day strategy trends
4. Predictive Intelligence - 30-day forecasts
```

### Level 3: Actionable Interventions (10 minutes to consume)
```
- Strategic Velocity Analysis
- Pattern Resonance Detection
- Market Rhythm Analysis
- Momentum Scoring
- White Space Opportunities
- Counter-Strategy Matrix
- Influence Cascade Predictions
```

### Level 4: Deep-Dive SQL Dashboards (Self-service)
```
- Generated SQL queries for BigQuery console
- Parameterized templates for common analyses
- Natural language query examples
- Export capabilities
```

---

## 📋 Task Breakdown & Implementation Roadmap

### Phase 1: Core Pipeline Infrastructure (Days 1-2)

#### Task 1.1: Pipeline Orchestrator Framework
**File**: `scripts/compete_intel_pipeline.py`

**Implementation**:
```python
class CompetitiveIntelligencePipeline:
    def __init__(self, brand: str, vertical: Optional[str] = None):
        self.brand = brand
        self.vertical = vertical
        self.start_time = time.time()
        self.results = {}
        self.logger = self._setup_logging()
        
    def execute_pipeline(self) -> PipelineResults:
        """Main orchestration method"""
        try:
            self._print_header()
            competitors = self._stage_1_discovery()
            validated = self._stage_2_curation(competitors)
            ads = self._stage_3_ingestion(validated)
            embeddings = self._stage_4_embeddings(ads)
            analysis = self._stage_5_analysis(embeddings)
            output = self._stage_6_output(analysis)
            return PipelineResults(success=True, output=output)
        except Exception as e:
            return self._handle_failure(e)
```

**Checkpoints**:
- [ ] Class structure with 6 stage methods defined
- [ ] Logging system captures all operations
- [ ] Error handling with graceful degradation
- [ ] Progress tracking with time estimates

**Test**:
```bash
python compete_intel_pipeline.py --brand "TestBrand" --dry-run
# Should show all 6 stages without executing
```

---

#### Task 1.2: Command-Line Interface
**Implementation**:
```python
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--brand", required=True)
    parser.add_argument("--vertical", help="Industry vertical")
    parser.add_argument("--output-format", choices=["terminal", "json", "csv"], default="terminal")
    parser.add_argument("--output-dir", default="data/output")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    
    pipeline = CompetitiveIntelligencePipeline(args.brand, args.vertical)
    results = pipeline.execute_pipeline()
```

**Checkpoints**:
- [ ] All arguments parsed correctly
- [ ] Help text is clear and comprehensive
- [ ] Validation for required parameters

**Test**:
```bash
python compete_intel_pipeline.py --help
# Should show all options clearly
```

---

### Phase 2: Stage Integration (Days 2-3)

#### Task 2.1: Stage 1 - Discovery Integration
**Implementation**:
```python
def _stage_1_discovery(self) -> List[CompetitorCandidate]:
    """Integrate discover_competitors_v2.py functionality"""
    self._print_stage_header(1, "COMPETITOR DISCOVERY")
    
    # Auto-detect vertical if not provided
    if not self.vertical:
        from scripts.discover_competitors_v2 import CompetitorDiscovery
        discovery = CompetitorDiscovery()
        self.vertical = discovery.detect_brand_vertical(self.brand)
        self._log(f"Detected vertical: {self.vertical}")
    
    # Run discovery
    candidates = discovery.discover_competitors(
        brand=self.brand,
        vertical=self.vertical,
        max_results_per_query=10
    )
    
    self._validate_stage_1(candidates)
    return candidates
```

**Checkpoints**:
- [ ] Google CSE API integration working
- [ ] Returns 40-60 candidates typically
- [ ] Vertical auto-detection functional
- [ ] Score-based ranking implemented

**Test**:
```python
# Unit test
candidates = pipeline._stage_1_discovery()
assert len(candidates) >= 20
assert all(c.raw_score > 0 for c in candidates)
```

---

#### Task 2.2: Stage 2 - Curation Integration
**Implementation**:
```python
def _stage_2_curation(self, candidates: List[CompetitorCandidate]) -> List[ValidatedCompetitor]:
    """Integrate AI curation via BigQuery"""
    self._print_stage_header(2, "AI COMPETITOR CURATION")
    
    # Load to BigQuery
    df = self._candidates_to_dataframe(candidates)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.competitors_raw_{self.run_id}"
    load_dataframe_to_bq(df, table_id)
    
    # Run AI.GENERATE_TABLE curation
    curation_sql = self._generate_curation_sql(table_id)
    validated_df = run_query(curation_sql, BQ_PROJECT)
    
    # Filter by confidence
    validated = validated_df[
        (validated_df['is_competitor'] == True) & 
        (validated_df['confidence'] >= 0.6)
    ].to_dict('records')
    
    self._validate_stage_2(validated)
    return validated
```

**Checkpoints**:
- [ ] BigQuery table creation successful
- [ ] AI.GENERATE_TABLE execution works
- [ ] Confidence threshold filtering applied
- [ ] Returns 10-15 validated competitors

**Test**:
```python
validated = pipeline._stage_2_curation(candidates)
assert len(validated) >= 3
assert all(v['confidence'] >= 0.6 for v in validated)
```

---

#### Task 2.3: Stage 3 - Ad Ingestion Integration
**Implementation**:
```python
def _stage_3_ingestion(self, competitors: List[ValidatedCompetitor]) -> IngestionResults:
    """Collect ads from top competitors"""
    self._print_stage_header(3, "META ADS INGESTION")
    
    # Select top 5 by confidence * market_overlap
    top_competitors = sorted(
        competitors, 
        key=lambda x: x['confidence'] * x.get('market_overlap_pct', 50) / 100,
        reverse=True
    )[:5]
    
    from scripts.ingest_fb_ads import MetaAdsFetcher
    fetcher = MetaAdsFetcher()
    
    all_ads = []
    for comp in top_competitors:
        ads = fetcher.fetch_company_ads(
            comp['company_name'],
            max_ads=30,
            delay=0.5
        )
        all_ads.extend(ads)
    
    self._validate_stage_3(all_ads)
    return IngestionResults(ads=all_ads, brands=[c['company_name'] for c in top_competitors])
```

**Checkpoints**:
- [ ] Meta Ads API authentication works
- [ ] Page ID resolution successful
- [ ] Collects 60-100 total ads
- [ ] Handles rate limiting gracefully

**Test**:
```python
ads = pipeline._stage_3_ingestion(validated)
assert len(ads.ads) >= 60
assert len(ads.brands) >= 3
```

---

#### Task 2.4: Stage 4 - Embeddings Generation
**Implementation**:
```python
def _stage_4_embeddings(self, ads: IngestionResults) -> EmbeddingResults:
    """Generate embeddings for all ads"""
    self._print_stage_header(4, "EMBEDDING GENERATION")
    
    # Load ads to BigQuery
    ads_table = f"{BQ_PROJECT}.{BQ_DATASET}.ads_temp_{self.run_id}"
    load_dataframe_to_bq(ads.to_dataframe(), ads_table)
    
    # Generate embeddings using ML.GENERATE_EMBEDDING
    embedding_sql = f"""
    CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.ads_embeddings_{self.run_id}` AS
    SELECT 
      ad_archive_id,
      brand,
      creative_text,
      ML.GENERATE_EMBEDDING(
        MODEL `{BQ_PROJECT}.{BQ_DATASET}.text_embedding_model`,
        CONCAT(COALESCE(creative_text, ''), ' ', COALESCE(title, ''))
      ) AS content_embedding
    FROM `{ads_table}`
    WHERE LENGTH(COALESCE(creative_text, '') || COALESCE(title, '')) >= 10
    """
    
    run_query(embedding_sql, BQ_PROJECT)
    self._validate_stage_4()
    return EmbeddingResults(table_id=f"ads_embeddings_{self.run_id}")
```

**Checkpoints**:
- [ ] ML.GENERATE_EMBEDDING executes successfully
- [ ] 768-dimensional vectors created
- [ ] All ads with content get embeddings
- [ ] Table persisted for analysis

**Test**:
```sql
SELECT COUNT(*), AVG(ARRAY_LENGTH(content_embedding))
FROM ads_embeddings_[run_id]
-- Should show count >= 60, avg = 768
```

---

### Phase 3: Strategic Analysis Implementation (Days 3-4)

#### Task 3.1: Stage 5 - Strategic Analysis Suite
**Implementation**:
```python
def _stage_5_analysis(self, embeddings: EmbeddingResults) -> AnalysisResults:
    """Run comprehensive strategic analysis"""
    self._print_stage_header(5, "COMPETITIVE ANALYSIS")
    
    analysis = AnalysisResults()
    
    # 1. Current State Analysis
    analysis.current_state = self._analyze_current_state()
    
    # 2. Influence Detection  
    analysis.influence = self._detect_influence_patterns()
    
    # 3. Temporal Evolution
    analysis.evolution = self._analyze_temporal_evolution()
    
    # 4. Predictive Intelligence
    analysis.forecasts = self._generate_forecasts()
    
    # 5. Advanced Analytics (Level 3)
    analysis.velocity = self._calculate_strategic_velocity()
    analysis.patterns = self._identify_winning_patterns()
    analysis.rhythms = self._detect_market_rhythms()
    analysis.momentum = self._calculate_momentum_scores()
    analysis.white_spaces = self._find_white_spaces()
    analysis.cascades = self._predict_influence_cascades()
    
    self._validate_stage_5(analysis)
    return analysis
```

**Sub-task 3.1.1: Current State Analysis**
```python
def _analyze_current_state(self) -> CurrentStateAnalysis:
    """Strategic mix distributions"""
    query = f"""
    WITH brand_mix AS (
      SELECT 
        brand,
        AVG(promotional_intensity) as avg_promo,
        AVG(urgency_score) as avg_urgency,
        COUNTIF(funnel = 'Upper') / COUNT(*) as upper_funnel_pct,
        COUNTIF(funnel = 'Mid') / COUNT(*) as mid_funnel_pct,
        COUNTIF(funnel = 'Lower') / COUNT(*) as lower_funnel_pct,
        COUNTIF(persona = 'Eco-Conscious') / COUNT(*) as eco_persona_pct
      FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_strategic_labels`
      WHERE brand IN ('{self.brand}', {', '.join(f"'{b}'" for b in self.competitor_brands)})
        AND created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      GROUP BY brand
    )
    SELECT * FROM brand_mix
    """
    
    df = run_query(query, BQ_PROJECT)
    return self._process_current_state(df)
```

**Checkpoints**:
- [ ] All strategic dimensions calculated
- [ ] Comparison matrix generated
- [ ] Advantages/gaps identified
- [ ] Market averages computed

---

#### Task 3.2: Level 3 Enhanced Analytics
**Implementation**:
```python
def _calculate_strategic_velocity(self) -> VelocityAnalysis:
    """How fast strategies are changing"""
    query = f"""
    WITH weekly_changes AS (
      SELECT 
        brand,
        week_start,
        promotional_intensity - LAG(promotional_intensity) OVER (
          PARTITION BY brand ORDER BY week_start
        ) AS promo_change,
        urgency_score - LAG(urgency_score) OVER (
          PARTITION BY brand ORDER BY week_start
        ) AS urgency_change
      FROM weekly_strategy_aggregates
    )
    SELECT 
      brand,
      AVG(ABS(promo_change)) as avg_promo_velocity,
      AVG(ABS(urgency_change)) as avg_urgency_velocity,
      STDDEV(promo_change) as promo_volatility
    FROM weekly_changes
    GROUP BY brand
    """
    
    df = run_query(query, BQ_PROJECT)
    return self._process_velocity(df)
```

---

### Phase 4: Output Generation (Days 4-5)

#### Task 4.1: Stage 6 - 4-Level Output Generation
**Implementation**:
```python
def _stage_6_output(self, analysis: AnalysisResults) -> IntelligenceOutput:
    """Generate 4-level progressive disclosure output"""
    self._print_stage_header(6, "DASHBOARD GENERATION")
    
    output = IntelligenceOutput()
    
    # Level 1: Executive Summary
    output.level_1 = self._generate_executive_summary(analysis)
    
    # Level 2: Strategic Intelligence Dashboard
    output.level_2 = self._generate_strategic_dashboard(analysis)
    
    # Level 3: Actionable Interventions
    output.level_3 = self._generate_interventions(analysis)
    
    # Level 4: SQL Dashboards
    output.level_4 = self._generate_sql_dashboards(analysis)
    
    # Format and display
    self._display_output(output)
    
    # Save to files
    self._save_output_files(output)
    
    return output
```

**Sub-task 4.1.1: Executive Summary Generation**
```python
def _generate_executive_summary(self, analysis: AnalysisResults) -> ExecutiveSummary:
    """30-second consumable summary"""
    
    # Identify top 3 alerts
    alerts = []
    
    # Alert 1: Copying detection
    if analysis.influence.max_similarity > 0.85:
        alerts.append(Alert(
            priority="HIGH",
            message=f"{analysis.influence.top_copier} copying detected (94% similarity, {analysis.influence.lag_days}-day lag)",
            action="Review creative differentiation strategy"
        ))
    
    # Alert 2: Market shifts
    if analysis.evolution.market_promo_change > 0.3:
        alerts.append(Alert(
            priority="MEDIUM",
            message=f"Market promotional intensity +{analysis.evolution.market_promo_change*100:.0f}% (14 days)",
            action="Prepare defensive campaign"
        ))
    
    # Strategic position
    position = self._assess_strategic_position(analysis)
    
    return ExecutiveSummary(
        duration=self.pipeline_duration,
        competitors_analyzed=len(self.competitor_brands),
        ads_analyzed=self.total_ads,
        alerts=alerts[:3],
        strategic_position=position,
        primary_recommendation=self._get_top_recommendation(analysis)
    )
```

---

#### Task 4.2: SQL Dashboard Generation
**Implementation**:
```python
def _generate_sql_dashboards(self, analysis: AnalysisResults) -> SQLDashboards:
    """Generate ready-to-run BigQuery queries"""
    
    dashboards = {}
    
    # Strategic Mix Comparison
    dashboards['strategic_mix'] = f"""
    -- Strategic Mix Analysis for {self.brand}
    -- Compare your positioning vs top competitors
    
    WITH brand_strategy AS (
      SELECT 
        brand,
        DATE_TRUNC(created_date, WEEK) as week,
        AVG(promotional_intensity) as promo_intensity,
        AVG(urgency_score) as urgency,
        COUNTIF(funnel = 'Upper') / COUNT(*) as upper_funnel_pct
      FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_strategic_labels`
      WHERE brand IN ('{self.brand}', {', '.join(f"'{c}'" for c in self.competitor_brands)})
        AND created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      GROUP BY brand, week
    )
    SELECT * FROM brand_strategy
    ORDER BY week DESC, brand
    """
    
    # Copying Detection Monitor
    dashboards['copying_monitor'] = f"""
    -- Real-time Copying Detection for {self.brand}
    -- Alert when competitors launch similar campaigns
    
    WITH similarity_detection AS (
      SELECT 
        a1.brand as your_brand,
        a2.brand as competitor_brand,
        a1.ad_archive_id as your_ad,
        a2.ad_archive_id as competitor_ad,
        ML.COSINE_SIMILARITY(a1.content_embedding, a2.content_embedding) as similarity,
        DATE_DIFF(a2.created_date, a1.created_date, DAY) as lag_days
      FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_embeddings` a1
      CROSS JOIN `{BQ_PROJECT}.{BQ_DATASET}.ads_embeddings` a2
      WHERE a1.brand = '{self.brand}'
        AND a2.brand != '{self.brand}'
        AND a2.created_date > a1.created_date
        AND DATE_DIFF(a2.created_date, a1.created_date, DAY) <= 14
    )
    SELECT * FROM similarity_detection
    WHERE similarity >= 0.85
    ORDER BY similarity DESC
    """
    
    # Save dashboards
    self._save_sql_files(dashboards)
    return SQLDashboards(queries=dashboards)
```

---

### Phase 5: Testing & Validation (Day 5)

#### Task 5.1: End-to-End Integration Test
**Test Script**: `scripts/test_pipeline_integration.py`

```python
def test_warby_parker_demo():
    """Full pipeline test with Warby Parker"""
    
    # Run pipeline
    result = subprocess.run(
        ["python", "compete_intel_pipeline.py", "--brand", "Warby Parker", "--vertical", "Eyewear"],
        capture_output=True,
        text=True,
        timeout=600  # 10 minute timeout
    )
    
    # Validate execution
    assert result.returncode == 0
    assert "Pipeline complete" in result.stdout
    
    # Check timing
    duration_match = re.search(r"Duration: ([\d.]+) minutes", result.stdout)
    assert float(duration_match.group(1)) < 10.0
    
    # Validate output files
    assert os.path.exists("data/output/warby_parker_report.json")
    assert os.path.exists("data/output/warby_parker_dashboards.sql")
    
    # Check content quality
    with open("data/output/warby_parker_report.json") as f:
        report = json.load(f)
    
    assert len(report['competitors']) >= 3
    assert report['total_ads'] >= 60
    assert 'Zenni' in report['competitors'] or 'EyeBuyDirect' in report['competitors']
```

---

## 🎯 Quality Gates & Checkpoints

### Stage Checkpoints
| Stage | Success Criteria | Test Command |
|-------|-----------------|--------------|
| Discovery | 40+ candidates found | `pytest tests/test_discovery.py` |
| Curation | 10+ validated competitors | `pytest tests/test_curation.py` |
| Ingestion | 60+ ads from 3+ brands | `pytest tests/test_ingestion.py` |
| Embeddings | All ads have 768-dim vectors | `pytest tests/test_embeddings.py` |
| Analysis | All 4 questions answered | `pytest tests/test_analysis.py` |
| Output | 4 levels generated | `pytest tests/test_output.py` |

### Performance Benchmarks
| Component | Target | Measurement |
|-----------|--------|-------------|
| Discovery | <2 min | `time.time() - stage_start` |
| Curation | <2 min | BigQuery job duration |
| Ingestion | <5 min | API call timing |
| Embeddings | <1 min | ML.GENERATE_EMBEDDING duration |
| Analysis | <1 min | Query execution time |
| Output | <30 sec | Generation + file writing |
| **TOTAL** | **<10 min** | **End-to-end duration** |

---

## 📁 File Structure

```
scripts/
├── compete_intel_pipeline.py          # Main orchestrator
├── pipeline_components/
│   ├── discovery_integration.py       # Stage 1
│   ├── curation_integration.py        # Stage 2
│   ├── ingestion_integration.py       # Stage 3
│   ├── embedding_integration.py       # Stage 4
│   ├── analysis_integration.py        # Stage 5
│   └── output_generation.py           # Stage 6
├── tests/
│   ├── test_pipeline_integration.py   # End-to-end tests
│   ├── test_stage_1_discovery.py
│   ├── test_stage_2_curation.py
│   ├── test_stage_3_ingestion.py
│   ├── test_stage_4_embeddings.py
│   ├── test_stage_5_analysis.py
│   └── test_stage_6_output.py
└── utils/
    ├── formatting.py                   # Output formatting utilities
    ├── progress_tracker.py             # Progress bar implementation
    └── error_handler.py                # Rollback and recovery

data/
└── output/
    ├── {brand}_report.json             # Complete analysis results
    ├── {brand}_dashboards.sql          # SQL queries for BigQuery
    ├── {brand}_executive_summary.txt   # Level 1 output
    └── {brand}_detailed_analysis.csv   # Level 3 data export
```

---

## 🚀 Implementation Timeline

### Day 1: Infrastructure
- [ ] Pipeline orchestrator class
- [ ] Command-line interface
- [ ] Progress tracking system
- [ ] Error handling framework

### Day 2: Stage Integration (1-3)
- [ ] Discovery integration
- [ ] Curation integration
- [ ] Ingestion integration

### Day 3: Stage Integration (4-6)
- [ ] Embeddings generation
- [ ] Strategic analysis suite
- [ ] Output generation framework

### Day 4: Level 3 Enhancements
- [ ] Velocity analysis
- [ ] Pattern resonance
- [ ] Market rhythms
- [ ] White space detection
- [ ] Cascade predictions

### Day 5: Testing & Polish
- [ ] End-to-end Warby Parker test
- [ ] Performance optimization
- [ ] Documentation
- [ ] Demo preparation

---

## 🎬 Demo Script

```bash
# Live Demo Command
python compete_intel_pipeline.py --brand "Warby Parker" --vertical "Eyewear"

# Expected Output Flow:
# 1. Discovery: Find Zenni, EyeBuyDirect, Glasses.com, etc.
# 2. Curation: Validate as true competitors
# 3. Ingestion: Collect 80+ ads
# 4. Embeddings: Generate vectors
# 5. Analysis: Complete strategic intelligence
# 6. Output: 4-level report + SQL dashboards

# Total Time: <10 minutes
# Result: Enterprise competitive intelligence delivered
```

---

## ✅ Definition of Done

### Functional Requirements
- [ ] Pipeline executes end-to-end without manual intervention
- [ ] Warby Parker demo completes successfully
- [ ] All 6 stages integrate seamlessly
- [ ] 4-level output structure implemented
- [ ] SQL dashboards generated and valid

### Performance Requirements
- [ ] Total execution <10 minutes
- [ ] Each stage meets individual benchmarks
- [ ] Memory usage <4GB
- [ ] API rate limits respected

### Quality Requirements
- [ ] 95% test coverage
- [ ] Error recovery tested
- [ ] Logging comprehensive
- [ ] Documentation complete
- [ ] Demo rehearsed and polished

---

## 🎯 Risk Mitigation

| Risk | Mitigation Strategy |
|------|-------------------|
| API Rate Limits | Implement exponential backoff, caching |
| BigQuery Quotas | Batch operations, optimize queries |
| Poor Data Quality | Validation at each stage, fallback options |
| Performance Issues | Parallel processing where possible |
| Integration Failures | Modular design, stage isolation |

---

*Last Updated: 2025-01-15*  
*Phase: CRAWL Subgoal 7*  
*Status: Ready for Implementation*