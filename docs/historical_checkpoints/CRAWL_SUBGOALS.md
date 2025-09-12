# CRAWL Phase - Atomic Subgoals & Branch Strategy

## Overview
Breaking CRAWL phase (Days 1-7) into atomic, testable components with dedicated branches for clean development discipline.

## Branch Naming Convention
`feature/crawl-[number]-[description]`

## Development Discipline
1. **Create branch**: `git checkout -b feature/crawl-0X-description`
2. **Develop & test**: Work on single atomic goal
3. **Self-validate**: All checkpoints pass
4. **Commit**: Clean commit message with 🤖 signature
5. **Merge to main**: `git checkout main && git merge feature/crawl-0X-description`
6. **Tag milestone**: `git tag crawl-milestone-X`

---

## Subgoal 1: Foundation & Environment
**Branch**: `feature/crawl-01-foundation`  
**Goal**: Clean project structure + updated dependencies  
**Deliverable**: Working development environment with all tools

### Checkpoints:
- [ ] Directory structure organized
- [ ] Updated `requirements.txt` with new dependencies  
- [ ] Virtual environment `.us-ads-radar` activated and working
- [ ] Updated `.gitignore` for new structure
- [ ] **Test**: `pip install -r requirements.txt` succeeds
- [ ] **Test**: `python -c "from google.cloud import bigquery; print('BQ client works')"` 

---

## Subgoal 2: Enhanced Google Custom Search Discovery
**Branch**: `feature/crawl-02-discovery`  
**Goal**: Replace Wikipedia approach with proper Google CSE API  
**Deliverable**: `scripts/discover_competitors_v2.py` with CSE integration

### Checkpoints:
- [ ] Google Custom Search API integration working
- [ ] Query templates for competitor discovery (`"{brand} competitors"`, `"alternatives to {brand}"`)
- [ ] Result scoring and company name extraction
- [ ] CSV output to `data/temp/competitors_raw.csv`
- [ ] **Test**: `python scripts/discover_competitors_v2.py --brand "Nike" --vertical "Athletic Apparel"` returns 10+ candidates
- [ ] **Test**: Output CSV has proper structure: `company_name,source,raw_score,url`

### Dependencies Added:
```txt
google-api-python-client     # Google Custom Search API
beautifulsoup4              # HTML parsing for search results
nltk                        # Text processing for company name extraction
```

---

## Subgoal 3: BigQuery AI Competitor Curation
**Branch**: `feature/crawl-03-ai-curation`  
**Goal**: SQL-based AI competitor validation using `AI.GENERATE_TABLE`  
**Deliverable**: `sql/06_curate_competitors.sql` working with real data

### Checkpoints:
- [ ] `competitors_raw` table loaded to BigQuery from CSV
- [ ] `AI.GENERATE_TABLE` query working with proper schema
- [ ] Output: `is_competitor BOOL`, `segment STRING`, `confidence FLOAT`
- [ ] **Test**: Query runs in < 2 minutes on 50+ candidates
- [ ] **Test**: Results show logical competitor decisions (Nike → Adidas = TRUE, Nike → McDonald's = FALSE)
- [ ] **Manual validation**: Spot-check 10 results for accuracy

### SQL Schema:
```sql
OUTPUT_SCHEMA => 'is_competitor BOOL, segment STRING, confidence FLOAT, reason STRING'
```

---

## Subgoal 4: Enhanced Ad Ingestion with Embeddings
**Branch**: `feature/crawl-04-embeddings`  
**Goal**: Add vector embeddings to existing ad pipeline  
**Deliverable**: Enhanced `ingest_fb_ads.py` + `sql/07_embeddings.sql`

### Checkpoints:
- [ ] `ML.GENERATE_EMBEDDING` integration in SQL
- [ ] Modified `ingest_fb_ads.py` to work with curated competitors
- [ ] Embeddings generated for `creative_text + title` combined
- [ ] **Test**: Ingest ads for 3 competitor brands (20+ ads each)
- [ ] **Test**: Embeddings table populated with vector data
- [ ] **Test**: Basic similarity query works: find ads similar to a given ad

### New Tables:
- `ads_embeddings` - Vector embeddings for creative content
- `competitors_curated` - AI-validated competitor list

---

## Subgoal 5: Vector Similarity Search
**Branch**: `feature/crawl-05-vector-search`  
**Goal**: Working `VECTOR_SEARCH` queries for "find similar ads"  
**Deliverable**: `sql/08_similarity_search.sql` with demo queries

### Checkpoints:
- [ ] `VECTOR_SEARCH` function working with embeddings
- [ ] Sample queries: "Find 5 most similar ads to ad_id X"
- [ ] Performance optimization (< 30 seconds for similarity search)
- [ ] **Test**: Similarity search returns semantically similar ads
- [ ] **Manual validation**: Results make business sense (discount ads cluster together, etc.)

### Key Queries:
```sql
-- Find similar ads
SELECT ad_id, brand, creative_text, similarity_score
FROM VECTOR_SEARCH(
  TABLE `project.dataset.ads_embeddings`, 
  'embedding',
  (SELECT embedding FROM `project.dataset.ads_embeddings` WHERE ad_id = 'target_ad'),
  top_k => 5
)
```

---

## Subgoal 6: AI-Enhanced Creative Analysis & Strategic Intelligence
**Branch**: `feature/crawl-06-ai-analysis`  
**Goal**: Comprehensive strategic competitive intelligence system answering key growth marketing questions  
**Deliverable**: Enhanced creative analysis + strategic intelligence views + forecasting models

### Strategic Questions to Answer:
1. **Ad Focus Analysis**: "In the past n weeks, what have my Facebook ads focused on?"
2. **Competitor Strategy**: "In the same period, what has been my competitors' focus?"
3. **Strategy Evolution**: "How have our strategies evolved over time over the last 3 months?"
4. **Trend Forecasting**: "Can we forecast these trends into the near future with confidence bands?"

### Core Strategic Features:
- [ ] **Enhanced Ad Classification** - Strategic labels beyond basic funnel
- [ ] **Time-Series Strategy Tracking** - Evolution analysis with trend detection
- [ ] **Competitive Response Detection** - Identify when competitors copy strategies
- [ ] **Creative Fatigue Detection** - Using content embeddings to detect diminishing returns
- [ ] **CTA Aggressiveness & Promotional Calendar** - Track promotional intensity and timing
- [ ] **Platform Strategy Analysis** - Cross-platform messaging consistency
- [ ] **Brand Voice Consistency** - Detect messaging drift and positioning changes
- [ ] **AI.FORECAST Models** - Strategic trend prediction with confidence intervals

### Checkpoints:

#### Enhanced Creative Classification:
- [ ] Improved `AI.GENERATE_TABLE` prompts for multi-dimensional classification
- [ ] Strategic labels: funnel stage, persona, topics, urgency score, promotional intensity
- [ ] Message angle extraction with confidence scoring
- [ ] **Test**: All ads get comprehensive strategic labels with >85% accuracy
- [ ] **Business validation**: Manual review confirms strategic relevance

#### Time-Series Strategy Intelligence:
- [ ] Funnel mix evolution tracking (Upper/Mid/Lower distribution over time)
- [ ] Message angle trend detection (shifting from features to benefits, etc.)
- [ ] Promotional intensity cycles and seasonal patterns
- [ ] **Test**: Can identify clear strategy shifts in 3-month historical data
- [ ] **Test**: Trend detection works across multiple competitor brands

#### Competitive Response System:
- [ ] Content similarity spike detection (>0.85 similarity appearing within 2 weeks)
- [ ] Cross-brand copying identification with timing analysis
- [ ] Strategic response recommendations based on competitor moves
- [ ] **Test**: Correctly identifies known copying cases in sample data
- [ ] **Test**: Response system flags <5% false positives

#### Creative Fatigue Detection:
- [ ] Content embedding clustering to identify overused themes
- [ ] Performance correlation with creative repetition
- [ ] Automated recommendations for creative refreshes
- [ ] **Test**: Identifies ads using similar messaging with declining performance
- [ ] **Test**: Recommendations align with creative best practices

#### Promotional & CTA Intelligence:
- [ ] CTA aggressiveness scoring (urgency, discount intensity, action pressure)
- [ ] Promotional calendar extraction (sale periods, seasonal campaigns)
- [ ] Cross-competitor promotional timing analysis
- [ ] **Test**: Accurately identifies promotional periods vs. brand messaging
- [ ] **Test**: CTA scoring correlates with business intuition

#### Platform & Brand Voice Analysis:
- [ ] Cross-platform messaging consistency scoring
- [ ] Brand voice drift detection over time periods
- [ ] Positioning change identification and quantification
- [ ] **Test**: Detects intentional brand voice changes in sample data
- [ ] **Test**: Platform consistency scores align with manual assessment

#### AI.FORECAST Integration:
- [ ] Strategic trend forecasting models with confidence bands
- [ ] Seasonal adjustment and cycle detection
- [ ] Competitive response prediction based on historical patterns
- [ ] **Test**: Forecast accuracy >70% for 4-week ahead predictions
- [ ] **Test**: Confidence bands capture actual outcomes 80% of the time

### Enhanced Schema:
```sql
-- Core classification
OUTPUT_SCHEMA => 'funnel STRING, angles ARRAY<STRING>, persona STRING, topics ARRAY<STRING>, 
                  urgency_score FLOAT64, promotional_intensity FLOAT64, brand_voice_score FLOAT64'

-- Strategic intelligence views
CREATE VIEW v_strategy_evolution AS ...     -- Time-series strategy tracking
CREATE VIEW v_competitive_responses AS ... -- Copy detection and timing
CREATE VIEW v_creative_fatigue AS ...      -- Content repetition analysis
CREATE VIEW v_promotional_calendar AS ...  -- CTA and promotional patterns
CREATE VIEW v_brand_voice_consistency AS ...-- Voice drift detection
```

### Success Criteria:
- [ ] **Strategic Question Coverage**: All 4 core questions answerable with <30 second query response
- [ ] **Competitive Intelligence**: Can identify strategy shifts, copying, and opportunities within 1 week of occurrence
- [ ] **Forecasting Accuracy**: >70% accuracy on 4-week strategic trend predictions
- [ ] **Creative Insights**: Fatigue detection prevents 80% of declining creative performance
- [ ] **Business Impact**: Growth marketers can make strategic decisions from dashboard insights

---

## Subgoal 7: End-to-End Integration & Demo
**Branch**: `feature/crawl-07-integration`  
**Goal**: Complete pipeline working from brand input to insights  
**Deliverable**: `scripts/compete_intel_pipeline.py` + demo queries

### Checkpoints:
- [ ] One-shot script combining all components
- [ ] Dashboard SQL queries for share-of-voice analysis
- [ ] Error handling and progress tracking
- [ ] **FINAL TEST**: `python compete_intel_pipeline.py --brand "Warby Parker"` works end-to-end
- [ ] **Demo ready**: Can show live discovery → curation → analysis → insights
- [ ] **Performance**: Full pipeline completes in < 10 minutes

### Pipeline Flow:
```bash
python compete_intel_pipeline.py --brand "Warby Parker" --vertical "Eyewear"
# ↓
# 1. Discover competitors via Google CSE
# 2. Curate with BigQuery AI  
# 3. Ingest Meta ads for top competitors
# 4. Generate embeddings and similarity analysis
# 5. Create insights dashboard
# 6. Output: Competitor intelligence report
```

---

## Success Criteria (CRAWL Phase Complete)

### Technical Validation:
- [ ] All 7 subgoals completed and merged to main
- [ ] All BigQuery AI functions working: `AI.GENERATE_TABLE`, `ML.GENERATE_EMBEDDING`, `VECTOR_SEARCH`, `AI.GENERATE`, `AI.FORECAST`
- [ ] End-to-end demo works: Brand Input → Competitor Discovery → Ad Analysis → Similar Ad Search
- [ ] Code runs in < 10 minutes total execution time
- [ ] Zero manual intervention required after initial setup

### Demo-Ready Check:
- [ ] Can show live: "Enter 'Warby Parker' → Get Zenni, EyeBuyDirect, Glasses.com ads analyzed"
- [ ] Has at least 3 competitor brands with 20+ ads each in BigQuery
- [ ] Vector similarity search returns meaningful results
- [ ] AI competitor curation shows logical decisions

### Performance Benchmarks:
- [ ] Google CSE discovery: < 2 minutes for 50+ candidates
- [ ] BigQuery AI curation: < 2 minutes for competitor validation
- [ ] Ad ingestion: < 5 minutes for 3 brands with 60+ total ads
- [ ] Vector similarity: < 30 seconds for similarity search
- [ ] Full pipeline: < 10 minutes end-to-end

---

## Risk Mitigation

### Critical Dependencies:
1. **Google Custom Search API quota** - Monitor usage, have backup manual lists
2. **BigQuery AI quotas** - Track function calls, optimize batch processing
3. **ScrapeCreators API limits** - Implement retry logic and rate limiting

### Quality Gates:
- Manual validation of AI decisions on sample data
- Performance testing on each subgoal
- Integration testing before merging to main
- Demo rehearsal before CRAWL phase sign-off

---

*Last Updated: September 3, 2025*  
*Phase: CRAWL (Days 1-7)*  
*Next Phase: WALK (Days 8-14)*