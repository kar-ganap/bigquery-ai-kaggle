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

## Subgoal 6: AI-Enhanced Creative Analysis
**Branch**: `feature/crawl-06-ai-analysis`  
**Goal**: Upgrade existing labeling with better prompts and new features  
**Deliverable**: Enhanced `sql/02_label_ads.sql` + trend analysis

### Checkpoints:
- [ ] Improved `AI.GENERATE` prompts for funnel classification
- [ ] Additional analysis: persona detection, urgency scoring
- [ ] `AI.FORECAST` integration for trend prediction
- [ ] **Test**: All ads get proper funnel/angle labels
- [ ] **Test**: Forecast shows reasonable trend projections
- [ ] **Business validation**: Label accuracy > 80% on manual sample

### Enhanced Schema:
```sql
OUTPUT_SCHEMA => 'funnel STRING, angles ARRAY<STRING>, persona STRING, urgency_score FLOAT64'
```

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