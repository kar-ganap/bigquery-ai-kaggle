# US Ads Strategy Radar - Project Roadmap

## Vision Statement
**"A SQL-first competitive intelligence workspace in BigQuery that auto-discovers a brand's competitors, pulls all their live Meta ads, and lets you analyze creative strategy with vector search, LLM summarization, and simple SQL."**

## Competition Brief Alignment
- **🧠 AI Architect**: Using `AI.GENERATE_TABLE` for competitor curation and `AI.GENERATE` for creative analysis
- **🕵️‍♀️ Semantic Detective**: `VECTOR_SEARCH` for "find similar ads" and creative clustering  
- **🖼️ Multimodal Pioneer**: Object Tables for image/video analysis

## Scoring Strategy (Target: 100/100 + 10 Bonus)

### Technical Implementation (35%)
- **Code Quality (20%)**: Clean, modular Python with proper error handling, fast execution (< 5min)
- **BigQuery AI Usage (15%)**: Core functions throughout pipeline, not bolt-on features

### Innovation & Creativity (25%) 
- **Novelty (10%)**: "First SQL-native competitive intelligence workspace"
- **Business Impact (15%)**: "23% campaign ROI improvement, 40 hours → 10 minutes"

### Demo & Presentation (20%)
- **Problem/Solution (10%)**: Clear business case with live demo
- **Technical Explanation (10%)**: Architecture diagrams and BigQuery AI usage

### Assets (20%)
- **Blog/Video (10%)**: Technical blog with demo video
- **GitHub Repository (10%)**: Clean, public code repository

### Bonus (10%)
- **Feedback (5%)**: BigQuery AI friction points and suggestions  
- **Survey (5%)**: Completed official feature survey

---

## Frontloaded 2-Week Timeline

### Week 1 (Days 1-7): **BUILD EVERYTHING CORE**
**Goal: Working end-to-end demo by Day 7**

#### Days 1-2: Discovery + AI Curation
- [ ] Build `scripts/discover_competitors.py` with Google Custom Search Engine API
- [ ] Create `sql/06_curate_competitors.sql` with `AI.GENERATE_TABLE`
- [ ] Test: Brand input → BigQuery AI competitor validation

#### Days 3-4: Enhanced Ad Pipeline  
- [ ] Add vector embeddings to `ingest_fb_ads.py`
- [ ] Build `sql/07_similarity_search.sql` with `VECTOR_SEARCH`
- [ ] Test: Similar ads query working

#### Days 5-6: Integration + One-shot Script
- [ ] Build `scripts/compete_intel_pipeline.py` - full automation
- [ ] Create dashboard SQL queries for share-of-voice
- [ ] Add `AI.FORECAST` for trend analysis

#### Day 7: Demo Validation
- [ ] **CHECKPOINT:** Full demo works end-to-end
- [ ] Record initial demo video
- [ ] Draft GitHub README

### Week 2 (Days 8-14): **POLISH + ASSETS**
**Goal: Maximum scoring submission**

#### Days 8-10: Multimodal + Advanced Features
- [ ] Object Tables for image analysis
- [ ] Enhanced clustering and insights
- [ ] Performance optimization

#### Days 11-12: Documentation + Blog
- [ ] Technical blog post with architecture
- [ ] Clean up code, add error handling
- [ ] Create compelling demo dataset

#### Days 13-14: Final Submission
- [ ] Polish demo video
- [ ] Final testing and bug fixes
- [ ] Submit with confidence

---

## "DONE" Checkpoints for Each Stage

### 🐛 CRAWL Phase - **Week 1 End Criteria**

#### ✅ Technical Milestone:
- [ ] `python scripts/discover_competitors.py --brand "Nike" --vertical "Athletic Apparel"` returns 10+ competitor candidates
- [ ] `competitors_raw` table exists in BigQuery with data
- [ ] `sql/06_curate_competitors.sql` successfully runs `AI.GENERATE_TABLE` and produces `is_competitor: BOOL` decisions
- [ ] Enhanced `ingest_fb_ads.py` pulls ads for discovered competitors (test with 2-3 brands)
- [ ] `VECTOR_SEARCH` query returns "similar ads" results in < 30 seconds

#### ✅ Scoring Validation:
- [ ] All 4 core BigQuery AI functions working: `AI.GENERATE_TABLE`, `ML.GENERATE_EMBEDDING`, `VECTOR_SEARCH`, `AI.GENERATE`
- [ ] End-to-end demo works: Brand Input → Competitor Discovery → Ad Analysis → Similar Ad Search
- [ ] Code runs in < 10 minutes total execution time
- [ ] Zero manual intervention required after initial setup

#### ✅ Demo-Ready Check:
- [ ] Can show live: "Enter 'Warby Parker' → Get Zenni, EyeBuyDirect, Glasses.com ads analyzed"
- [ ] Has at least 3 competitor brands with 20+ ads each in BigQuery

**Checkpoint Question:** *"Can I demo the core workflow end-to-end with real data?"*

### 🚶 WALK Phase - **Week 2 End Criteria**  

#### ✅ Scale & Polish:
- [ ] One-shot script: `python compete_intel_pipeline.py --brand "Casper"` runs full pipeline
- [ ] Object Tables integration: Images/videos stored in GCS, queryable via BigQuery
- [ ] `AI.FORECAST` working on ad cadence trends
- [ ] Share-of-voice dashboard queries return results for 5+ brands simultaneously
- [ ] Multi-modal analysis: Can find "similar looking ads" using image embeddings

#### ✅ Business Metrics:
- [ ] Documented time savings: Manual process (hours) vs automated (minutes)  
- [ ] Sample insight: "Brand X shifted from 60% discount ads to 40% social proof ads in Q4"
- [ ] Competitor angle analysis: "Top 3 angles by brand with trend direction"

#### ✅ Asset Creation:
- [ ] GitHub repo with clear README, installation instructions, example commands
- [ ] Architecture diagram showing BigQuery AI integration points
- [ ] Draft blog post with technical explanation and business case

**Checkpoint Question:** *"Does this look like a production-ready business tool?"*

### 🏃 RUN Phase - **Week 3 End Criteria** (Final Submission)

#### ✅ Submission Ready:
- [ ] **Demo video (5-10 min):** Shows problem → solution → results with real data
- [ ] **Technical blog:** Published with architecture diagrams, code snippets, business metrics
- [ ] **Clean code:** All scripts documented, error handling, consistent formatting
- [ ] **BigQuery queries:** Saved views showing key insights (share-of-voice, trends, clustering)

#### ✅ Scoring Maximization:
- [ ] **Innovation proof:** No competing solutions exist (document competitor landscape)
- [ ] **Business impact:** Quantified ROI/time savings with real examples
- [ ] **Technical depth:** All BigQuery AI features used meaningfully, not just for show
- [ ] **Presentation quality:** Clear problem statement, solution walkthrough, next steps

#### ✅ Bonus Points:
- [ ] Detailed feedback document on BigQuery AI friction points and suggestions
- [ ] Completed official feature survey

**Checkpoint Question:** *"Would I pay for this solution if I were a marketing director?"*

---

## Architecture Overview

### BigQuery AI Discovery Architecture
```python
# Step 1: Python scrapes raw candidates
discover_competitors.py → competitors_raw table

# Step 2: BigQuery AI curates intelligently  
AI.GENERATE_TABLE(
  PROMPT => 'Is {company} a competitor to {brand} in {vertical}? 
             Assign segment: Incumbent|Challenger|SMB-Emerging',
  OUTPUT_SCHEMA => 'is_competitor BOOL, segment STRING, confidence FLOAT'
)
```

### Core Data Flow
```
Brand Input 
  ↓
Google Custom Search → competitors_raw (BigQuery)
  ↓  
AI.GENERATE_TABLE → competitors_curated
  ↓
Meta Ads API → ads_raw (partitioned, clustered)
  ↓
ML.GENERATE_EMBEDDING → ads_embeddings
  ↓
VECTOR_SEARCH + AI.GENERATE → insights & analysis
  ↓
Dashboard Queries → Business Intelligence
```

### Key Components to Build

#### New Scripts:
1. `scripts/discover_competitors.py` - Google Custom Search integration
2. `scripts/compete_intel_pipeline.py` - One-shot orchestration script

#### New SQL Files:
1. `sql/06_curate_competitors.sql` - AI-powered competitor validation
2. `sql/07_similarity_search.sql` - Vector embeddings and search
3. `sql/08_multimodal_analysis.sql` - Image/video processing  
4. `sql/09_competitive_intelligence.sql` - Share of voice analytics

#### Enhanced Features:
- Vector similarity search for "find similar ads"
- Automated competitor discovery and curation
- Share-of-voice trending dashboards
- Multimodal creative analysis
- AI-powered funnel stage and angle classification

---

## Success Metrics

### Innovation Metrics:
- **Uniqueness**: First SQL-native competitive intelligence workspace
- **Automation**: Reduces 40-hour manual research to 10 minutes
- **Scale**: Analyze entire market verticals, not just individual competitors

### Technical Metrics:
- **Performance**: Full pipeline execution in < 10 minutes
- **Accuracy**: 85%+ competitor identification precision
- **Coverage**: 4+ BigQuery AI functions used meaningfully throughout

### Business Impact:
- **Time Savings**: 40 hours → 10 minutes for competitive research
- **ROI Impact**: 23% campaign improvement through competitor insights
- **Scale Impact**: Analyze 50+ competitors simultaneously vs. 3-5 manual

---

## Risk Mitigation

### Critical Path Dependencies:
1. **Google Custom Search API** - Have backup manual competitor lists ready
2. **BigQuery AI quotas** - Monitor usage and have fallback logic
3. **ScrapeCreators API limits** - Batch requests and implement retry logic

### Fallback Plans:
- **If discovery fails**: Manual competitor seed list for demo
- **If BigQuery AI is slow**: Pre-computed results for demo
- **If embeddings fail**: Rule-based similarity for basic demo

### Quality Gates:
- Daily checkpoint meetings to assess progress
- Working demo validation every 2 days
- Code review and testing before each phase completion

---

*Last Updated: [Date]*
*Next Review: Daily during development*