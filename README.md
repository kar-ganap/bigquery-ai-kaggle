# BigQuery AI Competitive Intelligence Pipeline

A **BigQuery AI-native** competitive intelligence system that transforms Meta Ad Library data into strategic insights through progressive disclosure (L1→L4). Built entirely with BigQuery's native AI primitives—no external ML infrastructure required.

> **Competition Highlight**: *"From 466 competitor candidates to 5 critical insights using only BigQuery AI—detecting 73.7% copying similarity, identifying 6 untapped market opportunities, and forecasting competitive moves 30 days ahead."*

## Overview

- **10-Stage Pipeline**: Automated competitor discovery → validation → analysis → progressive intelligence
- **Real-Time Intelligence**: Copying detection, creative fatigue analysis, and market gap identification
- **Temporal Forecasting**: 30-day competitive trend predictions with confidence intervals
- **Multi-Dimensional Analysis**: Audience, creative, channel, visual, and whitespace intelligence
- **Progressive Disclosure**: L1 executive insights → L4 detailed SQL dashboards
- **100% BigQuery Native**: No external vector databases, ML services, or orchestration complexity

## 🛠️ Prerequisites & Setup

### 1. Google Cloud Platform Setup

#### Enable Required APIs
```bash
# Enable BigQuery, Storage, and Vertex AI APIs
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable aiplatform.googleapis.com
gcloud services enable customsearch.googleapis.com
```

#### Create BigQuery Dataset
```bash
# Set your project variables
export BQ_PROJECT="your-gcp-project-id"
export BQ_DATASET="competitive_intelligence"

# Create dataset
bq mk --dataset --location=US $BQ_PROJECT:$BQ_DATASET
```

#### Create Google Cloud Storage Bucket
```bash
# Create bucket for media storage (visual intelligence)
export GCS_BUCKET="your-project-competitive-intel"
gsutil mb gs://$GCS_BUCKET
```

#### Authentication
```bash
# Authenticate with Google Cloud
gcloud auth application-default login

# Verify BigQuery access
bq query --use_legacy_sql=false 'SELECT 1 as test'
```

### 2. API Keys & Services

#### ScrapeCreators API Key
1. Visit [ScrapeCreators.com](https://scrapecreators.com)
2. Sign up and obtain your API key for Meta Ad Library access
3. Note: This provides structured access to public Meta Ad Library data

#### Google Custom Search API (Optional)
1. Create a [Google Custom Search Engine](https://cse.google.com)
2. Get API key from [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
3. Used for automatic competitor discovery

### 3. Environment Configuration

Create `.env` file in project root:
```bash
# Required: BigQuery configuration
BQ_PROJECT=your-gcp-project-id
BQ_DATASET=competitive_intelligence

# Required: Storage configuration
GCS_BUCKET=your-project-competitive-intel

# Required: Meta Ad Library access
SC_API_KEY=your_scrapecreators_api_key

# Optional: Google Custom Search (for auto-discovery)
GOOGLE_CSE_API_KEY=your_google_cse_api_key
GOOGLE_CSE_ENGINE_ID=your_search_engine_id

# Optional: Advanced features
VERTEX_AI_REGION=us-central1
```

### 4. Installation

#### Option A: Package Installation (Recommended)
```bash
# Clone repository
git clone https://github.com/your-username/bigquery-ai-competitive-intelligence.git
cd bigquery-ai-competitive-intelligence

# Install with pip
pip install .

# Or install with development dependencies
pip install .[dev]

# Or install everything including notebook extras
pip install .[all]
```

#### Option B: Development Setup
```bash
# Clone repository
git clone https://github.com/your-username/bigquery-ai-competitive-intelligence.git
cd bigquery-ai-competitive-intelligence

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate

# Install in development mode
pip install -e .[dev]
```

## 🚀 Quick Start

### 1. Run Demo Notebook
```bash
# Launch Jupyter
jupyter notebook

# Open and run: notebooks/demo_competitive_intelligence.ipynb
```
The demo notebook walks through the complete pipeline using Warby Parker as an example.

### 2. Run the Pipeline

#### Option A: Complete Pipeline (One-Shot)
```bash
# Run entire 10-stage pipeline
python -m src.pipeline.orchestrator --brand "Your Brand Name"

# With optional parameters
python -m src.pipeline.orchestrator \
  --brand "Your Brand Name" \
  --vertical "eyewear" \
  --verbose

# Dry run for testing
python -m src.pipeline.orchestrator \
  --brand "Test Brand" \
  --dry-run
```

#### Option B: Stage-by-Stage Testing
```bash
# Run all stages sequentially with caching
python tests/stage_testing_framework.py \
  --brand "Your Brand Name" \
  --vertical "eyewear"

# Run a specific stage (uses cached results from previous stages)
python tests/stage_testing_framework.py \
  --brand "Your Brand Name" \
  --stage 1    # Discovery

python tests/stage_testing_framework.py \
  --brand "Your Brand Name" \
  --stage 2    # AI Curation

# Continue with stages 3-10...
python tests/stage_testing_framework.py --brand "Your Brand Name" --stage 3  # Ranking
python tests/stage_testing_framework.py --brand "Your Brand Name" --stage 4  # Ingestion
python tests/stage_testing_framework.py --brand "Your Brand Name" --stage 5  # Strategic Labeling
python tests/stage_testing_framework.py --brand "Your Brand Name" --stage 6  # Embeddings
python tests/stage_testing_framework.py --brand "Your Brand Name" --stage 7  # Visual Intelligence
python tests/stage_testing_framework.py --brand "Your Brand Name" --stage 8  # Strategic Analysis
python tests/stage_testing_framework.py --brand "Your Brand Name" --stage 9  # Multi-Dimensional Intelligence
python tests/stage_testing_framework.py --brand "Your Brand Name" --stage 10 # Enhanced Output

# Force re-run (ignore cache)
python tests/stage_testing_framework.py \
  --brand "Your Brand Name" \
  --stage 5 \
  --force

# Clean tables before testing
python tests/stage_testing_framework.py \
  --brand "Your Brand Name" \
  --clean
```

#### Stage Testing Benefits
- **Cached Results**: Each stage caches its output for subsequent stages
- **Independent Testing**: Test any stage without re-running earlier stages
- **Full Traceability**: Results saved in `data/output/stage_tests/[test_id]/`
- **Debugging Support**: Detailed logs and intermediate outputs

#### Check Results
```bash
# View run ID from output
echo "Run ID displayed in pipeline output"

# Query BigQuery results
bq query --use_legacy_sql=false '
SELECT * FROM `'$BQ_PROJECT'.'$BQ_DATASET'.strategic_intelligence_[RUN_ID]`
WHERE signal_strength = "CRITICAL"
'

# Check output files
ls -la data/output/systematic_intelligence_*.json
ls -la data/output/interventions_*.json
ls -la data/output/whitespace_*.json
ls -la data/output/sql_dashboards_*/
```

### 3. Explore Intelligence Levels

**L1 Executive (5 critical insights)**:
```sql
SELECT insight_text, confidence, business_impact
FROM `your-project.competitive_intelligence.strategic_intelligence_[RUN_ID]`
WHERE disclosure_level = 'L1_EXECUTIVE'
ORDER BY composite_score DESC
```

**L2 Strategic (15 strategic signals)**:
```sql
SELECT * FROM `your-project.competitive_intelligence.audience_intelligence_[RUN_ID]`
```

**L3 Interventions (25 actionable recommendations)**:
```sql
SELECT * FROM `your-project.competitive_intelligence.interventions_[RUN_ID]`
```

**L4 Dashboards (Full analytical detail)**:
```sql
-- See all generated SQL dashboards in:
-- data/output/sql_dashboards_[RUN_ID]/
```

## 📖 Documentation

### Technical Architecture
- **[Technical Architecture](docs/technical_architecture_section.md)** - High-level system design and BigQuery AI integration
- **[Pipeline Architecture](docs/pipeline_architecture_documentation.md)** - Detailed 10-stage pipeline specifications
- **[BigQuery Command Reference](docs/bigquery_command_reference.md)** - Complete AI primitive usage guide

### Demo
- **[Demo Notebook](notebooks/demo_competitive_intelligence.ipynb)** - Interactive pipeline walkthrough
- **[Demo Video](https://www.youtube.com/watch?v=2m_0HYDXGnY)** - Video of Demo Notebook walkthrough  


### Competition Submission
- **[Kaggle Competition Writeup](docs/kaggle_competition_writeup.md)** - Complete competition submission with innovation highlights

## Innovations

### 1. Real-Time Copying Detection
Using `ML.DISTANCE()` on 768-dimensional embeddings with temporal lag analysis:
```sql
-- Detect copying with mathematical proof
SELECT
  ML.DISTANCE(a.embedding, b.embedding, 'COSINE') as similarity,
  DATE_DIFF(b.start_date, a.start_date, DAY) as copy_lag
WHERE similarity < 0.3  -- 70%+ similarity threshold
```
**Result**: Detected Zenni Optical copying Warby Parker at 73.7% similarity

### 2. 3D Market Gap Analysis
Multi-dimensional whitespace detection across messaging × funnel × persona:
```sql
-- Find untapped market opportunities
GROUP BY messaging_angle, funnel_stage, target_persona
HAVING competitor_count = 0  -- VIRGIN_TERRITORY
```
**Result**: Identified 6 market opportunities worth $150K-300K each

### 3. Progressive Intelligence Disclosure
Prevents executive information overload through smart filtering:
- **L1**: 5 critical insights (80%+ confidence)
- **L2**: 15 strategic signals (60%+ confidence)
- **L3**: 25 actionable interventions (high actionability)
- **L4**: Complete analytical transparency

### 4. Native Temporal Forecasting
30-day competitive predictions using `ML.FORECAST()`:
```sql
ML.FORECAST(
  MODEL competitive_trends,
  STRUCT(30 AS horizon, 0.95 AS confidence_level)
)
```

## 💡 Usage Examples

### Competitive Threat Monitoring
```python
# Detect copying patterns
from src.intelligence.framework import CompetitiveCopyingDetector

detector = CompetitiveCopyingDetector(project_id=BQ_PROJECT)
threats = detector.detect_copying_threats(
    target_brand="Your Brand",
    similarity_threshold=0.3
)
```

### Market Opportunity Discovery
```python
# Find market gaps
from src.intelligence.framework import Enhanced3DWhiteSpaceDetector

whitespace = Enhanced3DWhiteSpaceDetector(project_id=BQ_PROJECT)
opportunities = whitespace.detect_opportunities(
    target_brand="Your Brand",
    min_investment_potential=100000
)
```

### Creative Fatigue Analysis
```python
# Monitor creative exhaustion
from src.intelligence.framework import CreativeFatigueAnalyzer

fatigue = CreativeFatigueAnalyzer(project_id=BQ_PROJECT)
status = fatigue.analyze_fatigue_risk(
    target_brand="Your Brand",
    window_days=30
)
```

## 🔧 Advanced Configuration

### Custom Competitor Lists
```python
# Override auto-discovery with manual competitor list
competitors = [
    "Warby Parker", "Zenni Optical", "EyeBuyDirect",
    "Glasses.com", "LensCrafters"
]

pipeline = CompetitiveIntelligencePipeline(
    target_brand="Your Brand",
    manual_competitors=competitors
)
```

### Adaptive Sampling Control
```python
# Control visual analysis budget
config = {
    "visual_analysis_budget": 200,  # Total images
    "per_brand_limit": 20,          # Images per competitor
    "sampling_strategy": "adaptive"  # Adjusts by portfolio size
}
```

## 🧪 Testing & Development

```bash
# Run tests
pytest tests/

# Code formatting
black src/ tests/

# Linting
flake8 src/ tests/

# Type checking (if using mypy)
mypy src/
```

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## [BigQuery AI Hackathon](https://www.kaggle.com/competitions/bigquery-ai-hackathon/overview)

This project demonstrates breakthrough innovations in competitive intelligence using BigQuery AI:

- **SQL-Native AI Workflows**: Complex multi-round consensus validation entirely within BigQuery
- **Real-Time Competitive Threat Detection**: Mathematical copying detection with temporal analysis
- **True Multimodal Intelligence**: Simultaneous visual-text analysis for story alignment
- **Progressive Disclosure Architecture**: Information hierarchy preventing executive overwhelm

Built for the [BigQuery AI Hackathon](https://www.kaggle.com/competitions/bigquery-ai-hackathon/overview) - transforming competitive analysis from reactive reporting to proactive strategic advantage.

---

**Ready to transform your competitive intelligence?** Start with the [demo notebook](notebooks/demo_competitive_intelligence.ipynb) or dive into the [technical architecture](docs/technical_architecture_section.md).
