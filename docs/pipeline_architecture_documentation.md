# BigQuery AI Competitive Intelligence Pipeline Architecture

## Pipeline Overview

This document provides comprehensive technical documentation for the 10-stage competitive intelligence pipeline that transforms raw advertising data into actionable strategic insights through progressive disclosure (L1→L4). The system employs a modular, clean architecture built on BigQuery ML and advanced AI capabilities.

## Stage Summary Table

| # | Stage Name | Primary Goal | Key Features |
|--------|------------|--------------|--------------|
| 1 | Competitor Discovery | Discover competitor candidates using Google Custom Search | Auto-detection, Search Strategy, Standardization, Dry-run Support |
| 2 | AI Competitor Curation | AI-powered competitor validation with hybrid consensus | Aggressive Pre-filtering, Deterministic Scoring, 3-Round AI Consensus, Quality Scoring |
| 3 | Meta Ad Activity Ranking | Rank competitors by Meta advertising activity | Meta Activity Probing, Intelligent Re-ranking, Activity Tiers, Smart Capping |
| 4 | Meta Ads Ingestion | Full ad data collection with media classification | Parallel/Sequential Processing, Media Classification, Target Brand Inclusion, Data Normalization |
| 5 | Strategic Labeling | Generate comprehensive strategic labels using AI | Batch-Optimized AI, Deduplication Logic, Strategic Intelligence, Temporal Integration |
| 6 | Embeddings Generation | Generate semantic embeddings for similarity analysis | Structured Content, Quality Indicators, Semantic Analysis, Data Preservation |
| 7 | Visual Intelligence | Multimodal analysis with adaptive sampling | Adaptive Sampling, Budget Management, Multi-factor Scoring, Dual AI Analysis |
| 8 | Strategic Analysis | Comprehensive competitive intelligence analysis | Current State Analysis, Copying Detection, Creative Fatigue, Temporal Intelligence, CTA Intelligence |
| 9 | Multi-Dimensional Intelligence | Comprehensive data-driven intelligence synthesis | Audience Intelligence, Creative Intelligence, Channel Intelligence, Visual Intelligence Metrics, Whitespace Intelligence |
| 10 | Enhanced Output Generation | L1→L4 progressive disclosure with intelligent filtering | Executive Summary, Strategic Dashboard, Actionable Interventions, SQL Dashboards, Signal Intelligence |

---

## Detailed Stage Analysis

### 1. Competitor Discovery

**Primary Goal**: Discover competitor candidates using Google Custom Search Engine to establish the competitive landscape foundation.

#### a) Auto-detection System
The auto-detection feature automatically identifies brand verticals when not explicitly provided. This system analyzes domain patterns, industry keywords, and contextual signals to classify businesses into appropriate competitive categories. The detection algorithm employs heuristic rules combined with pattern matching to ensure accurate vertical assignment.

#### b) Search Strategy Engine
The search strategy implements comprehensive competitor discovery queries using Google Custom Search Engine. Multiple query patterns are executed including direct brand searches, category-based searches, and adjacency searches. The system employs intelligent query construction with vertical-specific keywords and Boolean operators to maximize discovery coverage.

#### c) Candidate Standardization
Raw search results undergo standardization through the `CompetitorCandidate` object model. This process normalizes disparate data sources into a consistent schema including company names, domains, confidence scores, and discovery metadata. Standardization ensures downstream stages receive clean, structured data.

#### d) Dry-run Support
Mock candidate generation provides testing capabilities without consuming API quotas. The dry-run mode generates realistic competitor candidates with configurable parameters, enabling development and validation workflows without external dependencies.

### 2. AI Competitor Curation

**Primary Goal**: AI-powered competitor validation with hybrid consensus to ensure high-quality competitive sets.

#### a) Aggressive Pre-filtering
Enhanced name validation employs confidence thresholding with dual-tier acceptance criteria. High-confidence candidates require 0.7+ confidence scores while fallback acceptance operates at 0.4+ thresholds. The pre-filtering system eliminates obviously invalid candidates through pattern matching, domain validation, and name quality assessment.

#### b) Deterministic Scoring
Pre-filter scoring combines multiple signals including raw discovery scores, discovery method reliability, and source location quality. The algorithm weights each factor according to empirical validation studies, creating composite scores that predict downstream AI validation success rates.

#### c) 3-Round AI Consensus
Multiple BigQuery ML validation rounds employ Gemini models to assess competitor relevance through structured analysis. Each round examines different aspects: market overlap, competitive positioning, and brand similarity. The consensus mechanism requires 2/3 majority agreement across rounds to accept candidates.

#### d) Quality Scoring System
Comprehensive scoring integrates AI confidence metrics, market overlap assessments, and discovery quality indicators. The scoring formula balances automated confidence with human-interpretable quality signals, enabling threshold-based filtering while maintaining transparency in selection criteria.

### 3. Meta Ad Activity Ranking

**Primary Goal**: Rank competitors by Meta advertising activity with intelligent capping to prioritize active advertisers.

#### a) Meta Activity Probing
The `MetaAdsFetcher` component assesses advertising presence across Meta platforms through systematic API queries. Probing includes ad volume estimation, recency analysis, and platform coverage assessment. The system handles rate limiting and API errors gracefully while maximizing data collection efficiency.

#### b) Intelligent Re-ranking Algorithm
Combines AI confidence scores with Meta activity metrics using weighted formulas. The re-ranking algorithm applies platform-specific weights and activity tier multipliers to create composite relevance scores. Quality score calculation follows: `(ai_quality * 0.4) + (meta_weight * 0.6)`.

#### c) Activity Tier Classification
3-tier classification system categorizes competitors as Major, Moderate, or Minor players based on advertising volume and consistency. Tier assignments influence downstream processing priorities and budget allocations. Classification thresholds are calibrated based on industry benchmarks and campaign objectives.

#### d) Smart Capping Mechanism
Maximum 10 competitor limitation with early exit optimization prevents processing overhead while maintaining analytical completeness. The capping algorithm prioritizes high-activity competitors while preserving diversity across activity tiers. Early exit triggers activate when diminishing returns thresholds are reached.

### 4. Meta Ads Ingestion

**Primary Goal**: Full ad data collection with media classification to build comprehensive competitive intelligence datasets.

#### a) Parallel/Sequential Processing Engine
Configurable fetching strategies balance throughput with rate limiting requirements. Parallel processing utilizes worker pools for concurrent API calls while sequential processing ensures compliance with strict rate limits. The system dynamically adapts processing modes based on API response patterns and error rates.

#### b) Media Classification System
Real-time media type detection employs the `MediaStorageManager` for automatic content categorization. Classification algorithms analyze file metadata, content headers, and visual features to distinguish between images, videos, and mixed media. Storage paths are organized hierarchically for efficient retrieval.

#### c) Target Brand Inclusion Logic
Fetches advertisements for both competitor brands and target brands to enable comparative analysis. The inclusion system ensures balanced datasets while respecting API quotas and processing constraints. Target brand data provides baseline metrics for competitive benchmarking.

#### d) Data Normalization Framework
Standardizes ad data across different API formats and temporal schemas. Normalization handles schema variations, missing fields, and data type inconsistencies. The framework creates the foundational `ads_raw_{run_id}` BigQuery table with consistent field mappings.

### 5. Strategic Labeling

**Primary Goal**: Generate comprehensive strategic labels using AI to enable sophisticated competitive analysis.

#### a) Batch-Optimized AI Processing
Utilizes `sql/02_label_ads_batch.sql` for significant performance improvements over individual API calls. Batch processing reduces individual AI calls to single optimized operations. The system employs BigQuery `AI.GENERATE_TABLE` with structured prompts for consistent labeling.

#### b) Deduplication Logic Engine
Intelligent handling of first-run versus incremental data scenarios through marker-based SQL modification. The deduplication system identifies existing records, merges new data, and preserves historical labels. Logic prevents duplicate processing while enabling incremental updates.

#### c) Strategic Intelligence Generation
Produces promotional_intensity, urgency_score, and brand_voice_score metrics through structured AI analysis. Each metric employs 0.0-1.0 scales with calibrated prompts for consistent scoring. Intelligence generation includes funnel classification (Upper/Mid/Lower) and angle identification.

#### d) Temporal Integration System
Creates `ads_with_dates` table incorporating temporal intelligence fields for time-series analysis. Integration includes timestamp parsing, duration calculations, and temporal metadata extraction. The system establishes temporal foundations for downstream temporal intelligence modules.

### 6. Embeddings Generation

**Primary Goal**: Generate semantic embeddings for similarity analysis and competitive positioning assessment.

#### a) Structured Content Concatenation
Combines title, creative_text, cta_text, and brand fields using semantic structuring patterns. Concatenation follows established templates that optimize embedding quality while preserving content relationships. The structured approach improves downstream similarity calculations.

#### b) Quality Indicators Framework
Tracks content completeness and length metrics to assess embedding reliability. Quality indicators include presence flags (has_title, has_body) and content length measurements. These metrics enable quality-weighted similarity analysis and confidence scoring.

#### c) Semantic Analysis Engine
Employs BigQuery ML `ML.GENERATE_EMBEDDING` with 768-dimensional vector generation. The analysis uses SEMANTIC_SIMILARITY task types optimized for competitive intelligence applications. Embedding generation preserves semantic relationships across brand and content variations.

#### d) Data Preservation Architecture
Maintains all core inviolable fields throughout the embedding process to ensure downstream compatibility. The preservation system protects essential metadata while adding embedding-specific enhancements. Data integrity checks validate field preservation across transformations.

### 7. Visual Intelligence

**Primary Goal**: Multimodal analysis with adaptive sampling to extract visual competitive insights efficiently.

#### a) Adaptive Sampling Algorithm
Dynamic sample sizes based on brand portfolio characteristics optimize cost-effectiveness while maintaining analytical coverage. Sampling rates include 50% for portfolios ≤20 ads, 30% for 21-50 ads, with progressive reduction for larger portfolios. Algorithm balances representativeness with processing constraints.

#### b) Budget Management System
Per-brand budget allocation (20 images) with maximum total budget (200 images) ensures cost control while enabling comprehensive analysis. Budget management includes priority-based allocation and overflow handling. The system optimizes budget utilization across brand portfolios.

#### c) Multi-factor Scoring Engine
Combines recency, visual complexity, content length, and strategic diversity factors for intelligent image selection. Scoring weights are calibrated based on analytical value and processing efficiency. The algorithm ensures diverse, representative sampling for visual analysis.

#### d) Dual AI Analysis Framework
Separate visual-text alignment and competitive positioning analysis streams provide comprehensive multimodal insights. Dual analysis includes brand consistency assessment and competitive differentiation analysis. Framework employs Vertex AI integration for advanced visual processing.

### 8. Strategic Analysis

**Primary Goal**: Comprehensive competitive intelligence analysis integrating temporal, creative, and strategic dimensions.

#### a) Current State Analysis Engine
Evaluates promotional intensity, market position, and volatility metrics across competitive landscape. Analysis includes brand positioning assessment, message consistency evaluation, and strategic alignment scoring. The engine provides snapshot insights for strategic decision-making.

#### b) Copying Detection Algorithm
Embedding-based similarity analysis with temporal lag detection identifies competitive copying patterns. Detection employs cosine similarity calculations with threshold-based classification (threshold < 0.3 indicates potential copying). Temporal analysis tracks copying direction and timing patterns.

#### c) Creative Fatigue Analysis
Semantic repetition analysis with originality scoring detects creative exhaustion across campaigns. Fatigue analysis employs 30-day sliding windows for temporal assessment and originality scoring based on content diversity. The system identifies optimization opportunities and creative refresh needs.

#### d) Temporal Intelligence Module
Momentum analysis, velocity tracking, and evolution pattern detection provide dynamic competitive insights. The `TemporalIntelligenceEngine` processes time-series data for trend identification and forecasting. Intelligence includes competitive threat detection and market opportunity identification.

#### e) CTA Intelligence Framework
Aggressiveness scoring (0-10 scale) with strategy classification analyzes call-to-action effectiveness and competitive positioning. Framework includes urgency detection, promotional intensity assessment, and strategic alignment analysis. Intelligence supports CTA optimization and competitive response strategies.

### 9. Multi-Dimensional Intelligence

**Primary Goal**: Comprehensive data-driven intelligence synthesis across audience, creative, channel, and whitespace dimensions.

#### a) P0 Audience Intelligence
Platform strategy, communication style, and psychographic profiling provide comprehensive audience insights. Intelligence includes demographic analysis, behavioral pattern identification, and engagement strategy assessment. Analysis supports audience targeting optimization and competitive positioning.

#### b) P1 Creative Intelligence
Messaging themes, emotional tone, content complexity, and AI sentiment analysis deliver creative strategy insights. Intelligence includes message effectiveness scoring, emotional resonance analysis, and creative differentiation assessment. Framework enables creative strategy optimization and competitive creative analysis.

#### c) P1 Channel Intelligence
Platform optimization, cross-channel synergy, and content richness analysis provide channel strategy insights. Intelligence includes platform performance assessment, channel mix optimization, and content adaptation analysis. Framework supports multi-channel strategy development and competitive channel analysis.

#### d) Visual Intelligence Metrics
Extraction and synthesis of insights from Stage 7 visual analysis provides comprehensive visual competitive intelligence. Metrics include visual consistency scoring, brand differentiation analysis, and visual trend identification. Intelligence supports visual strategy optimization and competitive visual analysis.

#### e) P0 Whitespace Intelligence
Market gap identification through hybrid/parallel processing reveals competitive opportunities and threats. Intelligence employs the `Enhanced3DWhiteSpaceDetector` for comprehensive gap analysis across multiple dimensions. Framework identifies market opportunities, competitive vulnerabilities, and strategic positioning gaps.

### 10. Enhanced Output Generation

**Primary Goal**: L1→L4 progressive disclosure with intelligent filtering to deliver actionable insights at appropriate detail levels.

#### a) L1 Executive Summary
Top 5 critical insights with 80%+ confidence provide executive-level strategic overview. Summary includes highest-impact competitive threats, strategic opportunities, and immediate action requirements. Filtering ensures only mission-critical insights reach executive attention.

#### b) L2 Strategic Dashboard
10-15 strategic signals with 60%+ confidence deliver comprehensive strategic overview for tactical planning. Dashboard includes competitive positioning analysis, market trend identification, and strategic opportunity assessment. Signal strength classification ensures appropriate prioritization.

#### c) L3 Actionable Interventions
20-25 tactical opportunities with high actionability scores provide detailed implementation guidance. Interventions include specific competitive responses, optimization opportunities, and strategic adjustments. Framework ensures practical, implementable recommendations.

#### d) L4 SQL Dashboards
Full analytical detail with executable queries provides complete analytical transparency and customization capability. Dashboards include comprehensive data access, advanced analytical capabilities, and custom query support. Framework enables deep-dive analysis and custom intelligence development.

#### e) Signal Intelligence Classification
Automatic classification and threshold-based filtering optimize signal-to-noise ratio across all disclosure levels. Classification employs CRITICAL/HIGH/MEDIUM/LOW/NOISE categories with confidence-based thresholding. Intelligence framework ensures appropriate insight delivery based on strategic importance and confidence levels.

---

## Technical Architecture Components

### Core Infrastructure
- **Pipeline Orchestration**: Modular stage-based architecture with dependency management
- **Error Handling**: Comprehensive exception management with graceful degradation
- **Data Integrity**: Core inviolable fields preservation across all transformations
- **Performance Optimization**: Parallel processing, batch operations, and intelligent sampling

### AI Integration Framework
- **BigQuery ML**: Extensive use of `AI.GENERATE_TABLE` and `ML.GENERATE_EMBEDDING`
- **Vertex AI**: Direct integration for multimodal visual analysis
- **Gemini Models**: Strategic labeling and competitive analysis
- **Structured Output**: JSON extraction with robust error handling and validation

### Intelligence Engines
- **Temporal Intelligence**: Advanced time-series analysis and forecasting capabilities
- **Whitespace Detection**: Multi-dimensional market gap identification
- **Creative Analysis**: Fatigue detection, copying analysis, and originality scoring
- **Visual Analysis**: Multimodal AI with brand consistency and differentiation metrics