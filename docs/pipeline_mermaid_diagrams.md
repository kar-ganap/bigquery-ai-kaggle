# Pipeline Mermaid Diagrams

## 1. Data Flow & Pipeline Overview

```mermaid
flowchart TD
    A[Google Search<br/>466 Candidates] --> B[AI Curation<br/>AI.GENERATE_TABLE<br/>3-Round Consensus]
    B --> C[15 Filtered<br/>Candidates]
    C --> D[Meta Activity<br/>Ranking]
    D --> E[7 Validated<br/>Competitors]
    E --> F[Ad Ingestion<br/>391 Ads Collected]
    F --> G[Strategic Labeling<br/>Batch AI Processing]
    G --> H[Embeddings<br/>ML.GENERATE_EMBEDDING<br/>768-dim vectors]
    H --> I[Visual Intelligence<br/>AI.GENERATE<br/>Multimodal Analysis]
    I --> J[Strategic Analysis<br/>ML.DISTANCE + ML.FORECAST]
    J --> K[Multi-Dimensional<br/>Intelligence]
    K --> L[Progressive Disclosure<br/>L1→L2→L3→L4]

    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style H fill:#f3e5f5
    style I fill:#f3e5f5
    style J fill:#f3e5f5
    style L fill:#e8f5e8
```

## 2. BigQuery AI Primitives Flow

```mermaid
graph LR
    subgraph "Stage 2: AI Curation"
        A1[AI.GENERATE_TABLE<br/>3 Consensus Rounds<br/>466→15→7]
    end

    subgraph "Stage 6: Embeddings"
        B1[ML.GENERATE_EMBEDDING<br/>768-dimensional<br/>Semantic Vectors]
    end

    subgraph "Stage 7: Visual"
        C1[AI.GENERATE<br/>Multimodal Analysis<br/>Visual + Text]
    end

    subgraph "Stage 8: Analysis"
        D1[ML.DISTANCE<br/>Cosine Similarity<br/>Copying Detection]
        D2[ML.FORECAST<br/>4-week Horizon<br/>Trend Prediction]
    end

    subgraph "Stage 9: Intelligence"
        E1[AI.GENERATE<br/>Creative Intelligence<br/>Emotion Analysis]
        E2[AI.GENERATE<br/>5 Intelligence<br/>Dimensions]
    end

    A1 --> B1
    B1 --> C1
    B1 --> D1
    C1 --> D2
    C1 --> E1
    D1 --> E2
    D2 --> E2

    style A1 fill:#e3f2fd
    style B1 fill:#e3f2fd
    style C1 fill:#e3f2fd
    style D1 fill:#e3f2fd
    style D2 fill:#e3f2fd
    style E1 fill:#e3f2fd
    style E2 fill:#e3f2fd
```

## 3. Progressive Disclosure Hierarchy

```mermaid
graph TD
    A[Raw Competitive Data<br/>391 Ads, 7 Competitors] --> B[Signal Processing<br/>40+ Algorithmic Features]
    B --> C[Intelligence Classification<br/>CRITICAL/HIGH/MEDIUM/LOW/NOISE]
    C --> D[L1: Executive Summary<br/>5 Critical Insights<br/>80%+ Confidence]
    C --> E[L2: Strategic Dashboard<br/>10-15 Strategic Signals<br/>60%+ Confidence]
    C --> F[L3: Actionable Interventions<br/>20-25 Tactical Opportunities<br/>High Actionability]
    C --> G[L4: SQL Dashboards<br/>Full Analytical Detail<br/>Custom Intelligence]

    D --> H[Executive Decision Making]
    E --> I[Strategic Planning]
    F --> J[Tactical Implementation]
    G --> K[Deep Dive Analysis]

    style D fill:#ffcdd2
    style E fill:#fff3e0
    style F fill:#e8f5e8
    style G fill:#e1f5fe
```

## 4. Multi-Dimensional Intelligence Architecture

```mermaid
mindmap
  root((Competitive Intelligence))
    Audience Intelligence
      Platform Strategy
      Demographics
      Psychographics
    Creative Intelligence
      Fatigue Analysis (42% Risk)
      Sentiment Scoring
      Message Themes
    Channel Intelligence
      Cross-Platform Synergy
      Platform Mix
      Content Adaptation
    Visual Intelligence
      Brand Consistency
      Creative Differentiation
      Visual Fatigue (42% Risk)
    Whitespace Intelligence
      Market Gaps
      Competitive Opportunities
      Strategic Positioning
```

## 5. Temporal Intelligence Timeline

```mermaid
timeline
    title Temporal Intelligence Framework

    section Historical Analysis (8 weeks)
        Week -8 to -1 : Momentum Tracking
                      : Velocity Changes
                      : Evolution Patterns
                      : Campaign Cycles

    section Current State
        Now : Fatigue: 42% Risk
            : Threat: CRITICAL
            : Position: Offensive
            : CTA Score: 9.7/10
            : Copying: 72.9% Similarity

    section Predictive Forecasting (4 weeks)
        Week +1 to +4 : ML.FORECAST Models
                      : Trend Prediction
                      : Confidence Bands
                      : Strategic Alerts
```

## 6. System Architecture Overview

```mermaid
C4Context
    title BigQuery AI Competitive Intelligence System

    Person(user, "Business User", "Strategic decision maker")
    System(pipeline, "Competitive Intelligence Pipeline", "10-stage BigQuery AI native system")

    System_Ext(google, "Google Search", "Competitor discovery")
    System_Ext(meta, "Meta Ads API", "Ad data collection")
    System_Ext(bigquery, "BigQuery ML", "AI processing & storage")
    System_Ext(vertex, "Vertex AI", "Visual intelligence")

    Rel(user, pipeline, "Gets intelligence")
    Rel(pipeline, google, "Discovers competitors")
    Rel(pipeline, meta, "Fetches ad data")
    Rel(pipeline, bigquery, "Processes with AI")
    Rel(pipeline, vertex, "Analyzes visuals")
```

## 7. Real-time Processing Flow

```mermaid
sequenceDiagram
    participant U as User
    participant P as Pipeline
    participant BQ as BigQuery AI
    participant M as Meta API
    participant V as Vertex AI

    U->>P: Request competitive analysis
    P->>BQ: AI.GENERATE_TABLE (Curation)
    BQ-->>P: 7 validated competitors
    P->>M: Fetch competitor ads
    M-->>P: 391 ads collected
    P->>BQ: ML.GENERATE_EMBEDDING
    BQ-->>P: 768-dim vectors
    P->>V: AI.GENERATE (Visual analysis)
    V-->>P: Multimodal insights
    P->>BQ: ML.DISTANCE + ML.FORECAST
    BQ-->>P: Similarity & trends
    P-->>U: L1→L4 Progressive intelligence
```

## Usage Benefits

### **Mermaid Advantages over ASCII:**
- **Professional Rendering**: Clean, publication-ready diagrams
- **GitHub Integration**: Native rendering in GitHub/GitLab
- **Interactive Elements**: Clickable nodes and hover effects
- **Export Options**: SVG/PNG export for presentations
- **Responsive Design**: Scales well across devices

### **Best Use Cases:**
- **Technical Documentation**: GitHub README files
- **Presentation Materials**: Export to slides
- **Interactive Demos**: Live rendering in web interfaces
- **Competition Submission**: Professional visual appeal

### **Combination Strategy:**
- **ASCII diagrams**: Always work, great for terminals/text-only environments
- **Mermaid diagrams**: Enhanced visual appeal for web/presentation contexts
- Use both to maximize compatibility and impact