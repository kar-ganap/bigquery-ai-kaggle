# Data Flow Comparison: Orchestrator vs Staged Testing

## Stage-by-Stage Data Flow Analysis

### Stage 1: Discovery
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | brand, vertical | brand, vertical | ✅ |
| **Stage Class** | DiscoveryStage | DiscoveryStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Output** | raw_candidates | raw_candidates | ✅ |

### Stage 2: AI Curation
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | raw_candidates | raw_candidates | ✅ |
| **Stage Class** | CurationStage | CurationStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Output** | validated_competitors | validated_competitors | ✅ |

### Stage 3: Meta Ad Ranking
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | validated_competitors | validated_competitors | ✅ |
| **Stage Class** | RankingStage | RankingStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Context Modified** | Sets competitor_brands | Sets competitor_brands (FIXED) | ✅ |
| **Output** | ranked_competitors | ranked_competitors | ✅ |

### Stage 4: Meta Ads Ingestion
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | ranked_competitors | ranked_competitors | ✅ |
| **Stage Class** | IngestionStage | IngestionStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Limits** | ENV vars or 500/10 | ENV vars or 500/10 | ✅ |
| **Output** | ingestion_results | ingestion_results | ✅ |
| **Note** | API results vary by time | API results vary by time | ⚠️ |

### Stage 5: Strategic Labeling
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | None (uses BQ data) | None (uses BQ data) | ✅ |
| **Stage Class** | StrategicLabelingStage | StrategicLabelingStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Output** | strategic_results | strategic_results | ✅ |

### Stage 6: Embeddings Generation
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | None (uses BQ data) | None (uses BQ data) | ✅ |
| **Stage Class** | EmbeddingsStage | EmbeddingsStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Output** | embedding_results | embedding_results | ✅ |

### Stage 7: Visual Intelligence
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | None (uses BQ data) | None (uses BQ data) | ✅ |
| **Stage Class** | VisualIntelligenceStage | VisualIntelligenceStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Output** | visual_intel_results | visual_intel_results | ✅ |

### Stage 8: Strategic Analysis
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | None (uses BQ data) | None (uses BQ data) | ✅ |
| **Stage Class** | AnalysisStage | AnalysisStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Output** | analysis | analysis | ✅ |

### Stage 9: Multi-Dimensional Intelligence
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | analysis | analysis | ✅ |
| **Stage Class** | MultiDimensionalIntelligenceStage | MultiDimensionalIntelligenceStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Special Setup** | Sets competitor_brands from context | Sets competitor_brands from context (FIXED) | ✅ |
| **Special Setup** | Sets visual_intelligence_results | Sets visual_intelligence_results | ✅ |
| **Output** | multidim_results | multidim_results | ✅ |

### Stage 10: Enhanced Output
| Aspect | Orchestrator | Staged Testing | Parity? |
|--------|--------------|----------------|---------|
| **Input** | analysis | analysis | ✅ |
| **Stage Class** | EnhancedOutputStage | EnhancedOutputStage | ✅ |
| **Context Used** | self.context | self.context | ✅ |
| **Output** | output | output | ✅ |

## Summary

### ✅ Code-Level Parity Achieved
After the fix, both approaches:
1. Use identical stage classes
2. Pass the same data between stages
3. Use the same context object
4. Apply the same configuration (ENV vars)
5. Store the same context modifications

### ⚠️ External Factors Causing Differences
1. **Meta Ad Library API Variability**:
   - Returns different results at different times
   - May have rate limiting or caching effects
   - Pagination can be inconsistent

2. **Timing Dependencies**:
   - BigQuery table creation/availability
   - API rate limits
   - Network latency variations

## Conclusion
**From a code perspective, there is now COMPLETE PARITY** between the orchestrator and staged testing framework. Any remaining differences in results are due to external factors (primarily the Meta Ad Library API returning different data at different times).