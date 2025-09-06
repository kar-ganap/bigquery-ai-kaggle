# Ad Embedding Strategy Roadmap

## Current Scope (Subgoal 4 - Sprint 1)

### Phase 1: Text-Only Embeddings (NOW)
**Approach**: Pre-concatenation with semantic separators

```sql
-- Combine text components BEFORE embedding
combined_text = CONCAT(
  'Title: ', COALESCE(title, ''), 
  ' Description: ', COALESCE(body_text, ''),
  ' Action: ', COALESCE(cta_text, '')
)
embedding = ML.GENERATE_EMBEDDING(combined_text)
```

**Rationale**:
- ✅ BigQuery-native with `ML.GENERATE_EMBEDDING`
- ✅ Single vector per ad for simple similarity search
- ✅ Proven approach used by Meta/Google ads
- ✅ 80% of value with 20% of complexity

**Key Features**:
- Text deduplication using `ML.DISTANCE()` for similarity
- Semantic search: "Find ads similar to this one"
- Creative clustering by message themes
- Strategic insights from competitor patterns

## Future Scope (Beyond Subgoal 4)

### Phase 2: Component-Specific Embeddings (Q2 2025)
**Approach**: Late fusion with separate embeddings per component

```sql
CREATE TABLE ads_embeddings_v2 (
  ad_id STRING,
  title_embedding ARRAY<FLOAT64>,
  body_embedding ARRAY<FLOAT64>,
  cta_embedding ARRAY<FLOAT64>,
  combined_embedding ARRAY<FLOAT64>
)
```

**Benefits**:
- Component-specific search ("Find ads with similar CTAs")
- Weighted importance by component
- Better interpretability of matches
- A/B testing different weighting strategies

### Phase 3: Multimodal Embeddings (Q3 2025)
**Approach**: Add visual embeddings alongside text

```sql
-- Text + Image embeddings
image_embedding = ML.GENERATE_EMBEDDING(image_url, model='multimodal-embedding')
combined_multimodal = weighted_average(text_embedding, image_embedding)
```

**Technical Requirements**:
- Vertex AI Vision API integration
- Image preprocessing pipeline
- Storage for image features
- Updated similarity metrics

### Phase 4: Advanced Video/Carousel Support (Q4 2025)
**Approach**: Temporal and sequential modeling

**Video Processing**:
- Frame extraction at key intervals
- Temporal pooling strategies (mean, max, attention-based)
- Scene change detection for smart sampling
- Motion and transition embeddings

**Carousel Processing**:
- Sequence modeling for narrative flow
- Position-weighted embeddings
- Transition pattern detection
- Multi-frame storytelling analysis

**Implementation Challenges**:
- Requires external preprocessing (not BigQuery-native)
- High computational cost
- Complex storage requirements
- Need for specialized models

## Key Technical Considerations

### Deduplication Strategy
**Current (Phase 1)**:
- Exact text matching on `ad_archive_id`
- Near-duplicate detection using cosine similarity > 0.95

**Future (Phase 2+)**:
- Cross-modal deduplication (same creative, different format)
- Variant detection (A/B test variations)
- Campaign-level deduplication

### Embedding Combination Strategies

| Strategy | Use Case | Pros | Cons |
|----------|----------|------|------|
| **Pre-concatenation** | Simple search | Single vector, easy | Loses granularity |
| **Weighted Average** | Component importance | Flexible weighting | Arbitrary weights |
| **Late Fusion** | Component search | Maximum flexibility | Complex storage |
| **Hierarchical** | Long text | Handles length well | Complex in SQL |

### BigQuery AI Limitations & Workarounds

**Current Limitations**:
- Text-only embeddings in `ML.GENERATE_EMBEDDING`
- Vector size limits (768 dimensions typical)
- No native video/image processing
- Rate limits on embedding generation

**Workarounds**:
- Batch processing with reasonable delays
- Caching embeddings to avoid regeneration
- External preprocessing for multimodal
- Hybrid approach with Vertex AI for advanced features

## Cost & Performance Optimization

### Phase 1 Costs (Estimated)
- Embedding generation: ~$0.0001 per 1K tokens
- Storage: ~$0.02 per GB/month for vectors
- Similarity search: Standard BigQuery compute costs

### Optimization Strategies
1. **Incremental Updates**: Only embed new/changed ads
2. **Sampling**: Test with subset before full processing
3. **Caching**: Store embeddings permanently
4. **Batch Processing**: Maximize API efficiency
5. **Dimension Reduction**: Consider smaller embeddings for cost

## Migration Path

### From Phase 1 → Phase 2
1. Keep existing combined embeddings
2. Add component embeddings incrementally
3. A/B test search quality
4. Gradually shift to component-based search

### From Phase 2 → Phase 3
1. Add image embedding column
2. Create multimodal combination logic
3. Backfill historical ads with image embeddings
4. Update similarity metrics

### From Phase 3 → Phase 4
1. Build external video processing pipeline
2. Create video feature storage
3. Implement temporal pooling
4. Integrate with existing search

## Success Metrics

### Phase 1 (Current)
- ✅ >90% deduplication accuracy
- ✅ <2 second search latency
- ✅ >80% relevant results in top 5

### Future Phases
- Component-specific precision >85%
- Multimodal search recall >75%
- Video similarity accuracy >70%
- Cross-campaign pattern detection >60%

## Research & References

### Industry Approaches
- **Meta**: Uses multimodal late fusion for ads ranking
- **Google**: Hierarchical embeddings for long documents
- **TikTok**: Temporal attention for video ads
- **Amazon**: Component-specific embeddings for products

### Academic Papers
- "Late Fusion for Multimodal Retrieval" (2023)
- "Temporal Pooling Strategies for Video Embeddings" (2024)
- "Efficient Deduplication at Scale" (2023)

### Tools & Technologies
- **Current**: BigQuery ML, Vertex AI
- **Future**: Cloud Vision API, Video Intelligence API, Custom Vertex AI models

## Decision Log

**2024-09-04**: Chose pre-concatenation for Phase 1
- Reason: Simplicity and BigQuery-native support
- Alternative considered: Component embeddings (too complex for MVP)

**Future Decisions**:
- [ ] Embedding model selection (text-embedding-004 vs gemini)
- [ ] Vector dimension (384 vs 768 vs 1536)
- [ ] Similarity threshold for deduplication
- [ ] Component weighting strategy