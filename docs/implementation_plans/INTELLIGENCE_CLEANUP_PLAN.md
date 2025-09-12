# Intelligence Pipeline Cleanup Plan

## Critical Issues Found

### 1. Misleading "Performance" Metrics
**Problem**: `avg_performance` is just `promotional_intensity` renamed
- **Lines affected**: 1738, 2233, 2367, 2426, 3664, 3816
- **Impact**: Reports show "performance vs market -36%" but it's comparing promotional tone, not marketing metrics
- **Real data available**: None - Meta Ad Library doesn't provide CPM, CTR, ROAS

### 2. Meaningless "Strategy Diversity" 
**Problem**: Just `COUNT(DISTINCT primary_angle)` 
- **Lines affected**: 2234, 3157, 3194, 3465, 3667, 3682
- **Impact**: "Strategy diversity: 2" tells marketers nothing actionable

### 3. "Other_Strategy" Pattern Analysis
**Problem**: Catch-all category for undefined angles
- **Line**: 2975
- **Impact**: "Adopt 'Other_Strategy' pattern used by 5 top performers" is useless advice

### 4. Mock Data in Dashboards
**Tables using mock data**: 4 out of 7 dashboards
- `ads_strategic_labels_mock` - should use real strategic analysis
- Missing real data integration plan

## Implementation Plan

### Phase 1: Remove Misleading Metrics (HIGH PRIORITY)
1. **Replace `avg_performance`**:
   - Rename to `avg_promotional_intensity` 
   - Add clear disclaimers about what it measures
   - Remove "performance vs market" comparisons

2. **Fix Strategy Diversity**:
   - Replace with "angle_variety_score" (more descriptive)
   - Add context about what angles are used
   - Or remove if not actionable

3. **Fix Pattern Analysis**:
   - Replace "Other_Strategy" with specific angle analysis
   - Provide actual tactical insights instead of generic categories

### Phase 2: Real Data Integration Plan
1. **Data Source Mapping**:
   ```
   REAL DATA AVAILABLE:
   ✅ ads_raw_* (Meta ad content, platforms, dates)  
   ✅ ads_embeddings (content analysis, similarity)
   ✅ cta_aggressiveness_analysis (CTA scoring)
   
   MOCK DATA TO REPLACE:
   ❌ ads_strategic_labels_mock → use real ML.GENERATE_TEXT analysis
   ❌ Hardcoded promotional_intensity → derive from real content analysis
   ```

2. **Dashboard Conversion Priority**:
   - **Keep as-is**: Copying Monitor, CTA Analysis, Content Quality (use real data)
   - **Convert to real data**: Strategic Mix, Evolution Trends, Channel Performance, Lifecycle
   - **Remove/replace**: Performance comparisons, generic pattern analysis

### Phase 3: Honest Metric Framework
1. **What We Can Actually Measure**:
   - Ad content similarity and copying detection
   - CTA aggressiveness competitive benchmarking  
   - Channel distribution (FB/IG/YouTube presence)
   - Creative format analysis (video/photo/carousel)
   - Temporal activity patterns
   - Content quality and length analysis

2. **What We Cannot Measure** (add disclaimers):
   - Actual ad performance (CPM, CTR, ROAS)
   - True marketing effectiveness
   - ROI or conversion metrics

### Phase 4: Actionable Intelligence Focus
Replace generic metrics with specific, actionable insights:

**Instead of**: "Strategy diversity: 2"
**Provide**: "You use 2 angles (Emotional, Functional) vs competitors using 4+ angles. Missing: Aspirational messaging for premium positioning."

**Instead of**: "Performance vs market -36%"  
**Provide**: "Your promotional intensity (0.4) is lower than competitors (0.7). This may indicate more premium positioning or missed promotional opportunities."

**Instead of**: "Adopt Other_Strategy pattern"
**Provide**: "EyeBuyDirect's aggressive discount messaging ('70% OFF + Free Shipping') drives high CTA scores. Consider testing limited-time offers."

## Implementation Steps

### Immediate (This Sprint):
1. ✅ Add disclaimers to all "performance" metrics
2. ✅ Rename `avg_performance` to `avg_promotional_intensity`  
3. ✅ Remove misleading "performance vs market" calculations
4. ✅ Fix "Other_Strategy" pattern analysis

### Next Sprint:
1. Replace mock data tables with real ML.GENERATE_TEXT analysis
2. Create proper strategic labeling from real ad content
3. Build content-driven promotional intensity scoring

### Future:
1. Advanced competitive intelligence using real embeddings
2. Predictive modeling based on historical competitive moves
3. White space analysis using real market coverage data

## Success Metrics
- Zero misleading "performance" metrics in reports
- All dashboards use real data or clearly marked proxies
- Actionable recommendations that marketers can immediately implement
- Honest assessment when insufficient data exists for recommendations

This plan ensures we deliver genuine competitive intelligence rather than statistical manipulation of basic counts.