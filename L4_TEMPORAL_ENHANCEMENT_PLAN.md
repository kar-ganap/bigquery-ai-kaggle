# L4 Temporal Intelligence Enhancement Plan

## Overview
Transform L4 from **static competitive snapshots** to **dynamic temporal intelligence** - a "competitive time machine" that reveals where we are, where we came from, and where we're going.

## Current State vs Vision

### Current L4 Reality
- ❌ **Static snapshots**: "Here's what everyone looks like right now"
- ❌ **No temporal language** in L1-L3 outputs
- ❌ **Missing 3-way framing**: No "where we came from/going" context
- ✅ **Basic temporal fields**: `first_seen`, `last_seen`, `active_days`
- ✅ **Module-specific dashboards**: Separate SQL for each intelligence area

### Target Vision: "Competitive Time Machine"
- ✅ **Temporal competitive positioning**: Strategic evolution over time
- ✅ **Strategic inflection detection**: "3 weeks ago, competitor X shifted strategy"
- ✅ **Competitive velocity analysis**: Who's moving fastest in each dimension
- ✅ **Predictive scenario modeling**: "If trends continue, competitor Y will overtake us by Q2"

## Implementation Phases

### Phase 1: Foundation (2-3 hours) ⭐ **START HERE**
**Goal**: Add temporal dimensions to existing L4 queries

**Existing Infrastructure**:
- ✅ `weekly_strategy_metrics` table with full time-series data
- ✅ `ads_with_dates` with proper temporal fields
- ✅ L4 SQL generation framework in `src/intelligence/framework.py:299`

**Changes**:
1. Modify `_generate_module_sql()` to include temporal CTEs
2. Add week-over-week calculations to existing intelligence modules
3. Include competitive ranking evolution (WHO is moving up/down)

**Example Enhancement**:
```python
# BEFORE: Static snapshot
def _generate_module_sql(self, module, project_id, dataset_id):
    return f"""
    SELECT brand, COUNT(*) as total_ads
    FROM {dataset_id}.ads_with_dates
    GROUP BY brand
    """

# AFTER: Temporal analysis
def _generate_module_sql(self, module, project_id, dataset_id):
    return f"""
    WITH temporal_metrics AS (
      SELECT brand, week_start,
        COUNT(*) as weekly_ads,
        LAG(COUNT(*)) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_ads,
        RANK() OVER (PARTITION BY week_start ORDER BY COUNT(*) DESC) as weekly_rank
      FROM {dataset_id}.ads_with_dates
      GROUP BY brand, week_start
    )
    SELECT brand,
      AVG(weekly_ads) as avg_weekly_ads,
      (weekly_ads - prev_week_ads) as wow_change,
      weekly_rank,
      LAG(weekly_rank) OVER (PARTITION BY brand ORDER BY week_start) as prev_rank
    FROM temporal_metrics
    WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
    """
```

### Phase 2: Velocity Intelligence (3-4 hours)
**Goal**: Add competitive acceleration/deceleration metrics

**New L4 Capabilities**:
- Creative Velocity: How fast is each brand changing creative strategy?
- Channel Momentum: Who's expanding cross-platform presence fastest?
- Visual Evolution Speed: Rate of visual style changes

**Example Implementation**:
```sql
-- New L4 query: competitive_velocity_analysis.sql
WITH velocity_metrics AS (
  SELECT brand, week_start,
    -- Creative velocity
    ABS(avg_creative_length - LAG(avg_creative_length, 2) OVER (
      PARTITION BY brand ORDER BY week_start
    )) / 2.0 as creative_velocity,

    -- Channel expansion rate
    (cross_platform_pct - LAG(cross_platform_pct, 4) OVER (
      PARTITION BY brand ORDER BY week_start
    )) / 4.0 as channel_expansion_velocity
  FROM weekly_strategy_metrics
)
SELECT brand,
  ROUND(AVG(creative_velocity), 2) as avg_creative_velocity,
  ROUND(AVG(channel_expansion_velocity), 2) as avg_channel_velocity,
  RANK() OVER (ORDER BY AVG(creative_velocity) DESC) as velocity_rank
FROM velocity_metrics
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK)
GROUP BY brand
```

### Phase 3: Strategic Inflection Detection (4-5 hours)
**Goal**: Identify when competitive dynamics shifted

**New L4 Capabilities**:
- Strategy Change Points: "3 weeks ago, EyeBuyDirect increased video content by 40%"
- Competitive Response Analysis: "Our response to competitor moves"
- Market Shift Detection: When did the overall landscape change?

### Phase 4: Predictive Integration (2-3 hours)
**Goal**: Connect existing forecasting models to L4 dashboards

**Existing Assets**:
- ✅ `strategic_forecasting_models.sql` with ML.FORECAST models
- ✅ Forecasting for ad volume, aggressiveness, cross-platform strategy

**Enhancement**: Integrate predictions directly into L4 competitive analysis

### Phase 5: L1-L3 Temporal Language (1-2 hours)
**Goal**: Add "where we came from/going" framing to executive insights

**Example Enhancement**:
- **Current L1**: "Competitive copying detected from EyeBuyDirect (similarity: 72.3%)"
- **Enhanced L1**: "EyeBuyDirect similarity increasing from 45% → 72% over 6 weeks - competitive copying threat accelerating"

## Effort Estimate

| Phase | Effort | Value Add | Risk |
|-------|--------|-----------|------|
| Phase 1: Temporal Foundation | 2-3 hours | HIGH | LOW |
| Phase 2: Velocity Intelligence | 3-4 hours | HIGH | LOW |
| Phase 3: Inflection Detection | 4-5 hours | MEDIUM | MEDIUM |
| Phase 4: Predictive Integration | 2-3 hours | MEDIUM | LOW |
| Phase 5: L1-L3 Enhancement | 1-2 hours | HIGH | LOW |
| **TOTAL** | **12-17 hours** | | |

## Technical Approach

**Key Files to Modify**:
- `src/intelligence/framework.py:299` - SQL generation
- `src/intelligence/framework.py:286` - L4 dashboard creation
- SQL templates for each intelligence module

**Why Low Risk**:
- ✅ No changes to data ingestion
- ✅ No changes to L1-L3 core logic
- ✅ Purely additive to existing L4 capabilities
- ✅ All temporal data infrastructure already exists

## Success Metrics

**Before**: L4 provides static competitive snapshots
**After**: L4 provides competitive time-travel capabilities:
1. **Rewind**: "Show me the 3-month evolution that led to EyeBuyDirect's advantage"
2. **Fast-forward**: "Where will competitive landscape be in Q2?"
3. **Compare timelines**: "Our strategy moves vs competitor reactions"
4. **Scenario modeling**: "What if we had launched video strategy 2 months earlier?"

---

## Next Steps
1. **START**: Phase 1 implementation
2. Test temporal queries with existing data
3. Validate week-over-week calculations
4. Iterate on competitive ranking logic