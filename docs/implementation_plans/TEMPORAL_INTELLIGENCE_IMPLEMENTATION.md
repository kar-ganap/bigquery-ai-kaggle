# Temporal Intelligence Implementation Plan

## Executive Summary
Transform our static competitive snapshot into a full temporal intelligence system that answers:
1. **Where are we?** (current state) ✅ Already working
2. **Where did we come from?** (historical changes) ❌ Need to implement  
3. **Where are we going?** (predictive forecasting) ❌ Built but not integrated

## Deliverable 1: Temporal Intelligence Integration

### A. "Where Did We Come From?" - Historical Context

**Implementation using real ad timestamps:**
```sql
-- Real temporal analysis without mock data
WITH temporal_movements AS (
  SELECT 
    brand,
    -- 7-day windows for velocity tracking
    COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 1 END) as ads_last_7d,
    COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) 
                AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) THEN 1 END) as ads_prior_7d,
    
    -- 30-day comparison for trend detection
    COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) as ads_last_30d,
    COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
                AND DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) as ads_prior_30d,
    
    -- CTA intensity evolution
    AVG(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
             THEN c.final_aggressiveness_score END) as recent_cta_score,
    AVG(CASE WHEN DATE(start_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
             THEN c.final_aggressiveness_score END) as historical_cta_score,
    
    -- Creative freshness indicators
    AVG(DATE_DIFF(CURRENT_DATE(), DATE(start_timestamp), DAY)) as avg_campaign_age,
    COUNT(DISTINCT creative_text) / NULLIF(COUNT(*), 0) as creative_diversity
    
  FROM `{project}.{dataset}.ads_raw_{run_id}` r
  LEFT JOIN `{project}.{dataset}.cta_aggressiveness_analysis` c 
    ON r.ad_id = c.ad_id
  WHERE r.brand IN (target_brand, competitors)
  GROUP BY brand
)
SELECT *,
  -- Calculate momentum indicators
  (ads_last_7d - ads_prior_7d) / NULLIF(ads_prior_7d, 0) as velocity_change_7d,
  (ads_last_30d - ads_prior_30d) / NULLIF(ads_prior_30d, 0) as velocity_change_30d,
  (recent_cta_score - historical_cta_score) as cta_intensity_shift,
  
  -- Categorize changes
  CASE 
    WHEN ads_last_7d > ads_prior_7d * 1.2 THEN 'ACCELERATING'
    WHEN ads_last_7d < ads_prior_7d * 0.8 THEN 'DECELERATING'
    ELSE 'STABLE'
  END as momentum_status,
  
  -- Fatigue risk assessment  
  CASE
    WHEN avg_campaign_age > 30 THEN 'HIGH_FATIGUE_RISK'
    WHEN avg_campaign_age > 21 THEN 'MODERATE_FATIGUE'
    ELSE 'FRESH_CREATIVES'
  END as creative_status
  
FROM temporal_movements
```

### B. "Where Are We Going?" - Wide Net Forecasting

**Integrate sophisticated forecasting from test files:**
```sql
-- Wide Net Forecasting with Signal Prioritization
WITH signal_detection AS (
  SELECT 
    brand,
    -- Tier 1 Strategic Signals
    AVG(promotional_intensity) as current_promo,
    LAG(AVG(promotional_intensity), 1) OVER (PARTITION BY brand ORDER BY week) as prev_promo,
    AVG(urgency_score) as current_urgency,
    LAG(AVG(urgency_score), 1) OVER (PARTITION BY brand ORDER BY week) as prev_urgency,
    
    -- Tier 2 Message Angle Signals (10+ types)
    COUNT(CASE WHEN primary_angle = 'EMOTIONAL' THEN 1 END) / COUNT(*) as emotional_pct,
    COUNT(CASE WHEN primary_angle = 'PROMOTIONAL' THEN 1 END) / COUNT(*) as promotional_pct,
    COUNT(CASE WHEN primary_angle = 'ASPIRATIONAL' THEN 1 END) / COUNT(*) as aspirational_pct,
    
    -- Business Impact Scoring
    CASE 
      WHEN ABS(current_promo - prev_promo) >= 0.15 THEN 5  -- High impact
      WHEN ABS(current_promo - prev_promo) >= 0.10 THEN 4
      WHEN ABS(current_promo - prev_promo) >= 0.08 THEN 3
      ELSE 2
    END as business_impact_weight
    
  FROM strategic_positions
  GROUP BY brand, week
),
noise_filtered_signals AS (
  SELECT *,
    -- Apply noise thresholds
    CASE 
      WHEN ABS(current_promo - prev_promo) < 0.10 THEN 'BELOW_THRESHOLD'
      WHEN ABS(current_urgency - prev_urgency) < 0.10 THEN 'BELOW_THRESHOLD'
      ELSE 'ABOVE_THRESHOLD'
    END as signal_quality,
    
    -- Generate executive summaries
    CASE
      WHEN business_impact_weight >= 5 AND current_promo > prev_promo * 1.15
        THEN CONCAT('🚨 CRITICAL: ', brand, ' promotional surge (+', 
                    ROUND((current_promo - prev_promo) * 100), '%) - immediate response needed')
      WHEN business_impact_weight >= 4 
        THEN CONCAT('⚠️ MULTIPLE_SHIFTS: ', brand, ' showing multiple strategic changes')
      WHEN business_impact_weight >= 3
        THEN CONCAT('📊 MODERATE: ', brand, ' tactical adjustments detected')
      ELSE NULL
    END as executive_forecast
    
  FROM signal_detection
  WHERE signal_quality = 'ABOVE_THRESHOLD'
)
SELECT 
  brand,
  executive_forecast,
  business_impact_weight,
  -- Top 5 prioritized signals only
  ARRAY_AGG(
    STRUCT(
      signal_type,
      magnitude_of_change,
      confidence_level,
      expected_timeline
    )
    ORDER BY business_impact_weight DESC
    LIMIT 5
  ) as prioritized_signals
FROM noise_filtered_signals
WHERE executive_forecast IS NOT NULL
```

## Deliverable 2: 3-Question Framework Integration

### Update Level 1 Executive Summary:
```json
{
  "temporal_intelligence": {
    "where_we_are": {
      "market_position": "balanced",
      "cta_gap": -3.94,
      "active_competitors": 9
    },
    "where_we_came_from": {
      "momentum": "DECELERATING",
      "key_changes": [
        "↑ Competitive intensity +18% in 30 days",
        "↓ Our ad velocity -12% week-over-week", 
        "⚠️ 2 campaigns entering HIGH_FATIGUE_RISK"
      ],
      "velocity_change_30d": -0.12
    },
    "where_we_are_going": {
      "forecast_summary": "🚨 CRITICAL: EyeBuyDirect promotional surge (+25%) expected",
      "top_predictions": [
        {"signal": "Competitor escalation", "confidence": "HIGH", "timeline": "7-14 days"},
        {"signal": "White space closing in FUNCTIONAL", "confidence": "MEDIUM", "timeline": "30 days"}
      ],
      "recommended_action": "Preemptive entry into PROBLEM_SOLUTION×CONSIDERATION space"
    }
  }
}
```

### Update Level 2 Strategic Dashboard:
Add dedicated temporal sections for each major metric:
- CTA Strategy: Show 7/30 day trends
- Channel Performance: Week-over-week shifts  
- White Spaces: Emergence/closure tracking
- Campaign Lifecycle: Fatigue progression

### Update Level 3 Actionable Interventions:
Frame all interventions with temporal context:
- "IMMEDIATE (based on last 7 days activity)"
- "UPCOMING (predicted in next 30 days)"
- "STRATEGIC (3-6 month horizon)"

## Deliverable 3: Enhanced 3D White Space Integration

### Replace current basic white space detection (lines 1629-1962) with:
```python
def _detect_white_spaces_enhanced(self) -> pd.DataFrame:
    """Enhanced 3D white space detection with real ML.GENERATE_TEXT analysis"""
    
    # Import the enhanced detector
    from scripts.enhanced_whitespace_detection import Enhanced3DWhiteSpaceDetector
    
    detector = Enhanced3DWhiteSpaceDetector(
        project_id=self.project_id,
        dataset_id=self.dataset_id,
        brand=self.brand,
        competitors=self.competitor_brands
    )
    
    # Generate SQL with real data analysis
    white_space_sql = detector.analyze_real_strategic_positions(self.run_id)
    
    # Execute and get results
    result = run_query(white_space_sql)
    
    if not result.empty:
        # Add temporal context to white spaces
        for idx, row in result.iterrows():
            # Check if this is a newly emerged opportunity
            row['emergence_status'] = self._check_white_space_emergence(
                row['messaging_angle'], 
                row['funnel_stage'],
                row['target_persona']
            )
            
            # Add predictive element
            row['closure_risk'] = self._predict_white_space_closure(row)
            
        return result
    return pd.DataFrame()

def _check_white_space_emergence(self, angle, funnel, persona):
    """Check if white space is new (last 30 days) or existing"""
    emergence_sql = f"""
    SELECT 
      COUNT(CASE WHEN DATE(start_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) as historical_presence,
      COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) as recent_presence
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_raw_{self.run_id}`
    WHERE messaging_angle = '{angle}'
      AND funnel_stage = '{funnel}'  
      AND target_persona LIKE '%{persona}%'
    """
    # Return NEW_OPPORTUNITY, EXPANDING, STABLE, or CLOSING
```

## Implementation Priority

### Phase 1 (Immediate):
1. ✅ Add temporal movement analysis using real timestamps
2. ✅ Frame Level 1 with 3-question structure
3. ✅ Surface existing cascade correlations properly

### Phase 2 (Today):
1. ✅ Integrate Wide Net forecasting from test files
2. ✅ Replace white space detection with enhanced version
3. ✅ Add temporal context to all metrics

### Phase 3 (Testing):
1. ✅ Run end-to-end pipeline with all enhancements
2. ✅ Validate temporal intelligence quality
3. ✅ Ensure no mock data remains

## Success Metrics
- All 3 questions answered at every level
- Wide Net forecasting generating 🚨 CRITICAL alerts when appropriate
- White space detection using real ML.GENERATE_TEXT not mock data
- Temporal trends visible for all key metrics
- Executive summary shows momentum status clearly

This implementation transforms our static snapshot into a living, breathing temporal intelligence system.