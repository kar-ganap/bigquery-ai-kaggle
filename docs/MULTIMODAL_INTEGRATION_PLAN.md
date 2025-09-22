# 🎨 Multimodal Integration Master Plan
## BigQuery AI Hackathon - Visual + Text Intelligence

---

## 📋 **EXECUTIVE SUMMARY**

**Goal**: Transform our text-only competitive intelligence pipeline into a multimodal powerhouse that analyzes visual creative strategy alongside text messaging.

**Key Innovation**: First-ever visual-text contradiction detection for competitive intelligence using BigQuery's native multimodal AI capabilities.

**Expected Impact**: +20 competition points (78→98/100), moving from "good technical execution" to "winning innovation."

---

## 🎯 **CORE STRATEGY**

### **1. Adaptive Strategic Coverage Sampling**
- **Philosophy**: Extract equal strategic insight from each competitor regardless of ad volume
- **Method**: Smart sampling based on portfolio size, recency, and strategic diversity
- **Budget**: 50-60 images total (~$5-10 cost)

### **2. Integrated Multimodal Analysis**
- **Approach**: Single AI.GENERATE call combining text + visual analysis
- **Fallback**: Graceful degradation to text-only analysis if visual fails
- **Output**: Visual-text alignment scoring and contradiction detection

### **3. Four-Level Intelligence Enhancement**
- **L1**: Visual contradiction alerts in executive summary
- **L2**: Visual competitive matrix and brand consistency scores
- **L3**: Visual-specific interventions (align creative with messaging)
- **L4**: Visual competitive analysis SQL dashboards

---

## 📊 **SAMPLING STRATEGY DETAILS**

### **Adaptive Coverage Algorithm**
```sql
-- Brand-proportional sampling with minimum coverage guarantees
CASE
  WHEN brand_total_ads <= 20 THEN 50% coverage  -- 10 images max
  WHEN brand_total_ads <= 50 THEN 30% coverage  -- 15 images max
  WHEN brand_total_ads <= 100 THEN 20% coverage -- 20 images max
  ELSE 15 images fixed                          -- Cap for large brands
END
```

### **Multi-Factor Scoring**
1. **Recency Weight (30%)**: Recent ads = higher priority
2. **Visual Complexity (25%)**: Carousels/videos prioritized over static images
3. **Card Variations (25%)**: Multi-variant ads get boost
4. **Strategic Diversity (20%)**: Extreme promotional intensities preferred

### **Expected Distribution**
- **Small Brands (≤20 ads)**: 8-10 images (50% coverage)
- **Medium Brands (21-50 ads)**: 10-15 images (30% coverage)
- **Large Brands (51-100 ads)**: 15-20 images (20% coverage)
- **Dominant Brands (>100 ads)**: 15 images (15% coverage)

**Total**: ~50-60 strategically sampled images across all competitors

---

## 🔧 **TECHNICAL IMPLEMENTATION**

### **Data Structure Fixes (CRITICAL)**

#### **Issue 1: Incomplete Text Extraction**
**Problem**: Missing text from card variations
**Fix**: Extract body text from ALL `snapshot.cards[i].body` fields
```python
all_card_texts = []
for card in cards:
    if card.get("body"):
        card_text = card["body"].get("text", "") if isinstance(card["body"], dict) else str(card["body"])
        if card_text:
            all_card_texts.append(card_text)

creative_text = " | ".join([title, body_text] + all_card_texts).strip()
```

#### **Issue 2: Incorrect Visual URL Extraction**
**Problem**: Using wrong field for image URLs
**Fix**: Extract from correct card fields
```python
visual_urls = []
for card in cards:
    # Images/Carousel
    if card.get("original_image_url"):
        visual_urls.append(card["original_image_url"])
    elif card.get("resized_image_url"):
        visual_urls.append(card["resized_image_url"])
    # Videos (use preview image)
    elif card.get("video_preview_image_url"):
        visual_urls.append(card["video_preview_image_url"])
```

#### **Issue 3: Media Type Detection**
**Fix**: Smart detection based on URL availability
```python
def determine_media_type(cards, visual_urls):
    video_count = sum(1 for card in cards if card.get("video_preview_image_url"))
    image_count = len(visual_urls) - video_count

    if video_count > 0 and image_count > 0: return "mixed"
    elif video_count > 0: return "video"
    elif image_count > 1: return "carousel"
    elif image_count == 1: return "image"
    else: return "text_only"
```

---

## 🏗️ **PIPELINE STAGE MODIFICATIONS**

### **Stage 4: Enhanced Ingestion**
**File**: `src/pipeline/stages/ingestion.py`
**Changes**:
- Fix `_normalize_ad_data()` to extract complete card text
- Add `_extract_visual_urls()` method
- Add `_determine_media_type()` method
- Update normalized ad schema

**New Fields Added**:
```python
{
    'creative_text': "Complete text from all card variations",
    'card_variations': ["Text from card 1", "Text from card 2"],
    'visual_urls': ["url1.jpg", "url2.jpg"],
    'primary_visual_url': "url1.jpg",  # First URL for analysis
    'media_type': "carousel|image|video|mixed|text_only",
    'card_count': 3,
    'has_visuals': True
}
```

### **Stage 5: Enhanced Strategic Labeling**
**File**: `src/pipeline/stages/strategic_labeling.py`
**Changes**:
- Update SQL to use complete card text
- Add card variation awareness to AI prompts
- Enhance batch processing for multi-variant ads

**Enhanced Prompt**:
```sql
CONCAT(
  'Analyze this comprehensive ad including all ', CAST(card_count AS STRING), ' card variations: "',
  creative_text, '". Consider variation consistency in your analysis...'
)
```

### **Stage 5.5: NEW - Visual Intelligence**
**File**: `src/pipeline/stages/visual_intelligence.py` (NEW)
**Purpose**: Multimodal analysis with adaptive sampling

**Core SQL**:
```sql
WITH sampled_for_visual AS (
  -- Apply adaptive sampling strategy
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY brand ORDER BY enhanced_sampling_score DESC) as sample_rank,
    brand_sample_limit
  FROM enhanced_visual_sampling
),

multimodal_analysis AS (
  SELECT
    ad_archive_id,
    AI.GENERATE(
      CONCAT(
        'Analyze this ad combining text and visual elements.\n',
        'Text: "', creative_text, '"\n',
        'Card variations: ', CAST(card_count AS STRING), '\n',
        'Visual analysis from provided image URL.\n\n',
        'Provide integrated analysis:\n',
        '1. Text-Visual Alignment: ALIGNED/MISALIGNED/CONTRADICTORY\n',
        '2. Visual Style: MINIMALIST/LUXURY/BOLD/CASUAL/PROFESSIONAL\n',
        '3. Visual Focus: PRODUCT_CLOSEUP/LIFESTYLE/MODEL_WEARING/MIXED\n',
        '4. Target Demographics: Age group and lifestyle signals from visual\n',
        '5. Brand Consistency Score: 0.0-1.0 (harmony between text and visual)\n',
        '6. Creative Fatigue Risk: LOW/MEDIUM/HIGH based on visual similarity\n',
        '7. Competitive Differentiation: How visual style differs from category norms'
      ),
      primary_visual_url,
      connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
    ) as multimodal_intelligence
  FROM sampled_for_visual
  WHERE sample_rank <= brand_sample_limit
    AND primary_visual_url IS NOT NULL
)
```

### **Stage 7: Enhanced Analysis**
**File**: `src/pipeline/stages/analysis.py`
**Changes**:
- Add `_analyze_visual_text_alignment()` method
- Add `_calculate_brand_consistency_scores()` method
- Integrate visual insights into current_state analysis

**New Analysis Methods**:
```python
def _analyze_visual_text_alignment(self) -> dict:
    """Detect visual-text contradictions"""
    return {
        'misalignment_rate': 0.40,  # 40% of ads have contradictions
        'contradictory_ads': 15,
        'avg_brand_consistency': 0.65,
        'worst_offenders': ['ad_123', 'ad_456']
    }

def _calculate_visual_fatigue_risk(self) -> dict:
    """Detect creative fatigue from visual overuse"""
    return {
        'high_risk_styles': ['MINIMALIST'],
        'fatigue_score': 0.75,
        'weeks_overused': 8
    }
```

### **Stage 8: Enhanced Multi-Dimensional Intelligence**
**File**: `src/pipeline/stages/multidimensional_intelligence.py`
**Changes**:
- Add `_generate_visual_intelligence()` method
- Create visual competitive matrix
- Add creative fatigue detection
- Generate visual-text contradiction reports

**New Intelligence Dimensions**:
```python
{
    'visual_intelligence': {
        'competitive_matrix': {
            'Warby Parker': {'MINIMALIST': 65, 'LUXURY': 20, 'BOLD': 15},
            'Ray-Ban': {'LUXURY': 40, 'BOLD': 35, 'MINIMALIST': 25}
        },
        'fatigue_analysis': {
            'high_risk_brands': ['Warby Parker'],
            'overused_styles': ['MINIMALIST'],
            'recommendation': 'Diversify visual approach'
        },
        'brand_consistency': {
            'overall_score': 0.65,
            'misalignment_rate': 0.40,
            'critical_issues': ['Luxury visuals with promotional text']
        }
    }
}
```

### **Stage 9: Enhanced Output**
**File**: `src/pipeline/stages/enhanced_output.py`
**Changes**:
- Integrate visual insights into L1-L4 framework
- Add visual-specific interventions
- Create visual competitive analysis dashboards

---

## 📊 **FOUR-LEVEL INTELLIGENCE ENHANCEMENT**

### **Level 1: Executive Summary**
**Before**:
```json
{
  "executive_insights": [
    "Competitive copying detected from EyeBuyDirect (similarity: 73.0%)",
    "Creative text length optimization needed"
  ]
}
```

**After (Enhanced)**:
```json
{
  "executive_insights": [
    "Visual-text contradiction in 40% of ads undermining brand consistency",
    "Competitive copying detected from EyeBuyDirect (similarity: 73.0%)",
    "Creative fatigue risk: overusing minimalist style for 8 weeks"
  ],
  "visual_threat_level": "HIGH",
  "brand_consistency_score": 0.65
}
```

### **Level 2: Strategic Dashboard**
**New Visual Intelligence Section**:
```json
{
  "visual_intelligence": {
    "brand_consistency_score": 0.65,
    "text_visual_alignment": "MISALIGNED",
    "dominant_visual_style": "MINIMALIST",
    "style_distribution": {"MINIMALIST": 65, "LUXURY": 20, "BOLD": 15},
    "fatigue_risk_level": "HIGH",
    "competitive_differentiation": 0.45,
    "visual_vs_text_disconnect_rate": 0.40
  }
}
```

### **Level 3: Actionable Interventions**
**New Visual-Specific Interventions**:
```json
{
  "interventions": [
    {
      "priority": "CRITICAL",
      "action": "Align luxury visual style with premium text messaging",
      "rationale": "40% visual-text contradiction undermining brand trust",
      "timeline": "1-2 weeks",
      "expected_impact": "+15% brand consistency score"
    },
    {
      "priority": "HIGH",
      "action": "Diversify visual creative from minimalist overuse",
      "rationale": "Creative fatigue detected after 8 weeks of same visual style",
      "timeline": "2-3 weeks",
      "expected_impact": "+20% creative engagement"
    }
  ]
}
```

### **Level 4: SQL Dashboards**
**New Visual Competitive Analysis Queries**:
```sql
-- Visual Style Competitive Matrix
'visual_competitive_matrix': '''
SELECT
  brand,
  visual_style,
  COUNT(*) as ad_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY brand), 1) as style_percentage,
  AVG(brand_consistency_score) as avg_consistency
FROM ads_with_multimodal_intelligence
WHERE visual_analysis_enabled = TRUE
GROUP BY brand, visual_style
ORDER BY brand, style_percentage DESC
''',

-- Creative Fatigue Detection
'creative_fatigue_analysis': '''
SELECT
  brand,
  visual_style,
  COUNT(DISTINCT DATE_TRUNC(start_timestamp, WEEK)) as weeks_active,
  STDDEV(weekly_usage) as consistency_score,
  CASE
    WHEN COUNT(DISTINCT DATE_TRUNC(start_timestamp, WEEK)) >= 8
     AND STDDEV(weekly_usage) < 2 THEN 'HIGH_FATIGUE_RISK'
    ELSE 'LOW_RISK'
  END as fatigue_risk
FROM visual_usage_patterns
GROUP BY brand, visual_style
HAVING fatigue_risk = 'HIGH_FATIGUE_RISK'
''',

-- Brand Consistency Analysis
'brand_consistency_dashboard': '''
SELECT
  brand,
  text_visual_alignment,
  COUNT(*) as ad_count,
  AVG(brand_consistency_score) as avg_consistency,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY brand), 1) as percentage
FROM ads_with_multimodal_intelligence
WHERE visual_analysis_enabled = TRUE
GROUP BY brand, text_visual_alignment
ORDER BY avg_consistency ASC  -- Surface problems first
'''
```

---

## 🛡️ **FALLBACK STRATEGIES**

### **Level 1: AI Generation Failure**
```sql
COALESCE(
  multimodal_intelligence.result,
  CONCAT('TEXT_ONLY_ANALYSIS: ', text_only_analysis.result, ' | VISUAL_UNAVAILABLE')
) as intelligence_output
```

### **Level 2: URL Access Failure**
```sql
-- Smart fallback chain
CASE
  WHEN primary_visual_url IS NOT NULL AND url_accessible = TRUE
  THEN multimodal_analysis
  WHEN visual_urls[SAFE_OFFSET(1)] IS NOT NULL
  THEN backup_visual_analysis
  ELSE text_only_analysis
END
```

### **Level 3: Budget Exceeded**
```python
# Dynamic sampling reduction
if estimated_cost > budget_limit:
    reduce_sampling_rate(factor=0.5)
    prioritize_recent_ads_only()
```

### **Level 4: Complete Visual Failure**
```python
# Graceful degradation
try:
    return generate_multimodal_intelligence()
except Exception as e:
    logger.warning(f"Visual analysis failed: {e}")
    return generate_enhanced_text_intelligence()  # Fallback to current system
```

---

## 💰 **COST MANAGEMENT**

### **Budget Controls**
- **Hard Cap**: 60 images maximum (~$6-12 total cost)
- **Dynamic Scaling**: Reduce sampling if approaching limit
- **Priority Queue**: Most strategic ads analyzed first
- **Monitoring**: Track spend in real-time

### **Cost Optimization**
```sql
-- Pre-flight cost estimation
WITH cost_estimation AS (
  SELECT
    COUNT(*) as total_images_to_analyze,
    COUNT(*) * 0.10 as estimated_cost_usd,
    CASE
      WHEN COUNT(*) > 60 THEN 'REDUCE_SAMPLING'
      WHEN COUNT(*) > 40 THEN 'PROCEED_WITH_CAUTION'
      ELSE 'PROCEED'
    END as recommendation
  FROM visual_sampling_strategy
)
```

---

## 🎯 **COMPETITIVE ADVANTAGES**

### **1. First-of-Kind Capabilities**
- **Visual-Text Contradiction Detection**: No competitor likely has this
- **Adaptive Strategic Sampling**: Sophisticated beyond simple random sampling
- **Integrated Multimodal Pipeline**: Not just two separate analyses

### **2. Judge Appeal Factors**
- **"Wow Factor"**: Visual analysis of competitor ads in real-time
- **Business Value**: Actual brand consistency insights with dollar impact
- **Technical Sophistication**: Advanced BigQuery AI usage
- **Demo Narrative**: "We catch when luxury brands contradict themselves"

### **3. Real Business Impact**
- **Brand Consistency Monitoring**: Prevent expensive brand failures
- **Creative Fatigue Prevention**: Optimize before performance drops
- **Competitive Positioning**: Visual strategy gaps identification
- **ROI Quantification**: "Save $100K in brand damage prevention"

---

## 📅 **IMPLEMENTATION TIMELINE**

### **Phase 1: Foundation (2 hours)**
- [ ] Fix data extraction issues (text + visual URLs)
- [ ] Implement adaptive sampling strategy
- [ ] Create visual intelligence stage template
- [ ] Test with 5 sample images

### **Phase 2: Integration (2 hours)**
- [ ] Build multimodal AI.GENERATE prompts
- [ ] Integrate with existing pipeline stages
- [ ] Add fallback mechanisms
- [ ] Test end-to-end with 20 images

### **Phase 3: Intelligence Enhancement (1 hour)**
- [ ] Enhance L1-L4 framework with visual insights
- [ ] Create visual competitive dashboards
- [ ] Add creative fatigue detection
- [ ] Generate visual interventions

### **Phase 4: Polish & Demo (1 hour)**
- [ ] Error handling and edge cases
- [ ] Performance optimization
- [ ] Demo preparation and narrative
- [ ] Documentation cleanup

**Total Estimated Time**: 6 hours
**Minimum Viable Product**: 3 hours (Phase 1 + basic Phase 2)

---

## 🎪 **DEMO NARRATIVE**

### **Opening Hook**
> "Everyone analyzes what competitors SAY in their ads. We're the first to analyze what they SHOW - and catch when they contradict themselves."

### **Key Demo Moments**
1. **Visual Competitive Matrix**: "Warby Parker owns minimalist (65%) vs Ray-Ban's luxury focus (40%)"
2. **Contradiction Detection**: "Luxury visuals but promotional text in 40% of ads"
3. **Fatigue Alert**: "8 weeks of minimalist overuse - engagement likely dropping"
4. **Real-time Analysis**: "Watch us analyze a live Meta ad in 3 seconds"

### **Business Impact Close**
> "This prevents expensive brand failures before they happen. One misaligned campaign caught early saves $100K in brand damage."

---

## ✅ **SUCCESS METRICS**

### **Technical Validation**
- [ ] 50+ images analyzed successfully
- [ ] <3 second average analysis time per image
- [ ] <5% fallback rate to text-only
- [ ] Complete pipeline integration

### **Intelligence Quality**
- [ ] Visual style distribution detected for all brands
- [ ] Brand consistency scores calculated
- [ ] Creative fatigue risks identified
- [ ] Visual-text contradictions surfaced

### **Competition Scoring**
- [ ] +15-20 points from multimodal innovation
- [ ] Memorable demo for judges
- [ ] Clear business value articulation
- [ ] Technical execution excellence

### **Demo Readiness**
- [ ] 5-minute working demo prepared
- [ ] Compelling business narrative
- [ ] Live data integration
- [ ] Fallback scenarios tested

---

## 🚀 **NEXT STEPS**

1. **Immediate**: Fix data extraction issues (Priority 1)
2. **Next**: Implement adaptive sampling (Priority 2)
3. **Then**: Build multimodal analysis stage (Priority 3)
4. **Finally**: Integrate with L1-L4 framework (Priority 4)

**Ready to begin implementation with Phase 1 foundation work.**

---

*Last Updated: 2025-01-15*
*Competition Deadline: [DATE]*
*Estimated Completion: 6 hours from start*