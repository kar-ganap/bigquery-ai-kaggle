-- Creative Fatigue Detection View - Fixed Version
-- Uses proper column names from ads_with_dates table

CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_creative_fatigue_analysis` AS

WITH recent_ads_originality AS (
  -- Calculate originality score for recent ads (inverse of competitor influence)
  SELECT 
    a.ad_archive_id,
    a.brand,
    a.start_date,
    a.funnel,
    a.persona,
    a.page_category AS category,
    a.active_days,
    
    -- Average influence score from competitors (weighted by quality)
    COALESCE(AVG(ci.influence_score), 0) AS avg_competitor_influence,
    
    -- Count of quality competitor influences
    COUNT(CASE WHEN ci.influence_confidence IN ('High Confidence', 'Medium Confidence') 
               THEN 1 END) AS quality_influences_count,
    
    -- Originality score (inverse of competitor influence)
    1 - COALESCE(AVG(ci.influence_score), 0) AS originality_score,
    
    -- Originality classification
    CASE 
      WHEN COALESCE(AVG(ci.influence_score), 0) <= 0.2 THEN 'Highly Original'
      WHEN COALESCE(AVG(ci.influence_score), 0) <= 0.4 THEN 'Moderately Original'  
      WHEN COALESCE(AVG(ci.influence_score), 0) <= 0.6 THEN 'Somewhat Derivative'
      ELSE 'Heavily Influenced'
    END AS originality_level
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` a
  LEFT JOIN `bigquery-ai-kaggle-469620.ads_demo.v_competitive_influence` ci
    ON a.ad_archive_id = ci.current_ad_id
  WHERE a.start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)  -- Last 60 days
    AND a.start_date IS NOT NULL
  GROUP BY 
    a.ad_archive_id, a.brand, a.start_date, a.funnel, 
    a.persona, a.page_category, a.active_days
),

-- Identify highly original ads launched recently (evidence of creative refresh)
original_refresh_signals AS (
  SELECT 
    *,
    -- Refresh signal strength based on originality and ad quality
    CASE 
      WHEN originality_level = 'Highly Original' AND active_days >= 7 THEN 1.0
      WHEN originality_level = 'Moderately Original' AND active_days >= 5 THEN 0.7
      WHEN originality_level = 'Highly Original' AND active_days >= 3 THEN 0.5
      ELSE 0.0
    END AS refresh_signal_strength
  FROM recent_ads_originality
  WHERE start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)  -- Very recent
    AND originality_score >= 0.6  -- Only original ads
),

-- Calculate fatigue scores for all ads based on refresh signals
fatigue_scores AS (
  SELECT DISTINCT
    all_ads.ad_archive_id,
    all_ads.brand,
    all_ads.start_date,
    all_ads.funnel,
    all_ads.persona,
    all_ads.category,
    all_ads.active_days,
    
    -- Originality metrics
    all_ads.originality_score,
    all_ads.originality_level,
    all_ads.avg_competitor_influence,
    all_ads.quality_influences_count,
    
    -- Days since launch
    DATE_DIFF(CURRENT_DATE(), all_ads.start_date, DAY) AS days_since_launch,
    
    -- Count refresh signals in same category
    COUNT(refresh.ad_archive_id) AS refresh_signals_count,
    MAX(refresh.refresh_signal_strength) AS max_refresh_signal,
    
    -- Fatigue score calculation
    CASE 
      -- High fatigue: derivative content with fresh replacements appearing
      WHEN all_ads.originality_score < 0.4 
           AND COUNT(refresh.ad_archive_id) > 0 
      THEN 0.8 + (0.2 * COUNT(refresh.ad_archive_id) / 5.0)  -- Cap at 1.0
      
      -- Medium fatigue: older derivative content
      WHEN all_ads.originality_score < 0.5 
           AND DATE_DIFF(CURRENT_DATE(), all_ads.start_date, DAY) > 21
      THEN 0.6 + (0.2 * DATE_DIFF(CURRENT_DATE(), all_ads.start_date, DAY) / 60.0)
      
      -- Low fatigue: somewhat original but aging
      WHEN all_ads.originality_score < 0.7 
           AND DATE_DIFF(CURRENT_DATE(), all_ads.start_date, DAY) > 14
      THEN 0.3 + (0.2 * DATE_DIFF(CURRENT_DATE(), all_ads.start_date, DAY) / 60.0)
      
      -- Minimal fatigue: fresh or highly original
      ELSE DATE_DIFF(CURRENT_DATE(), all_ads.start_date, DAY) / 90.0  -- Gradual aging
    END AS fatigue_score,
    
    -- Refresh signal strength from any matching category
    COALESCE(MAX(refresh.refresh_signal_strength), 0) AS refresh_signal_strength
    
  FROM recent_ads_originality all_ads
  LEFT JOIN original_refresh_signals refresh
    ON all_ads.brand = refresh.brand
    AND all_ads.funnel = refresh.funnel
    AND all_ads.persona = refresh.persona
    AND all_ads.category = refresh.category
    AND refresh.start_date > all_ads.start_date  -- Only consider newer refresh signals
  GROUP BY 
    all_ads.ad_archive_id, all_ads.brand, all_ads.start_date, 
    all_ads.funnel, all_ads.persona, all_ads.category, all_ads.active_days,
    all_ads.originality_score, all_ads.originality_level,
    all_ads.avg_competitor_influence, all_ads.quality_influences_count
)

-- Final output with fatigue levels and recommendations
SELECT 
  ad_archive_id,
  brand,
  start_date,
  funnel,
  persona,
  category,
  active_days,
  days_since_launch,
  
  -- Originality metrics
  originality_score,
  originality_level,
  avg_competitor_influence,
  quality_influences_count,
  
  -- Fatigue metrics
  fatigue_score,
  refresh_signals_count,
  refresh_signal_strength,
  
  -- Fatigue level classification
  CASE 
    WHEN fatigue_score >= 0.8 THEN 'Critical Fatigue'
    WHEN fatigue_score >= 0.6 THEN 'High Fatigue'
    WHEN fatigue_score >= 0.4 THEN 'Moderate Fatigue'
    WHEN fatigue_score >= 0.2 THEN 'Low Fatigue'
    ELSE 'Fresh Content'
  END AS fatigue_level,
  
  -- Confidence in fatigue assessment
  CASE 
    WHEN refresh_signals_count > 0 AND originality_score < 0.4 THEN 'High Confidence'
    WHEN quality_influences_count >= 3 THEN 'Medium Confidence'
    WHEN days_since_launch > 30 THEN 'Age-Based Assessment'
    ELSE 'Low Confidence'
  END AS fatigue_confidence,
  
  -- Recommended action
  CASE 
    WHEN fatigue_score >= 0.8 THEN 'Urgent: Replace with original creative immediately'
    WHEN fatigue_score >= 0.6 THEN 'High Priority: Develop new creative concepts'
    WHEN fatigue_score >= 0.4 THEN 'Monitor: Consider testing new variations'
    WHEN fatigue_score >= 0.2 THEN 'Healthy: Continue monitoring performance'
    ELSE 'Fresh: Focus on distribution and optimization'
  END AS recommended_action

FROM fatigue_scores
ORDER BY brand, fatigue_score DESC;