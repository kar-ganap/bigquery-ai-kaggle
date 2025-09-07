-- Creative Fatigue Detection Based on Competitor Influence
-- Logic: Low competitor influence in new ads = evidence of fatigue in previous ads
-- Uses the enhanced influence scoring with duration weighting

CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_creative_fatigue_analysis` AS

WITH recent_ads_originality AS (
  -- Calculate originality score for recent ads (inverse of competitor influence)
  SELECT 
    a.ad_archive_id,
    a.brand,
    a.start_date,
    a.funnel,
    a.persona,
    a.page_category as category,
    DATE_DIFF(COALESCE(a.end_date, CURRENT_DATE()), a.start_date, DAY) as active_days,
    
    -- Average influence score from competitors (weighted by quality)
    COALESCE(AVG(ci.influence_score), 0) as avg_competitor_influence,
    
    -- Count of quality competitor influences
    COUNT(CASE WHEN ci.influence_confidence IN ('High Confidence', 'Medium Confidence') 
               THEN 1 END) as quality_influences_count,
    
    -- Originality score (inverse of competitor influence)
    1 - COALESCE(AVG(ci.influence_score), 0) as originality_score,
    
    -- Originality classification
    CASE 
      WHEN COALESCE(AVG(ci.influence_score), 0) <= 0.2 THEN 'Highly Original'
      WHEN COALESCE(AVG(ci.influence_score), 0) <= 0.4 THEN 'Moderately Original'  
      WHEN COALESCE(AVG(ci.influence_score), 0) <= 0.6 THEN 'Somewhat Derivative'
      ELSE 'Heavily Influenced'
    END as originality_level
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` a
  LEFT JOIN `bigquery-ai-kaggle-469620.ads_demo.v_competitive_influence` ci
    ON a.ad_archive_id = ci.current_ad_id
  WHERE a.start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)  -- Last 60 days
    AND a.start_date IS NOT NULL
  GROUP BY a.ad_archive_id, a.brand, a.start_date, a.funnel, a.persona, a.page_category, active_days
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
    END as refresh_signal_strength
  FROM recent_ads_originality
  WHERE start_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)  -- Very recent
    AND originality_score >= 0.6  -- Only original ads
),

-- Calculate fatigue scores for all ads based on refresh signals
fatigue_calculation AS (
  SELECT 
    all_ads.ad_archive_id,
    all_ads.brand,
    all_ads.start_date as ad_start_date,
    all_ads.funnel,
    all_ads.persona,
    all_ads.page_category as category,
    all_ads.active_days,
    
    -- Age-based fatigue (0-1 scale, saturating at 45 days)
    LEAST(1.0, DATE_DIFF(CURRENT_DATE(), all_ads.start_date, DAY) / 45.0) as age_fatigue_component,
    
    -- Days since ad launched
    DATE_DIFF(CURRENT_DATE(), all_ads.start_date, DAY) as days_since_launch,
    
    -- Count and strength of refresh signals in same segment
    COUNT(refresh.ad_archive_id) as refresh_signals_count,
    COALESCE(MAX(refresh.refresh_signal_strength), 0) as max_refresh_signal,
    COALESCE(AVG(refresh.refresh_signal_strength), 0) as avg_refresh_signal,
    
    -- Latest refresh signal date
    MAX(refresh.start_date) as latest_refresh_date,
    
    -- Fatigue boost based on refresh signals
    CASE 
      WHEN COALESCE(MAX(refresh.refresh_signal_strength), 0) >= 1.0 THEN 0.4  -- Strong original ad launched
      WHEN COALESCE(MAX(refresh.refresh_signal_strength), 0) >= 0.7 THEN 0.3  -- Moderate refresh
      WHEN COALESCE(MAX(refresh.refresh_signal_strength), 0) >= 0.5 THEN 0.2  -- Weak refresh
      WHEN COUNT(refresh.ad_archive_id) > 0 THEN 0.1                          -- Any refresh activity
      ELSE 0.0
    END as refresh_fatigue_boost,
    
    -- Time decay of refresh signal (recent refreshes indicate more fatigue)
    CASE 
      WHEN MAX(refresh.start_date) IS NOT NULL 
      THEN EXP(-0.693 * DATE_DIFF(CURRENT_DATE(), MAX(refresh.start_date), DAY) / 7.0)
      ELSE 0.0
    END as refresh_recency_factor
    
  FROM recent_ads_originality all_ads
  LEFT JOIN original_refresh_signals refresh
    ON all_ads.brand = refresh.brand
    AND all_ads.funnel = refresh.funnel
    AND all_ads.persona = refresh.persona
    AND all_ads.page_category = refresh.category
    AND refresh.start_date > all_ads.start_date  -- Only consider newer refresh signals
  GROUP BY 
    all_ads.ad_archive_id, all_ads.brand, all_ads.start_date, 
    all_ads.funnel, all_ads.persona, all_ads.page_category, all_ads.active_days
),

-- Final fatigue scoring
fatigue_scores AS (
  SELECT 
    *,
    
    -- Combined fatigue score (0-1 scale)
    LEAST(1.0, 
      age_fatigue_component +                                    -- Base age component
      (refresh_fatigue_boost * refresh_recency_factor)          -- Refresh signal component
    ) as fatigue_score,
    
    -- Confidence in fatigue assessment
    CASE 
      WHEN refresh_signals_count >= 2 AND max_refresh_signal >= 0.7 
        THEN 'High Confidence'
      WHEN refresh_signals_count >= 1 AND max_refresh_signal >= 0.5
        THEN 'Medium Confidence'
      WHEN days_since_launch >= 30
        THEN 'Age-Based Assessment'
      ELSE 'Low Confidence'
    END as fatigue_confidence
    
  FROM fatigue_calculation
)

SELECT 
  ad_archive_id,
  brand,
  ad_start_date,
  funnel,
  persona,
  category,
  active_days,
  days_since_launch,
  
  -- Fatigue components
  ROUND(age_fatigue_component, 3) as age_fatigue,
  refresh_signals_count,
  ROUND(max_refresh_signal, 3) as max_refresh_signal,
  latest_refresh_date,
  ROUND(refresh_fatigue_boost, 3) as refresh_fatigue_boost,
  ROUND(refresh_recency_factor, 3) as refresh_recency_factor,
  
  -- Final assessment
  ROUND(fatigue_score, 3) as fatigue_score,
  fatigue_confidence,
  
  -- Fatigue level classification
  CASE 
    WHEN fatigue_score >= 0.8 THEN 'Critical Fatigue'
    WHEN fatigue_score >= 0.6 THEN 'High Fatigue'
    WHEN fatigue_score >= 0.4 THEN 'Moderate Fatigue'
    WHEN fatigue_score >= 0.2 THEN 'Low Fatigue'
    ELSE 'Fresh Content'
  END as fatigue_level,
  
  -- Recommended actions
  CASE 
    WHEN fatigue_score >= 0.8 AND refresh_signals_count = 0
      THEN 'URGENT: Launch original creative immediately'
    WHEN fatigue_score >= 0.8 AND refresh_signals_count > 0
      THEN 'PHASE OUT: Replace with newer original ads'
    WHEN fatigue_score >= 0.6 AND fatigue_confidence = 'High Confidence'
      THEN 'REFRESH: Test new creative variations'
    WHEN fatigue_score >= 0.4 AND days_since_launch >= 30
      THEN 'MONITOR: Watch performance closely'
    WHEN fatigue_score >= 0.2
      THEN 'CONTINUE: Performance monitoring sufficient'
    ELSE 'MAINTAIN: Content still fresh'
  END as recommended_action,
  
  -- Strategic insights
  CASE 
    WHEN refresh_signals_count >= 2 AND max_refresh_signal >= 0.7
      THEN CONCAT('Brand launched ', CAST(refresh_signals_count AS STRING), ' original ads recently - strong fatigue signal')
    WHEN latest_refresh_date IS NOT NULL
      THEN CONCAT('Last creative refresh: ', CAST(latest_refresh_date AS STRING))
    WHEN days_since_launch >= 45
      THEN 'Long-running ad - consider refresh based on performance'
    ELSE 'No clear fatigue signals detected'
  END as strategic_insight

FROM fatigue_scores
WHERE days_since_launch >= 1  -- Exclude brand new ads
ORDER BY brand, fatigue_score DESC;