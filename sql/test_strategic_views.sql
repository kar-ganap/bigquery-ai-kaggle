-- Test strategic intelligence views with our classified data

-- Test the competitive intelligence summary view
SELECT 
  brand,
  'Last 4 Weeks' as time_period,
  COUNT(*) as total_ads,
  
  -- Funnel focus
  ROUND(COUNT(CASE WHEN funnel = 'Upper' THEN 1 END) / COUNT(*) * 100, 1) as pct_upper_funnel,
  ROUND(COUNT(CASE WHEN funnel = 'Mid' THEN 1 END) / COUNT(*) * 100, 1) as pct_mid_funnel,
  ROUND(COUNT(CASE WHEN funnel = 'Lower' THEN 1 END) / COUNT(*) * 100, 1) as pct_lower_funnel,
  
  -- Strategic focus
  ARRAY_AGG(DISTINCT topic IGNORE NULLS ORDER BY topic LIMIT 8) as focus_topics,
  ROUND(AVG(urgency_score), 3) as avg_urgency,
  ROUND(AVG(promotional_intensity), 3) as avg_promotional_intensity,
  
  -- Dominant strategy
  CASE 
    WHEN COUNT(CASE WHEN funnel = 'Upper' THEN 1 END) / COUNT(*) >= 0.6 THEN 'Brand Awareness Focused'
    WHEN COUNT(CASE WHEN funnel = 'Lower' THEN 1 END) / COUNT(*) >= 0.6 THEN 'Conversion Focused'  
    WHEN AVG(promotional_intensity) >= 0.7 THEN 'Promotion Heavy'
    ELSE 'Balanced Strategy'
  END as dominant_strategy,
  
  -- Competitive archetype
  CASE 
    WHEN COUNT(CASE WHEN funnel = 'Upper' THEN 1 END) / COUNT(*) >= 0.6 THEN 'Brand Builder'
    WHEN COUNT(CASE WHEN funnel = 'Lower' THEN 1 END) / COUNT(*) >= 0.6 THEN 'Performance Driven'
    WHEN AVG(promotional_intensity) >= 0.7 THEN 'Promotion Focused'
    WHEN AVG(urgency_score) >= 0.6 THEN 'Urgency Driven'
    ELSE 'Balanced Approach'
  END as competitive_archetype
  
FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels`,
UNNEST(topics) as topic
GROUP BY brand
ORDER BY brand;