-- Platform Consistency Analysis & Cross-Platform Strategy Detection
-- Analyzes Facebook vs Instagram messaging alignment and platform-specific adaptations
-- Uses real publisher_platform data from ScrapeCreators API

CREATE OR REPLACE TABLE `your-project.ads_demo.platform_consistency_analysis` AS
WITH platform_features AS (
  SELECT 
    ad_id,
    brand,
    creative_text,
    title,
    media_type,
    start_timestamp,
    end_timestamp,
    active_days,
    publisher_platforms,  -- This is the comma-separated string from ingestion
    final_aggressiveness_score,
    aggressiveness_tier,
    
    -- Parse platform string into array for analysis
    ARRAY(
      SELECT TRIM(platform) 
      FROM UNNEST(SPLIT(publisher_platforms, ',')) AS platform
      WHERE TRIM(platform) != ''
    ) AS platforms_array,
    
    -- Platform presence detection (case insensitive)
    CASE 
      WHEN UPPER(publisher_platforms) LIKE '%FACEBOOK%' THEN TRUE 
      ELSE FALSE 
    END AS on_facebook,
    CASE 
      WHEN UPPER(publisher_platforms) LIKE '%INSTAGRAM%' THEN TRUE 
      ELSE FALSE 
    END AS on_instagram,
    
    -- Platform strategy classification
    CASE 
      WHEN UPPER(publisher_platforms) LIKE '%FACEBOOK%' AND UPPER(publisher_platforms) LIKE '%INSTAGRAM%' 
      THEN 'CROSS_PLATFORM'
      WHEN UPPER(publisher_platforms) LIKE '%FACEBOOK%' AND NOT UPPER(publisher_platforms) LIKE '%INSTAGRAM%' 
      THEN 'FACEBOOK_ONLY'
      WHEN UPPER(publisher_platforms) LIKE '%INSTAGRAM%' AND NOT UPPER(publisher_platforms) LIKE '%FACEBOOK%' 
      THEN 'INSTAGRAM_ONLY'
      ELSE 'OTHER_PLATFORM'
    END AS platform_strategy,
    
    -- Content analysis for platform suitability
    COALESCE(creative_text, '') || ' ' || COALESCE(title, '') AS full_text,
    
    -- Instagram-optimized content indicators
    CASE 
      WHEN REGEXP_CONTAINS(COALESCE(creative_text, '') || ' ' || COALESCE(title, ''), r'#\w+') 
      THEN 1.0 ELSE 0.0 
    END AS has_hashtags,
    
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(SWIPE|TAP|DOUBLE TAP|STORY|STORIES|REELS|IGTV|EXPLORE|INSTA)') 
      THEN 1.0 ELSE 0.0 
    END AS has_instagram_native_language,
    
    -- Facebook-optimized content indicators  
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(CLICK|LEARN MORE|READ MORE|COMMENT|SHARE|LIKE THIS|WATCH|VIEW|VISIT|FACEBOOK)') 
      THEN 1.0 ELSE 0.0 
    END AS has_facebook_native_language,
    
    -- Visual-first content scoring (better for Instagram)
    CASE media_type
      WHEN 'VIDEO' THEN 0.9
      WHEN 'IMAGE' THEN 0.7
      WHEN 'DCO' THEN 0.5  -- Dynamic creative can be more versatile
      ELSE 0.3
    END AS visual_content_score,
    
    -- Text length analysis (shorter = better for Instagram)
    LENGTH(COALESCE(creative_text, '')) AS text_length,
    CASE 
      WHEN LENGTH(COALESCE(creative_text, '')) <= 125 THEN 1.0  -- Instagram optimal
      WHEN LENGTH(COALESCE(creative_text, '')) <= 250 THEN 0.8  -- Good for both
      WHEN LENGTH(COALESCE(creative_text, '')) <= 400 THEN 0.5  -- Better for Facebook
      ELSE 0.2  -- Long-form, Facebook-oriented
    END AS instagram_text_optimality,
    
    -- Emoji usage (more common on Instagram)
    CASE 
      WHEN REGEXP_CONTAINS(COALESCE(creative_text, '') || ' ' || COALESCE(title, ''), 
        r'[\x{1F600}-\x{1F64F}]|[\x{1F300}-\x{1F5FF}]|[\x{1F680}-\x{1F6FF}]|[\x{1F1E0}-\x{1F1FF}]') 
      THEN 1.0 ELSE 0.0 
    END AS has_emojis,
    
    -- Week grouping for time-series analysis
    DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week_start

  FROM `your-project.ads_demo.ads_with_dates`
  JOIN `your-project.ads_demo.cta_aggressiveness_analysis` 
    USING (ad_id)  -- Get aggressiveness scores
  WHERE publisher_platforms IS NOT NULL 
    AND publisher_platforms != ''
    AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
),

-- Calculate platform-specific optimization scores
platform_optimization_scores AS (
  SELECT 
    *,
    
    -- Instagram optimization score (0.0 to 1.0)
    -- Weights: hashtags(15%), native language(25%), visual content(30%), text length(20%), emojis(10%)
    (has_hashtags * 0.15 + 
     has_instagram_native_language * 0.25 + 
     visual_content_score * 0.30 + 
     instagram_text_optimality * 0.20 +
     has_emojis * 0.10) AS instagram_optimization_score,
    
    -- Facebook optimization score (0.0 to 1.0)  
    -- Weights: native language(40%), visual content(20%), longer text(25%), DCO bonus(15%)
    (has_facebook_native_language * 0.40 + 
     visual_content_score * 0.20 + 
     (1.0 - instagram_text_optimality) * 0.25 +  -- Longer text better for Facebook
     CASE WHEN media_type = 'DCO' THEN 0.15 ELSE 0.0 END) AS facebook_optimization_score,
    
    -- Platform optimization gap (how different the content suitability is)
    ABS((has_hashtags * 0.15 + has_instagram_native_language * 0.25 + visual_content_score * 0.30 + instagram_text_optimality * 0.20 + has_emojis * 0.10) - 
        (has_facebook_native_language * 0.40 + visual_content_score * 0.20 + (1.0 - instagram_text_optimality) * 0.25 + CASE WHEN media_type = 'DCO' THEN 0.15 ELSE 0.0 END)) AS platform_optimization_gap

  FROM platform_features
),

-- Brand-level platform strategy analysis  
brand_platform_patterns AS (
  SELECT 
    brand,
    week_start,
    
    -- Platform distribution
    COUNT(*) AS total_ads,
    COUNTIF(platform_strategy = 'CROSS_PLATFORM') AS cross_platform_ads,
    COUNTIF(platform_strategy = 'FACEBOOK_ONLY') AS facebook_only_ads,
    COUNTIF(platform_strategy = 'INSTAGRAM_ONLY') AS instagram_only_ads,
    
    -- Platform strategy percentages
    COUNTIF(platform_strategy = 'CROSS_PLATFORM') / COUNT(*) * 100 AS pct_cross_platform,
    COUNTIF(platform_strategy = 'FACEBOOK_ONLY') / COUNT(*) * 100 AS pct_facebook_only,
    COUNTIF(platform_strategy = 'INSTAGRAM_ONLY') / COUNT(*) * 100 AS pct_instagram_only,
    
    -- Platform optimization analysis
    AVG(instagram_optimization_score) AS avg_instagram_optimization,
    AVG(facebook_optimization_score) AS avg_facebook_optimization,
    AVG(platform_optimization_gap) AS avg_platform_optimization_gap,
    
    -- Content adaptation analysis for cross-platform ads specifically
    AVG(CASE WHEN platform_strategy = 'CROSS_PLATFORM' THEN platform_optimization_gap END) AS cross_platform_adaptation_gap,
    AVG(CASE WHEN platform_strategy = 'INSTAGRAM_ONLY' THEN instagram_optimization_score END) AS instagram_specific_optimization,
    AVG(CASE WHEN platform_strategy = 'FACEBOOK_ONLY' THEN facebook_optimization_score END) AS facebook_specific_optimization,
    
    -- Platform-specific aggressiveness patterns
    AVG(CASE WHEN on_instagram THEN final_aggressiveness_score END) AS avg_instagram_aggressiveness,
    AVG(CASE WHEN on_facebook AND NOT on_instagram THEN final_aggressiveness_score END) AS avg_facebook_only_aggressiveness,
    AVG(CASE WHEN platform_strategy = 'CROSS_PLATFORM' THEN final_aggressiveness_score END) AS avg_cross_platform_aggressiveness,
    
    -- Content characteristics by platform
    AVG(CASE WHEN on_instagram THEN text_length END) AS avg_instagram_text_length,
    AVG(CASE WHEN on_facebook AND NOT on_instagram THEN text_length END) AS avg_facebook_text_length,
    AVG(CASE WHEN platform_strategy = 'CROSS_PLATFORM' THEN text_length END) AS avg_cross_platform_text_length,
    
    -- Media type distribution by platform
    COUNTIF(on_instagram AND media_type = 'VIDEO') / NULLIF(COUNTIF(on_instagram), 0) * 100 AS instagram_video_pct,
    COUNTIF(on_facebook AND NOT on_instagram AND media_type = 'VIDEO') / NULLIF(COUNTIF(on_facebook AND NOT on_instagram), 0) * 100 AS facebook_video_pct,
    COUNTIF(platform_strategy = 'CROSS_PLATFORM' AND media_type = 'VIDEO') / NULLIF(COUNTIF(platform_strategy = 'CROSS_PLATFORM'), 0) * 100 AS cross_platform_video_pct,
    
    -- Platform-specific features prevalence
    COUNTIF(on_instagram AND has_hashtags = 1) / NULLIF(COUNTIF(on_instagram), 0) * 100 AS instagram_hashtag_usage_pct,
    COUNTIF(on_instagram AND has_emojis = 1) / NULLIF(COUNTIF(on_instagram), 0) * 100 AS instagram_emoji_usage_pct,
    COUNTIF(on_facebook AND has_facebook_native_language = 1) / NULLIF(COUNTIF(on_facebook), 0) * 100 AS facebook_native_language_pct

  FROM platform_optimization_scores
  GROUP BY brand, week_start
),

-- Cross-brand competitive platform benchmarks
competitive_platform_benchmarks AS (
  SELECT 
    week_start,
    
    -- Market benchmarks for strategy distribution
    AVG(pct_cross_platform) AS market_avg_cross_platform_pct,
    AVG(pct_facebook_only) AS market_avg_facebook_only_pct,
    AVG(pct_instagram_only) AS market_avg_instagram_only_pct,
    
    -- Market benchmarks for optimization
    AVG(avg_instagram_optimization) AS market_avg_instagram_optimization,
    AVG(avg_facebook_optimization) AS market_avg_facebook_optimization,
    AVG(avg_platform_optimization_gap) AS market_avg_optimization_gap,
    AVG(cross_platform_adaptation_gap) AS market_avg_cross_platform_gap,
    
    -- Best-in-class benchmarks
    MAX(avg_instagram_optimization) AS best_instagram_optimization,
    MAX(avg_facebook_optimization) AS best_facebook_optimization,
    MIN(avg_platform_optimization_gap) AS best_platform_consistency,
    MIN(cross_platform_adaptation_gap) AS best_cross_platform_adaptation

  FROM brand_platform_patterns
  WHERE total_ads >= 3  -- Only include weeks with meaningful sample size
  GROUP BY week_start
)

-- Main platform consistency analysis table
SELECT 
  bpp.brand,
  bpp.week_start,
  
  -- Volume and strategy distribution
  bpp.total_ads,
  bpp.cross_platform_ads,
  bpp.facebook_only_ads,
  bpp.instagram_only_ads,
  ROUND(bpp.pct_cross_platform, 1) AS pct_cross_platform,
  ROUND(bpp.pct_facebook_only, 1) AS pct_facebook_only,
  ROUND(bpp.pct_instagram_only, 1) AS pct_instagram_only,
  
  -- Platform optimization scores
  ROUND(bpp.avg_instagram_optimization, 3) AS avg_instagram_optimization,
  ROUND(bpp.avg_facebook_optimization, 3) AS avg_facebook_optimization,
  ROUND(bpp.avg_platform_optimization_gap, 3) AS avg_platform_optimization_gap,
  
  -- Strategy-specific optimization
  ROUND(bpp.cross_platform_adaptation_gap, 3) AS cross_platform_adaptation_gap,
  ROUND(bpp.instagram_specific_optimization, 3) AS instagram_specific_optimization,
  ROUND(bpp.facebook_specific_optimization, 3) AS facebook_specific_optimization,
  
  -- Platform-specific aggressiveness patterns
  ROUND(bpp.avg_instagram_aggressiveness, 3) AS avg_instagram_aggressiveness,
  ROUND(bpp.avg_facebook_only_aggressiveness, 3) AS avg_facebook_only_aggressiveness,
  ROUND(bpp.avg_cross_platform_aggressiveness, 3) AS avg_cross_platform_aggressiveness,
  
  -- Content adaptation metrics
  ROUND(bpp.avg_instagram_text_length, 0) AS avg_instagram_text_length,
  ROUND(bpp.avg_facebook_text_length, 0) AS avg_facebook_text_length,
  ROUND(bpp.avg_cross_platform_text_length, 0) AS avg_cross_platform_text_length,
  
  -- Media distribution
  ROUND(bpp.instagram_video_pct, 1) AS instagram_video_pct,
  ROUND(bpp.facebook_video_pct, 1) AS facebook_video_pct,
  ROUND(bpp.cross_platform_video_pct, 1) AS cross_platform_video_pct,
  
  -- Platform-specific features
  ROUND(bpp.instagram_hashtag_usage_pct, 1) AS instagram_hashtag_usage_pct,
  ROUND(bpp.instagram_emoji_usage_pct, 1) AS instagram_emoji_usage_pct,
  ROUND(bpp.facebook_native_language_pct, 1) AS facebook_native_language_pct,
  
  -- Competitive positioning
  ROUND(bpp.avg_instagram_optimization - cpb.market_avg_instagram_optimization, 3) AS instagram_opt_vs_market,
  ROUND(bpp.avg_facebook_optimization - cpb.market_avg_facebook_optimization, 3) AS facebook_opt_vs_market,
  ROUND(bpp.avg_platform_optimization_gap - cpb.market_avg_optimization_gap, 3) AS consistency_vs_market,
  ROUND(bpp.cross_platform_adaptation_gap - cpb.market_avg_cross_platform_gap, 3) AS cross_platform_adaptation_vs_market,
  
  -- Strategic classification
  CASE 
    WHEN bpp.pct_cross_platform >= 80 THEN 'UNIFIED_CROSS_PLATFORM'
    WHEN bpp.pct_cross_platform >= 50 THEN 'BALANCED_CROSS_PLATFORM'  
    WHEN bpp.pct_instagram_only >= 60 THEN 'INSTAGRAM_FOCUSED'
    WHEN bpp.pct_facebook_only >= 60 THEN 'FACEBOOK_FOCUSED'
    ELSE 'MIXED_PLATFORM_STRATEGY'
  END AS platform_strategy_classification,
  
  CASE 
    WHEN bpp.avg_platform_optimization_gap <= 0.15 THEN 'HIGHLY_CONSISTENT'
    WHEN bpp.avg_platform_optimization_gap <= 0.30 THEN 'MODERATELY_CONSISTENT'
    WHEN bpp.avg_platform_optimization_gap <= 0.50 THEN 'SOMEWHAT_INCONSISTENT'  
    ELSE 'HIGHLY_INCONSISTENT'
  END AS platform_consistency_tier,
  
  CASE 
    WHEN bpp.cross_platform_adaptation_gap IS NULL THEN 'NO_CROSS_PLATFORM_ADS'
    WHEN bpp.cross_platform_adaptation_gap <= 0.20 THEN 'WELL_ADAPTED_CROSS_PLATFORM'
    WHEN bpp.cross_platform_adaptation_gap <= 0.40 THEN 'MODERATELY_ADAPTED_CROSS_PLATFORM'
    ELSE 'POORLY_ADAPTED_CROSS_PLATFORM'
  END AS cross_platform_adaptation_quality,
  
  -- Week-over-week changes for trend detection
  ROUND(bpp.pct_cross_platform - LAG(bpp.pct_cross_platform) OVER (
    PARTITION BY bpp.brand ORDER BY bpp.week_start
  ), 1) AS cross_platform_pct_change_wow,
  
  ROUND(bpp.avg_instagram_optimization - LAG(bpp.avg_instagram_optimization) OVER (
    PARTITION BY bpp.brand ORDER BY bpp.week_start  
  ), 3) AS instagram_optimization_change_wow

FROM brand_platform_patterns bpp
LEFT JOIN competitive_platform_benchmarks cpb ON bpp.week_start = cpb.week_start
WHERE bpp.total_ads >= 3  -- Filter out weeks with too few ads for meaningful analysis
ORDER BY bpp.brand, bpp.week_start DESC;

-- Create strategic insights summary view
CREATE OR REPLACE VIEW `your-project.ads_demo.v_platform_strategy_insights` AS
SELECT 
  brand,
  
  -- Overall strategy profile (last 12 weeks average)
  AVG(pct_cross_platform) AS avg_cross_platform_pct,
  AVG(pct_facebook_only) AS avg_facebook_only_pct,
  AVG(pct_instagram_only) AS avg_instagram_only_pct,
  
  -- Dominant strategy classification
  CASE 
    WHEN AVG(pct_cross_platform) >= 60 THEN 'CROSS_PLATFORM_DOMINANT'
    WHEN AVG(pct_instagram_only) >= 50 THEN 'INSTAGRAM_FOCUSED'  
    WHEN AVG(pct_facebook_only) >= 50 THEN 'FACEBOOK_FOCUSED'
    ELSE 'PLATFORM_BALANCED'
  END AS dominant_platform_strategy,
  
  -- Optimization performance  
  AVG(avg_instagram_optimization) AS overall_instagram_optimization,
  AVG(avg_facebook_optimization) AS overall_facebook_optimization,
  AVG(avg_platform_optimization_gap) AS overall_platform_gap,
  AVG(cross_platform_adaptation_gap) AS overall_cross_platform_gap,
  
  -- Competitive positioning
  AVG(instagram_opt_vs_market) AS avg_instagram_advantage_vs_market,
  AVG(facebook_opt_vs_market) AS avg_facebook_advantage_vs_market,
  AVG(consistency_vs_market) AS avg_consistency_advantage_vs_market,
  
  -- Content characteristics
  AVG(avg_instagram_text_length) AS typical_instagram_text_length,
  AVG(avg_facebook_text_length) AS typical_facebook_text_length,
  AVG(instagram_hashtag_usage_pct) AS typical_instagram_hashtag_usage,
  AVG(instagram_emoji_usage_pct) AS typical_instagram_emoji_usage,
  
  -- Strategic insights
  CASE 
    WHEN AVG(consistency_vs_market) > 0.05 THEN 'MORE_CONSISTENT_THAN_MARKET'
    WHEN AVG(consistency_vs_market) < -0.05 THEN 'LESS_CONSISTENT_THAN_MARKET'  
    ELSE 'MARKET_ALIGNED_CONSISTENCY'
  END AS consistency_competitive_position,
  
  CASE 
    WHEN AVG(cross_platform_adaptation_gap) <= 0.25 THEN 'STRONG_CROSS_PLATFORM_ADAPTATION'
    WHEN AVG(cross_platform_adaptation_gap) <= 0.40 THEN 'MODERATE_CROSS_PLATFORM_ADAPTATION'
    ELSE 'WEAK_CROSS_PLATFORM_ADAPTATION'  
  END AS cross_platform_adaptation_strength

FROM `your-project.ads_demo.platform_consistency_analysis`
GROUP BY brand
ORDER BY overall_platform_gap ASC, brand;

-- Validation and testing query
SELECT 
  'PLATFORM_CONSISTENCY_VALIDATION' AS test_name,
  COUNT(*) AS total_brand_weeks_analyzed,
  COUNT(DISTINCT brand) AS unique_brands,
  ROUND(AVG(avg_platform_optimization_gap), 3) AS avg_optimization_gap,
  COUNTIF(platform_consistency_tier = 'HIGHLY_CONSISTENT') AS highly_consistent_periods,
  COUNTIF(platform_strategy_classification = 'UNIFIED_CROSS_PLATFORM') AS unified_cross_platform_periods,
  ROUND(AVG(cross_platform_adaptation_gap), 3) AS avg_cross_platform_adaptation_gap,
  COUNTIF(cross_platform_adaptation_quality = 'WELL_ADAPTED_CROSS_PLATFORM') AS well_adapted_cross_platform_periods
FROM `your-project.ads_demo.platform_consistency_analysis`;