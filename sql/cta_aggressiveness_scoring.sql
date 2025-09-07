-- CTA Aggressiveness & Promotional Intensity Scoring
-- Analyzes urgency signals, discount intensity, and action pressure in creative text

CREATE OR REPLACE TABLE `your-project.ads_demo.cta_aggressiveness_analysis` AS
WITH cta_features AS (
  SELECT 
    ad_id,
    brand,
    creative_text,
    title,
    media_type,
    start_timestamp,
    end_timestamp,
    active_days,
    cta_type,
    
    -- Combine all text for analysis
    COALESCE(creative_text, '') || ' ' || COALESCE(title, '') AS full_text,
    
    -- Urgency signal detection (case insensitive)
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(LIMITED TIME|HURRY|URGENT|NOW|TODAY ONLY|EXPIRES|DEADLINE|LAST CHANCE|FINAL|ENDING|WHILE SUPPLIES LAST|DON\'T WAIT|ACT FAST|QUICK|IMMEDIATE)') 
      THEN 1.0 ELSE 0.0 
    END AS has_urgency_signals,
    
    -- Discount/offer intensity detection
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(\d+%\s*(OFF|DISCOUNT)|SAVE \$\d+|FREE|SALE|DEAL|OFFER|SPECIAL|PROMOTION|DISCOUNT|COUPON)') 
      THEN 1.0 ELSE 0.0 
    END AS has_promotional_signals,
    
    -- High-pressure action words
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(BUY NOW|SHOP NOW|GET NOW|CLAIM|GRAB|SECURE|RESERVE|ORDER|PURCHASE|CHECKOUT|ADD TO CART|DOWNLOAD NOW)') 
      THEN 1.0 ELSE 0.0 
    END AS has_action_pressure,
    
    -- Scarcity signals
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(ONLY \d+ LEFT|FEW LEFT|ALMOST GONE|SELLING FAST|HIGH DEMAND|POPULAR|TRENDING|EXCLUSIVE|RARE|SCARCE)') 
      THEN 1.0 ELSE 0.0 
    END AS has_scarcity_signals,
    
    -- Extract discount percentages for intensity measurement
    REGEXP_EXTRACT(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), r'(\d+)%') AS discount_percentage_str

  FROM `your-project.ads_demo.ads_with_dates`
  WHERE creative_text IS NOT NULL OR title IS NOT NULL
),

cta_scoring AS (
  SELECT 
    *,
    
    -- Convert discount percentage to numeric
    CASE 
      WHEN discount_percentage_str IS NOT NULL 
      THEN SAFE_CAST(discount_percentage_str AS INT64)
      ELSE 0
    END AS discount_percentage,
    
    -- Calculate base aggressiveness score (0.0 to 1.0)
    (has_urgency_signals + has_promotional_signals + has_action_pressure + has_scarcity_signals) / 4.0 AS base_aggressiveness_score,
    
    -- Weight urgency signals higher for time-sensitive campaigns
    has_urgency_signals * 0.4 + 
    has_promotional_signals * 0.25 + 
    has_action_pressure * 0.25 + 
    has_scarcity_signals * 0.1 AS weighted_aggressiveness_score

  FROM cta_features
),

enhanced_scoring AS (
  SELECT 
    *,
    
    -- Discount intensity bonus (higher discounts = more aggressive)
    CASE 
      WHEN discount_percentage >= 50 THEN 0.3
      WHEN discount_percentage >= 30 THEN 0.2
      WHEN discount_percentage >= 20 THEN 0.1
      WHEN discount_percentage > 0 THEN 0.05
      ELSE 0.0
    END AS discount_intensity_bonus,
    
    -- CTA type aggressiveness mapping
    CASE cta_type
      WHEN 'BUY_NOW' THEN 0.2
      WHEN 'SHOP_NOW' THEN 0.15
      WHEN 'SIGN_UP' THEN 0.05
      WHEN 'LEARN_MORE' THEN 0.0
      WHEN 'DOWNLOAD' THEN 0.1
      ELSE 0.05
    END AS cta_type_bonus,
    
    -- Media type consideration (video can be more persuasive)
    CASE media_type
      WHEN 'VIDEO' THEN 0.05
      WHEN 'DCO' THEN 0.03  -- Dynamic creative typically more targeted
      ELSE 0.0
    END AS media_type_bonus

  FROM cta_scoring
)

SELECT 
  ad_id,
  brand,
  creative_text,
  title,
  media_type,
  cta_type,
  start_timestamp,
  end_timestamp,
  active_days,
  
  -- Feature flags
  CAST(has_urgency_signals AS BOOL) AS has_urgency_signals,
  CAST(has_promotional_signals AS BOOL) AS has_promotional_signals,
  CAST(has_action_pressure AS BOOL) AS has_action_pressure,
  CAST(has_scarcity_signals AS BOOL) AS has_scarcity_signals,
  
  -- Extracted values
  discount_percentage,
  
  -- Scoring components
  ROUND(base_aggressiveness_score, 3) AS base_aggressiveness_score,
  ROUND(weighted_aggressiveness_score, 3) AS weighted_aggressiveness_score,
  ROUND(discount_intensity_bonus, 3) AS discount_intensity_bonus,
  ROUND(cta_type_bonus, 3) AS cta_type_bonus,
  ROUND(media_type_bonus, 3) AS media_type_bonus,
  
  -- Final aggressiveness score (capped at 1.0)
  LEAST(1.0, ROUND(
    weighted_aggressiveness_score + 
    discount_intensity_bonus + 
    cta_type_bonus + 
    media_type_bonus, 
    3
  )) AS final_aggressiveness_score,
  
  -- Categorical classification
  CASE 
    WHEN LEAST(1.0, weighted_aggressiveness_score + discount_intensity_bonus + cta_type_bonus + media_type_bonus) >= 0.8 
    THEN 'HIGHLY_AGGRESSIVE'
    WHEN LEAST(1.0, weighted_aggressiveness_score + discount_intensity_bonus + cta_type_bonus + media_type_bonus) >= 0.6 
    THEN 'MODERATELY_AGGRESSIVE'
    WHEN LEAST(1.0, weighted_aggressiveness_score + discount_intensity_bonus + cta_type_bonus + media_type_bonus) >= 0.4 
    THEN 'MILDLY_AGGRESSIVE'
    WHEN LEAST(1.0, weighted_aggressiveness_score + discount_intensity_bonus + cta_type_bonus + media_type_bonus) >= 0.2 
    THEN 'LOW_PRESSURE'
    ELSE 'BRAND_FOCUSED'
  END AS aggressiveness_tier,
  
  -- Business context
  DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week_start,
  
  -- Extract key promotional themes for calendar analysis
  CASE 
    WHEN REGEXP_CONTAINS(UPPER(full_text), r'(BLACK FRIDAY|CYBER MONDAY|HOLIDAY|CHRISTMAS|NEW YEAR)') THEN 'SEASONAL_MAJOR'
    WHEN REGEXP_CONTAINS(UPPER(full_text), r'(SPRING|SUMMER|FALL|WINTER|BACK TO SCHOOL)') THEN 'SEASONAL_GENERAL'
    WHEN REGEXP_CONTAINS(UPPER(full_text), r'(ANNIVERSARY|BIRTHDAY|LAUNCH|GRAND OPENING)') THEN 'MILESTONE'
    WHEN has_promotional_signals = 1.0 THEN 'PROMOTIONAL'
    ELSE 'EVERGREEN'
  END AS promotional_theme

FROM enhanced_scoring
ORDER BY final_aggressiveness_score DESC, brand, start_timestamp DESC;

-- Create summary view for competitive analysis
CREATE OR REPLACE VIEW `your-project.ads_demo.v_cta_competitive_summary` AS
SELECT 
  brand,
  week_start,
  
  -- Volume metrics
  COUNT(*) AS total_ads,
  COUNT(DISTINCT ad_id) AS unique_ads,
  
  -- Aggressiveness distribution
  AVG(final_aggressiveness_score) AS avg_aggressiveness_score,
  
  COUNTIF(aggressiveness_tier = 'HIGHLY_AGGRESSIVE') / COUNT(*) * 100 AS pct_highly_aggressive,
  COUNTIF(aggressiveness_tier = 'MODERATELY_AGGRESSIVE') / COUNT(*) * 100 AS pct_moderately_aggressive,
  COUNTIF(aggressiveness_tier = 'BRAND_FOCUSED') / COUNT(*) * 100 AS pct_brand_focused,
  
  -- Feature prevalence
  COUNTIF(has_urgency_signals) / COUNT(*) * 100 AS pct_urgency_signals,
  COUNTIF(has_promotional_signals) / COUNT(*) * 100 AS pct_promotional_signals,
  COUNTIF(has_action_pressure) / COUNT(*) * 100 AS pct_action_pressure,
  COUNTIF(has_scarcity_signals) / COUNT(*) * 100 AS pct_scarcity_signals,
  
  -- Discount analysis
  AVG(CASE WHEN discount_percentage > 0 THEN discount_percentage ELSE NULL END) AS avg_discount_percentage,
  COUNTIF(discount_percentage > 0) / COUNT(*) * 100 AS pct_with_discounts,
  
  -- Promotional themes
  COUNTIF(promotional_theme = 'SEASONAL_MAJOR') / COUNT(*) * 100 AS pct_seasonal_major,
  COUNTIF(promotional_theme = 'PROMOTIONAL') / COUNT(*) * 100 AS pct_promotional,
  COUNTIF(promotional_theme = 'EVERGREEN') / COUNT(*) * 100 AS pct_evergreen,
  
  -- Week-over-week change in aggressiveness
  AVG(final_aggressiveness_score) - LAG(AVG(final_aggressiveness_score)) OVER (
    PARTITION BY brand ORDER BY week_start
  ) AS aggressiveness_change_wow

FROM `your-project.ads_demo.cta_aggressiveness_analysis`
GROUP BY brand, week_start
ORDER BY brand, week_start DESC;

-- Validation query for testing
SELECT 
  'CTA_AGGRESSIVENESS_VALIDATION' AS test_name,
  COUNT(*) AS total_ads_analyzed,
  COUNT(DISTINCT brand) AS unique_brands,
  AVG(final_aggressiveness_score) AS avg_aggressiveness_score,
  COUNTIF(aggressiveness_tier = 'HIGHLY_AGGRESSIVE') AS highly_aggressive_ads,
  COUNTIF(has_urgency_signals) AS ads_with_urgency,
  COUNTIF(discount_percentage > 0) AS ads_with_discounts,
  MAX(discount_percentage) AS max_discount_found
FROM `your-project.ads_demo.cta_aggressiveness_analysis`;