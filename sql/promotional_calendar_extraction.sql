-- Promotional Calendar Extraction & Cross-Competitor Timing Analysis
-- Identifies promotional periods, seasonal campaigns, and competitive promotional timing

CREATE OR REPLACE TABLE `your-project.ads_demo.promotional_calendar` AS
WITH promotional_periods AS (
  SELECT 
    ad_id,
    brand,
    creative_text,
    title,
    start_timestamp,
    end_timestamp,
    active_days,
    final_aggressiveness_score,
    aggressiveness_tier,
    promotional_theme,
    discount_percentage,
    has_promotional_signals,
    has_urgency_signals,
    
    -- Extract specific promotional events with regex patterns
    CASE 
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(BLACK FRIDAY|BFCM|CYBER MONDAY|CYBER WEEK)') THEN 'BLACK_FRIDAY_CYBER_MONDAY'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(CHRISTMAS|HOLIDAY|XMAS|DECEMBER|FESTIVE)') THEN 'CHRISTMAS_HOLIDAY'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(NEW YEAR|JANUARY|RESOLUTION|FRESH START)') THEN 'NEW_YEAR'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(VALENTINE|LOVE|FEBRUARY)') THEN 'VALENTINES_DAY'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(SPRING|MARCH|APRIL|EASTER)') THEN 'SPRING_EASTER'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(SUMMER|JUNE|JULY|AUGUST|VACATION|BEACH)') THEN 'SUMMER'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(BACK TO SCHOOL|SEPTEMBER|FALL|AUTUMN)') THEN 'BACK_TO_SCHOOL'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(ANNIVERSARY|BIRTHDAY|\d+TH|YEARS|CELEBRATION)') THEN 'ANNIVERSARY_MILESTONE'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(FLASH SALE|24 HOUR|48 HOUR|WEEKEND|TODAY ONLY)') THEN 'FLASH_SALE'
      WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
        r'(END OF|CLEARANCE|FINAL|CLOSEOUT|LIQUIDATION)') THEN 'CLEARANCE_EOL'
      WHEN has_promotional_signals AND discount_percentage >= 30 THEN 'HIGH_DISCOUNT_GENERIC'
      WHEN has_promotional_signals THEN 'GENERAL_PROMOTION'
      ELSE 'NON_PROMOTIONAL'
    END AS promotional_event_type,
    
    -- Promotional intensity scoring
    CASE 
      WHEN has_promotional_signals AND has_urgency_signals AND discount_percentage >= 40 THEN 'MEGA_SALE'
      WHEN has_promotional_signals AND discount_percentage >= 30 THEN 'MAJOR_SALE'
      WHEN has_promotional_signals AND discount_percentage >= 20 THEN 'MODERATE_SALE'
      WHEN has_promotional_signals AND discount_percentage > 0 THEN 'LIGHT_PROMOTION'
      WHEN has_promotional_signals THEN 'SOFT_PROMOTION'
      ELSE 'BRAND_MESSAGING'
    END AS promotional_intensity_tier,
    
    -- Time-based analysis
    DATE(start_timestamp) AS start_date,
    DATE(COALESCE(end_timestamp, CURRENT_TIMESTAMP())) AS end_date,
    DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week_start,
    DATE_TRUNC(DATE(start_timestamp), MONTH) AS month_start,
    EXTRACT(DAYOFWEEK FROM DATE(start_timestamp)) AS start_day_of_week,  -- 1=Sunday
    EXTRACT(MONTH FROM DATE(start_timestamp)) AS start_month,
    
    -- Generate daily promotional coverage
    GENERATE_DATE_ARRAY(
      DATE(start_timestamp), 
      DATE(COALESCE(end_timestamp, CURRENT_TIMESTAMP())), 
      INTERVAL 1 DAY
    ) AS active_date_range

  FROM `your-project.ads_demo.cta_aggressiveness_analysis`
  WHERE start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 MONTH)  -- Last 12 months
),

-- Expand to daily promotional coverage for overlap analysis
daily_promotional_coverage AS (
  SELECT 
    brand,
    promotional_event_type,
    promotional_intensity_tier,
    final_aggressiveness_score,
    active_date AS promotional_date,
    COUNT(*) OVER (PARTITION BY brand, active_date) AS ads_active_on_date,
    COUNT(*) OVER (PARTITION BY promotional_date) AS total_brands_active_on_date
    
  FROM promotional_periods
  CROSS JOIN UNNEST(active_date_range) AS active_date
  WHERE promotional_event_type != 'NON_PROMOTIONAL'
),

-- Cross-competitor promotional overlap analysis
competitive_promotional_overlap AS (
  SELECT 
    promotional_date,
    promotional_event_type,
    
    -- Brand participation
    COUNT(DISTINCT brand) AS brands_participating,
    STRING_AGG(DISTINCT brand ORDER BY brand) AS participating_brands,
    
    -- Intensity analysis
    AVG(final_aggressiveness_score) AS avg_aggressiveness_across_brands,
    MAX(final_aggressiveness_score) AS max_aggressiveness,
    
    -- Most common intensity tier
    ARRAY_AGG(promotional_intensity_tier ORDER BY promotional_intensity_tier)[OFFSET(0)] AS dominant_intensity_tier,
    
    -- Competition level indicator
    CASE 
      WHEN COUNT(DISTINCT brand) >= 4 THEN 'HIGH_COMPETITION'
      WHEN COUNT(DISTINCT brand) >= 2 THEN 'MODERATE_COMPETITION'
      ELSE 'LOW_COMPETITION'
    END AS competition_level

  FROM daily_promotional_coverage
  WHERE promotional_event_type != 'GENERAL_PROMOTION'  -- Focus on specific events
  GROUP BY promotional_date, promotional_event_type
  HAVING COUNT(DISTINCT brand) >= 2  -- Only show overlapping promotional periods
),

-- Brand-specific promotional patterns
brand_promotional_patterns AS (
  SELECT 
    brand,
    promotional_event_type,
    
    -- Frequency and timing
    COUNT(*) AS promotional_campaigns,
    COUNT(DISTINCT week_start) AS weeks_with_promotions,
    COUNT(DISTINCT month_start) AS months_with_promotions,
    
    -- Duration analysis
    AVG(active_days) AS avg_campaign_duration_days,
    MIN(active_days) AS min_campaign_duration,
    MAX(active_days) AS max_campaign_duration,
    
    -- Aggressiveness analysis
    AVG(final_aggressiveness_score) AS avg_aggressiveness,
    AVG(CASE WHEN discount_percentage > 0 THEN discount_percentage END) AS avg_discount_percentage,
    
    -- Seasonal timing preferences
    AVG(start_month) AS avg_start_month,
    MODE() OVER (PARTITION BY brand, promotional_event_type ORDER BY start_day_of_week) AS preferred_start_day,
    
    -- Most recent campaign
    MAX(start_date) AS last_campaign_date,
    DATE_DIFF(CURRENT_DATE(), MAX(start_date), DAY) AS days_since_last_campaign

  FROM promotional_periods
  WHERE promotional_event_type != 'NON_PROMOTIONAL'
  GROUP BY brand, promotional_event_type
  HAVING COUNT(*) >= 2  -- Only patterns with multiple campaigns
)

-- Final promotional calendar table
SELECT 
  pp.brand,
  pp.promotional_event_type,
  pp.promotional_intensity_tier,
  pp.start_date,
  pp.end_date,
  pp.active_days AS campaign_duration_days,
  pp.final_aggressiveness_score,
  pp.discount_percentage,
  pp.week_start,
  pp.month_start,
  
  -- Competitive context
  co.brands_participating AS competing_brands_count,
  co.participating_brands AS competing_brands_list,
  co.competition_level,
  co.avg_aggressiveness_across_brands AS market_avg_aggressiveness,
  
  -- Brand pattern context  
  bpp.promotional_campaigns AS brand_total_campaigns_this_type,
  bpp.avg_campaign_duration_days AS brand_avg_duration_this_type,
  bpp.avg_aggressiveness AS brand_avg_aggressiveness_this_type,
  bpp.days_since_last_campaign AS days_since_last_similar_campaign,
  
  -- Strategic insights
  CASE 
    WHEN pp.final_aggressiveness_score > co.avg_aggressiveness_across_brands + 0.2 
    THEN 'MORE_AGGRESSIVE_THAN_MARKET'
    WHEN pp.final_aggressiveness_score < co.avg_aggressiveness_across_brands - 0.2 
    THEN 'LESS_AGGRESSIVE_THAN_MARKET'
    ELSE 'ALIGNED_WITH_MARKET'
  END AS competitive_positioning,
  
  CASE 
    WHEN pp.active_days > bpp.avg_campaign_duration_days + 7 THEN 'LONGER_THAN_USUAL'
    WHEN pp.active_days < bpp.avg_campaign_duration_days - 7 THEN 'SHORTER_THAN_USUAL'
    ELSE 'TYPICAL_DURATION'
  END AS duration_vs_brand_pattern

FROM promotional_periods pp
LEFT JOIN competitive_promotional_overlap co 
  ON pp.start_date = co.promotional_date 
  AND pp.promotional_event_type = co.promotional_event_type
LEFT JOIN brand_promotional_patterns bpp 
  ON pp.brand = bpp.brand 
  AND pp.promotional_event_type = bpp.promotional_event_type
WHERE pp.promotional_event_type != 'NON_PROMOTIONAL'
ORDER BY pp.brand, pp.start_date DESC;

-- Create summary view for dashboard
CREATE OR REPLACE VIEW `your-project.ads_demo.v_promotional_calendar_summary` AS
SELECT 
  brand,
  DATE_TRUNC(start_date, MONTH) AS month,
  
  -- Promotional volume
  COUNT(*) AS total_promotional_campaigns,
  COUNT(DISTINCT promotional_event_type) AS unique_event_types,
  SUM(campaign_duration_days) AS total_promotional_days,
  
  -- Average metrics
  AVG(final_aggressiveness_score) AS avg_aggressiveness,
  AVG(discount_percentage) AS avg_discount_percentage,
  AVG(campaign_duration_days) AS avg_campaign_duration,
  
  -- Competition analysis
  AVG(competing_brands_count) AS avg_competitors_per_campaign,
  COUNTIF(competitive_positioning = 'MORE_AGGRESSIVE_THAN_MARKET') / COUNT(*) * 100 AS pct_more_aggressive,
  
  -- Event type distribution
  STRING_AGG(DISTINCT promotional_event_type ORDER BY promotional_event_type) AS event_types_this_month

FROM `your-project.ads_demo.promotional_calendar`
GROUP BY brand, DATE_TRUNC(start_date, MONTH)
ORDER BY brand, month DESC;

-- Validation query
SELECT 
  'PROMOTIONAL_CALENDAR_VALIDATION' AS test_name,
  COUNT(*) AS total_promotional_campaigns,
  COUNT(DISTINCT brand) AS brands_with_promotions,
  COUNT(DISTINCT promotional_event_type) AS unique_promotional_events,
  AVG(campaign_duration_days) AS avg_campaign_duration,
  MAX(competing_brands_count) AS max_competitors_same_event,
  COUNTIF(competition_level = 'HIGH_COMPETITION') AS high_competition_periods
FROM `your-project.ads_demo.promotional_calendar`;