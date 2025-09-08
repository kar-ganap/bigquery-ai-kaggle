-- Create ads_with_dates table from mock data
-- This table provides cleaner date columns for easier analysis

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` AS
SELECT 
  ad_id AS ad_archive_id,  -- Match expected column name
  brand,
  
  -- Date fields
  DATE(start_timestamp) AS start_date,
  DATE_ADD(DATE(start_timestamp), INTERVAL active_days DAY) AS end_date,  -- Calculate end date
  
  -- Duration
  active_days,
  
  -- Strategic fields
  funnel,
  primary_angle,
  persona,
  
  -- Categories (map platform to page_category for compatibility)
  COALESCE(publisher_platforms, 'UNKNOWN') AS page_category,
  
  -- Metrics
  promotional_intensity,
  urgency_score,
  brand_voice_score,
  
  -- Content
  creative_text,
  title,
  
  -- Original timestamps
  start_timestamp,
  TIMESTAMP_ADD(start_timestamp, INTERVAL active_days * 24 HOUR) AS end_timestamp,  -- Calculate end timestamp
  
  -- Additional fields (create if missing)
  week_start,
  DATE_TRUNC(DATE(start_timestamp), MONTH) AS month_start,
  
  -- Promotional signals (derive from content)
  CASE 
    WHEN REGEXP_CONTAINS(UPPER(creative_text), r'SALE|DISCOUNT|OFF|SAVE|DEAL') 
    THEN 'DISCOUNT'
    WHEN REGEXP_CONTAINS(UPPER(creative_text), r'NEW|LAUNCH|INTRODUCING') 
    THEN 'NEW_PRODUCT'
    WHEN REGEXP_CONTAINS(UPPER(creative_text), r'LIMITED|EXCLUSIVE|SPECIAL') 
    THEN 'EXCLUSIVE'
    ELSE 'BRAND'
  END AS promotional_theme,
  
  -- Extract discount percentage if present
  CAST(REGEXP_EXTRACT(creative_text, r'(\d+)%\s*OFF') AS INT64) AS discount_percentage,
  
  -- Promotional signals
  REGEXP_CONTAINS(UPPER(creative_text), r'SALE|DISCOUNT|OFF|SAVE|DEAL|PROMO') AS has_promotional_signals,
  REGEXP_CONTAINS(UPPER(creative_text), r'LIMITED|HURRY|NOW|TODAY|FAST|QUICK') AS has_urgency_signals,
  
  -- Aggressiveness scoring (combine urgency and promotional intensity)
  (urgency_score + promotional_intensity) / 2.0 AS final_aggressiveness_score,
  
  CASE 
    WHEN (urgency_score + promotional_intensity) / 2.0 > 0.7 THEN 'HIGH'
    WHEN (urgency_score + promotional_intensity) / 2.0 > 0.4 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS aggressiveness_tier
  
FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels_mock`
WHERE ad_id IS NOT NULL;