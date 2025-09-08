-- Create mock time-series data to test our framework
-- This simulates ads with realistic start/end dates until we get real Meta Ad Library dates

CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` AS

WITH mock_date_assignments AS (
  SELECT 
    ad_archive_id,
    brand,
    funnel,
    persona,
    urgency_score,
    promotional_intensity,
    brand_voice_score,
    topics,
    classified_at,
    
    -- Mock start dates: spread over last 30 days
    DATE_SUB(CURRENT_DATE(), 
      INTERVAL MOD(CAST(SUBSTR(ad_archive_id, -3) AS INT64), 30) DAY
    ) as start_date,
    
    -- Mock end dates based on promotional intensity (higher promo = shorter runs)
    CASE 
      WHEN promotional_intensity >= 0.8 THEN 
        DATE_ADD(
          DATE_SUB(CURRENT_DATE(), INTERVAL MOD(CAST(SUBSTR(ad_archive_id, -3) AS INT64), 30) DAY),
          INTERVAL CAST(3 + MOD(CAST(SUBSTR(ad_archive_id, -2) AS INT64), 5) AS INT64) DAY  -- 3-7 days
        )
      WHEN promotional_intensity >= 0.5 THEN
        DATE_ADD(
          DATE_SUB(CURRENT_DATE(), INTERVAL MOD(CAST(SUBSTR(ad_archive_id, -3) AS INT64), 30) DAY),
          INTERVAL CAST(7 + MOD(CAST(SUBSTR(ad_archive_id, -2) AS INT64), 14) AS INT64) DAY  -- 7-20 days  
        )
      ELSE 
        DATE_ADD(
          DATE_SUB(CURRENT_DATE(), INTERVAL MOD(CAST(SUBSTR(ad_archive_id, -3) AS INT64), 30) DAY),
          INTERVAL CAST(14 + MOD(CAST(SUBSTR(ad_archive_id, -2) AS INT64), 21) AS INT64) DAY  -- 14-35 days
        )
    END as end_date
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels`
)

SELECT 
  ad_archive_id,
  brand,
  start_date,
  
  -- Business logic: if end_date is in future, set to NULL (still active)
  CASE 
    WHEN end_date > CURRENT_DATE() THEN NULL
    ELSE end_date
  END as end_date,
  
  funnel,
  persona,
  urgency_score,
  promotional_intensity,
  brand_voice_score,
  topics,
  classified_at,
  
  -- Calculate active days for validation
  DATE_DIFF(
    COALESCE(
      CASE WHEN end_date > CURRENT_DATE() THEN NULL ELSE end_date END,
      CURRENT_DATE()
    ), 
    start_date, 
    DAY
  ) as active_days_calculated

FROM mock_date_assignments;