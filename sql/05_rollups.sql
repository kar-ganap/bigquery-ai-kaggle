CREATE OR REPLACE TABLE `yourproj.ads_demo.ads_activity` AS
SELECT
  ad_id,
  brand,
  day AS active_day,
  DATE_TRUNC(day, WEEK(MONDAY)) AS week_start,
  1 AS active_variant_day
FROM `yourproj.ads_demo.ads_raw`,
UNNEST(GENERATE_DATE_ARRAY(first_seen, COALESCE(last_seen, first_seen))) AS day;

CREATE OR REPLACE TABLE `yourproj.ads_demo.brand_week_funnel_mix` AS
WITH joined AS (
  SELECT a.week_start, r.brand, l.funnel, SUM(a.active_variant_day) AS active_days
  FROM `yourproj.ads_demo.ads_activity` a
  JOIN `yourproj.ads_demo.ads_raw` r USING (ad_id)
  JOIN `yourproj.ads_demo.ads_labels` l USING (ad_id, brand)
  GROUP BY 1,2,3
),
tot AS (
  SELECT week_start, brand, SUM(active_days) AS total_days
  FROM joined GROUP BY 1,2
)
SELECT j.week_start, j.brand, j.funnel,
       j.active_days,
       ROUND(100 * j.active_days / t.total_days, 1) AS pct_of_activity
FROM joined j
JOIN tot t USING (week_start, brand)
ORDER BY week_start, brand, pct_of_activity DESC;
