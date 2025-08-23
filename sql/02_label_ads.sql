CREATE OR REPLACE TABLE `yourproj.ads_demo.ads_labels` AS
SELECT
  ad_id,
  brand,
  (AI.GENERATE_TABLE(
     PROMPT => '''
       Classify the ad’s funnel stage as one of:
       - Upper (brand/storytelling/category awareness)
       - Mid (consideration: benefits, education, social proof)
       - Lower (offer/price/urgency/retargeting)

       Return up to 3 message angles chosen only from:
       ["discount","benefits","UGC","social proof","launch","brand story",
        "urgency","feature","sustainability","guarantee","testimonial"].

       Keep it terse and categorical.
     ''',
     INPUTS => STRUCT(creative_text AS text, title AS headline),
     OUTPUT_SCHEMA => 'funnel STRING, angles ARRAY<STRING>'
   )).*
FROM `yourproj.ads_demo.ads_raw`;
