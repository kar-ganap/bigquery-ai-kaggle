-- Test strategic classification with Gemini 2.0 Flash

WITH test_ads AS (
  SELECT 
    ad_archive_id,
    brand,
    SUBSTR(creative_text, 1, 100) as creative_sample,
    CONCAT(
      'Classify this Facebook ad funnel stage:\n\n',
      'Headline: ', COALESCE(title, 'None'), '\n',
      'Body: ', COALESCE(creative_text, 'None'), '\n',
      'CTA: ', COALESCE(cta_text, 'None'), '\n\n',
      'Return one of: Upper, Mid, Lower'
    ) as prompt
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels_test`
  LIMIT 2
)

SELECT 
  ad_archive_id,
  brand,
  creative_sample,
  ml_generate_text_result as funnel_classification,
  prompt
FROM ML.GENERATE_TEXT(
  MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
  (SELECT prompt, ad_archive_id, brand, creative_sample FROM test_ads),
  STRUCT(
    0.1 AS temperature,
    20 AS max_output_tokens
  )
);