-- Enhanced Ad Classification using working ML.GENERATE_TEXT syntax
-- Based on successful Gemini 2.0 Flash test

-- First create temporary table with raw classifications
CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.strategic_raw_temp` AS

WITH strategic_prompts AS (
  SELECT 
    ad_archive_id,
    brand,
    creative_text,
    title,
    cta_text,
    page_category,
    CONCAT(
      'Analyze this Facebook ad and provide strategic classification in JSON format:\n\n',
      'Ad Content:\n',
      'Headline: ', COALESCE(title, 'None'), '\n',
      'Body: ', COALESCE(creative_text, 'None'), '\n', 
      'CTA: ', COALESCE(cta_text, 'None'), '\n',
      'Category: ', COALESCE(page_category, 'None'), '\n\n',
      
      'Classify across these dimensions:\n\n',
      
      '1. FUNNEL STAGE (required):\n',
      '   - "Upper": Brand awareness, storytelling, category education\n',
      '   - "Mid": Consideration, benefits, social proof, comparisons\n', 
      '   - "Lower": Direct offers, pricing, urgency, retargeting\n\n',
      
      '2. PERSONA TARGET (required):\n',
      '   - "New Customer": First-time buyer focus\n',
      '   - "Existing Customer": Retention, upsell, cross-sell\n',
      '   - "Competitor Switch": Win from competitors\n',
      '   - "General Market": Broad appeal\n\n',
      
      '3. TOPIC THEMES (array, max 3):\n',
      '   Choose from: ["discount", "benefits", "features", "social_proof", "urgency", \n',
      '   "brand_story", "launch", "sustainability", "guarantee", "testimonial", \n',
      '   "comparison", "education", "lifestyle", "problem_solution"]\n\n',
      
      '4. URGENCY SCORE (0.0-1.0):\n',
      '   0.0 = No urgency (brand/educational)\n',
      '   0.5 = Moderate urgency (limited time offers)\n', 
      '   1.0 = High urgency (flash sales, scarcity)\n\n',
      
      '5. PROMOTIONAL INTENSITY (0.0-1.0):\n',
      '   0.0 = No promotional content (brand/info)\n',
      '   0.5 = Moderate promotion (benefits focus)\n',
      '   1.0 = Heavy promotion (discounts/deals)\n\n',
      
      '6. BRAND VOICE SCORE (0.0-1.0):\n',
      '   0.0 = Informal/conversational\n',
      '   0.5 = Professional but approachable\n',
      '   1.0 = Formal/corporate\n\n',
      
      'Return ONLY valid JSON in this exact format:\n',
      '{\n',
      '  "funnel": "Upper|Mid|Lower",\n',
      '  "persona": "New Customer|Existing Customer|Competitor Switch|General Market",\n',
      '  "topics": ["topic1", "topic2", "topic3"],\n',
      '  "urgency_score": 0.0,\n',
      '  "promotional_intensity": 0.0,\n',
      '  "brand_voice_score": 0.0\n',
      '}'
    ) as prompt
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings`
  WHERE creative_text IS NOT NULL OR title IS NOT NULL
  LIMIT 10  -- Start with small batch for testing
)

SELECT 
  ad_archive_id,
  brand,
  creative_text,
  title,
  cta_text,
  page_category,
  ml_generate_text_result as classification_json
FROM ML.GENERATE_TEXT(
  MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
  (SELECT prompt, ad_archive_id, brand, creative_text, title, cta_text, page_category FROM strategic_prompts),
  STRUCT(
    0.1 AS temperature,
    1000 AS max_output_tokens
  )
);