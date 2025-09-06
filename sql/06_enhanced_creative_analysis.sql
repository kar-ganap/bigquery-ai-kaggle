-- Enhanced Creative Analysis & Strategic Intelligence
-- Subgoal 6: Comprehensive strategic competitive intelligence system

-- Step 1: Enhanced Ad Classification with Strategic Labels
CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels` AS
SELECT
  ad_archive_id,
  brand,
  creative_text,
  title,
  cta_text,
  page_category,
  (ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
    PROMPT => CONCAT(
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
      
      'Return valid JSON only:'
    ),
    GENERATION_CONFIG => JSON_OBJECT(
      "temperature", 0.1,
      "maxOutputTokens", 1000,
      "responseMimeType", "application/json",
      "responseSchema", JSON_OBJECT(
        "type", "object",
        "properties", JSON_OBJECT(
          "funnel", JSON_OBJECT("type", "string", "enum", JSON_ARRAY("Upper", "Mid", "Lower")),
          "persona", JSON_OBJECT("type", "string", "enum", JSON_ARRAY("New Customer", "Existing Customer", "Competitor Switch", "General Market")),
          "topics", JSON_OBJECT("type", "array", "items", JSON_OBJECT("type", "string"), "maxItems", 3),
          "urgency_score", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1),
          "promotional_intensity", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1),
          "brand_voice_score", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1)
        ),
        "required", JSON_ARRAY("funnel", "persona", "topics", "urgency_score", "promotional_intensity", "brand_voice_score")
      )
    )
  )) AS classification_json,
  
  -- Extract individual fields from JSON
  JSON_VALUE(ML.GENERATE_TEXT(
    MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
    PROMPT => CONCAT(
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
      
      'Return valid JSON only:'
    ),
    GENERATION_CONFIG => JSON_OBJECT(
      "temperature", 0.1,
      "maxOutputTokens", 1000,
      "responseMimeType", "application/json",
      "responseSchema", JSON_OBJECT(
        "type", "object",
        "properties", JSON_OBJECT(
          "funnel", JSON_OBJECT("type", "string", "enum", JSON_ARRAY("Upper", "Mid", "Lower")),
          "persona", JSON_OBJECT("type", "string", "enum", JSON_ARRAY("New Customer", "Existing Customer", "Competitor Switch", "General Market")),
          "topics", JSON_OBJECT("type", "array", "items", JSON_OBJECT("type", "string"), "maxItems", 3),
          "urgency_score", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1),
          "promotional_intensity", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1),
          "brand_voice_score", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1)
        ),
        "required", JSON_ARRAY("funnel", "persona", "topics", "urgency_score", "promotional_intensity", "brand_voice_score")
      )
    )
  ), '$.funnel') AS funnel,
  
  JSON_VALUE(classification_json, '$.persona') AS persona,
  JSON_EXTRACT_ARRAY(classification_json, '$.topics') AS topics,
  CAST(JSON_VALUE(classification_json, '$.urgency_score') AS FLOAT64) AS urgency_score,
  CAST(JSON_VALUE(classification_json, '$.promotional_intensity') AS FLOAT64) AS promotional_intensity,
  CAST(JSON_VALUE(classification_json, '$.brand_voice_score') AS FLOAT64) AS brand_voice_score,
  
  -- Add timestamp for time-series analysis
  CURRENT_TIMESTAMP() AS classified_at

FROM `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_production`
WHERE creative_text IS NOT NULL OR title IS NOT NULL;  -- Only classify ads with content


-- Step 2: Time-Series Strategy Intelligence Views
-- Track funnel mix evolution over time
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_strategy_evolution` AS

WITH daily_strategy AS (
  SELECT 
    brand,
    DATE(classified_at) as analysis_date,
    
    -- Funnel distribution
    COUNT(*) as total_ads,
    COUNT(CASE WHEN funnel = 'Upper' THEN 1 END) as upper_funnel_ads,
    COUNT(CASE WHEN funnel = 'Mid' THEN 1 END) as mid_funnel_ads,
    COUNT(CASE WHEN funnel = 'Lower' THEN 1 END) as lower_funnel_ads,
    
    -- Percentages
    ROUND(COUNT(CASE WHEN funnel = 'Upper' THEN 1 END) / COUNT(*) * 100, 1) as pct_upper_funnel,
    ROUND(COUNT(CASE WHEN funnel = 'Mid' THEN 1 END) / COUNT(*) * 100, 1) as pct_mid_funnel,
    ROUND(COUNT(CASE WHEN funnel = 'Lower' THEN 1 END) / COUNT(*) * 100, 1) as pct_lower_funnel,
    
    -- Average scores
    ROUND(AVG(urgency_score), 3) as avg_urgency_score,
    ROUND(AVG(promotional_intensity), 3) as avg_promotional_intensity,
    ROUND(AVG(brand_voice_score), 3) as avg_brand_voice_score,
    
    -- Top topics
    ARRAY_AGG(DISTINCT topic IGNORE NULLS ORDER BY topic LIMIT 5) as trending_topics
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels`,
  UNNEST(topics) as topic
  GROUP BY brand, DATE(classified_at)
),

-- Calculate week-over-week changes
strategy_changes AS (
  SELECT 
    *,
    
    -- Week-over-week funnel shift
    pct_upper_funnel - LAG(pct_upper_funnel, 7) OVER (PARTITION BY brand ORDER BY analysis_date) as upper_funnel_change_7d,
    pct_mid_funnel - LAG(pct_mid_funnel, 7) OVER (PARTITION BY brand ORDER BY analysis_date) as mid_funnel_change_7d,
    pct_lower_funnel - LAG(pct_lower_funnel, 7) OVER (PARTITION BY brand ORDER BY analysis_date) as lower_funnel_change_7d,
    
    -- Score changes
    avg_urgency_score - LAG(avg_urgency_score, 7) OVER (PARTITION BY brand ORDER BY analysis_date) as urgency_change_7d,
    avg_promotional_intensity - LAG(avg_promotional_intensity, 7) OVER (PARTITION BY brand ORDER BY analysis_date) as promo_intensity_change_7d,
    
    -- Strategy shift indicators
    CASE 
      WHEN ABS(pct_upper_funnel - LAG(pct_upper_funnel, 7) OVER (PARTITION BY brand ORDER BY analysis_date)) > 20 THEN 'Major Funnel Shift'
      WHEN ABS(avg_promotional_intensity - LAG(avg_promotional_intensity, 7) OVER (PARTITION BY brand ORDER BY analysis_date)) > 0.3 THEN 'Promotional Strategy Change'
      WHEN ABS(avg_brand_voice_score - LAG(avg_brand_voice_score, 7) OVER (PARTITION BY brand ORDER BY analysis_date)) > 0.2 THEN 'Brand Voice Shift'
      ELSE 'Stable Strategy'
    END as strategy_change_type
    
  FROM daily_strategy
)

SELECT * FROM strategy_changes
WHERE analysis_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAYS)  -- Last 3 months
ORDER BY brand, analysis_date DESC;


-- Step 3: Competitive Response Detection System
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitive_responses` AS

WITH similarity_with_timing AS (
  SELECT 
    base.brand as base_brand,
    base.ad_archive_id as base_ad_id,
    base.creative_text as base_content,
    base.classified_at as base_timestamp,
    
    comp.brand as competitor_brand, 
    comp.ad_archive_id as competitor_ad_id,
    comp.creative_text as competitor_content,
    comp.classified_at as competitor_timestamp,
    
    -- Semantic similarity from existing embeddings
    (1 - ML.DISTANCE(base_emb.content_embedding, comp_emb.content_embedding, 'COSINE')) as content_similarity,
    
    -- Strategic similarity
    CASE WHEN base.funnel = comp.funnel THEN 1 ELSE 0 END as same_funnel,
    CASE WHEN base.persona = comp.persona THEN 1 ELSE 0 END as same_persona,
    ABS(base.urgency_score - comp.urgency_score) as urgency_diff,
    ABS(base.promotional_intensity - comp.promotional_intensity) as promo_intensity_diff,
    
    -- Time difference in days
    DATE_DIFF(DATE(comp.classified_at), DATE(base.classified_at), DAY) as days_between,
    
    -- Strategic alignment score
    (CASE WHEN base.funnel = comp.funnel THEN 1 ELSE 0 END +
     CASE WHEN base.persona = comp.persona THEN 1 ELSE 0 END +
     (1 - ABS(base.urgency_score - comp.urgency_score)) +
     (1 - ABS(base.promotional_intensity - comp.promotional_intensity))) / 4 as strategic_alignment
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels` base
  JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels` comp
    ON base.brand != comp.brand  -- Cross-competitor only
  JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_production` base_emb
    ON base.ad_archive_id = base_emb.ad_archive_id
  JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_production` comp_emb
    ON comp.ad_archive_id = comp_emb.ad_archive_id
  
  WHERE DATE_DIFF(DATE(comp.classified_at), DATE(base.classified_at), DAY) BETWEEN 1 AND 14  -- 2 week window
    AND (1 - ML.DISTANCE(base_emb.content_embedding, comp_emb.content_embedding, 'COSINE')) >= 0.75  -- High similarity threshold
),

potential_copies AS (
  SELECT 
    *,
    
    -- Copy likelihood score (0-1)
    (content_similarity * 0.4 + 
     strategic_alignment * 0.3 + 
     (CASE WHEN days_between <= 7 THEN 0.3 ELSE 0.15 END)) as copy_likelihood_score,
    
    -- Copy type classification
    CASE 
      WHEN content_similarity >= 0.9 AND days_between <= 3 THEN 'Direct Copy'
      WHEN content_similarity >= 0.85 AND strategic_alignment >= 0.8 AND days_between <= 7 THEN 'Strategic Copy'  
      WHEN content_similarity >= 0.8 AND days_between <= 14 THEN 'Inspired Copy'
      ELSE 'Similar Strategy'
    END as copy_type,
    
    -- Response recommendation
    CASE 
      WHEN content_similarity >= 0.9 THEN 'Consider IP protection review'
      WHEN content_similarity >= 0.85 AND strategic_alignment >= 0.8 THEN 'Monitor and potentially escalate creative differentiation'
      WHEN strategic_alignment >= 0.7 THEN 'Opportunity to differentiate positioning'
      ELSE 'Monitor for patterns'
    END as response_recommendation
    
  FROM similarity_with_timing
)

SELECT *
FROM potential_copies  
WHERE copy_likelihood_score >= 0.6  -- Only show high-confidence potential copies
ORDER BY copy_likelihood_score DESC, days_between ASC;


-- Step 4: Creative Fatigue Detection
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_creative_fatigue` AS

WITH content_clusters AS (
  SELECT 
    brand,
    ad_archive_id,
    creative_text,
    title,
    
    -- Group similar content using embedding clustering
    -- Using content similarity > 0.8 as same creative cluster
    ROW_NUMBER() OVER (
      PARTITION BY brand 
      ORDER BY ad_archive_id
    ) as cluster_seed,
    
    COUNT(*) OVER (
      PARTITION BY brand,
      -- Simplified clustering - group by topic similarity
      funnel, persona,
      ARRAY_TO_STRING(topics, ',')
    ) as cluster_size,
    
    -- Strategic dimensions for clustering
    funnel,
    persona, 
    topics,
    urgency_score,
    promotional_intensity,
    classified_at
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels`
),

semantic_clusters AS (
  SELECT 
    c1.brand,
    c1.ad_archive_id,
    c1.creative_text,
    
    -- Count semantically similar ads (>0.8 similarity)
    COUNT(c2.ad_archive_id) as similar_ad_count,
    AVG((1 - ML.DISTANCE(e1.content_embedding, e2.content_embedding, 'COSINE'))) as avg_similarity_in_cluster,
    
    -- Time span of similar content
    MAX(DATE_DIFF(DATE(c2.classified_at), DATE(c1.classified_at), DAY)) as content_age_days,
    
    c1.funnel,
    c1.persona,
    c1.topics,
    c1.urgency_score,
    c1.promotional_intensity,
    c1.classified_at
    
  FROM content_clusters c1
  JOIN content_clusters c2 ON c1.brand = c2.brand
  JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_production` e1 
    ON c1.ad_archive_id = e1.ad_archive_id  
  JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_embeddings_production` e2
    ON c2.ad_archive_id = e2.ad_archive_id
  
  WHERE (1 - ML.DISTANCE(e1.content_embedding, e2.content_embedding, 'COSINE')) >= 0.8
    AND c1.ad_archive_id != c2.ad_archive_id
    
  GROUP BY c1.brand, c1.ad_archive_id, c1.creative_text, c1.funnel, c1.persona, 
           c1.topics, c1.urgency_score, c1.promotional_intensity, c1.classified_at
),

fatigue_analysis AS (
  SELECT 
    *,
    
    -- Fatigue risk scoring
    CASE 
      WHEN similar_ad_count >= 5 AND content_age_days >= 30 THEN 'High Risk'
      WHEN similar_ad_count >= 3 AND content_age_days >= 14 THEN 'Medium Risk'  
      WHEN similar_ad_count >= 2 AND content_age_days >= 7 THEN 'Low Risk'
      ELSE 'Fresh Content'
    END as fatigue_risk_level,
    
    -- Fatigue score (0-1, higher = more fatigued)
    LEAST(1.0, 
      (similar_ad_count * 0.2) + 
      (content_age_days / 60.0) + 
      (avg_similarity_in_cluster - 0.8) * 2
    ) as fatigue_score,
    
    -- Refresh recommendations
    CASE 
      WHEN similar_ad_count >= 5 THEN 'Immediate creative refresh needed'
      WHEN similar_ad_count >= 3 AND avg_similarity_in_cluster >= 0.9 THEN 'Consider new creative angles' 
      WHEN content_age_days >= 30 THEN 'Test new messaging themes'
      ELSE 'Content is fresh, continue testing'
    END as refresh_recommendation
    
  FROM semantic_clusters
)

SELECT *
FROM fatigue_analysis
WHERE fatigue_risk_level != 'Fresh Content'  -- Only show potentially fatigued content
ORDER BY fatigue_score DESC, brand;


-- Step 5: CTA Aggressiveness & Promotional Calendar Intelligence
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_promotional_calendar` AS

WITH cta_analysis AS (
  SELECT 
    brand,
    ad_archive_id,
    creative_text,
    title,
    cta_text,
    
    -- Extract promotional signals using ML.GENERATE_TEXT
    (ML.GENERATE_TEXT(
      MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
      PROMPT => CONCAT(
        'Analyze this ad content for promotional and CTA aggressiveness signals:\n\n',
        'Headline: ', COALESCE(title, 'None'), '\n',
        'Body: ', COALESCE(creative_text, 'None'), '\n',
        'CTA: ', COALESCE(cta_text, 'None'), '\n\n',
        
        'Extract in JSON format:\n',
        '1. discount_mentioned: true/false if specific discount mentioned\n',
        '2. discount_percentage: number (0-100) if percentage discount found\n', 
        '3. urgency_words: array of urgency words found (limited, now, today, hurry, etc.)\n',
        '4. sale_type: "flash_sale", "seasonal", "clearance", "regular_promo", or "none"\n',
        '5. cta_aggressiveness: 0.0-1.0 score based on action pressure\n',
        '6. promotional_keywords: array of promotional terms found\n\n',
        
        'Return only valid JSON:'
      ),
      GENERATION_CONFIG => JSON_OBJECT(
        "temperature", 0.1,
        "maxOutputTokens", 500,
        "responseMimeType", "application/json",
        "responseSchema", JSON_OBJECT(
          "type", "object",
          "properties", JSON_OBJECT(
            "discount_mentioned", JSON_OBJECT("type", "boolean"),
            "discount_percentage", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 100),
            "urgency_words", JSON_OBJECT("type", "array", "items", JSON_OBJECT("type", "string")),
            "sale_type", JSON_OBJECT("type", "string", "enum", JSON_ARRAY("flash_sale", "seasonal", "clearance", "regular_promo", "none")),
            "cta_aggressiveness", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1),
            "promotional_keywords", JSON_OBJECT("type", "array", "items", JSON_OBJECT("type", "string"))
          )
        )
      )
    )) AS promo_analysis_json,
    
    funnel,
    persona,
    urgency_score,
    promotional_intensity,
    classified_at
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels`
  WHERE creative_text IS NOT NULL OR title IS NOT NULL
),

promotional_signals AS (
  SELECT 
    *,
    
    -- Extract promotional signals from JSON
    CAST(JSON_VALUE(promo_analysis_json, '$.discount_mentioned') AS BOOL) as has_discount,
    CAST(JSON_VALUE(promo_analysis_json, '$.discount_percentage') AS FLOAT64) as discount_percentage,
    JSON_EXTRACT_ARRAY(promo_analysis_json, '$.urgency_words') as urgency_words,
    JSON_VALUE(promo_analysis_json, '$.sale_type') as sale_type,
    CAST(JSON_VALUE(promo_analysis_json, '$.cta_aggressiveness') AS FLOAT64) as cta_aggressiveness,
    JSON_EXTRACT_ARRAY(promo_analysis_json, '$.promotional_keywords') as promotional_keywords,
    
    -- Time-based grouping for calendar analysis
    DATE(classified_at) as ad_date,
    EXTRACT(WEEK FROM classified_at) as week_of_year,
    EXTRACT(MONTH FROM classified_at) as month_of_year,
    EXTRACT(DAYOFWEEK FROM classified_at) as day_of_week,
    
    -- Combined promotional intensity score
    (promotional_intensity + urgency_score + CAST(JSON_VALUE(promo_analysis_json, '$.cta_aggressiveness') AS FLOAT64)) / 3 as combined_promo_score
    
  FROM cta_analysis
),

daily_promotional_intensity AS (
  SELECT 
    brand,
    ad_date,
    week_of_year,
    month_of_year,
    day_of_week,
    
    -- Daily metrics
    COUNT(*) as total_ads,
    COUNT(CASE WHEN has_discount THEN 1 END) as discount_ads,
    COUNT(CASE WHEN sale_type != 'none' THEN 1 END) as promotional_ads,
    
    -- Percentages
    ROUND(COUNT(CASE WHEN has_discount THEN 1 END) / COUNT(*) * 100, 1) as pct_discount_ads,
    ROUND(COUNT(CASE WHEN sale_type != 'none' THEN 1 END) / COUNT(*) * 100, 1) as pct_promotional_ads,
    
    -- Intensity scores
    ROUND(AVG(combined_promo_score), 3) as avg_promotional_intensity,
    ROUND(AVG(cta_aggressiveness), 3) as avg_cta_aggressiveness, 
    ROUND(AVG(urgency_score), 3) as avg_urgency_score,
    ROUND(AVG(COALESCE(discount_percentage, 0)), 1) as avg_discount_percentage,
    
    -- Sale type distribution
    COUNT(CASE WHEN sale_type = 'flash_sale' THEN 1 END) as flash_sale_count,
    COUNT(CASE WHEN sale_type = 'seasonal' THEN 1 END) as seasonal_sale_count,
    COUNT(CASE WHEN sale_type = 'clearance' THEN 1 END) as clearance_sale_count,
    COUNT(CASE WHEN sale_type = 'regular_promo' THEN 1 END) as regular_promo_count,
    
    -- Top promotional themes
    ARRAY_AGG(DISTINCT promo_keyword IGNORE NULLS ORDER BY promo_keyword LIMIT 5) as daily_promo_themes
    
  FROM promotional_signals,
  UNNEST(promotional_keywords) as promo_keyword
  GROUP BY brand, ad_date, week_of_year, month_of_year, day_of_week
)

SELECT 
  *,
  
  -- Week-over-week promotional intensity change
  avg_promotional_intensity - LAG(avg_promotional_intensity, 7) OVER (
    PARTITION BY brand ORDER BY ad_date
  ) as promo_intensity_change_7d,
  
  -- Promotional calendar classification
  CASE 
    WHEN avg_promotional_intensity >= 0.8 AND pct_discount_ads >= 50 THEN 'Heavy Promotional Period'
    WHEN avg_promotional_intensity >= 0.6 OR pct_promotional_ads >= 30 THEN 'Moderate Promotional Period'
    WHEN flash_sale_count > 0 OR avg_cta_aggressiveness >= 0.8 THEN 'Flash Sale/Urgency Period'
    WHEN seasonal_sale_count >= 2 THEN 'Seasonal Campaign Period'
    ELSE 'Regular Content Period'
  END as promotional_period_type,
  
  -- Strategic insights
  CASE 
    WHEN day_of_week = 1 AND avg_promotional_intensity >= 0.7 THEN 'Monday Launch Strategy'
    WHEN day_of_week IN (6,7) AND flash_sale_count > 0 THEN 'Weekend Flash Sale Strategy'
    WHEN pct_discount_ads >= 80 THEN 'Discount-Heavy Campaign'
    WHEN avg_cta_aggressiveness >= 0.8 AND pct_discount_ads < 20 THEN 'Urgency Without Discounts'
    ELSE 'Standard Strategy'
  END as promotional_strategy_type

FROM daily_promotional_intensity
WHERE ad_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAYS)  -- Last 3 months
ORDER BY brand, ad_date DESC;


-- Step 6: Platform Strategy & Brand Voice Consistency Analysis  
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_brand_voice_consistency` AS

WITH voice_analysis AS (
  SELECT 
    brand,
    ad_archive_id,
    creative_text,
    title,
    
    -- Analyze brand voice consistency using ML.GENERATE_TEXT
    (ML.GENERATE_TEXT(
      MODEL `bigquery-ai-kaggle-469620.ads_demo.gemini_model`,
      PROMPT => CONCAT(
        'Analyze the brand voice and messaging consistency of this ad:\n\n',
        'Headline: ', COALESCE(title, 'None'), '\n',
        'Body: ', COALESCE(creative_text, 'None'), '\n\n',
        
        'Extract brand voice characteristics in JSON:\n',
        '1. tone: "professional", "casual", "playful", "authoritative", "empathetic"\n',
        '2. formality_level: 0.0-1.0 (0=very casual, 1=very formal)\n',
        '3. brand_keywords: array of brand-specific terms/phrases\n',
        '4. messaging_pillars: array of key message themes (quality, innovation, value, etc.)\n',
        '5. voice_confidence: 0.0-1.0 how confident this represents brand voice\n',
        '6. positioning_signals: array of competitive positioning indicators\n\n',
        
        'Return only valid JSON:'
      ),
      GENERATION_CONFIG => JSON_OBJECT(
        "temperature", 0.1, 
        "maxOutputTokens", 600,
        "responseMimeType", "application/json",
        "responseSchema", JSON_OBJECT(
          "type", "object",
          "properties", JSON_OBJECT(
            "tone", JSON_OBJECT("type", "string", "enum", JSON_ARRAY("professional", "casual", "playful", "authoritative", "empathetic")),
            "formality_level", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1),
            "brand_keywords", JSON_OBJECT("type", "array", "items", JSON_OBJECT("type", "string")),
            "messaging_pillars", JSON_OBJECT("type", "array", "items", JSON_OBJECT("type", "string")),
            "voice_confidence", JSON_OBJECT("type", "number", "minimum", 0, "maximum", 1),
            "positioning_signals", JSON_OBJECT("type", "array", "items", JSON_OBJECT("type", "string"))
          )
        )
      )
    )) AS voice_analysis_json,
    
    funnel,
    persona,
    brand_voice_score,
    classified_at
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels`
  WHERE creative_text IS NOT NULL OR title IS NOT NULL
),

brand_voice_metrics AS (
  SELECT 
    *,
    
    -- Extract voice characteristics
    JSON_VALUE(voice_analysis_json, '$.tone') as detected_tone,
    CAST(JSON_VALUE(voice_analysis_json, '$.formality_level') AS FLOAT64) as formality_level,
    JSON_EXTRACT_ARRAY(voice_analysis_json, '$.brand_keywords') as brand_keywords,
    JSON_EXTRACT_ARRAY(voice_analysis_json, '$.messaging_pillars') as messaging_pillars,
    CAST(JSON_VALUE(voice_analysis_json, '$.voice_confidence') AS FLOAT64) as voice_confidence,
    JSON_EXTRACT_ARRAY(voice_analysis_json, '$.positioning_signals') as positioning_signals,
    
    -- Time grouping
    DATE(classified_at) as ad_date,
    DATE_TRUNC(classified_at, WEEK) as week_start,
    DATE_TRUNC(classified_at, MONTH) as month_start
    
  FROM voice_analysis
),

brand_consistency_analysis AS (
  SELECT 
    brand,
    week_start,
    month_start,
    
    -- Voice consistency metrics
    COUNT(*) as total_ads,
    COUNT(DISTINCT detected_tone) as tone_variation,
    STDDEV(formality_level) as formality_consistency,
    STDDEV(brand_voice_score) as voice_score_consistency,
    AVG(voice_confidence) as avg_voice_confidence,
    
    -- Dominant characteristics
    APPROX_TOP_COUNT(detected_tone, 1)[OFFSET(0)].value as dominant_tone,
    ROUND(AVG(formality_level), 3) as avg_formality_level,
    ROUND(AVG(brand_voice_score), 3) as avg_brand_voice_score,
    
    -- Messaging pillar analysis
    ARRAY_AGG(DISTINCT pillar IGNORE NULLS ORDER BY pillar LIMIT 10) as week_messaging_pillars,
    ARRAY_AGG(DISTINCT keyword IGNORE NULLS ORDER BY keyword LIMIT 15) as week_brand_keywords,
    
    -- Consistency scoring
    CASE 
      WHEN COUNT(DISTINCT detected_tone) = 1 AND STDDEV(formality_level) < 0.2 THEN 'Highly Consistent'
      WHEN COUNT(DISTINCT detected_tone) <= 2 AND STDDEV(formality_level) < 0.3 THEN 'Mostly Consistent'  
      WHEN COUNT(DISTINCT detected_tone) <= 3 AND STDDEV(formality_level) < 0.4 THEN 'Moderately Consistent'
      ELSE 'Inconsistent Voice'
    END as voice_consistency_level
    
  FROM brand_voice_metrics,
  UNNEST(messaging_pillars) as pillar,
  UNNEST(brand_keywords) as keyword
  GROUP BY brand, week_start, month_start
)

SELECT 
  *,
  
  -- Week-over-week voice drift detection
  ABS(avg_formality_level - LAG(avg_formality_level) OVER (
    PARTITION BY brand ORDER BY week_start
  )) as formality_drift_weekly,
  
  -- Voice evolution indicators  
  CASE 
    WHEN ABS(avg_formality_level - LAG(avg_formality_level) OVER (PARTITION BY brand ORDER BY week_start)) >= 0.3 
      THEN 'Significant Voice Shift'
    WHEN dominant_tone != LAG(dominant_tone) OVER (PARTITION BY brand ORDER BY week_start)
      THEN 'Tone Change Detected'
    WHEN voice_consistency_level != LAG(voice_consistency_level) OVER (PARTITION BY brand ORDER BY week_start)
      THEN 'Consistency Change'
    ELSE 'Stable Voice'
  END as voice_evolution_type,
  
  -- Strategic recommendations
  CASE 
    WHEN voice_consistency_level = 'Inconsistent Voice' THEN 'Review brand guidelines and creative approval process'
    WHEN formality_drift_weekly >= 0.4 THEN 'Investigate intentional voice pivot or creative drift'
    WHEN avg_voice_confidence < 0.6 THEN 'Strengthen brand voice in creative execution'
    ELSE 'Voice management performing well'
  END as voice_management_recommendation

FROM brand_consistency_analysis
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)  -- Last 3 months
ORDER BY brand, week_start DESC;


-- Step 7: AI.FORECAST Models for Strategic Trend Prediction
-- Create time-series forecasting for key strategic metrics

-- Forecast Model 1: Funnel Strategy Evolution
CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.funnel_strategy_forecast`
OPTIONS(
  MODEL_TYPE='ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL='week_start',
  TIME_SERIES_DATA_COL='pct_upper_funnel',
  TIME_SERIES_ID_COL='brand',
  HOLIDAY_REGION='US'
) AS
SELECT 
  brand,
  week_start,
  pct_upper_funnel,
  pct_mid_funnel, 
  pct_lower_funnel
FROM `bigquery-ai-kaggle-469620.ads_demo.v_strategy_evolution`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 20 WEEK)  -- Need sufficient history
  AND total_ads >= 3;  -- Minimum ad volume for reliable signal


-- Forecast Model 2: Promotional Intensity Prediction  
CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.promotional_intensity_forecast`
OPTIONS(
  MODEL_TYPE='ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL='week_start',
  TIME_SERIES_DATA_COL='avg_promotional_intensity',
  TIME_SERIES_ID_COL='brand',
  HOLIDAY_REGION='US',
  AUTO_ARIMA_MAX_ORDER=5
) AS
WITH weekly_promo_data AS (
  SELECT 
    brand,
    DATE_TRUNC(ad_date, WEEK) as week_start,
    AVG(avg_promotional_intensity) as avg_promotional_intensity,
    AVG(pct_discount_ads) as avg_pct_discount_ads,
    AVG(avg_cta_aggressiveness) as avg_cta_aggressiveness
  FROM `bigquery-ai-kaggle-469620.ads_demo.v_promotional_calendar`
  GROUP BY brand, week_start
  HAVING COUNT(*) >= 2  -- Minimum data points per week
)
SELECT * FROM weekly_promo_data
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 20 WEEK);


-- Forecast Model 3: Brand Voice Consistency Trends
CREATE OR REPLACE MODEL `bigquery-ai-kaggle-469620.ads_demo.brand_voice_forecast`  
OPTIONS(
  MODEL_TYPE='ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL='week_start',
  TIME_SERIES_DATA_COL='avg_formality_level',
  TIME_SERIES_ID_COL='brand'
) AS
SELECT 
  brand,
  week_start,
  avg_formality_level,
  avg_voice_confidence
FROM `bigquery-ai-kaggle-469620.ads_demo.v_brand_voice_consistency`
WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 WEEK)  -- Sufficient history
  AND total_ads >= 2;


-- Strategic Forecast Dashboard View
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_strategic_forecasts` AS

WITH funnel_forecasts AS (
  SELECT 
    brand,
    forecast_timestamp as forecast_week,
    forecast_value as predicted_upper_funnel_pct,
    confidence_level,
    confidence_interval_lower_bound as upper_funnel_lower_bound,
    confidence_interval_upper_bound as upper_funnel_upper_bound,
    'Funnel Strategy' as forecast_type
  FROM ML.FORECAST(MODEL `bigquery-ai-kaggle-469620.ads_demo.funnel_strategy_forecast`,
                   STRUCT(4 as horizon, 0.8 as confidence_level))
),

promotional_forecasts AS (
  SELECT 
    brand,
    forecast_timestamp as forecast_week,
    forecast_value as predicted_promotional_intensity,
    confidence_level,
    confidence_interval_lower_bound as promo_intensity_lower_bound,
    confidence_interval_upper_bound as promo_intensity_upper_bound,
    'Promotional Intensity' as forecast_type
  FROM ML.FORECAST(MODEL `bigquery-ai-kaggle-469620.ads_demo.promotional_intensity_forecast`,
                   STRUCT(4 as horizon, 0.8 as confidence_level))
),

voice_forecasts AS (
  SELECT 
    brand,
    forecast_timestamp as forecast_week,
    forecast_value as predicted_formality_level,
    confidence_level,
    confidence_interval_lower_bound as formality_lower_bound,
    confidence_interval_upper_bound as formality_upper_bound,
    'Brand Voice' as forecast_type
  FROM ML.FORECAST(MODEL `bigquery-ai-kaggle-469620.ads_demo.brand_voice_forecast`,
                   STRUCT(4 as horizon, 0.8 as confidence_level))
)

-- Unified forecast view
SELECT 
  brand,
  forecast_week,
  forecast_type,
  
  -- Generic forecast fields
  CASE 
    WHEN forecast_type = 'Funnel Strategy' THEN predicted_upper_funnel_pct
    WHEN forecast_type = 'Promotional Intensity' THEN predicted_promotional_intensity  
    WHEN forecast_type = 'Brand Voice' THEN predicted_formality_level
  END as forecast_value,
  
  confidence_level,
  
  CASE 
    WHEN forecast_type = 'Funnel Strategy' THEN upper_funnel_lower_bound
    WHEN forecast_type = 'Promotional Intensity' THEN promo_intensity_lower_bound
    WHEN forecast_type = 'Brand Voice' THEN formality_lower_bound
  END as confidence_lower_bound,
  
  CASE 
    WHEN forecast_type = 'Funnel Strategy' THEN upper_funnel_upper_bound
    WHEN forecast_type = 'Promotional Intensity' THEN promo_intensity_upper_bound
    WHEN forecast_type = 'Brand Voice' THEN formality_upper_bound
  END as confidence_upper_bound,
  
  -- Strategic insights
  CASE 
    WHEN forecast_type = 'Funnel Strategy' AND predicted_upper_funnel_pct >= 60 THEN 'Brand Awareness Focus Predicted'
    WHEN forecast_type = 'Funnel Strategy' AND predicted_upper_funnel_pct <= 20 THEN 'Conversion Focus Predicted'
    WHEN forecast_type = 'Promotional Intensity' AND predicted_promotional_intensity >= 0.8 THEN 'Heavy Promotional Period Ahead'
    WHEN forecast_type = 'Promotional Intensity' AND predicted_promotional_intensity <= 0.3 THEN 'Brand Content Period Ahead'
    WHEN forecast_type = 'Brand Voice' AND predicted_formality_level >= 0.8 THEN 'Formal Voice Trend'
    WHEN forecast_type = 'Brand Voice' AND predicted_formality_level <= 0.3 THEN 'Casual Voice Trend'
    ELSE 'Stable Trend Predicted'
  END as strategic_prediction
  
FROM (
  SELECT * FROM funnel_forecasts
  UNION ALL
  SELECT * FROM promotional_forecasts  
  UNION ALL
  SELECT * FROM voice_forecasts
)
ORDER BY brand, forecast_week, forecast_type;


-- Competitive Intelligence Summary View
-- Answers the 4 core strategic questions
CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.v_competitive_intelligence_summary` AS

WITH recent_focus AS (
  -- Question 1: "What have my Facebook ads focused on in past N weeks?"
  SELECT 
    brand,
    'Last 4 Weeks' as time_period,
    COUNT(*) as total_ads,
    
    -- Funnel focus
    ROUND(COUNT(CASE WHEN funnel = 'Upper' THEN 1 END) / COUNT(*) * 100, 1) as pct_upper_funnel,
    ROUND(COUNT(CASE WHEN funnel = 'Mid' THEN 1 END) / COUNT(*) * 100, 1) as pct_mid_funnel,
    ROUND(COUNT(CASE WHEN funnel = 'Lower' THEN 1 END) / COUNT(*) * 100, 1) as pct_lower_funnel,
    
    -- Strategic focus
    APPROX_TOP_COUNT(persona, 3) as top_personas,
    ARRAY_AGG(DISTINCT topic IGNORE NULLS ORDER BY topic LIMIT 8) as focus_topics,
    ROUND(AVG(urgency_score), 3) as avg_urgency,
    ROUND(AVG(promotional_intensity), 3) as avg_promotional_intensity,
    
    -- Dominant strategy
    CASE 
      WHEN COUNT(CASE WHEN funnel = 'Upper' THEN 1 END) / COUNT(*) >= 0.6 THEN 'Brand Awareness Focused'
      WHEN COUNT(CASE WHEN funnel = 'Lower' THEN 1 END) / COUNT(*) >= 0.6 THEN 'Conversion Focused'  
      WHEN AVG(promotional_intensity) >= 0.7 THEN 'Promotion Heavy'
      ELSE 'Balanced Strategy'
    END as dominant_strategy
    
  FROM `bigquery-ai-kaggle-469620.ads_demo.ads_strategic_labels`,
  UNNEST(topics) as topic
  WHERE classified_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
  GROUP BY brand
),

strategy_evolution AS (
  -- Question 3: "How have strategies evolved over 3 months?"
  SELECT 
    brand,
    
    -- Compare first month vs latest month
    AVG(CASE WHEN month_rank = 1 THEN pct_upper_funnel END) as month1_upper_funnel,
    AVG(CASE WHEN month_rank = 3 THEN pct_upper_funnel END) as month3_upper_funnel,
    AVG(CASE WHEN month_rank = 3 THEN pct_upper_funnel END) - AVG(CASE WHEN month_rank = 1 THEN pct_upper_funnel END) as upper_funnel_evolution,
    
    AVG(CASE WHEN month_rank = 1 THEN avg_promotional_intensity END) as month1_promo_intensity,
    AVG(CASE WHEN month_rank = 3 THEN avg_promotional_intensity END) as month3_promo_intensity,
    AVG(CASE WHEN month_rank = 3 THEN avg_promotional_intensity END) - AVG(CASE WHEN month_rank = 1 THEN avg_promotional_intensity END) as promo_evolution,
    
    -- Evolution classification
    CASE 
      WHEN AVG(CASE WHEN month_rank = 3 THEN pct_upper_funnel END) - AVG(CASE WHEN month_rank = 1 THEN pct_upper_funnel END) >= 20 THEN 'Shifted to Brand Focus'
      WHEN AVG(CASE WHEN month_rank = 3 THEN pct_lower_funnel END) - AVG(CASE WHEN month_rank = 1 THEN pct_lower_funnel END) >= 20 THEN 'Shifted to Conversion Focus'
      WHEN ABS(AVG(CASE WHEN month_rank = 3 THEN avg_promotional_intensity END) - AVG(CASE WHEN month_rank = 1 THEN avg_promotional_intensity END)) >= 0.3 THEN 'Major Promotional Strategy Change'
      ELSE 'Stable Strategy Evolution'
    END as evolution_type
    
  FROM (
    SELECT 
      brand,
      month_start,
      pct_upper_funnel,
      pct_mid_funnel,
      pct_lower_funnel,
      avg_promotional_intensity,
      ROW_NUMBER() OVER (PARTITION BY brand ORDER BY month_start) as month_rank
    FROM `bigquery-ai-kaggle-469620.ads_demo.v_strategy_evolution`
    WHERE month_start >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
  )
  WHERE month_rank IN (1, 3)  -- Compare first and latest months
  GROUP BY brand
)

-- Question 2: Competitor comparison is implicit in having multiple brands
-- Question 4: Future trends from forecast view
SELECT 
  rf.brand,
  rf.time_period,
  rf.total_ads,
  rf.dominant_strategy,
  rf.pct_upper_funnel,
  rf.pct_mid_funnel, 
  rf.pct_lower_funnel,
  rf.focus_topics,
  rf.avg_urgency,
  rf.avg_promotional_intensity,
  
  -- Strategy evolution insights
  se.evolution_type,
  ROUND(se.upper_funnel_evolution, 1) as upper_funnel_change_3mo,
  ROUND(se.promo_evolution, 3) as promotional_intensity_change_3mo,
  
  -- Competitive positioning
  CASE 
    WHEN rf.pct_upper_funnel >= 60 THEN 'Brand Builder'
    WHEN rf.pct_lower_funnel >= 60 THEN 'Performance Driven'
    WHEN rf.avg_promotional_intensity >= 0.7 THEN 'Promotion Focused'
    WHEN rf.avg_urgency >= 0.6 THEN 'Urgency Driven'
    ELSE 'Balanced Approach'
  END as competitive_archetype

FROM recent_focus rf
LEFT JOIN strategy_evolution se ON rf.brand = se.brand
ORDER BY rf.brand;