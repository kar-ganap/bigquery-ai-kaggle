-- Create a diverse test dataset to properly evaluate basic vs enhanced embeddings
-- This includes ads with similar content but different categories/CTAs to test enhancement value

-- First, create basic embeddings with simple concatenation
CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.test_basic_embeddings` AS

WITH diverse_ad_data AS (
  -- Similar content, same category (should have HIGH similarity in both approaches)
  SELECT 'Chase' as brand, 'chase_001' as ad_archive_id,
    'Banking & Credit' as page_category,
    'Earn 5% cash back' as title,
    'Get 5% cash back on travel and dining with the Chase Sapphire card. No annual fee for the first year.' as body_text,
    'Apply Now' as cta_text
  UNION ALL
  SELECT 'Chase', 'chase_002',
    'Banking & Credit',
    'Get 5% rewards',
    'Earn 5% rewards on travel and restaurants with Chase Sapphire. First year annual fee waived.',
    'Apply Today'
    
  -- Similar content, different category (should have MEDIUM similarity with enhanced, HIGH with basic)
  UNION ALL
  SELECT 'Capital One', 'cap_001',
    'Banking & Credit',  -- Financial category
    'Travel more, worry less',
    'Book your dream vacation with no foreign transaction fees. Earn miles on every purchase.',
    'Learn More'
  UNION ALL
  SELECT 'Expedia', 'exp_001', 
    'Travel & Tourism',  -- Travel category
    'Travel more, save more',
    'Book your perfect vacation with exclusive member discounts. Earn rewards on every booking.',
    'Book Now'
    
  -- Same CTA/category, different content (enhanced should differentiate better)
  UNION ALL
  SELECT 'Nike', 'nike_001',
    'Retail & Shopping',
    'Just Do It',
    'New Air Max collection available. Experience ultimate comfort and style.',
    'Shop Now'
  UNION ALL  
  SELECT 'Adidas', 'adidas_001',
    'Retail & Shopping', 
    'Impossible is Nothing',
    'Latest Ultraboost technology. Run faster, go further.',
    'Shop Now'
    
  -- Completely different (should have LOW similarity in both)
  UNION ALL
  SELECT 'Netflix', 'netflix_001',
    'Entertainment',
    'Stream unlimited',
    'Watch thousands of movies and shows. First month free for new members.',
    'Start Watching'
  UNION ALL
  SELECT 'Geico', 'geico_001',
    'Insurance',
    'Save 15% or more',  
    'Switch to Geico and save hundreds on car insurance. Get a quote in 15 minutes.',
    'Get Quote'
    
  -- Cross-industry with similar messaging (test semantic understanding)
  UNION ALL
  SELECT 'Spotify', 'spotify_001',
    'Entertainment',
    'Your music, everywhere',
    'Get 3 months of Premium for free. No ads, unlimited skips, offline listening.',
    'Try Free'
  UNION ALL
  SELECT 'YouTube Premium', 'youtube_001',
    'Entertainment',
    'Ad-free viewing',
    'Enjoy YouTube without ads. Download videos for offline viewing. 1 month free trial.',
    'Start Trial'
)

-- Generate basic embeddings (title + body only)
SELECT *
FROM ML.GENERATE_EMBEDDING(
  MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
  (
    SELECT 
      brand,
      ad_archive_id,
      page_category,
      title,
      body_text,
      cta_text,
      CONCAT(
        COALESCE(title, ''), ' ',
        COALESCE(body_text, '')
      ) as content  -- Basic concatenation
    FROM diverse_ad_data
  ),
  STRUCT('SEMANTIC_SIMILARITY' as task_type)
);

-- Now create enhanced embeddings with structured content
CREATE OR REPLACE TABLE `bigquery-ai-kaggle-469620.ads_demo.test_enhanced_embeddings` AS

WITH diverse_ad_data AS (
  -- Same data as above
  SELECT 'Chase' as brand, 'chase_001' as ad_archive_id,
    'Banking & Credit' as page_category,
    'Earn 5% cash back' as title,
    'Get 5% cash back on travel and dining with the Chase Sapphire card. No annual fee for the first year.' as body_text,
    'Apply Now' as cta_text
  UNION ALL
  SELECT 'Chase', 'chase_002',
    'Banking & Credit',
    'Get 5% rewards',
    'Earn 5% rewards on travel and restaurants with Chase Sapphire. First year annual fee waived.',
    'Apply Today'
    
  UNION ALL
  SELECT 'Capital One', 'cap_001',
    'Banking & Credit',
    'Travel more, worry less',
    'Book your dream vacation with no foreign transaction fees. Earn miles on every purchase.',
    'Learn More'
  UNION ALL
  SELECT 'Expedia', 'exp_001',
    'Travel & Tourism',
    'Travel more, save more',
    'Book your perfect vacation with exclusive member discounts. Earn rewards on every booking.',
    'Book Now'
    
  UNION ALL
  SELECT 'Nike', 'nike_001',
    'Retail & Shopping',
    'Just Do It',
    'New Air Max collection available. Experience ultimate comfort and style.',
    'Shop Now'
  UNION ALL  
  SELECT 'Adidas', 'adidas_001',
    'Retail & Shopping',
    'Impossible is Nothing',
    'Latest Ultraboost technology. Run faster, go further.',
    'Shop Now'
    
  UNION ALL
  SELECT 'Netflix', 'netflix_001',
    'Entertainment',
    'Stream unlimited',
    'Watch thousands of movies and shows. First month free for new members.',
    'Start Watching'
  UNION ALL
  SELECT 'Geico', 'geico_001',
    'Insurance',
    'Save 15% or more',
    'Switch to Geico and save hundreds on car insurance. Get a quote in 15 minutes.',
    'Get Quote'
    
  UNION ALL
  SELECT 'Spotify', 'spotify_001',
    'Entertainment',
    'Your music, everywhere',
    'Get 3 months of Premium for free. No ads, unlimited skips, offline listening.',
    'Try Free'
  UNION ALL
  SELECT 'YouTube Premium', 'youtube_001',
    'Entertainment',
    'Ad-free viewing',
    'Enjoy YouTube without ads. Download videos for offline viewing. 1 month free trial.',
    'Start Trial'
)

-- Generate enhanced embeddings with structured content
SELECT *
FROM ML.GENERATE_EMBEDDING(
  MODEL `bigquery-ai-kaggle-469620.ads_demo.text_embedding_model`,
  (
    SELECT 
      brand,
      ad_archive_id,
      page_category,
      title,
      body_text,
      cta_text,
      -- Enhanced structured content
      CONCAT(
        'Brand: ', COALESCE(brand, ''),
        ' Category: ', COALESCE(page_category, ''),
        ' Title: ', COALESCE(title, ''),
        ' Content: ', COALESCE(body_text, ''),
        ' Action: ', COALESCE(cta_text, '')
      ) as content
    FROM diverse_ad_data
  ),
  STRUCT('SEMANTIC_SIMILARITY' as task_type)
);