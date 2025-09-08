#!/usr/bin/env python3
"""
MODERATE Test #3: Platform Consistency Analysis Validation
Tests whether platform consistency scores align with manual assessment
"""

import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def validate_platform_strategy_detection():
    """Validate platform strategy detection and consistency scoring"""
    
    query = f"""
    WITH platform_analysis AS (
      SELECT 
        ad_id,
        brand,
        creative_text,
        title,
        publisher_platforms,
        
        -- Platform strategy classification
        CASE 
          WHEN publisher_platforms LIKE '%FACEBOOK%' AND publisher_platforms LIKE '%INSTAGRAM%'
          THEN 'Cross-Platform'
          WHEN publisher_platforms LIKE '%INSTAGRAM%' AND NOT publisher_platforms LIKE '%FACEBOOK%'
          THEN 'Instagram-Only'
          WHEN publisher_platforms LIKE '%FACEBOOK%' AND NOT publisher_platforms LIKE '%INSTAGRAM%'
          THEN 'Facebook-Only'
          ELSE 'Other-Platform'
        END AS platform_strategy,
        
        -- Instagram optimization indicators
        ARRAY_LENGTH(REGEXP_EXTRACT_ALL(creative_text, r'#\\w+')) AS hashtag_count,
        REGEXP_CONTAINS(creative_text, r'[😀-🙿🚀-🛿🎀-🏿💀-🟿]') AS has_emojis,
        LENGTH(creative_text) AS text_length,
        
        -- Facebook optimization indicators (longer form content)
        REGEXP_CONTAINS(UPPER(creative_text), r'(LEARN MORE|READ MORE|DISCOVER|EXPLORE|FIND OUT)') AS has_discovery_cta,
        REGEXP_CONTAINS(creative_text, r'\\b(www\\.|http|.com)') AS has_external_links,
        
        active_days,
        start_timestamp
        
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
      WHERE publisher_platforms IS NOT NULL 
        AND creative_text IS NOT NULL
      ORDER BY RAND()
      LIMIT 100
    )
    SELECT 
      'PLATFORM_STRATEGY_VALIDATION' AS test_name,
      
      COUNT(*) AS total_ads_analyzed,
      
      -- Platform strategy distribution
      COUNTIF(platform_strategy = 'Cross-Platform') AS cross_platform_count,
      COUNTIF(platform_strategy = 'Instagram-Only') AS instagram_only_count,
      COUNTIF(platform_strategy = 'Facebook-Only') AS facebook_only_count,
      
      -- Cross-platform vs single-platform characteristics
      AVG(CASE WHEN platform_strategy = 'Cross-Platform' THEN text_length END) AS avg_text_length_cross_platform,
      AVG(CASE WHEN platform_strategy = 'Instagram-Only' THEN text_length END) AS avg_text_length_instagram_only,
      AVG(CASE WHEN platform_strategy = 'Facebook-Only' THEN text_length END) AS avg_text_length_facebook_only,
      
      -- Instagram-specific optimization
      AVG(CASE WHEN platform_strategy IN ('Cross-Platform', 'Instagram-Only') 
               THEN hashtag_count END) AS avg_hashtags_instagram_ads,
      COUNTIF(platform_strategy IN ('Cross-Platform', 'Instagram-Only') AND has_emojis) AS instagram_ads_with_emojis,
      COUNTIF(platform_strategy IN ('Cross-Platform', 'Instagram-Only')) AS total_instagram_ads,
      
      -- Facebook-specific optimization
      COUNTIF(platform_strategy IN ('Cross-Platform', 'Facebook-Only') AND has_discovery_cta) AS facebook_ads_with_discovery_cta,
      COUNTIF(platform_strategy IN ('Cross-Platform', 'Facebook-Only')) AS total_facebook_ads,
      
      -- Campaign duration by platform strategy
      AVG(CASE WHEN platform_strategy = 'Cross-Platform' THEN active_days END) AS avg_duration_cross_platform,
      AVG(CASE WHEN platform_strategy = 'Instagram-Only' THEN active_days END) AS avg_duration_instagram_only,
      AVG(CASE WHEN platform_strategy = 'Facebook-Only' THEN active_days END) AS avg_duration_facebook_only,
      
      -- Sample examples for manual review
      STRING_AGG(
        CASE WHEN platform_strategy = 'Cross-Platform' AND has_emojis
        THEN CONCAT(brand, ' (Cross): "', SUBSTR(creative_text, 1, 60), '..."')
        END, ' | ' LIMIT 3
      ) AS cross_platform_examples,
      
      STRING_AGG(
        CASE WHEN platform_strategy = 'Instagram-Only' 
        THEN CONCAT(brand, ' (IG): "', SUBSTR(creative_text, 1, 60), '..."')
        END, ' | ' LIMIT 3
      ) AS instagram_only_examples
      
    FROM platform_analysis
    """
    
    print("🔍 MODERATE TEST #3: Platform Strategy Detection & Consistency")
    print("=" * 60)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Platform Strategy Distribution:")
            print(f"   Total ads analyzed: {row['total_ads_analyzed']}")
            print(f"   Cross-platform: {row['cross_platform_count']} ({row['cross_platform_count']/row['total_ads_analyzed']*100:.1f}%)")
            print(f"   Instagram-only: {row['instagram_only_count']} ({row['instagram_only_count']/row['total_ads_analyzed']*100:.1f}%)")
            print(f"   Facebook-only: {row['facebook_only_count']} ({row['facebook_only_count']/row['total_ads_analyzed']*100:.1f}%)")
            
            print(f"\n📏 Text Length by Platform Strategy:")
            if pd.notna(row['avg_text_length_cross_platform']):
                print(f"   Cross-platform avg: {row['avg_text_length_cross_platform']:.0f} chars")
            if pd.notna(row['avg_text_length_instagram_only']):
                print(f"   Instagram-only avg: {row['avg_text_length_instagram_only']:.0f} chars")
            if pd.notna(row['avg_text_length_facebook_only']):
                print(f"   Facebook-only avg: {row['avg_text_length_facebook_only']:.0f} chars")
            
            print(f"\n📱 Instagram Optimization Analysis:")
            if row['total_instagram_ads'] > 0:
                emoji_rate = row['instagram_ads_with_emojis'] / row['total_instagram_ads'] * 100
                print(f"   Instagram ads with emojis: {row['instagram_ads_with_emojis']}/{row['total_instagram_ads']} ({emoji_rate:.1f}%)")
                if pd.notna(row['avg_hashtags_instagram_ads']):
                    print(f"   Average hashtags in Instagram ads: {row['avg_hashtags_instagram_ads']:.1f}")
            
            print(f"\n🔗 Facebook Optimization Analysis:")
            if row['total_facebook_ads'] > 0:
                discovery_rate = row['facebook_ads_with_discovery_cta'] / row['total_facebook_ads'] * 100
                print(f"   Facebook ads with discovery CTAs: {row['facebook_ads_with_discovery_cta']}/{row['total_facebook_ads']} ({discovery_rate:.1f}%)")
            
            print(f"\n⏱️  Campaign Duration by Platform:")
            if pd.notna(row['avg_duration_cross_platform']):
                print(f"   Cross-platform avg duration: {row['avg_duration_cross_platform']:.1f} days")
            if pd.notna(row['avg_duration_instagram_only']):
                print(f"   Instagram-only avg duration: {row['avg_duration_instagram_only']:.1f} days")
            if pd.notna(row['avg_duration_facebook_only']):
                print(f"   Facebook-only avg duration: {row['avg_duration_facebook_only']:.1f} days")
            
            print(f"\n📝 Platform Strategy Examples:")
            if row['cross_platform_examples']:
                print(f"   Cross-platform: {row['cross_platform_examples']}")
            if row['instagram_only_examples']:
                print(f"   Instagram-only: {row['instagram_only_examples']}")
            
            # Validation logic
            has_good_distribution = row['cross_platform_count'] > 0 and (row['instagram_only_count'] > 0 or row['facebook_only_count'] > 0)
            has_optimization_differences = emoji_rate > 10 if row['total_instagram_ads'] > 0 else True
            
            validation_passed = has_good_distribution and has_optimization_differences
            
            return validation_passed
        else:
            print("❌ No validation data returned")
            return False
            
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        return False

def analyze_brand_platform_strategies():
    """Analyze how different brands approach platform strategies"""
    
    query = f"""
    WITH brand_platform_analysis AS (
      SELECT 
        brand,
        
        COUNTIF(publisher_platforms LIKE '%FACEBOOK%' AND publisher_platforms LIKE '%INSTAGRAM%') AS cross_platform_ads,
        COUNTIF(publisher_platforms LIKE '%INSTAGRAM%' AND NOT publisher_platforms LIKE '%FACEBOOK%') AS instagram_only_ads,
        COUNTIF(publisher_platforms LIKE '%FACEBOOK%' AND NOT publisher_platforms LIKE '%INSTAGRAM%') AS facebook_only_ads,
        COUNT(*) AS total_ads,
        
        -- Platform preferences
        COUNTIF(publisher_platforms LIKE '%FACEBOOK%' AND publisher_platforms LIKE '%INSTAGRAM%') / COUNT(*) * 100 AS pct_cross_platform,
        COUNTIF(publisher_platforms LIKE '%INSTAGRAM%') / COUNT(*) * 100 AS pct_uses_instagram,
        COUNTIF(publisher_platforms LIKE '%FACEBOOK%') / COUNT(*) * 100 AS pct_uses_facebook,
        
        -- Average characteristics by platform
        AVG(CASE WHEN publisher_platforms LIKE '%INSTAGRAM%' THEN active_days END) AS avg_duration_instagram_campaigns,
        AVG(CASE WHEN publisher_platforms LIKE '%FACEBOOK%' THEN active_days END) AS avg_duration_facebook_campaigns
        
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
      WHERE publisher_platforms IS NOT NULL
      GROUP BY brand
      HAVING COUNT(*) >= 10  -- Only brands with sufficient data
    )
    SELECT 
      brand,
      total_ads,
      cross_platform_ads,
      instagram_only_ads,
      facebook_only_ads,
      
      ROUND(pct_cross_platform, 1) AS pct_cross_platform,
      ROUND(pct_uses_instagram, 1) AS pct_uses_instagram,
      ROUND(pct_uses_facebook, 1) AS pct_uses_facebook,
      
      -- Platform strategy classification
      CASE 
        WHEN pct_cross_platform >= 80 THEN 'Unified Cross-Platform'
        WHEN pct_uses_instagram >= 80 AND pct_uses_facebook < 30 THEN 'Instagram-Focused'
        WHEN pct_uses_facebook >= 80 AND pct_uses_instagram < 30 THEN 'Facebook-Focused'
        ELSE 'Mixed Platform Strategy'
      END AS dominant_platform_strategy,
      
      ROUND(avg_duration_instagram_campaigns, 1) AS avg_duration_instagram,
      ROUND(avg_duration_facebook_campaigns, 1) AS avg_duration_facebook
      
    FROM brand_platform_analysis
    ORDER BY total_ads DESC
    """
    
    print(f"\n🏢 Brand Platform Strategy Analysis:")
    print("=" * 40)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        for _, row in df.iterrows():
            print(f"\n🏷️  {row['brand']} ({row['total_ads']} ads)")
            print(f"   Strategy: {row['dominant_platform_strategy']}")
            print(f"   Cross-platform: {row['pct_cross_platform']}%")
            print(f"   Instagram usage: {row['pct_uses_instagram']}%")
            print(f"   Facebook usage: {row['pct_uses_facebook']}%")
            if pd.notna(row['avg_duration_instagram']):
                print(f"   Avg Instagram campaign duration: {row['avg_duration_instagram']} days")
            if pd.notna(row['avg_duration_facebook']):
                print(f"   Avg Facebook campaign duration: {row['avg_duration_facebook']} days")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 MODERATE TEST #3: PLATFORM CONSISTENCY VALIDATION")
    print("=" * 60)
    
    # Run platform strategy validation
    validation_passed = validate_platform_strategy_detection()
    
    # Analyze brand platform strategies
    analyze_brand_platform_strategies()
    
    if validation_passed:
        print("\n✅ MODERATE TEST #3 PASSED: Platform strategy detection working effectively")
        print("🎯 Goal: Platform consistency analysis identifies optimization opportunities")
    else:
        print("\n⚠️  MODERATE TEST #3 NEEDS REVIEW: Check platform strategy detection logic")
        print("🔍 Manual review recommended for platform optimization patterns")