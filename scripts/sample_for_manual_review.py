#!/usr/bin/env python3
"""
Generate sample ads for manual strategic classification review
MODERATE Test #1: Manual review confirms strategic relevance
"""

import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def sample_ads_for_review():
    """Get a diverse sample of classified ads for manual review"""
    
    query = f"""
    SELECT 
      brand,
      ad_id,
      creative_text,
      title,
      media_type,
      
      -- Our AI classifications to validate
      -- (These would come from our strategic labels table when implemented)
      CASE 
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP%NOW%' 
             OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%BUY%NOW%'
        THEN 'Lower'
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%DISCOVER%'
             OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%EXPLORE%'
        THEN 'Upper' 
        ELSE 'Mid'
      END AS predicted_funnel,
      
      CASE 
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%NEW%'
             OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%INTRODUCE%'
        THEN 'New Customer'
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%COLLECTION%'
             OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%LATEST%'
        THEN 'General Market'
        ELSE 'Existing Customer'
      END AS predicted_persona,
      
      -- CTA aggressiveness (simplified version)
      CASE 
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%LIMITED%TIME%'
             OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%HURRY%'
             OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%NOW%'
        THEN 0.8
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SALE%'
             OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%OFF%'
        THEN 0.6
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP%'
             OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%BUY%'
        THEN 0.4
        ELSE 0.2
      END AS predicted_aggressiveness_score,
      
      -- Platform strategy
      publisher_platforms,
      CASE 
        WHEN publisher_platforms LIKE '%FACEBOOK%' AND publisher_platforms LIKE '%INSTAGRAM%'
        THEN 'Cross-Platform'
        WHEN publisher_platforms LIKE '%INSTAGRAM%'
        THEN 'Instagram-Only'  
        WHEN publisher_platforms LIKE '%FACEBOOK%'
        THEN 'Facebook-Only'
        ELSE 'Other'
      END AS platform_strategy,
      
      -- Duration context
      active_days,
      influence_tier,
      
      -- Add review columns
      '' AS manual_funnel_review,
      '' AS manual_persona_review, 
      '' AS manual_aggressiveness_review,
      '' AS manual_platform_review,
      '' AS overall_classification_quality,
      '' AS notes
      
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    WHERE creative_text IS NOT NULL 
      AND LENGTH(creative_text) >= 20  -- Substantial content for review
    ORDER BY RAND()  -- Random sampling
    LIMIT 25  -- Manageable sample for manual review
    """
    
    print("🔍 Generating sample ads for manual strategic classification review...")
    
    try:
        df = client.query(query).result().to_dataframe()
        
        print(f"📊 Sample generated: {len(df)} ads for review")
        print(f"   Brands: {df['brand'].unique()}")
        print(f"   Media types: {df['media_type'].value_counts().to_dict()}")
        print(f"   Platform strategies: {df['platform_strategy'].value_counts().to_dict()}")
        
        # Save to CSV for manual review
        output_file = "data/temp/manual_review_sample.csv"
        df.to_csv(output_file, index=False)
        
        print(f"✅ Saved to: {output_file}")
        print("\n📋 MANUAL REVIEW INSTRUCTIONS:")
        print("="*50)
        print("For each ad, please review and fill in:")
        print("1. manual_funnel_review: Upper/Mid/Lower (does our prediction make sense?)")
        print("2. manual_persona_review: New Customer/Existing Customer/General Market")
        print("3. manual_aggressiveness_review: 0.0-1.0 (how pushy/aggressive is the CTA?)")
        print("4. manual_platform_review: Good/Poor (is platform strategy appropriate?)")  
        print("5. overall_classification_quality: 1-5 (1=terrible, 5=excellent)")
        print("6. notes: Any observations or issues")
        
        return df
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def analyze_current_classifications():
    """Analyze what our current system is predicting"""
    
    query = f"""
    SELECT 
      'CURRENT_CLASSIFICATION_ANALYSIS' as analysis_type,
      
      -- Volume by predicted classifications
      COUNTIF(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP%NOW%' 
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%BUY%NOW%') as predicted_lower_funnel,
      COUNTIF(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%DISCOVER%'
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%EXPLORE%') as predicted_upper_funnel,
      
      -- Aggressiveness distribution
      AVG(CASE 
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%LIMITED%TIME%' THEN 0.8
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SALE%' THEN 0.6
        WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP%' THEN 0.4
        ELSE 0.2
      END) as avg_predicted_aggressiveness,
      
      -- Platform distribution
      COUNTIF(publisher_platforms LIKE '%FACEBOOK%' AND publisher_platforms LIKE '%INSTAGRAM%') as cross_platform_ads,
      COUNTIF(publisher_platforms LIKE '%INSTAGRAM%' AND NOT publisher_platforms LIKE '%FACEBOOK%') as instagram_only_ads,
      
      COUNT(*) as total_ads_analyzed
      
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    WHERE creative_text IS NOT NULL
    """
    
    print("\n🔍 Analyzing current classification patterns...")
    
    try:
        result = client.query(query).result()
        
        for row in result:
            print(f"📊 Current Classification Patterns:")
            print(f"   Total ads: {row.total_ads_analyzed}")
            print(f"   Predicted lower funnel: {row.predicted_lower_funnel}")
            print(f"   Predicted upper funnel: {row.predicted_upper_funnel}")
            print(f"   Avg aggressiveness: {row.avg_predicted_aggressiveness:.3f}")
            print(f"   Cross-platform ads: {row.cross_platform_ads}")
            print(f"   Instagram-only ads: {row.instagram_only_ads}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 MODERATE TEST #1: MANUAL STRATEGIC CLASSIFICATION REVIEW")
    print("="*60)
    
    # Generate sample for review
    sample_df = sample_ads_for_review()
    
    # Show current classification patterns
    analyze_current_classifications()
    
    if sample_df is not None:
        print(f"\n✅ SUCCESS: Generated {len(sample_df)} ads for manual review")
        print("📝 Next step: Manual review by growth marketing expert")
        print("🎯 Goal: Validate that our AI classifications align with business intuition")
    else:
        print("❌ Failed to generate sample")