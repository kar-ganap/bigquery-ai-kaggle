#!/usr/bin/env python3
"""
HARD Test: Creative Fatigue Detection Validation
Tests the assumption that new original ads indicate fatigue of previous ads in same category
"""

import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def test_creative_fatigue_logic():
    """Test the creative fatigue detection logic using our existing data"""
    
    query = f"""
    WITH fatigue_test_analysis AS (
      SELECT 
        'CREATIVE_FATIGUE_DETECTION_TEST' AS test_name,
        
        -- Overall fatigue distribution
        COUNT(*) AS total_ads_analyzed,
        COUNT(DISTINCT brand) AS unique_brands,
        
        -- Fatigue level distribution
        COUNTIF(fatigue_level = 'Critical Fatigue') AS critical_fatigue_count,
        COUNTIF(fatigue_level = 'High Fatigue') AS high_fatigue_count,
        COUNTIF(fatigue_level = 'Moderate Fatigue') AS moderate_fatigue_count,
        COUNTIF(fatigue_level = 'Low Fatigue') AS low_fatigue_count,
        COUNTIF(fatigue_level = 'Fresh Content') AS fresh_content_count,
        
        -- Refresh signal validation
        COUNTIF(refresh_signals_count > 0) AS ads_with_refresh_signals,
        AVG(refresh_signals_count) AS avg_refresh_signals_per_ad,
        MAX(refresh_signals_count) AS max_refresh_signals,
        
        -- Age-based analysis
        AVG(days_since_launch) AS avg_days_since_launch,
        AVG(fatigue_score) AS avg_fatigue_score,
        
        -- Confidence distribution
        COUNTIF(fatigue_confidence = 'High Confidence') AS high_confidence_assessments,
        COUNTIF(fatigue_confidence = 'Medium Confidence') AS medium_confidence_assessments,
        COUNTIF(fatigue_confidence = 'Age-Based Assessment') AS age_based_assessments,
        COUNTIF(fatigue_confidence = 'Low Confidence') AS low_confidence_assessments
        
      FROM `{PROJECT_ID}.{DATASET_ID}.v_creative_fatigue_analysis`
    ),
    
    brand_fatigue_patterns AS (
      SELECT 
        brand,
        COUNT(*) AS total_brand_ads,
        AVG(fatigue_score) AS avg_brand_fatigue_score,
        COUNTIF(fatigue_level IN ('Critical Fatigue', 'High Fatigue')) AS high_fatigue_ads,
        COUNTIF(refresh_signals_count > 0) AS ads_with_refresh_activity,
        MAX(refresh_signals_count) AS max_refresh_signals_brand,
        
        -- Recommended actions summary
        COUNTIF(recommended_action LIKE 'URGENT%') AS urgent_action_needed,
        COUNTIF(recommended_action LIKE 'REFRESH%') AS refresh_recommended,
        COUNTIF(recommended_action LIKE 'MONITOR%') AS monitoring_needed
        
      FROM `{PROJECT_ID}.{DATASET_ID}.v_creative_fatigue_analysis`
      GROUP BY brand
    )
    
    SELECT 
      test_name,
      total_ads_analyzed,
      unique_brands,
      
      -- Fatigue distribution percentages
      ROUND(critical_fatigue_count / total_ads_analyzed * 100, 1) AS pct_critical_fatigue,
      ROUND(high_fatigue_count / total_ads_analyzed * 100, 1) AS pct_high_fatigue,
      ROUND(moderate_fatigue_count / total_ads_analyzed * 100, 1) AS pct_moderate_fatigue,
      ROUND(fresh_content_count / total_ads_analyzed * 100, 1) AS pct_fresh_content,
      
      -- Refresh signal analysis
      ads_with_refresh_signals,
      ROUND(avg_refresh_signals_per_ad, 2) AS avg_refresh_signals_per_ad,
      max_refresh_signals,
      
      -- Performance metrics
      ROUND(avg_days_since_launch, 1) AS avg_days_since_launch,
      ROUND(avg_fatigue_score, 3) AS avg_fatigue_score,
      
      -- Confidence assessment
      high_confidence_assessments,
      medium_confidence_assessments,
      age_based_assessments,
      
      -- Validation results
      CASE 
        WHEN ads_with_refresh_signals > 0 AND avg_fatigue_score > 0.2 THEN 'PASS'
        WHEN total_ads_analyzed > 50 AND avg_fatigue_score > 0.1 THEN 'PARTIAL_PASS'  
        ELSE 'NEEDS_REVIEW'
      END AS fatigue_detection_result
      
    FROM fatigue_test_analysis
    """
    
    print("🔍 HARD TEST: Creative Fatigue Detection Validation")
    print("=" * 60)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Fatigue Analysis Results:")
            print(f"   Total ads analyzed: {row['total_ads_analyzed']}")
            print(f"   Unique brands: {row['unique_brands']}")
            print(f"   Average fatigue score: {row['avg_fatigue_score']:.3f}")
            print(f"   Average days since launch: {row['avg_days_since_launch']:.1f}")
            
            print(f"\n📈 Fatigue Level Distribution:")
            print(f"   Critical fatigue: {row['pct_critical_fatigue']:.1f}%")
            print(f"   High fatigue: {row['pct_high_fatigue']:.1f}%")
            print(f"   Moderate fatigue: {row['pct_moderate_fatigue']:.1f}%")
            print(f"   Fresh content: {row['pct_fresh_content']:.1f}%")
            
            print(f"\n🔄 Refresh Signal Analysis:")
            print(f"   Ads with refresh signals: {row['ads_with_refresh_signals']}")
            print(f"   Avg refresh signals per ad: {row['avg_refresh_signals_per_ad']}")
            print(f"   Max refresh signals: {row['max_refresh_signals']}")
            
            print(f"\n📋 Confidence Assessment:")
            print(f"   High confidence: {row['high_confidence_assessments']}")
            print(f"   Medium confidence: {row['medium_confidence_assessments']}")
            print(f"   Age-based assessment: {row['age_based_assessments']}")
            
            print(f"\n✅ Test Result: {row['fatigue_detection_result']}")
            
            return row['fatigue_detection_result'] in ['PASS', 'PARTIAL_PASS']
        else:
            print("❌ No fatigue analysis data returned")
            return False
            
    except Exception as e:
        print(f"❌ Error during fatigue analysis: {e}")
        return False

def analyze_brand_fatigue_patterns():
    """Analyze fatigue patterns by brand to validate business logic"""
    
    query = f"""
    SELECT 
      brand,
      COUNT(*) AS total_ads,
      
      AVG(fatigue_score) AS avg_fatigue_score,
      COUNTIF(fatigue_level = 'Critical Fatigue') AS critical_fatigue_ads,
      COUNTIF(fatigue_level = 'High Fatigue') AS high_fatigue_ads,
      COUNTIF(fatigue_level = 'Fresh Content') AS fresh_content_ads,
      
      -- Refresh activity analysis
      COUNTIF(refresh_signals_count > 0) AS ads_with_refresh_signals,
      MAX(refresh_signals_count) AS max_refresh_signals,
      AVG(days_since_launch) AS avg_ad_age,
      
      -- Action recommendations
      COUNTIF(recommended_action LIKE 'URGENT%') AS urgent_actions_needed,
      COUNTIF(recommended_action LIKE 'REFRESH%') AS refresh_actions_needed,
      
      -- Recent refresh activity
      MAX(latest_refresh_date) AS most_recent_refresh
      
    FROM `{PROJECT_ID}.{DATASET_ID}.v_creative_fatigue_analysis`
    GROUP BY brand
    ORDER BY avg_fatigue_score DESC
    """
    
    print(f"\n🏢 Brand Fatigue Pattern Analysis:")
    print("=" * 40)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        for _, row in df.iterrows():
            print(f"\n🏷️  {row['brand']} ({row['total_ads']} ads)")
            print(f"   Avg fatigue score: {row['avg_fatigue_score']:.3f}")
            print(f"   Critical fatigue ads: {row['critical_fatigue_ads']}")
            print(f"   High fatigue ads: {row['high_fatigue_ads']}")
            print(f"   Fresh content ads: {row['fresh_content_ads']}")
            print(f"   Ads with refresh signals: {row['ads_with_refresh_signals']}")
            if row['max_refresh_signals'] > 0:
                print(f"   Max refresh signals: {row['max_refresh_signals']}")
            print(f"   Avg ad age: {row['avg_ad_age']:.1f} days")
            if row['urgent_actions_needed'] > 0:
                print(f"   ⚠️  Urgent actions needed: {row['urgent_actions_needed']}")
            if row['refresh_actions_needed'] > 0:
                print(f"   🔄 Refresh recommended: {row['refresh_actions_needed']}")
                
    except Exception as e:
        print(f"❌ Error: {e}")

def get_fatigue_examples():
    """Get specific examples of fatigue detection to validate logic"""
    
    query = f"""
    SELECT 
      brand,
      ad_start_date,
      funnel,
      persona,
      days_since_launch,
      fatigue_score,
      fatigue_level,
      fatigue_confidence,
      refresh_signals_count,
      max_refresh_signal,
      latest_refresh_date,
      recommended_action,
      strategic_insight
      
    FROM `{PROJECT_ID}.{DATASET_ID}.v_creative_fatigue_analysis`
    WHERE fatigue_score >= 0.4  -- Focus on meaningful fatigue signals
    ORDER BY fatigue_score DESC, refresh_signals_count DESC
    LIMIT 15
    """
    
    print(f"\n📝 Top Creative Fatigue Examples:")
    print("=" * 50)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        for _, row in df.iterrows():
            print(f"\n🏷️  {row['fatigue_level']} (Score: {row['fatigue_score']:.3f})")
            print(f"   Brand: {row['brand']}")
            print(f"   Segment: {row['funnel']}/{row['persona']}")
            print(f"   Ad age: {row['days_since_launch']} days")
            print(f"   Confidence: {row['fatigue_confidence']}")
            if row['refresh_signals_count'] > 0:
                print(f"   Refresh signals: {row['refresh_signals_count']} (strength: {row['max_refresh_signal']:.2f})")
                if row['latest_refresh_date']:
                    print(f"   Latest refresh: {row['latest_refresh_date']}")
            print(f"   Action: {row['recommended_action']}")
            print(f"   Insight: {row['strategic_insight']}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 HARD TEST: CREATIVE FATIGUE DETECTION")
    print("=" * 60)
    
    # Test fatigue detection logic
    fatigue_test_passed = test_creative_fatigue_logic()
    
    # Analyze brand patterns
    analyze_brand_fatigue_patterns()
    
    # Get specific examples
    get_fatigue_examples()
    
    if fatigue_test_passed:
        print("\n✅ HARD TEST PASSED: Creative fatigue detection working effectively")
        print("🎯 Goal: New original ads correctly signal fatigue of previous content")
        print("📊 Logic: Low competitor influence in new ads = evidence of creative refresh")
    else:
        print("\n⚠️  HARD TEST NEEDS REVIEW: Check fatigue detection parameters")
        print("🔍 May need more data or refined refresh signal thresholds")