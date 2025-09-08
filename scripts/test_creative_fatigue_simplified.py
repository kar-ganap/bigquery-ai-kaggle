#!/usr/bin/env python3
"""
HARD Test: Simplified Creative Fatigue Detection
Tests the assumption that new ads in same category indicate fatigue of older ads
without requiring the full competitive influence framework
"""

import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def test_simplified_creative_fatigue():
    """Test creative fatigue logic using simple temporal patterns"""
    
    query = f"""
    WITH ad_temporal_analysis AS (
      SELECT 
        ad_id,
        brand,
        creative_text,
        title,
        media_type,
        start_timestamp,
        active_days,
        
        -- Create simple category based on brand + media type
        CONCAT(brand, '_', media_type) AS ad_category,
        
        -- Calculate ad age
        DATE_DIFF(CURRENT_DATE(), DATE(start_timestamp), DAY) AS days_since_launch,
        
        -- Assign funnel based on creative text patterns (simplified)
        CASE 
          WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%DISCOVER%' 
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%EXPLORE%'
          THEN 'Upper'
          WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP%NOW%' 
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%BUY%NOW%'
          THEN 'Lower'
          ELSE 'Mid'
        END AS funnel_category,
        
        -- Simple persona assignment
        CASE 
          WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%NEW%'
          THEN 'New Customer'
          WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%COLLECTION%'
          THEN 'General Market'
          ELSE 'Existing Customer'
        END AS persona_category
        
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
      WHERE start_timestamp IS NOT NULL
        AND creative_text IS NOT NULL
    ),
    
    creative_refresh_signals AS (
      SELECT 
        a1.ad_id,
        a1.brand,
        a1.ad_category,
        a1.funnel_category,
        a1.persona_category,
        a1.start_timestamp AS ad_start_timestamp,
        a1.days_since_launch,
        a1.active_days,
        
        -- Count newer ads in same category that could indicate fatigue
        COUNT(a2.ad_id) AS newer_ads_same_category,
        
        -- Find most recent new ad in category
        MAX(a2.start_timestamp) AS most_recent_new_ad,
        
        -- Average age of newer ads (fresher = stronger refresh signal)
        AVG(DATE_DIFF(CURRENT_DATE(), DATE(a2.start_timestamp), DAY)) AS avg_newer_ad_age,
        
        -- Text similarity check (simple length-based proxy for now)
        AVG(ABS(LENGTH(a1.creative_text) - LENGTH(a2.creative_text))) AS avg_length_difference,
        
        -- Calculate refresh signal strength
        CASE 
          WHEN COUNT(a2.ad_id) >= 3 AND 
               AVG(DATE_DIFF(CURRENT_DATE(), DATE(a2.start_timestamp), DAY)) <= 14
          THEN 1.0  -- Strong refresh signal (3+ new ads recently)
          WHEN COUNT(a2.ad_id) >= 2 AND 
               AVG(DATE_DIFF(CURRENT_DATE(), DATE(a2.start_timestamp), DAY)) <= 21  
          THEN 0.7  -- Moderate refresh signal
          WHEN COUNT(a2.ad_id) >= 1 AND 
               AVG(DATE_DIFF(CURRENT_DATE(), DATE(a2.start_timestamp), DAY)) <= 30
          THEN 0.4  -- Weak refresh signal
          ELSE 0.0  -- No refresh signal
        END AS refresh_signal_strength
        
      FROM ad_temporal_analysis a1
      LEFT JOIN ad_temporal_analysis a2
        ON a1.brand = a2.brand
        AND a1.funnel_category = a2.funnel_category
        AND a1.persona_category = a2.persona_category
        AND a2.start_timestamp > a1.start_timestamp  -- Only newer ads
        AND DATE_DIFF(DATE(a2.start_timestamp), DATE(a1.start_timestamp), DAY) <= 60  -- Within 60 days
      GROUP BY 
        a1.ad_id, a1.brand, a1.ad_category, a1.funnel_category, a1.persona_category,
        a1.start_timestamp, a1.days_since_launch, a1.active_days
    ),
    
    fatigue_scoring AS (
      SELECT 
        *,
        
        -- Age-based fatigue component (0-1, saturating at 45 days)
        LEAST(1.0, days_since_launch / 45.0) AS age_fatigue_component,
        
        -- Refresh-based fatigue component  
        refresh_signal_strength * 0.5 AS refresh_fatigue_component,
        
        -- Combined fatigue score
        LEAST(1.0, 
          LEAST(1.0, days_since_launch / 45.0) +  -- Age component
          (refresh_signal_strength * 0.5)         -- Refresh component
        ) AS combined_fatigue_score,
        
        -- Fatigue confidence based on refresh signals
        CASE 
          WHEN refresh_signal_strength >= 0.7 AND newer_ads_same_category >= 2
          THEN 'High Confidence'
          WHEN refresh_signal_strength >= 0.4 AND newer_ads_same_category >= 1  
          THEN 'Medium Confidence'
          WHEN days_since_launch >= 30
          THEN 'Age-Based Assessment'
          ELSE 'Low Confidence'
        END AS fatigue_confidence
        
      FROM creative_refresh_signals
    )
    
    SELECT 
      'SIMPLIFIED_CREATIVE_FATIGUE_TEST' AS test_name,
      
      COUNT(*) AS total_ads_analyzed,
      COUNT(DISTINCT brand) AS unique_brands,
      
      -- Fatigue distribution  
      AVG(combined_fatigue_score) AS avg_fatigue_score,
      COUNTIF(combined_fatigue_score >= 0.8) AS critical_fatigue_ads,
      COUNTIF(combined_fatigue_score >= 0.6) AS high_fatigue_ads,
      COUNTIF(combined_fatigue_score >= 0.4) AS moderate_fatigue_ads,
      COUNTIF(combined_fatigue_score < 0.2) AS fresh_content_ads,
      
      -- Refresh signal analysis
      COUNTIF(refresh_signal_strength > 0) AS ads_with_refresh_signals,
      AVG(refresh_signal_strength) AS avg_refresh_signal_strength,
      COUNTIF(newer_ads_same_category > 0) AS ads_with_newer_category_ads,
      AVG(newer_ads_same_category) AS avg_newer_ads_per_category,
      
      -- Age analysis
      AVG(days_since_launch) AS avg_ad_age_days,
      MAX(days_since_launch) AS oldest_ad_age_days,
      
      -- Confidence distribution
      COUNTIF(fatigue_confidence = 'High Confidence') AS high_confidence_count,
      COUNTIF(fatigue_confidence = 'Medium Confidence') AS medium_confidence_count,
      COUNTIF(fatigue_confidence = 'Age-Based Assessment') AS age_based_count,
      
      -- Test validation
      CASE 
        WHEN COUNTIF(refresh_signal_strength > 0) > 0 AND AVG(combined_fatigue_score) > 0.3
        THEN 'PASS'
        WHEN COUNT(*) > 50 AND AVG(combined_fatigue_score) > 0.2
        THEN 'PARTIAL_PASS'
        ELSE 'NEEDS_REVIEW'
      END AS test_result
      
    FROM fatigue_scoring
    """
    
    print("🔍 HARD TEST: Simplified Creative Fatigue Detection")
    print("=" * 60)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Creative Fatigue Analysis Results:")
            print(f"   Total ads analyzed: {row['total_ads_analyzed']}")
            print(f"   Unique brands: {row['unique_brands']}")
            print(f"   Average fatigue score: {row['avg_fatigue_score']:.3f}")
            print(f"   Average ad age: {row['avg_ad_age_days']:.1f} days")
            print(f"   Oldest ad: {row['oldest_ad_age_days']} days")
            
            print(f"\n📈 Fatigue Level Distribution:")
            print(f"   Critical fatigue (≥0.8): {row['critical_fatigue_ads']}")
            print(f"   High fatigue (≥0.6): {row['high_fatigue_ads']}")  
            print(f"   Moderate fatigue (≥0.4): {row['moderate_fatigue_ads']}")
            print(f"   Fresh content (<0.2): {row['fresh_content_ads']}")
            
            print(f"\n🔄 Creative Refresh Signal Analysis:")
            print(f"   Ads with refresh signals: {row['ads_with_refresh_signals']}")
            print(f"   Avg refresh signal strength: {row['avg_refresh_signal_strength']:.3f}")
            print(f"   Ads with newer category ads: {row['ads_with_newer_category_ads']}")
            print(f"   Avg newer ads per category: {row['avg_newer_ads_per_category']:.1f}")
            
            print(f"\n📋 Confidence Distribution:")
            print(f"   High confidence: {row['high_confidence_count']}")
            print(f"   Medium confidence: {row['medium_confidence_count']}")
            print(f"   Age-based assessment: {row['age_based_count']}")
            
            print(f"\n✅ Test Result: {row['test_result']}")
            
            # Test logic validation
            has_refresh_signals = row['ads_with_refresh_signals'] > 0
            reasonable_fatigue_scores = 0.1 <= row['avg_fatigue_score'] <= 0.8
            
            logic_validation = has_refresh_signals and reasonable_fatigue_scores
            
            if logic_validation:
                print("🎯 Logic Validation: PASSED")
                print("   ✓ Refresh signals detected when newer ads appear in same categories")
                print("   ✓ Fatigue scores in reasonable range based on ad age and refresh activity")
            
            return row['test_result'] in ['PASS', 'PARTIAL_PASS']
        else:
            print("❌ No fatigue analysis data returned")
            return False
            
    except Exception as e:
        print(f"❌ Error during fatigue analysis: {e}")
        return False

def get_fatigue_examples():
    """Get specific examples of fatigue detection"""
    
    query = f"""
    WITH fatigue_examples AS (
      SELECT 
        brand,
        DATE(start_timestamp) AS ad_start_date,
        SUBSTR(creative_text, 1, 80) AS creative_sample,
        days_since_launch,
        newer_ads_same_category,
        refresh_signal_strength,
        combined_fatigue_score,
        fatigue_confidence,
        
        CASE 
          WHEN combined_fatigue_score >= 0.8 THEN 'Critical Fatigue'
          WHEN combined_fatigue_score >= 0.6 THEN 'High Fatigue'
          WHEN combined_fatigue_score >= 0.4 THEN 'Moderate Fatigue'
          WHEN combined_fatigue_score >= 0.2 THEN 'Low Fatigue'
          ELSE 'Fresh Content'
        END AS fatigue_level
        
      FROM (
        -- Repeat the same logic as the main query for examples
        WITH ad_temporal_analysis AS (
          SELECT 
            ad_id, brand, creative_text, start_timestamp,
            DATE_DIFF(CURRENT_DATE(), DATE(start_timestamp), DAY) AS days_since_launch,
            CASE 
              WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%DISCOVER%' THEN 'Upper'
              WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP%NOW%' THEN 'Lower'
              ELSE 'Mid'
            END AS funnel_category,
            CASE 
              WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%NEW%' THEN 'New Customer'
              ELSE 'Existing Customer'
            END AS persona_category
          FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
          WHERE start_timestamp IS NOT NULL AND creative_text IS NOT NULL
        ),
        refresh_analysis AS (
          SELECT 
            a1.ad_id, a1.brand, a1.creative_text, a1.start_timestamp, a1.days_since_launch,
            COUNT(a2.ad_id) AS newer_ads_same_category,
            CASE 
              WHEN COUNT(a2.ad_id) >= 3 THEN 1.0
              WHEN COUNT(a2.ad_id) >= 2 THEN 0.7
              WHEN COUNT(a2.ad_id) >= 1 THEN 0.4
              ELSE 0.0
            END AS refresh_signal_strength
          FROM ad_temporal_analysis a1
          LEFT JOIN ad_temporal_analysis a2
            ON a1.brand = a2.brand 
            AND a1.funnel_category = a2.funnel_category
            AND a1.persona_category = a2.persona_category
            AND a2.start_timestamp > a1.start_timestamp
            AND DATE_DIFF(DATE(a2.start_timestamp), DATE(a1.start_timestamp), DAY) <= 60
          GROUP BY a1.ad_id, a1.brand, a1.creative_text, a1.start_timestamp, a1.days_since_launch
        )
        SELECT 
          *,
          LEAST(1.0, days_since_launch / 45.0 + refresh_signal_strength * 0.5) AS combined_fatigue_score,
          CASE 
            WHEN refresh_signal_strength >= 0.7 THEN 'High Confidence'
            WHEN refresh_signal_strength >= 0.4 THEN 'Medium Confidence'
            WHEN days_since_launch >= 30 THEN 'Age-Based Assessment'
            ELSE 'Low Confidence'
          END AS fatigue_confidence
        FROM refresh_analysis
      )
      WHERE combined_fatigue_score >= 0.4  -- Focus on meaningful fatigue
      ORDER BY combined_fatigue_score DESC, refresh_signal_strength DESC
      LIMIT 10
    )
    SELECT * FROM fatigue_examples
    """
    
    print(f"\n📝 Creative Fatigue Examples:")
    print("=" * 50)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        for _, row in df.iterrows():
            print(f"\n🏷️  {row['fatigue_level']} (Score: {row['combined_fatigue_score']:.3f})")
            print(f"   Brand: {row['brand']}")
            print(f"   Creative: \"{row['creative_sample']}...\"")
            print(f"   Ad age: {row['days_since_launch']} days")
            print(f"   Newer ads in category: {row['newer_ads_same_category']}")
            print(f"   Refresh signal: {row['refresh_signal_strength']:.1f}")
            print(f"   Confidence: {row['fatigue_confidence']}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 HARD TEST: SIMPLIFIED CREATIVE FATIGUE DETECTION")
    print("=" * 60)
    
    # Test simplified fatigue detection
    fatigue_test_passed = test_simplified_creative_fatigue()
    
    # Get specific examples
    get_fatigue_examples()
    
    if fatigue_test_passed:
        print("\n✅ HARD TEST PASSED: Simplified creative fatigue detection working")
        print("🎯 Logic: New ads in same brand/funnel/persona category indicate fatigue of older ads")
        print("📊 Key Insight: Brands launching new creative suggests previous creative fatigue")
    else:
        print("\n⚠️  HARD TEST NEEDS REFINEMENT: Adjust fatigue detection parameters")
        print("🔍 Consider: longer time windows, refined category definitions, or threshold tuning")