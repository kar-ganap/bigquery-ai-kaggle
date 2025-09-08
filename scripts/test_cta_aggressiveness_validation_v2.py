#!/usr/bin/env python3
"""
MODERATE Test #2: CTA Aggressiveness Scoring Validation 
Tests whether CTA scoring correlates with business intuition using real CTA analysis table
"""

import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def validate_cta_business_logic():
    """Validate that CTA aggressiveness scores correlate with business expectations"""
    
    query = f"""
    WITH business_expectation_validation AS (
      SELECT 
        brand,
        creative_text,
        title,
        final_aggressiveness_score,
        aggressiveness_tier,
        has_urgency_signals,
        has_promotional_signals,
        has_action_pressure,
        has_scarcity_signals,
        discount_percentage,
        
        -- Business intuition categories
        CASE 
          WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP NOW%' 
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%BUY NOW%'
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%LIMITED TIME%'
          THEN 'Expected High Aggressiveness'
          WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%DISCOVER%' 
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%EXPLORE%'
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%REWRITE THE RULES%'
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%LEARN MORE%'
          THEN 'Expected Low Aggressiveness'
          ELSE 'Expected Medium Aggressiveness'
        END AS business_expectation
        
      FROM `{PROJECT_ID}.{DATASET_ID}.cta_aggressiveness_analysis`
      ORDER BY RAND()
      LIMIT 100
    )
    SELECT 
      'CTA_BUSINESS_LOGIC_VALIDATION' AS test_name,
      
      COUNT(*) AS total_ads_tested,
      
      -- Score distribution by expectation
      AVG(CASE WHEN business_expectation = 'Expected High Aggressiveness' 
               THEN final_aggressiveness_score END) AS avg_score_high_expectation,
      AVG(CASE WHEN business_expectation = 'Expected Low Aggressiveness' 
               THEN final_aggressiveness_score END) AS avg_score_low_expectation,
      AVG(CASE WHEN business_expectation = 'Expected Medium Aggressiveness' 
               THEN final_aggressiveness_score END) AS avg_score_medium_expectation,
      
      -- Count by expectation
      COUNTIF(business_expectation = 'Expected High Aggressiveness') AS high_expectation_count,
      COUNTIF(business_expectation = 'Expected Low Aggressiveness') AS low_expectation_count,
      COUNTIF(business_expectation = 'Expected Medium Aggressiveness') AS medium_expectation_count,
      
      -- Tier distribution validation
      AVG(CASE WHEN aggressiveness_tier IN ('HIGHLY_AGGRESSIVE', 'MODERATELY_AGGRESSIVE') 
               AND business_expectation = 'Expected High Aggressiveness' THEN 1.0 ELSE 0.0 END) AS high_tier_accuracy,
      AVG(CASE WHEN aggressiveness_tier IN ('BRAND_FOCUSED', 'LOW_PRESSURE') 
               AND business_expectation = 'Expected Low Aggressiveness' THEN 1.0 ELSE 0.0 END) AS low_tier_accuracy,
      
      -- Sample examples for validation
      STRING_AGG(
        CASE WHEN business_expectation = 'Expected High Aggressiveness' 
        THEN CONCAT(brand, ': "', SUBSTR(creative_text, 1, 50), '..." (Score: ', 
                   ROUND(final_aggressiveness_score, 2), ', Tier: ', aggressiveness_tier, ')') 
        END, ' | ' LIMIT 3
      ) AS high_aggressiveness_examples,
      
      STRING_AGG(
        CASE WHEN business_expectation = 'Expected Low Aggressiveness' 
        THEN CONCAT(brand, ': "', SUBSTR(creative_text, 1, 50), '..." (Score: ', 
                   ROUND(final_aggressiveness_score, 2), ', Tier: ', aggressiveness_tier, ')') 
        END, ' | ' LIMIT 3
      ) AS low_aggressiveness_examples
      
    FROM business_expectation_validation
    """
    
    print("🔍 MODERATE TEST #2: CTA Aggressiveness Business Logic Validation")
    print("=" * 60)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Business Logic Validation Results:")
            print(f"   Total ads tested: {row['total_ads_tested']}")
            print(f"   High expectation count: {row['high_expectation_count']}")
            print(f"   Low expectation count: {row['low_expectation_count']}")
            print(f"   Medium expectation count: {row['medium_expectation_count']}")
            
            print(f"\n🎯 Score Correlation by Business Expectation:")
            high_score = row['avg_score_high_expectation'] if pd.notna(row['avg_score_high_expectation']) else 0
            low_score = row['avg_score_low_expectation'] if pd.notna(row['avg_score_low_expectation']) else 0
            medium_score = row['avg_score_medium_expectation'] if pd.notna(row['avg_score_medium_expectation']) else 0
            
            print(f"   High expectation avg score: {high_score:.3f}")
            print(f"   Medium expectation avg score: {medium_score:.3f}")  
            print(f"   Low expectation avg score: {low_score:.3f}")
            
            # Test correlation
            correlation_passes = high_score > low_score if high_score > 0 and low_score > 0 else False
            print(f"\n✅ Correlation Test: {'PASS' if correlation_passes else 'NEEDS_REVIEW'}")
            if correlation_passes:
                print(f"   High expectation scores ({high_score:.3f}) > Low expectation scores ({low_score:.3f})")
            
            print(f"\n📊 Tier Classification Accuracy:")
            if pd.notna(row['high_tier_accuracy']):
                print(f"   High aggressiveness tier accuracy: {row['high_tier_accuracy']:.1%}")
            if pd.notna(row['low_tier_accuracy']):
                print(f"   Low aggressiveness tier accuracy: {row['low_tier_accuracy']:.1%}")
            
            print(f"\n📝 Sample Examples:")
            if row['high_aggressiveness_examples']:
                print(f"   High aggressiveness: {row['high_aggressiveness_examples']}")
            if row['low_aggressiveness_examples']:
                print(f"   Low aggressiveness: {row['low_aggressiveness_examples']}")
                
            return correlation_passes
        else:
            print("❌ No validation data returned")
            return False
            
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        return False

def analyze_signal_effectiveness():
    """Analyze which CTA signals are most effective at driving aggressiveness scores"""
    
    query = f"""
    SELECT 
      'CTA_SIGNAL_EFFECTIVENESS' AS analysis_type,
      
      -- Signal prevalence
      COUNTIF(has_urgency_signals) AS urgency_signal_count,
      COUNTIF(has_promotional_signals) AS promotional_signal_count, 
      COUNTIF(has_action_pressure) AS action_pressure_count,
      COUNTIF(has_scarcity_signals) AS scarcity_signal_count,
      
      -- Average scores when signals are present
      AVG(CASE WHEN has_urgency_signals THEN final_aggressiveness_score END) AS avg_score_with_urgency,
      AVG(CASE WHEN has_promotional_signals THEN final_aggressiveness_score END) AS avg_score_with_promotional,
      AVG(CASE WHEN has_action_pressure THEN final_aggressiveness_score END) AS avg_score_with_action,
      AVG(CASE WHEN has_scarcity_signals THEN final_aggressiveness_score END) AS avg_score_with_scarcity,
      
      -- Average score without signals (baseline)
      AVG(CASE WHEN NOT has_urgency_signals AND NOT has_promotional_signals 
                    AND NOT has_action_pressure AND NOT has_scarcity_signals 
               THEN final_aggressiveness_score END) AS avg_score_no_signals,
      
      -- Tier distributions
      COUNTIF(aggressiveness_tier = 'HIGHLY_AGGRESSIVE') AS highly_aggressive_count,
      COUNTIF(aggressiveness_tier = 'BRAND_FOCUSED') AS brand_focused_count,
      
      COUNT(*) AS total_ads
      
    FROM `{PROJECT_ID}.{DATASET_ID}.cta_aggressiveness_analysis`
    """
    
    print("\n🔍 CTA Signal Effectiveness Analysis:")
    print("=" * 40)
    
    try:
        result = client.query(query).result()
        
        for row in result:
            print(f"📊 Signal Effectiveness:")
            print(f"   Total ads: {row.total_ads}")
            print(f"   Urgency signals: {row.urgency_signal_count} ({row.urgency_signal_count/row.total_ads*100:.1f}%)")
            print(f"   Promotional signals: {row.promotional_signal_count} ({row.promotional_signal_count/row.total_ads*100:.1f}%)")
            print(f"   Action pressure: {row.action_pressure_count} ({row.action_pressure_count/row.total_ads*100:.1f}%)")
            print(f"   Scarcity signals: {row.scarcity_signal_count} ({row.scarcity_signal_count/row.total_ads*100:.1f}%)")
            
            print(f"\n📈 Average Scores by Signal Type:")
            if row.avg_score_with_urgency:
                print(f"   With urgency: {row.avg_score_with_urgency:.3f}")
            if row.avg_score_with_promotional:
                print(f"   With promotional: {row.avg_score_with_promotional:.3f}")
            if row.avg_score_with_action:
                print(f"   With action pressure: {row.avg_score_with_action:.3f}")
            if row.avg_score_with_scarcity:
                print(f"   With scarcity: {row.avg_score_with_scarcity:.3f}")
            if row.avg_score_no_signals:
                print(f"   No signals (baseline): {row.avg_score_no_signals:.3f}")
                
            print(f"\n📊 Aggressiveness Distribution:")
            print(f"   Highly aggressive: {row.highly_aggressive_count} ({row.highly_aggressive_count/row.total_ads*100:.1f}%)")
            print(f"   Brand focused: {row.brand_focused_count} ({row.brand_focused_count/row.total_ads*100:.1f}%)")
            
    except Exception as e:
        print(f"❌ Error: {e}")

def get_top_examples():
    """Get top examples of each aggressiveness tier for manual validation"""
    
    query = f"""
    SELECT 
      aggressiveness_tier,
      brand,
      creative_text,
      title,
      final_aggressiveness_score,
      has_urgency_signals,
      has_promotional_signals,
      has_action_pressure,
      discount_percentage
    FROM `{PROJECT_ID}.{DATASET_ID}.cta_aggressiveness_analysis`
    WHERE creative_text IS NOT NULL
    ORDER BY 
      CASE aggressiveness_tier 
        WHEN 'HIGHLY_AGGRESSIVE' THEN 1
        WHEN 'MODERATELY_AGGRESSIVE' THEN 2
        WHEN 'BRAND_FOCUSED' THEN 3
        ELSE 4
      END,
      final_aggressiveness_score DESC
    LIMIT 15
    """
    
    print(f"\n📝 Top Examples by Aggressiveness Tier:")
    print("=" * 50)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        for _, row in df.iterrows():
            signals = []
            if row['has_urgency_signals']: signals.append('Urgency')
            if row['has_promotional_signals']: signals.append('Promotional')
            if row['has_action_pressure']: signals.append('Action')
            if row['discount_percentage'] > 0: signals.append(f'{row["discount_percentage"]}% off')
            
            signals_str = ', '.join(signals) if signals else 'No signals'
            
            print(f"🏷️  {row['aggressiveness_tier']} (Score: {row['final_aggressiveness_score']:.3f})")
            print(f"   {row['brand']}: \"{row['creative_text'][:80]}...\"")
            print(f"   Signals: {signals_str}\n")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 MODERATE TEST #2: CTA AGGRESSIVENESS SCORING VALIDATION")
    print("=" * 60)
    
    # Run business logic validation
    validation_passed = validate_cta_business_logic()
    
    # Analyze signal effectiveness
    analyze_signal_effectiveness()
    
    # Get top examples for manual review
    get_top_examples()
    
    if validation_passed:
        print("\n✅ MODERATE TEST #2 PASSED: CTA aggressiveness scoring correlates with business intuition")
        print("🎯 Goal: Aggressiveness scores differentiate high-pressure vs brand-focused messaging")
    else:
        print("\n⚠️  MODERATE TEST #2 NEEDS REVIEW: Check correlation between expectations and scores")
        print("🔍 Manual review recommended for edge cases")