#!/usr/bin/env python3
"""
Comprehensive Forecasting Toolkit Validation
Tests wide net forecasting with intelligent signal prioritization and noise threshold filtering
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")  
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def test_comprehensive_signal_detection():
    """Test comprehensive forecasting toolkit signal detection and prioritization"""
    
    query = f"""
    WITH signal_analysis AS (
      SELECT 
        brand,
        week_start,
        forecast_target_week,
        signal_count,
        top_signal_type,
        top_strategic_intelligence,
        top_signal_business_impact,
        executive_summary,
        
        -- Analyze signal quality and distribution
        CASE 
          WHEN executive_summary LIKE '🚨 CRITICAL:%' THEN 'CRITICAL_INTELLIGENCE'
          WHEN executive_summary LIKE '⚠️  MULTIPLE_SHIFTS:%' THEN 'MULTIPLE_CHANGES'  
          WHEN executive_summary LIKE '📊 MODERATE:%' THEN 'MODERATE_INTELLIGENCE'
          WHEN executive_summary LIKE '📈 MINOR:%' THEN 'MINOR_INTELLIGENCE'
          ELSE 'STABLE_STRATEGY'
        END AS intelligence_classification,
        
        -- Extract signal details from array for analysis
        (SELECT COUNT(*) FROM UNNEST(prioritized_signals) AS signal 
         WHERE signal.business_impact_weight >= 4) AS high_impact_signals_count,
         
        (SELECT COUNT(*) FROM UNNEST(prioritized_signals) AS signal  
         WHERE signal.signal_type = 'PROMOTIONAL_INTENSITY') AS promotional_intensity_signals,
         
        (SELECT COUNT(*) FROM UNNEST(prioritized_signals) AS signal
         WHERE signal.signal_type = 'BRAND_VOICE_POSITIONING') AS brand_voice_signals,
         
        (SELECT COUNT(*) FROM UNNEST(prioritized_signals) AS signal
         WHERE signal.signal_type = 'FUNNEL_STRATEGY') AS funnel_strategy_signals,
         
        -- Check for business-critical strategic interpretations
        CASE 
          WHEN top_strategic_intelligence LIKE '%DEEP_DISCOUNT_OFFENSIVE%' THEN 1 ELSE 0 
        END AS deep_discount_offensive_detected,
        
        CASE
          WHEN top_strategic_intelligence LIKE '%PREMIUM_POSITIONING_SHIFT%' THEN 1 ELSE 0
        END AS premium_positioning_detected,
        
        CASE  
          WHEN top_strategic_intelligence LIKE '%MASS_MARKET_PIVOT%' THEN 1 ELSE 0
        END AS mass_market_pivot_detected,
        
        CASE
          WHEN top_strategic_intelligence LIKE '%BRAND_BUILDING_PHASE%' THEN 1 ELSE 0  
        END AS brand_building_phase_detected
        
      FROM `{PROJECT_ID}.{DATASET_ID}.comprehensive_forecasting_signals`
      WHERE signal_count > 0  -- Only analyze brands with detected signals
    ),
    
    signal_performance_summary AS (
      SELECT 
        COUNT(*) AS total_brands_with_signals,
        COUNT(DISTINCT brand) AS unique_brands_with_intelligence,
        
        -- Intelligence quality distribution
        COUNTIF(intelligence_classification = 'CRITICAL_INTELLIGENCE') AS critical_intelligence_brands,
        COUNTIF(intelligence_classification = 'MULTIPLE_CHANGES') AS multiple_changes_brands,
        COUNTIF(intelligence_classification = 'MODERATE_INTELLIGENCE') AS moderate_intelligence_brands,
        COUNTIF(intelligence_classification = 'MINOR_INTELLIGENCE') AS minor_intelligence_brands,
        
        -- Signal type coverage
        SUM(promotional_intensity_signals) AS total_promotional_intensity_signals,
        SUM(brand_voice_signals) AS total_brand_voice_signals,
        SUM(funnel_strategy_signals) AS total_funnel_strategy_signals,
        
        -- High-impact signal detection
        SUM(high_impact_signals_count) AS total_high_impact_signals,
        AVG(signal_count) AS avg_signals_per_brand,
        AVG(top_signal_business_impact) AS avg_top_signal_business_impact,
        
        -- Business-critical pattern detection
        SUM(deep_discount_offensive_detected) AS deep_discount_offensives_detected,
        SUM(premium_positioning_detected) AS premium_positioning_shifts_detected,
        SUM(mass_market_pivot_detected) AS mass_market_pivots_detected, 
        SUM(brand_building_phase_detected) AS brand_building_phases_detected,
        
        -- Noise threshold effectiveness
        COUNTIF(top_signal_business_impact >= 4) AS brands_with_high_impact_top_signals,
        COUNTIF(signal_count >= 3) AS brands_with_multiple_significant_signals,
        
        -- Executive summary quality
        COUNTIF(executive_summary NOT LIKE 'STABLE_STRATEGY%') AS brands_with_actionable_intelligence,
        
        -- Strategic intelligence examples
        STRING_AGG(
          CASE WHEN intelligence_classification = 'CRITICAL_INTELLIGENCE'
          THEN CONCAT(brand, ': ', top_strategic_intelligence)
          END, '; ' LIMIT 5
        ) AS critical_intelligence_examples
        
      FROM signal_analysis
    )
    
    SELECT 
      'COMPREHENSIVE_FORECASTING_TOOLKIT' AS test_name,
      total_brands_with_signals,
      unique_brands_with_intelligence,
      
      -- Intelligence quality assessment  
      critical_intelligence_brands,
      multiple_changes_brands,
      moderate_intelligence_brands,
      minor_intelligence_brands,
      
      -- Signal detection capability
      total_promotional_intensity_signals,
      total_brand_voice_signals,
      total_funnel_strategy_signals,
      total_high_impact_signals,
      ROUND(avg_signals_per_brand, 1) AS avg_signals_per_brand,
      ROUND(avg_top_signal_business_impact, 1) AS avg_top_signal_business_impact,
      
      -- Strategic pattern recognition
      deep_discount_offensives_detected,
      premium_positioning_shifts_detected,
      mass_market_pivots_detected,
      brand_building_phases_detected,
      
      -- Noise filtering effectiveness
      brands_with_high_impact_top_signals,
      brands_with_multiple_significant_signals,
      brands_with_actionable_intelligence,
      
      -- Quality indicators
      CASE WHEN total_brands_with_signals > 0 
           THEN brands_with_actionable_intelligence / total_brands_with_signals * 100 
           ELSE 0 END AS pct_brands_with_actionable_intelligence,
           
      CASE WHEN unique_brands_with_intelligence > 0
           THEN critical_intelligence_brands / unique_brands_with_intelligence * 100
           ELSE 0 END AS pct_brands_with_critical_intelligence,
      
      -- Examples
      critical_intelligence_examples,
      
      -- Test assessment
      CASE 
        WHEN critical_intelligence_brands >= 3 AND brands_with_actionable_intelligence / total_brands_with_signals >= 0.70
        THEN 'SUCCESS: High-quality strategic intelligence with effective noise filtering'
        WHEN total_high_impact_signals >= 10 AND avg_top_signal_business_impact >= 3.0
        THEN 'SUCCESS: Strong signal detection with good business impact prioritization'  
        WHEN brands_with_actionable_intelligence / total_brands_with_signals >= 0.60
        THEN 'PARTIAL_SUCCESS: Good actionable intelligence rate, some noise filtering'
        WHEN total_brands_with_signals >= unique_brands_with_intelligence  
        THEN 'PARTIAL_SUCCESS: Signal detection working, refinement needed'
        ELSE 'NEEDS_IMPROVEMENT: Insufficient signal quality or too much noise'
      END AS comprehensive_toolkit_assessment
      
    FROM signal_performance_summary
    """
    
    print("🔧 COMPREHENSIVE FORECASTING TOOLKIT VALIDATION")
    print("=" * 60)
    print("Testing: Wide net forecasting + intelligent signal prioritization")
    print("Objective: Surface only meaningful distribution changes above noise threshold")
    print("-" * 60)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Signal Detection Coverage:")
            print(f"   Total brands with signals: {row['total_brands_with_signals']}")
            print(f"   Unique brands with intelligence: {row['unique_brands_with_intelligence']}")
            print(f"   Avg signals per brand: {row['avg_signals_per_brand']}")
            
            print(f"\n🎯 Intelligence Quality Distribution:")
            print(f"   Critical intelligence: {row['critical_intelligence_brands']} brands")
            print(f"   Multiple changes: {row['multiple_changes_brands']} brands")
            print(f"   Moderate intelligence: {row['moderate_intelligence_brands']} brands") 
            print(f"   Minor intelligence: {row['minor_intelligence_brands']} brands")
            
            print(f"\n🚀 Signal Type Detection:")
            print(f"   Promotional intensity signals: {row['total_promotional_intensity_signals']}")
            print(f"   Brand voice signals: {row['total_brand_voice_signals']}")
            print(f"   Funnel strategy signals: {row['total_funnel_strategy_signals']}")
            print(f"   Total high-impact signals: {row['total_high_impact_signals']}")
            
            print(f"\n💡 Strategic Pattern Recognition:")
            print(f"   Deep discount offensives: {row['deep_discount_offensives_detected']}")
            print(f"   Premium positioning shifts: {row['premium_positioning_shifts_detected']}")
            print(f"   Mass market pivots: {row['mass_market_pivots_detected']}")
            print(f"   Brand building phases: {row['brand_building_phases_detected']}")
            
            print(f"\n🎛️  Noise Filtering Effectiveness:")
            print(f"   Brands with high-impact signals: {row['brands_with_high_impact_top_signals']}")
            print(f"   Brands with multiple significant signals: {row['brands_with_multiple_significant_signals']}")
            print(f"   Actionable intelligence rate: {row['pct_brands_with_actionable_intelligence']:.1f}%")
            print(f"   Critical intelligence rate: {row['pct_brands_with_critical_intelligence']:.1f}%")
            
            if row['critical_intelligence_examples']:
                print(f"\n💎 Critical Intelligence Examples:")
                for example in row['critical_intelligence_examples'].split('; '):
                    if example.strip():
                        print(f"   • {example}")
            
            print(f"\n✅ Assessment: {row['comprehensive_toolkit_assessment']}")
            
            return row['comprehensive_toolkit_assessment'].startswith('SUCCESS')
            
        else:
            print("❌ No comprehensive forecasting results")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_noise_threshold_effectiveness():
    """Test effectiveness of noise threshold filtering in reducing false positives"""
    
    query = f"""
    WITH noise_analysis AS (
      -- Analyze what gets filtered out vs what gets through
      SELECT 
        brand,
        signal_count,
        top_signal_business_impact,
        executive_summary,
        
        -- Categorize by executive summary patterns
        CASE executive_summary
          WHEN 'STABLE_STRATEGY - No significant distribution changes predicted' THEN 'FILTERED_AS_STABLE'
          ELSE 'ABOVE_NOISE_THRESHOLD'  
        END AS noise_filter_result,
        
        -- Extract signal strength indicators
        CASE 
          WHEN signal_count = 0 THEN 'NO_SIGNALS'
          WHEN signal_count = 1 AND top_signal_business_impact <= 2 THEN 'WEAK_SINGLE_SIGNAL'
          WHEN signal_count = 1 AND top_signal_business_impact >= 3 THEN 'STRONG_SINGLE_SIGNAL' 
          WHEN signal_count >= 2 AND top_signal_business_impact >= 3 THEN 'MULTIPLE_STRONG_SIGNALS'
          WHEN signal_count >= 2 THEN 'MULTIPLE_WEAK_SIGNALS'
          ELSE 'UNCLASSIFIED'
        END AS signal_strength_category
        
      FROM `{PROJECT_ID}.{DATASET_ID}.comprehensive_forecasting_signals`
    ),
    
    threshold_effectiveness AS (
      SELECT 
        COUNT(*) AS total_brand_forecasts,
        
        -- Noise filtering results
        COUNTIF(noise_filter_result = 'FILTERED_AS_STABLE') AS brands_filtered_as_stable,
        COUNTIF(noise_filter_result = 'ABOVE_NOISE_THRESHOLD') AS brands_above_threshold,
        
        -- Signal strength distribution  
        COUNTIF(signal_strength_category = 'NO_SIGNALS') AS no_signals_brands,
        COUNTIF(signal_strength_category = 'WEAK_SINGLE_SIGNAL') AS weak_single_signal_brands,
        COUNTIF(signal_strength_category = 'STRONG_SINGLE_SIGNAL') AS strong_single_signal_brands,
        COUNTIF(signal_strength_category = 'MULTIPLE_STRONG_SIGNALS') AS multiple_strong_signals_brands,
        COUNTIF(signal_strength_category = 'MULTIPLE_WEAK_SIGNALS') AS multiple_weak_signals_brands,
        
        -- Quality metrics
        AVG(CASE WHEN noise_filter_result = 'ABOVE_NOISE_THRESHOLD' 
                 THEN top_signal_business_impact END) AS avg_above_threshold_impact,
        AVG(signal_count) AS avg_signal_count_all_brands,
        AVG(CASE WHEN noise_filter_result = 'ABOVE_NOISE_THRESHOLD' 
                 THEN signal_count END) AS avg_signal_count_above_threshold
        
      FROM noise_analysis
    )
    
    SELECT 
      'NOISE_THRESHOLD_EFFECTIVENESS' AS test_name,
      total_brand_forecasts,
      brands_filtered_as_stable,
      brands_above_threshold,
      
      -- Signal strength breakdown
      no_signals_brands,
      weak_single_signal_brands,
      strong_single_signal_brands, 
      multiple_strong_signals_brands,
      multiple_weak_signals_brands,
      
      -- Filtering effectiveness metrics
      CASE WHEN total_brand_forecasts > 0 
           THEN brands_above_threshold / total_brand_forecasts * 100
           ELSE 0 END AS pct_brands_above_noise_threshold,
           
      CASE WHEN total_brand_forecasts > 0
           THEN brands_filtered_as_stable / total_brand_forecasts * 100
           ELSE 0 END AS pct_brands_filtered_as_stable,
      
      -- Quality indicators
      ROUND(avg_above_threshold_impact, 2) AS avg_above_threshold_impact,
      ROUND(avg_signal_count_all_brands, 1) AS avg_signal_count_all_brands,
      ROUND(avg_signal_count_above_threshold, 1) AS avg_signal_count_above_threshold,
      
      -- Noise threshold assessment
      CASE 
        WHEN brands_above_threshold / total_brand_forecasts BETWEEN 0.20 AND 0.50 AND
             avg_above_threshold_impact >= 3.0
        THEN 'OPTIMAL: Good balance of signal detection vs noise filtering'
        WHEN brands_above_threshold / total_brand_forecasts < 0.20 AND  
             strong_single_signal_brands + multiple_strong_signals_brands >= 5
        THEN 'CONSERVATIVE: High-quality signals but may be missing some insights'
        WHEN brands_above_threshold / total_brand_forecasts > 0.70
        THEN 'PERMISSIVE: May be allowing too much noise through'
        WHEN avg_above_threshold_impact >= 3.5 
        THEN 'HIGH_QUALITY: Strong business impact from detected signals'
        ELSE 'NEEDS_TUNING: Threshold optimization required'
      END AS noise_threshold_assessment
      
    FROM threshold_effectiveness
    """
    
    print(f"\n🎛️  NOISE THRESHOLD EFFECTIVENESS ANALYSIS")
    print("=" * 50)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Filtering Results:")
            print(f"   Total brand forecasts: {row['total_brand_forecasts']}")
            print(f"   Above noise threshold: {row['brands_above_threshold']} ({row['pct_brands_above_noise_threshold']:.1f}%)")
            print(f"   Filtered as stable: {row['brands_filtered_as_stable']} ({row['pct_brands_filtered_as_stable']:.1f}%)")
            
            print(f"\n💪 Signal Strength Distribution:")
            print(f"   No signals: {row['no_signals_brands']} brands")
            print(f"   Weak single signal: {row['weak_single_signal_brands']} brands")
            print(f"   Strong single signal: {row['strong_single_signal_brands']} brands")
            print(f"   Multiple strong signals: {row['multiple_strong_signals_brands']} brands")  
            print(f"   Multiple weak signals: {row['multiple_weak_signals_brands']} brands")
            
            print(f"\n🎯 Quality Metrics:")
            print(f"   Avg impact (above threshold): {row['avg_above_threshold_impact']:.2f}/5.0")
            print(f"   Avg signals per brand (all): {row['avg_signal_count_all_brands']:.1f}")
            print(f"   Avg signals per brand (above threshold): {row['avg_signal_count_above_threshold']:.1f}")
            
            print(f"\n✅ Threshold Assessment: {row['noise_threshold_assessment']}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

def demonstrate_executive_summary_intelligence():
    """Demonstrate the executive summary intelligence for business users"""
    
    query = f"""
    SELECT 
      brand,
      week_start,
      forecast_target_week,
      signal_count,
      top_signal_type,
      executive_summary,
      
      -- Extract additional context
      (SELECT signal.change_magnitude FROM UNNEST(prioritized_signals) AS signal 
       WHERE signal.business_impact_weight = (
         SELECT MAX(s.business_impact_weight) FROM UNNEST(prioritized_signals) AS s
       ) LIMIT 1) AS top_signal_change_magnitude,
       
      (SELECT signal.trend_confidence FROM UNNEST(prioritized_signals) AS signal
       WHERE signal.business_impact_weight = (
         SELECT MAX(s.business_impact_weight) FROM UNNEST(prioritized_signals) AS s  
       ) LIMIT 1) AS top_signal_confidence,
       
      seasonal_context
      
    FROM `{PROJECT_ID}.{DATASET_ID}.comprehensive_forecasting_signals`
    WHERE signal_count > 0
    ORDER BY 
      CASE 
        WHEN executive_summary LIKE '🚨 CRITICAL:%' THEN 1
        WHEN executive_summary LIKE '⚠️  MULTIPLE_SHIFTS:%' THEN 2
        WHEN executive_summary LIKE '📊 MODERATE:%' THEN 3  
        ELSE 4
      END,
      signal_count DESC,
      brand
    LIMIT 10
    """
    
    print(f"\n📋 EXECUTIVE SUMMARY INTELLIGENCE DEMO")
    print("=" * 50)
    print("Customer-Facing Strategic Intelligence (Above Noise Threshold)")
    print("-" * 50)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        for _, row in df.iterrows():
            print(f"\n🏷️  {row['brand']} | {row['seasonal_context']}")
            print(f"   📅 Forecast Period: {row['week_start']} → {row['forecast_target_week']}")  
            print(f"   🎯 Signal Count: {row['signal_count']} | Top Signal: {row['top_signal_type']}")
            if pd.notna(row['top_signal_change_magnitude']):
                print(f"   📊 Change Magnitude: {row['top_signal_change_magnitude']:.3f} | Confidence: {row['top_signal_confidence']}")
            print(f"   💡 {row['executive_summary']}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 COMPREHENSIVE FORECASTING TOOLKIT VALIDATION")
    print("=" * 65)
    print("Approach: Wide net forecasting → Signal prioritization → Noise filtering")
    print("Objective: Surface only meaningful strategic changes for competitive intelligence")
    print("=" * 65)
    
    # Test comprehensive signal detection and prioritization
    toolkit_success = test_comprehensive_signal_detection()
    
    # Test noise threshold effectiveness
    test_noise_threshold_effectiveness()
    
    # Demonstrate executive summary intelligence
    demonstrate_executive_summary_intelligence()
    
    if toolkit_success:
        print("\n" + "=" * 65)
        print("✅ COMPREHENSIVE FORECASTING TOOLKIT VALIDATION PASSED")
        print("🎯 Achievement: Wide net forecasting with intelligent signal prioritization")
        print("🎛️  Capability: Noise threshold filtering prevents information overload")
        print("💼 Value: Executive-ready strategic intelligence summaries")
        print("🚀 Impact: Actionable competitive insights above meaningful thresholds")
    else:
        print("\n" + "=" * 65) 
        print("⚠️  COMPREHENSIVE TOOLKIT NEEDS OPTIMIZATION")
        print("🔧 Action: Adjust noise thresholds, improve signal prioritization")
        print("📈 Focus: Increase actionable intelligence rate, reduce false positives")
    
    print("=" * 65)