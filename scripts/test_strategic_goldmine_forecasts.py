#!/usr/bin/env python3
"""
Strategic Goldmine Forecasting Validation
Tests Tier 1 strategic intelligence predictions: promotional_intensity, primary_angle pivots, urgency_spikes
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def test_promotional_intensity_forecasting():
    """Test ability to predict promotional intensity spikes (Black Friday early prediction)"""
    
    query = f"""
    WITH promotional_intensity_test AS (
      -- Test promotional intensity forecasting accuracy
      SELECT 
        brand,
        week_start,
        current_promotional_intensity,
        forecast_promotional_intensity_4week,
        predicted_promotional_intensity_change,
        promotional_intensity_intelligence,
        forecast_confidence,
        seasonal_context,
        
        -- Validation: Does forecast match expected seasonal patterns?
        CASE 
          WHEN seasonal_context = 'BLACK_FRIDAY_SEASON' AND forecast_promotional_intensity_4week >= 0.70
          THEN 'BLACK_FRIDAY_INTENSITY_PREDICTED'
          WHEN predicted_promotional_intensity_change >= 0.20 AND forecast_confidence = 'HIGH_CONFIDENCE'  
          THEN 'MAJOR_PROMOTIONAL_SPIKE_PREDICTED'
          WHEN predicted_promotional_intensity_change <= -0.15
          THEN 'PROMOTIONAL_PULLBACK_PREDICTED'
          ELSE 'BASELINE_FORECAST'
        END AS forecast_classification,
        
        -- Business impact scoring
        CASE 
          WHEN promotional_intensity_intelligence LIKE 'DEEP_DISCOUNT_IMMINENT%'
          THEN 5  -- Highest business impact
          WHEN promotional_intensity_intelligence LIKE 'SEASONAL_PROMOTIONAL_SPIKE%'
          THEN 4
          WHEN promotional_intensity_intelligence LIKE 'PROMOTIONAL_ESCALATION%'
          THEN 3
          ELSE 1
        END AS business_impact_score
        
      FROM `{PROJECT_ID}.{DATASET_ID}.strategic_goldmine_forecasts`
      WHERE forecast_target_week IS NOT NULL
    ),
    
    forecast_performance AS (
      SELECT 
        COUNT(*) AS total_forecasts,
        COUNT(DISTINCT brand) AS brands_forecasted,
        
        -- Black Friday prediction capability
        COUNTIF(seasonal_context = 'BLACK_FRIDAY_SEASON') AS black_friday_forecasts,
        COUNTIF(forecast_classification = 'BLACK_FRIDAY_INTENSITY_PREDICTED') AS black_friday_intensity_predictions,
        COUNTIF(seasonal_context = 'BLACK_FRIDAY_SEASON' AND forecast_promotional_intensity_4week >= 0.70) / 
          NULLIF(COUNTIF(seasonal_context = 'BLACK_FRIDAY_SEASON'), 0) * 100 AS pct_black_friday_high_intensity_predicted,
        
        -- High-impact forecast detection
        COUNTIF(business_impact_score >= 4) AS high_impact_forecasts,
        COUNTIF(forecast_confidence = 'HIGH_CONFIDENCE') AS high_confidence_forecasts,
        
        -- Promotional spike prediction accuracy
        AVG(predicted_promotional_intensity_change) AS avg_predicted_change,
        AVG(CASE WHEN predicted_promotional_intensity_change >= 0.15 THEN predicted_promotional_intensity_change END) AS avg_major_spike_prediction,
        
        -- Distribution of forecast types
        COUNTIF(forecast_classification = 'MAJOR_PROMOTIONAL_SPIKE_PREDICTED') AS major_spike_predictions,
        COUNTIF(forecast_classification = 'PROMOTIONAL_PULLBACK_PREDICTED') AS pullback_predictions,
        
        -- Strategic intelligence quality
        COUNTIF(promotional_intensity_intelligence LIKE 'DEEP_DISCOUNT_IMMINENT%') AS deep_discount_alerts,
        COUNTIF(promotional_intensity_intelligence LIKE 'UNEXPECTED_PROMOTIONAL_AGGRESSION%') AS unexpected_aggression_alerts
        
      FROM promotional_intensity_test
    )
    
    SELECT 
      'PROMOTIONAL_INTENSITY_FORECASTING' AS test_name,
      total_forecasts,
      brands_forecasted,
      
      -- Black Friday prediction capability (key success metric)
      black_friday_forecasts,
      black_friday_intensity_predictions,
      pct_black_friday_high_intensity_predicted,
      
      -- Strategic intelligence metrics
      high_impact_forecasts,
      high_confidence_forecasts,
      ROUND(avg_predicted_change, 3) AS avg_predicted_change,
      ROUND(COALESCE(avg_major_spike_prediction, 0), 3) AS avg_major_spike_prediction,
      
      -- Alert quality
      deep_discount_alerts,
      unexpected_aggression_alerts,
      major_spike_predictions,
      pullback_predictions,
      
      -- Test success assessment
      CASE 
        WHEN black_friday_forecasts > 0 AND pct_black_friday_high_intensity_predicted >= 80.0
        THEN 'SUCCESS: Can predict Black Friday intensity 4+ weeks early'
        WHEN high_impact_forecasts >= brands_forecasted AND high_confidence_forecasts / total_forecasts >= 0.60
        THEN 'SUCCESS: High-quality strategic forecasting capability'
        WHEN major_spike_predictions >= 2 AND deep_discount_alerts >= 1
        THEN 'PARTIAL_SUCCESS: Some promotional spike prediction capability'
        ELSE 'NEEDS_IMPROVEMENT: Insufficient promotional forecasting accuracy'
      END AS promotional_forecasting_assessment
      
    FROM forecast_performance
    """
    
    print("💰 TIER 1 STRATEGIC GOLDMINE: Promotional Intensity Forecasting")
    print("=" * 65)
    print("Success Metric: Can we predict Black Friday intensity 4+ weeks early?")
    print("Business Impact: Pricing strategy, promotional calendar planning")
    print("-" * 65)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Forecast Coverage:")
            print(f"   Total forecasts: {row['total_forecasts']}")
            print(f"   Brands forecasted: {row['brands_forecasted']}")
            print(f"   High confidence forecasts: {row['high_confidence_forecasts']}")
            
            print(f"\n🎯 Black Friday Early Prediction:")
            print(f"   Black Friday forecasts: {row['black_friday_forecasts']}")
            print(f"   High intensity predictions: {row['black_friday_intensity_predictions']}")
            if row['black_friday_forecasts'] > 0:
                print(f"   Success rate: {row['pct_black_friday_high_intensity_predicted']:.1f}%")
            
            print(f"\n🚨 Strategic Intelligence Alerts:")
            print(f"   Deep discount alerts: {row['deep_discount_alerts']}")
            print(f"   Unexpected aggression alerts: {row['unexpected_aggression_alerts']}")
            print(f"   Major spike predictions: {row['major_spike_predictions']}")
            print(f"   Promotional pullback predictions: {row['pullback_predictions']}")
            
            print(f"\n📈 Prediction Quality:")
            print(f"   Average predicted change: {row['avg_predicted_change']:.3f}")
            print(f"   Average major spike magnitude: {row['avg_major_spike_prediction']:.3f}")
            
            print(f"\n✅ Assessment: {row['promotional_forecasting_assessment']}")
            
            return row['promotional_forecasting_assessment'].startswith('SUCCESS')
            
        else:
            print("❌ No promotional forecasting results")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_messaging_angle_pivot_prediction():
    """Test ability to predict primary_angle pivots for competitive messaging advantage"""
    
    query = f"""
    WITH angle_pivot_analysis AS (
      SELECT 
        brand,
        week_start,
        current_promotional_angle_pct,
        predicted_angle_pivot_4week,
        forecast_confidence,
        business_impact_assessment,
        recommended_competitive_response,
        
        -- Pivot significance assessment
        CASE 
          WHEN predicted_angle_pivot_4week = 'PROMOTIONAL_PIVOT_DETECTED'
          THEN 'ACTIVE_PROMOTIONAL_PIVOT'
          WHEN predicted_angle_pivot_4week = 'EMOTIONAL_PIVOT_DETECTED' 
          THEN 'ACTIVE_EMOTIONAL_PIVOT'
          WHEN predicted_angle_pivot_4week = 'PROMOTIONAL_PIVOT_IMMINENT'
          THEN 'IMMINENT_PROMOTIONAL_PIVOT'
          WHEN predicted_angle_pivot_4week = 'PROMOTIONAL_PIVOT_LIKELY'
          THEN 'LIKELY_PROMOTIONAL_PIVOT' 
          ELSE 'STABLE_MESSAGING'
        END AS pivot_classification,
        
        -- Strategic significance scoring
        CASE 
          WHEN business_impact_assessment = 'STRATEGIC_REPOSITIONING - Brand shifting to emotional messaging'
          THEN 5
          WHEN business_impact_assessment LIKE 'CRITICAL_COMPETITIVE_THREAT%'
          THEN 4
          WHEN predicted_angle_pivot_4week LIKE '%PIVOT_DETECTED%'
          THEN 3
          WHEN predicted_angle_pivot_4week LIKE '%PIVOT_IMMINENT%'
          THEN 2
          ELSE 1
        END AS strategic_significance_score
        
      FROM `{PROJECT_ID}.{DATASET_ID}.strategic_goldmine_forecasts`
      WHERE predicted_angle_pivot_4week IS NOT NULL
    ),
    
    pivot_intelligence AS (
      SELECT 
        COUNT(*) AS total_pivot_forecasts,
        COUNT(DISTINCT brand) AS brands_with_pivot_intelligence,
        
        -- Pivot detection capability
        COUNTIF(pivot_classification LIKE '%ACTIVE%') AS active_pivots_detected,
        COUNTIF(pivot_classification LIKE '%IMMINENT%' OR pivot_classification LIKE '%LIKELY%') AS imminent_pivots_predicted,
        COUNTIF(pivot_classification = 'STABLE_MESSAGING') AS stable_messaging_periods,
        
        -- Strategic significance
        COUNTIF(strategic_significance_score >= 4) AS high_strategic_significance_pivots,
        COUNTIF(strategic_significance_score >= 3) AS significant_pivots,
        AVG(strategic_significance_score) AS avg_strategic_significance,
        
        -- Pivot type distribution
        COUNTIF(pivot_classification LIKE '%PROMOTIONAL%') AS promotional_pivots,
        COUNTIF(pivot_classification LIKE '%EMOTIONAL%') AS emotional_pivots,
        
        -- Competitive response readiness
        COUNTIF(recommended_competitive_response LIKE 'COUNTER_MESSAGING%') AS counter_messaging_opportunities,
        COUNTIF(business_impact_assessment LIKE 'STRATEGIC_REPOSITIONING%') AS strategic_repositioning_detected,
        
        -- Example strategic insight
        STRING_AGG(
          CASE WHEN strategic_significance_score >= 4 
          THEN CONCAT(brand, ': ', business_impact_assessment)
          END, '; ' LIMIT 3
        ) AS top_strategic_insights
        
      FROM angle_pivot_analysis
    )
    
    SELECT 
      'MESSAGING_ANGLE_PIVOT_PREDICTION' AS test_name,
      total_pivot_forecasts,
      brands_with_pivot_intelligence,
      
      -- Pivot prediction capability
      active_pivots_detected,
      imminent_pivots_predicted,
      stable_messaging_periods,
      
      -- Strategic intelligence quality
      high_strategic_significance_pivots,
      significant_pivots,
      ROUND(avg_strategic_significance, 2) AS avg_strategic_significance,
      
      -- Pivot insights
      promotional_pivots,
      emotional_pivots,
      counter_messaging_opportunities,
      strategic_repositioning_detected,
      top_strategic_insights,
      
      -- Success assessment
      CASE 
        WHEN active_pivots_detected >= 2 AND high_strategic_significance_pivots >= 1
        THEN 'SUCCESS: Can predict messaging strategy changes with strategic impact'
        WHEN imminent_pivots_predicted >= 3 AND counter_messaging_opportunities >= 1  
        THEN 'SUCCESS: Good predictive capability for competitive messaging advantage'
        WHEN significant_pivots >= brands_with_pivot_intelligence
        THEN 'PARTIAL_SUCCESS: Some pivot prediction capability detected'
        ELSE 'NEEDS_IMPROVEMENT: Insufficient messaging pivot prediction'
      END AS angle_pivot_assessment
      
    FROM pivot_intelligence
    """
    
    print(f"\n📱 TIER 1 STRATEGIC GOLDMINE: Messaging Angle Pivot Prediction")
    print("=" * 65)
    print("Success Metric: Predict messaging strategy changes for competitive advantage")
    print("Example: 'Adidas shifting from FEATURE_FOCUSED → EMOTIONAL in 3 weeks'")
    print("-" * 65)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Pivot Intelligence Coverage:")
            print(f"   Total pivot forecasts: {row['total_pivot_forecasts']}")
            print(f"   Brands with pivot intelligence: {row['brands_with_pivot_intelligence']}")
            
            print(f"\n🎯 Pivot Detection Capability:")
            print(f"   Active pivots detected: {row['active_pivots_detected']}")
            print(f"   Imminent pivots predicted: {row['imminent_pivots_predicted']}")
            print(f"   Stable messaging periods: {row['stable_messaging_periods']}")
            
            print(f"\n🚀 Strategic Intelligence Quality:")
            print(f"   High strategic significance pivots: {row['high_strategic_significance_pivots']}")
            print(f"   Total significant pivots: {row['significant_pivots']}")
            print(f"   Average strategic significance: {row['avg_strategic_significance']:.2f}/5.0")
            
            print(f"\n📈 Pivot Type Analysis:")
            print(f"   Promotional pivots: {row['promotional_pivots']}")
            print(f"   Emotional pivots: {row['emotional_pivots']}")
            print(f"   Counter-messaging opportunities: {row['counter_messaging_opportunities']}")
            print(f"   Strategic repositioning detected: {row['strategic_repositioning_detected']}")
            
            if row['top_strategic_insights']:
                print(f"\n💡 Top Strategic Insights:")
                for insight in row['top_strategic_insights'].split('; '):
                    print(f"   • {insight}")
            
            print(f"\n✅ Assessment: {row['angle_pivot_assessment']}")
            
            return row['angle_pivot_assessment'].startswith('SUCCESS')
            
        else:
            print("❌ No angle pivot prediction results")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_urgency_spike_prediction():
    """Test ability to predict urgency_score spikes for launch timing intelligence"""
    
    query = f"""
    WITH urgency_spike_analysis AS (
      SELECT 
        brand,
        week_start,
        current_urgency_score,
        forecast_urgency_score_4week,
        predicted_urgency_change,
        urgency_spike_intelligence,
        recommended_competitive_response,
        seasonal_context,
        
        -- Urgency spike classification
        CASE 
          WHEN urgency_spike_intelligence = 'URGENCY_SPIKE_IMMINENT - Time pressure campaign likely'
          THEN 'IMMINENT_URGENCY_SPIKE'
          WHEN urgency_spike_intelligence = 'SEASONAL_URGENCY_BUILD - Holiday urgency escalating'
          THEN 'SEASONAL_URGENCY_BUILD'
          WHEN urgency_spike_intelligence = 'URGENCY_ESCALATION - Increasing time pressure tactics' 
          THEN 'URGENCY_ESCALATION'
          ELSE 'STABLE_URGENCY'
        END AS urgency_classification,
        
        -- Business timing impact
        CASE 
          WHEN recommended_competitive_response LIKE 'TIMING_STRATEGY%'
          THEN 'TIMING_INTELLIGENCE_AVAILABLE'
          WHEN forecast_urgency_score_4week >= 0.75
          THEN 'HIGH_URGENCY_PERIOD_PREDICTED'
          WHEN predicted_urgency_change >= 0.20
          THEN 'MAJOR_URGENCY_SHIFT_PREDICTED'
          ELSE 'ROUTINE_URGENCY_LEVELS'
        END AS timing_intelligence_value
        
      FROM `{PROJECT_ID}.{DATASET_ID}.strategic_goldmine_forecasts`
      WHERE forecast_urgency_score_4week IS NOT NULL
    ),
    
    urgency_intelligence_summary AS (
      SELECT 
        COUNT(*) AS total_urgency_forecasts,
        COUNT(DISTINCT brand) AS brands_with_urgency_intelligence,
        
        -- Urgency spike prediction capability
        COUNTIF(urgency_classification = 'IMMINENT_URGENCY_SPIKE') AS imminent_urgency_spikes,
        COUNTIF(urgency_classification = 'URGENCY_ESCALATION') AS urgency_escalations,
        COUNTIF(urgency_classification = 'SEASONAL_URGENCY_BUILD') AS seasonal_urgency_builds,
        
        -- High-urgency period predictions
        COUNTIF(timing_intelligence_value = 'HIGH_URGENCY_PERIOD_PREDICTED') AS high_urgency_periods_predicted,
        COUNTIF(timing_intelligence_value = 'TIMING_INTELLIGENCE_AVAILABLE') AS timing_intelligence_opportunities,
        
        -- Urgency change magnitude analysis
        AVG(predicted_urgency_change) AS avg_predicted_urgency_change,
        AVG(CASE WHEN predicted_urgency_change >= 0.15 THEN predicted_urgency_change END) AS avg_major_urgency_spike,
        COUNTIF(predicted_urgency_change >= 0.20) AS major_urgency_spikes_predicted,
        
        -- Seasonal urgency intelligence
        COUNTIF(seasonal_context = 'BLACK_FRIDAY_SEASON' AND forecast_urgency_score_4week >= 0.70) AS black_friday_urgency_predictions,
        COUNTIF(seasonal_context = 'HOLIDAY_SEASON' AND urgency_classification != 'STABLE_URGENCY') AS holiday_urgency_activity,
        
        -- Strategic examples for validation
        STRING_AGG(
          CASE WHEN urgency_classification = 'IMMINENT_URGENCY_SPIKE'
          THEN CONCAT(brand, ': ', urgency_spike_intelligence)
          END, '; ' LIMIT 3
        ) AS urgency_spike_examples
        
      FROM urgency_spike_analysis
    )
    
    SELECT 
      'URGENCY_SPIKE_PREDICTION' AS test_name,
      total_urgency_forecasts,
      brands_with_urgency_intelligence,
      
      -- Urgency spike prediction capability
      imminent_urgency_spikes,
      urgency_escalations,
      major_urgency_spikes_predicted,
      
      -- Timing intelligence value
      high_urgency_periods_predicted,
      timing_intelligence_opportunities,
      
      -- Prediction quality metrics
      ROUND(avg_predicted_urgency_change, 3) AS avg_predicted_urgency_change,
      ROUND(COALESCE(avg_major_urgency_spike, 0), 3) AS avg_major_urgency_spike,
      
      -- Seasonal intelligence
      black_friday_urgency_predictions,
      holiday_urgency_activity,
      seasonal_urgency_builds,
      
      -- Examples
      urgency_spike_examples,
      
      -- Success assessment
      CASE 
        WHEN imminent_urgency_spikes >= 2 AND timing_intelligence_opportunities >= 1
        THEN 'SUCCESS: Can predict urgency spikes for launch timing advantage'
        WHEN major_urgency_spikes_predicted >= 2 AND black_friday_urgency_predictions >= 1
        THEN 'SUCCESS: Strong urgency spike prediction with seasonal intelligence'
        WHEN urgency_escalations >= 3 AND high_urgency_periods_predicted >= 2
        THEN 'PARTIAL_SUCCESS: Some urgency prediction capability'
        ELSE 'NEEDS_IMPROVEMENT: Insufficient urgency spike prediction accuracy'
      END AS urgency_prediction_assessment
      
    FROM urgency_intelligence_summary
    """
    
    print(f"\n⏰ TIER 1 STRATEGIC GOLDMINE: Urgency Spike Prediction")
    print("=" * 60)
    print("Success Metric: Predict when brands will create time pressure")
    print("Business Impact: Launch timing, competitive response speed")
    print("-" * 60)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Urgency Intelligence Coverage:")
            print(f"   Total urgency forecasts: {row['total_urgency_forecasts']}")
            print(f"   Brands with urgency intelligence: {row['brands_with_urgency_intelligence']}")
            
            print(f"\n🚨 Urgency Spike Predictions:")
            print(f"   Imminent urgency spikes: {row['imminent_urgency_spikes']}")
            print(f"   Urgency escalations: {row['urgency_escalations']}")
            print(f"   Major urgency spikes predicted: {row['major_urgency_spikes_predicted']}")
            
            print(f"\n⏱️ Timing Intelligence Value:")
            print(f"   High urgency periods predicted: {row['high_urgency_periods_predicted']}")
            print(f"   Timing intelligence opportunities: {row['timing_intelligence_opportunities']}")
            
            print(f"\n📈 Prediction Quality:")
            print(f"   Average urgency change: {row['avg_predicted_urgency_change']:.3f}")
            print(f"   Average major spike magnitude: {row['avg_major_urgency_spike']:.3f}")
            
            print(f"\n🎄 Seasonal Intelligence:")
            print(f"   Black Friday urgency predictions: {row['black_friday_urgency_predictions']}")
            print(f"   Holiday urgency activity: {row['holiday_urgency_activity']}")
            print(f"   Seasonal urgency builds: {row['seasonal_urgency_builds']}")
            
            if row['urgency_spike_examples']:
                print(f"\n💡 Urgency Spike Examples:")
                for example in row['urgency_spike_examples'].split('; '):
                    if example.strip():
                        print(f"   • {example}")
            
            print(f"\n✅ Assessment: {row['urgency_prediction_assessment']}")
            
            return row['urgency_prediction_assessment'].startswith('SUCCESS')
            
        else:
            print("❌ No urgency spike prediction results")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def validate_tactical_intelligence_tier2():
    """Validate Tier 2 tactical intelligence forecasts"""
    
    query = f"""
    WITH tactical_validation AS (
      SELECT 
        COUNT(*) AS total_tactical_forecasts,
        COUNT(DISTINCT brand) AS brands_with_tactical_intelligence,
        
        -- Platform strategy intelligence
        COUNTIF(platform_strategy_intelligence LIKE 'PLATFORM_STRATEGY_SHIFT%') AS platform_shifts_predicted,
        COUNTIF(platform_strategy_intelligence LIKE 'PLATFORM_CONSOLIDATION%') AS platform_consolidations_predicted,
        AVG(ABS(forecast_cross_platform_pct_4week - current_cross_platform_pct)) AS avg_platform_change_magnitude,
        
        -- Media type intelligence  
        COUNTIF(media_type_intelligence LIKE 'VIDEO_CONTENT_SURGE%') AS video_surges_predicted,
        COUNTIF(media_type_intelligence LIKE 'VIDEO_DOMINANT_STRATEGY%') AS video_dominant_periods_predicted,
        AVG(forecast_video_pct_4week - current_video_pct) AS avg_video_pct_change,
        
        -- Discount intelligence
        COUNTIF(discount_intelligence LIKE 'DISCOUNT_DEEPENING%') AS discount_deepening_predicted,
        COUNTIF(discount_intelligence LIKE 'DEEP_DISCOUNT_STRATEGY%') AS deep_discount_periods_predicted,
        AVG(forecast_avg_discount_4week - current_avg_discount_pct) AS avg_discount_deepening
        
      FROM `{PROJECT_ID}.{DATASET_ID}.v_tactical_intelligence_forecasts`
    )
    
    SELECT 
      'TACTICAL_INTELLIGENCE_TIER2' AS test_name,
      total_tactical_forecasts,
      brands_with_tactical_intelligence,
      
      -- Platform intelligence
      platform_shifts_predicted,
      platform_consolidations_predicted,
      ROUND(avg_platform_change_magnitude, 3) AS avg_platform_change_magnitude,
      
      -- Media intelligence
      video_surges_predicted,
      video_dominant_periods_predicted, 
      ROUND(avg_video_pct_change, 3) AS avg_video_pct_change,
      
      -- Discount intelligence
      discount_deepening_predicted,
      deep_discount_periods_predicted,
      ROUND(avg_discount_deepening, 1) AS avg_discount_deepening,
      
      -- Overall tactical intelligence assessment
      CASE 
        WHEN platform_shifts_predicted + video_surges_predicted + discount_deepening_predicted >= 5
        THEN 'STRONG_TACTICAL_INTELLIGENCE: Multiple tactical predictions across categories'
        WHEN platform_shifts_predicted >= 2 OR video_surges_predicted >= 2 OR discount_deepening_predicted >= 2
        THEN 'MODERATE_TACTICAL_INTELLIGENCE: Some tactical prediction capability'
        ELSE 'BASIC_TACTICAL_INTELLIGENCE: Limited tactical forecasting'
      END AS tactical_intelligence_assessment
      
    FROM tactical_validation
    """
    
    print(f"\n🎯 TIER 2 TACTICAL INTELLIGENCE VALIDATION")
    print("=" * 50)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Tactical Coverage: {row['total_tactical_forecasts']} forecasts across {row['brands_with_tactical_intelligence']} brands")
            
            print(f"\n📱 Platform Strategy Intelligence:")
            print(f"   Platform shifts predicted: {row['platform_shifts_predicted']}")
            print(f"   Platform consolidations: {row['platform_consolidations_predicted']}")
            print(f"   Avg platform change magnitude: {row['avg_platform_change_magnitude']:.3f}")
            
            print(f"\n🎬 Media Type Intelligence:")
            print(f"   Video surges predicted: {row['video_surges_predicted']}")
            print(f"   Video dominant periods: {row['video_dominant_periods_predicted']}")
            print(f"   Avg video % change: {row['avg_video_pct_change']:+.3f}")
            
            print(f"\n💰 Discount Intelligence:")
            print(f"   Discount deepening predicted: {row['discount_deepening_predicted']}")
            print(f"   Deep discount periods predicted: {row['deep_discount_periods_predicted']}")
            print(f"   Avg discount deepening: {row['avg_discount_deepening']:+.1f}%")
            
            print(f"\n✅ Assessment: {row['tactical_intelligence_assessment']}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 STRATEGIC GOLDMINE FORECASTING VALIDATION")
    print("=" * 60)
    print("TIER 1 STRATEGIC GOLDMINE (Highest ROI):")
    print("  1. promotional_intensity shifts - Predict competitor discounts")
    print("  2. primary_angle pivots - Predict messaging strategy changes") 
    print("  3. urgency_score spikes - Predict time pressure campaigns")
    print("=" * 60)
    
    # Test Tier 1 Strategic Goldmine forecasting
    promotional_success = test_promotional_intensity_forecasting()
    angle_pivot_success = test_messaging_angle_pivot_prediction()
    urgency_success = test_urgency_spike_prediction()
    
    # Test Tier 2 Tactical Intelligence
    validate_tactical_intelligence_tier2()
    
    # Overall assessment
    tier1_success_count = sum([promotional_success, angle_pivot_success, urgency_success])
    
    print(f"\n" + "=" * 60)
    print(f"🏆 STRATEGIC GOLDMINE VALIDATION RESULTS:")
    print(f"   Tier 1 Success Rate: {tier1_success_count}/3 strategic goldmine forecasts")
    
    if tier1_success_count >= 2:
        print("✅ STRATEGIC GOLDMINE VALIDATION PASSED")
        print("🎯 Achievement: High-ROI competitive intelligence forecasting")
        print("💰 Business Impact: Pricing strategy, competitive positioning, launch timing")
        print("🚀 Capability: 4-week strategic forecasting with business recommendations")
    elif tier1_success_count >= 1:
        print("⚠️  PARTIAL SUCCESS: Some strategic forecasting capability")
        print("🔍 Recommend: Focus on successful forecasting areas, improve weaker predictions")
    else:
        print("❌ STRATEGIC GOLDMINE NEEDS DEVELOPMENT")
        print("🔧 Action Required: Improve forecasting models, add more historical data")
    
    print("=" * 60)