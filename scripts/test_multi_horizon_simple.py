#!/usr/bin/env python3
"""
Multi-Horizon Forecasting - Simplified Implementation
Focus on core business value with cleaner SQL
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def test_multi_horizon_simplified():
    """
    Simplified multi-horizon forecasting focusing on business value
    """
    
    print("🔮 MULTI-HORIZON FORECASTING - SIMPLIFIED")
    print("=" * 80)
    print("24-hour: Flash response | 7-day: Campaign tuning | 30-day: Strategic planning")
    print("-" * 80)
    
    # Simplified query focusing on core metrics
    query = f"""
    WITH daily_metrics AS (
      SELECT 
        brand,
        DATE(start_timestamp) AS date,
        
        -- Core metrics
        AVG(promotional_intensity) AS daily_promo,
        AVG(urgency_score) AS daily_urgency,
        AVG(brand_voice_score) AS daily_brand_voice,
        COUNT(*) AS daily_volume
        
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
      WHERE DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
      GROUP BY brand, date
    ),
    
    -- Calculate different moving averages for different horizons
    horizon_metrics AS (
      SELECT 
        brand,
        date,
        daily_promo,
        daily_urgency,
        daily_brand_voice,
        daily_volume,
        
        -- 24-hour: Use yesterday's value as baseline
        LAG(daily_promo, 1) OVER (PARTITION BY brand ORDER BY date) AS promo_1day_ago,
        LAG(daily_urgency, 1) OVER (PARTITION BY brand ORDER BY date) AS urgency_1day_ago,
        
        -- 7-day: Weekly average
        AVG(daily_promo) OVER (
          PARTITION BY brand ORDER BY date 
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS promo_7day_avg,
        AVG(daily_urgency) OVER (
          PARTITION BY brand ORDER BY date 
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS urgency_7day_avg,
        
        -- 30-day: Monthly average
        AVG(daily_promo) OVER (
          PARTITION BY brand ORDER BY date 
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS promo_30day_avg,
        AVG(daily_brand_voice) OVER (
          PARTITION BY brand ORDER BY date 
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS brand_voice_30day_avg,
        
        -- Volatility measures
        STDDEV(daily_promo) OVER (
          PARTITION BY brand ORDER BY date 
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS promo_7day_volatility,
        
        -- Volume spikes
        daily_volume / NULLIF(AVG(daily_volume) OVER (
          PARTITION BY brand ORDER BY date 
          ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ), 0) AS volume_spike_ratio
        
      FROM daily_metrics
    ),
    
    -- Create forecasts and confidence scores
    forecasts AS (
      SELECT 
        brand,
        date,
        
        -- Current state
        ROUND(daily_promo, 3) AS current_promo,
        ROUND(daily_urgency, 3) AS current_urgency,
        
        -- 24-HOUR FORECAST: Simple momentum
        ROUND(LEAST(1.0, GREATEST(0.0, 
          daily_promo + (daily_promo - COALESCE(promo_1day_ago, daily_promo))
        )), 3) AS forecast_24h_promo,
        
        -- 7-DAY FORECAST: Trend-adjusted average
        ROUND(LEAST(1.0, GREATEST(0.0,
          promo_7day_avg + 0.3 * (promo_7day_avg - COALESCE(
            LAG(promo_7day_avg, 7) OVER (PARTITION BY brand ORDER BY date),
            promo_7day_avg
          ))
        )), 3) AS forecast_7day_promo,
        
        -- 30-DAY FORECAST: Stable average
        ROUND(promo_30day_avg, 3) AS forecast_30day_promo,
        ROUND(brand_voice_30day_avg, 3) AS forecast_30day_brand_voice,
        
        -- Confidence based on volatility
        CASE
          WHEN promo_7day_volatility < 0.1 THEN 'HIGH'
          WHEN promo_7day_volatility < 0.2 THEN 'MEDIUM'
          ELSE 'LOW'
        END AS confidence_level,
        
        -- Spike detection
        CASE
          WHEN volume_spike_ratio > 2.0 THEN 'VOLUME_SURGE'
          WHEN daily_urgency > 0.8 THEN 'URGENCY_SPIKE'
          WHEN daily_promo > 0.8 THEN 'PROMO_SPIKE'
          ELSE 'NORMAL'
        END AS signal_type,
        
        -- Business actions
        CASE
          WHEN volume_spike_ratio > 2.0 AND daily_urgency > 0.7 
            THEN '🚨 IMMEDIATE: Competitor surge detected'
          WHEN daily_promo > promo_7day_avg * 1.5 
            THEN '⚡ TACTICAL: Promotional escalation'
          WHEN ABS(brand_voice_30day_avg - 0.5) > 0.3
            THEN '🎯 STRATEGIC: Brand positioning shift'
          ELSE '✓ STABLE: Normal operations'
        END AS recommended_action
        
      FROM horizon_metrics
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY)
    )
    
    SELECT 
      brand,
      date,
      current_promo,
      forecast_24h_promo,
      forecast_7day_promo,
      forecast_30day_promo,
      confidence_level,
      signal_type,
      recommended_action,
      
      -- Horizon divergence (disagreement between forecasts)
      ROUND(ABS(forecast_24h_promo - forecast_30day_promo), 3) AS horizon_divergence
      
    FROM forecasts
    ORDER BY brand, date DESC
    LIMIT 100
    """
    
    try:
        results = client.query(query).to_dataframe()
        
        if not results.empty:
            print(f"\n📊 Generated {len(results)} multi-horizon forecasts")
            
            # Analyze by brand
            for brand in results['brand'].unique()[:3]:  # Top 3 brands
                brand_data = results[results['brand'] == brand].head(5)
                
                print(f"\n{'='*60}")
                print(f"🏢 {brand} - MULTI-HORIZON ANALYSIS")
                print(f"{'='*60}")
                
                # Latest forecast
                latest = brand_data.iloc[0]
                print(f"\n📅 Latest Date: {latest['date']}")
                print(f"Current Promotional Intensity: {latest['current_promo']:.1%}")
                
                print(f"\n🔮 FORECASTS:")
                print(f"  24-hour: {latest['forecast_24h_promo']:.1%}")
                print(f"  7-day:   {latest['forecast_7day_promo']:.1%}")
                print(f"  30-day:  {latest['forecast_30day_promo']:.1%}")
                print(f"  Confidence: {latest['confidence_level']}")
                print(f"  Divergence: {latest['horizon_divergence']:.1%}")
                
                # Recent signals
                signals = brand_data[brand_data['signal_type'] != 'NORMAL']
                if not signals.empty:
                    print(f"\n🚨 RECENT SIGNALS:")
                    for _, signal in signals.iterrows():
                        print(f"  {signal['date']}: {signal['signal_type']}")
                
                # Recommended actions
                actions = brand_data[~brand_data['recommended_action'].str.contains('STABLE')]
                if not actions.empty:
                    print(f"\n💡 RECOMMENDED ACTIONS:")
                    for _, action in actions.head(3).iterrows():
                        print(f"  {action['recommended_action']}")
            
            # System-wide analysis
            print(f"\n{'='*80}")
            print("📈 SYSTEM-WIDE INSIGHTS")
            print(f"{'='*80}")
            
            # Confidence distribution
            high_conf = (results['confidence_level'] == 'HIGH').sum() / len(results)
            print(f"High Confidence Predictions: {high_conf:.1%}")
            
            # Signal distribution
            signal_counts = results['signal_type'].value_counts()
            if 'NORMAL' in signal_counts.index:
                abnormal_rate = 1 - (signal_counts['NORMAL'] / len(results))
                print(f"Abnormal Signal Rate: {abnormal_rate:.1%}")
            
            # Average divergence
            avg_divergence = results['horizon_divergence'].mean()
            print(f"Average Horizon Divergence: {avg_divergence:.1%}")
            
            # High divergence cases (uncertainty)
            high_divergence = results[results['horizon_divergence'] > 0.3]
            if not high_divergence.empty:
                print(f"\n⚠️ HIGH UNCERTAINTY PERIODS: {len(high_divergence)}")
                print("(When short-term and long-term forecasts strongly disagree)")
                for _, case in high_divergence.head(3).iterrows():
                    print(f"  {case['brand']} on {case['date']}: {case['horizon_divergence']:.1%} divergence")
            
            print(f"\n{'='*80}")
            print("✅ MULTI-HORIZON VALUE DELIVERED:")
            print("  • 24h forecasts for tactical responses")
            print("  • 7-day forecasts for campaign optimization")
            print("  • 30-day forecasts for strategic planning")
            print("  • Confidence scoring for decision support")
            print("  • Divergence detection for uncertainty management")
            
            return True
            
        else:
            print("❌ No forecast results")
            return False
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_multi_horizon_simplified()
    
    if success:
        print("\n✅ MULTI-HORIZON FORECASTING: OPERATIONAL")
        print("🎯 Advanced feature successfully implemented")
    else:
        print("\n⚠️ MULTI-HORIZON FORECASTING: NEEDS DEBUGGING")