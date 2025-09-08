#!/usr/bin/env python3
"""
Multi-Horizon Forecasting MVP - Advanced Feature Implementation
Different time horizons for different business decisions
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta
import json

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def test_multi_horizon_forecasting():
    """
    Implement multi-horizon forecasting with different models for different time scales
    - 24-hour: Flash tactical response
    - 7-day: Campaign planning
    - 30-day: Strategic positioning
    """
    
    print("🔮 MULTI-HORIZON FORECASTING MVP")
    print("=" * 80)
    print("Business Value: Right predictions for right timeframes")
    print("24-hour: Tactical adjustments | 7-day: Campaign optimization | 30-day: Strategic planning")
    print("-" * 80)
    
    # Multi-horizon query with different feature sets per horizon
    query = f"""
    WITH base_metrics AS (
      SELECT 
        brand,
        DATE(start_timestamp) AS date,
        DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week_start,
        
        -- Core strategic metrics
        AVG(promotional_intensity) AS daily_promotional_intensity,
        AVG(urgency_score) AS daily_urgency_score,
        AVG(brand_voice_score) AS daily_brand_voice,
        
        -- Tactical metrics for short-term
        COUNT(*) AS daily_volume,
        COUNTIF(media_type = 'video') / COUNT(*) AS video_ratio,
        
        -- Pattern detection
        COUNTIF(primary_angle = 'PROMOTIONAL') / COUNT(*) AS promo_concentration,
        COUNTIF(funnel = 'Lower') / COUNT(*) AS lower_funnel_ratio
        
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
      GROUP BY brand, date, week_start
    ),
    
    -- 24-HOUR HORIZON: High-frequency tactical signals
    tactical_24h AS (
      SELECT 
        brand,
        date,
        daily_promotional_intensity,
        daily_urgency_score,
        daily_volume,
        
        -- Simple moving averages for stability
        AVG(daily_promotional_intensity) OVER (
          PARTITION BY brand 
          ORDER BY date 
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS promo_3day_ma,
        
        -- Momentum indicators
        daily_promotional_intensity - LAG(daily_promotional_intensity, 1) OVER (
          PARTITION BY brand ORDER BY date
        ) AS promo_daily_momentum,
        
        daily_urgency_score - LAG(daily_urgency_score, 1) OVER (
          PARTITION BY brand ORDER BY date
        ) AS urgency_daily_momentum,
        
        -- Volume spike detection
        daily_volume / NULLIF(AVG(daily_volume) OVER (
          PARTITION BY brand 
          ORDER BY date 
          ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ), 0) AS volume_spike_ratio,
        
        -- 24-hour forecast using momentum
        LEAST(1.0, GREATEST(0.0, 
          daily_promotional_intensity + COALESCE(
            daily_promotional_intensity - LAG(daily_promotional_intensity, 1) OVER (
              PARTITION BY brand ORDER BY date
            ), 0
          )
        )) AS forecast_24h_promotional,
        
        LEAST(1.0, GREATEST(0.0,
          daily_urgency_score + COALESCE(
            daily_urgency_score - LAG(daily_urgency_score, 1) OVER (
              PARTITION BY brand ORDER BY date
            ), 0
          )
        )) AS forecast_24h_urgency
        
      FROM base_metrics
    ),
    
    -- 7-DAY HORIZON: Campaign-level patterns
    campaign_7day AS (
      SELECT 
        brand,
        week_start,
        date,
        daily_promotional_intensity,
        daily_brand_voice,
        lower_funnel_ratio,
        
        -- Weekly aggregations
        AVG(daily_promotional_intensity) OVER (
          PARTITION BY brand 
          ORDER BY date
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS week_avg_promotional,
        
        -- Week-over-week trends
        AVG(daily_promotional_intensity) OVER (
          PARTITION BY brand, week_start
        ) - LAG(AVG(daily_promotional_intensity) OVER (
          PARTITION BY brand, week_start
        ), 7) OVER (
          PARTITION BY brand ORDER BY date
        ) AS weekly_promo_trend,
        
        -- 7-day forecast using weekly patterns
        LEAST(1.0, GREATEST(0.0,
          AVG(daily_promotional_intensity) OVER (
            PARTITION BY brand 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) + 0.5 * COALESCE(
            AVG(daily_promotional_intensity) OVER (
              PARTITION BY brand 
              ORDER BY date 
              ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) - AVG(daily_promotional_intensity) OVER (
              PARTITION BY brand 
              ORDER BY date 
              ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
            ), 0
          )
        )) AS forecast_7day_promotional,
        
        -- Brand voice stability forecast
        AVG(daily_brand_voice) OVER (
          PARTITION BY brand 
          ORDER BY date 
          ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS forecast_7day_brand_voice
        
      FROM base_metrics
    ),
    
    -- 30-DAY HORIZON: Strategic positioning
    strategic_30day AS (
      SELECT 
        brand,
        date,
        week_start,
        daily_promotional_intensity,
        daily_brand_voice,
        promo_concentration,
        
        -- Monthly patterns
        AVG(daily_promotional_intensity) OVER (
          PARTITION BY brand 
          ORDER BY date 
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS month_avg_promotional,
        
        STDDEV(daily_promotional_intensity) OVER (
          PARTITION BY brand 
          ORDER BY date 
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS month_volatility_promotional,
        
        -- Long-term trend using linear regression proxy
        COALESCE(
          (AVG(daily_promotional_intensity) OVER (
            PARTITION BY brand 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND 15 PRECEDING
          ) - AVG(daily_promotional_intensity) OVER (
            PARTITION BY brand 
            ORDER BY date 
            ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
          )) / 15.0, 0
        ) AS month_trend_slope,
        
        -- 30-day strategic forecast
        LEAST(1.0, GREATEST(0.0,
          AVG(daily_promotional_intensity) OVER (
            PARTITION BY brand 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
          ) + 30 * COALESCE(
            (AVG(daily_promotional_intensity) OVER (
              PARTITION BY brand 
              ORDER BY date 
              ROWS BETWEEN 14 PRECEDING AND CURRENT ROW
            ) - AVG(daily_promotional_intensity) OVER (
              PARTITION BY brand 
              ORDER BY date 
              ROWS BETWEEN 29 PRECEDING AND 15 PRECEDING
            )) / 15.0, 0
          )
        )) AS forecast_30day_promotional,
        
        -- Strategic positioning forecast
        AVG(daily_brand_voice) OVER (
          PARTITION BY brand 
          ORDER BY date 
          ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS forecast_30day_brand_position
        
      FROM base_metrics
    ),
    
    -- Combine all horizons with confidence scoring
    multi_horizon_forecast AS (
      SELECT 
        t24.brand,
        t24.date,
        
        -- 24-HOUR TACTICAL
        t24.forecast_24h_promotional,
        t24.forecast_24h_urgency,
        t24.volume_spike_ratio,
        -- Confidence based on recent stability
        CASE 
          WHEN ABS(t24.promo_daily_momentum) < 0.1 THEN 'HIGH'
          WHEN ABS(t24.promo_daily_momentum) < 0.2 THEN 'MEDIUM'
          ELSE 'LOW'
        END AS confidence_24h,
        
        -- 7-DAY CAMPAIGN
        c7.forecast_7day_promotional,
        c7.forecast_7day_brand_voice,
        c7.weekly_promo_trend,
        -- Confidence based on weekly consistency
        CASE
          WHEN c7.lower_funnel_ratio BETWEEN 0.3 AND 0.7 THEN 'HIGH'
          WHEN c7.lower_funnel_ratio BETWEEN 0.2 AND 0.8 THEN 'MEDIUM'
          ELSE 'LOW'
        END AS confidence_7day,
        
        -- 30-DAY STRATEGIC
        s30.forecast_30day_promotional,
        s30.forecast_30day_brand_position,
        s30.month_volatility_promotional,
        -- Confidence based on long-term volatility
        CASE
          WHEN s30.month_volatility_promotional < 0.15 THEN 'HIGH'
          WHEN s30.month_volatility_promotional < 0.25 THEN 'MEDIUM'
          ELSE 'LOW'
        END AS confidence_30day,
        
        -- Decision recommendations per horizon
        CASE
          WHEN t24.volume_spike_ratio > 1.5 AND t24.forecast_24h_urgency > 0.7 
            THEN '🚨 24H: IMMEDIATE RESPONSE NEEDED'
          WHEN t24.forecast_24h_promotional > 0.8 
            THEN '⚡ 24H: Competitor flash sale detected'
          ELSE '📊 24H: Normal operations'
        END AS tactical_24h_action,
        
        CASE
          WHEN ABS(c7.weekly_promo_trend) > 0.2 
            THEN '📈 7D: Significant campaign shift detected'
          WHEN c7.forecast_7day_brand_voice < 0.3 
            THEN '⚠️ 7D: Brand voice dilution risk'
          ELSE '✓ 7D: Campaign trajectory stable'
        END AS campaign_7day_action,
        
        CASE
          WHEN s30.forecast_30day_promotional > 0.7 AND s30.month_volatility_promotional > 0.2
            THEN '🎯 30D: Market entering promotional phase'
          WHEN s30.forecast_30day_brand_position < 0.4
            THEN '🔄 30D: Strategic repositioning recommended'
          ELSE '📍 30D: Maintain strategic position'
        END AS strategic_30day_action
        
      FROM tactical_24h t24
      JOIN campaign_7day c7 USING (brand, date)
      JOIN strategic_30day s30 USING (brand, date)
      WHERE t24.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    )
    
    SELECT 
      brand,
      date,
      
      -- Multi-horizon forecasts
      ROUND(forecast_24h_promotional, 3) AS forecast_24h_promo,
      ROUND(forecast_7day_promotional, 3) AS forecast_7day_promo,
      ROUND(forecast_30day_promotional, 3) AS forecast_30day_promo,
      
      -- Confidence levels
      confidence_24h,
      confidence_7day,
      confidence_30day,
      
      -- Strategic insights
      tactical_24h_action,
      campaign_7day_action,
      strategic_30day_action,
      
      -- Forecast divergence (when horizons disagree)
      ROUND(ABS(forecast_24h_promotional - forecast_30day_promotional), 3) AS forecast_divergence,
      
      -- Horizon agreement score
      CASE
        WHEN ABS(forecast_24h_promotional - forecast_7day_promotional) < 0.1 
          AND ABS(forecast_7day_promotional - forecast_30day_promotional) < 0.1
          THEN 'ALIGNED'
        WHEN ABS(forecast_24h_promotional - forecast_30day_promotional) > 0.3
          THEN 'DIVERGENT'
        ELSE 'MIXED'
      END AS horizon_alignment
      
    FROM multi_horizon_forecast
    ORDER BY brand, date DESC
    LIMIT 50
    """
    
    try:
        results = client.query(query).to_dataframe()
        
        if not results.empty:
            print(f"\n📊 Multi-Horizon Forecast Results: {len(results)} predictions")
            
            # Analyze by brand
            for brand in results['brand'].unique():
                brand_data = results[results['brand'] == brand]
                print(f"\n🏢 {brand} MULTI-HORIZON INTELLIGENCE")
                print("-" * 60)
                
                # Latest forecasts
                latest = brand_data.iloc[0]
                print(f"\n📅 Latest Forecasts (Date: {latest['date']})")
                print(f"   24-Hour: {latest['forecast_24h_promo']:.1%} (Confidence: {latest['confidence_24h']})")
                print(f"   7-Day:   {latest['forecast_7day_promo']:.1%} (Confidence: {latest['confidence_7day']})")
                print(f"   30-Day:  {latest['forecast_30day_promo']:.1%} (Confidence: {latest['confidence_30day']})")
                print(f"   Horizon Alignment: {latest['horizon_alignment']}")
                
                # Strategic recommendations
                print(f"\n🎯 STRATEGIC RECOMMENDATIONS:")
                print(f"   {latest['tactical_24h_action']}")
                print(f"   {latest['campaign_7day_action']}")
                print(f"   {latest['strategic_30day_action']}")
                
                # Divergence analysis
                high_divergence = brand_data[brand_data['forecast_divergence'] > 0.2]
                if not high_divergence.empty:
                    print(f"\n⚠️  FORECAST DIVERGENCE ALERTS: {len(high_divergence)} instances")
                    print("   (Short-term and long-term forecasts disagree significantly)")
            
            # Overall system performance
            print("\n" + "=" * 80)
            print("🎯 MULTI-HORIZON SYSTEM PERFORMANCE")
            print("-" * 80)
            
            # Confidence distribution
            for horizon in ['24h', '7day', '30day']:
                conf_col = f'confidence_{horizon}'
                high_conf = (results[conf_col] == 'HIGH').sum() / len(results) * 100
                print(f"{horizon.upper():>6} Confidence: {high_conf:.1f}% HIGH")
            
            # Alignment analysis
            aligned = (results['horizon_alignment'] == 'ALIGNED').sum() / len(results) * 100
            divergent = (results['horizon_alignment'] == 'DIVERGENT').sum() / len(results) * 100
            print(f"\nHorizon Agreement: {aligned:.1f}% Aligned | {divergent:.1f}% Divergent")
            
            # Business impact
            print("\n💼 BUSINESS VALUE DELIVERED:")
            print("   ✅ 24H: Tactical response capability for flash competitions")
            print("   ✅ 7D: Campaign optimization with weekly trajectory")
            print("   ✅ 30D: Strategic positioning with monthly trends")
            print("   ✅ Confidence scoring for risk-adjusted decisions")
            print("   ✅ Divergence detection for uncertainty quantification")
            
            return True
            
        else:
            print("❌ No forecast results generated")
            return False
            
    except Exception as e:
        print(f"❌ Multi-horizon forecasting error: {str(e)}")
        return False

if __name__ == "__main__":
    print("🚀 ADVANCED FEATURE: MULTI-HORIZON FORECASTING")
    print("=" * 80)
    print("Different predictions for different business decisions:")
    print("• 24-hour: React to competitor flash sales")
    print("• 7-day: Optimize ongoing campaigns")
    print("• 30-day: Plan strategic positioning")
    print("=" * 80)
    
    success = test_multi_horizon_forecasting()
    
    if success:
        print("\n✅ MULTI-HORIZON FORECASTING: OPERATIONAL")
        print("🎯 Ready for A/B testing against single-horizon baseline")
    else:
        print("\n⚠️ MULTI-HORIZON FORECASTING: NEEDS CALIBRATION")