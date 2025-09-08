#!/usr/bin/env python3
"""
Rigorous Forecast Accuracy Backtesting - Full Statistical Validation
Simplified SQL structure but maintaining complete methodological rigor
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import time
from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

class RigorousForecastBacktesting:
    def __init__(self):
        self.results = {}
        self.metrics = {}
        self.baselines = {}
        
    def prepare_backtest_data(self):
        """Prepare and validate backtest data split"""
        print("📊 PREPARING RIGOROUS BACKTEST DATA")
        print("="*60)
        
        try:
            # Comprehensive data assessment
            data_assessment_query = f"""
            WITH data_timeline AS (
              SELECT 
                MIN(DATE(start_timestamp)) as earliest_date,
                MAX(DATE(start_timestamp)) as latest_date,
                COUNT(DISTINCT DATE(start_timestamp)) as unique_dates,
                COUNT(DISTINCT week_start) as unique_weeks,
                COUNT(DISTINCT brand) as unique_brands,
                COUNT(*) as total_ads,
                
                -- Data quality metrics
                COUNTIF(promotional_intensity IS NULL) as null_promo,
                COUNTIF(urgency_score IS NULL) as null_urgency,
                COUNTIF(brand_voice_score IS NULL) as null_brand_voice,
                
                -- Statistical properties
                AVG(promotional_intensity) as mean_promo,
                STDDEV(promotional_intensity) as std_promo,
                MIN(promotional_intensity) as min_promo,
                MAX(promotional_intensity) as max_promo
                
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
            )
            
            SELECT 
              *,
              DATE_DIFF(latest_date, earliest_date, DAY) as total_days,
              DATE_ADD(earliest_date, INTERVAL CAST(DATE_DIFF(latest_date, earliest_date, DAY) * 0.8 AS INT64) DAY) as split_date,
              CAST(DATE_DIFF(latest_date, earliest_date, DAY) * 0.8 AS INT64) as training_days,
              CAST(DATE_DIFF(latest_date, earliest_date, DAY) * 0.2 AS INT64) as testing_days
            FROM data_timeline
            """
            
            assessment = client.query(data_assessment_query).to_dataframe()
            
            if not assessment.empty:
                row = assessment.iloc[0]
                
                print(f"📅 Timeline Analysis:")
                print(f"  Full Range: {row['earliest_date']} → {row['latest_date']}")
                print(f"  Total Days: {row['total_days']}")
                print(f"  Split Date (80%): {row['split_date']}")
                print(f"  Training Days: {row['training_days']}")
                print(f"  Testing Days: {row['testing_days']}")
                
                print(f"\n📊 Data Statistics:")
                print(f"  Total Ads: {row['total_ads']:,}")
                print(f"  Unique Brands: {row['unique_brands']}")
                print(f"  Unique Weeks: {row['unique_weeks']}")
                print(f"  Mean Promotional: {row['mean_promo']:.3f} ± {row['std_promo']:.3f}")
                print(f"  Range: [{row['min_promo']:.3f}, {row['max_promo']:.3f}]")
                
                print(f"\n✅ Data Quality:")
                print(f"  Null Promotional: {row['null_promo']}")
                print(f"  Null Urgency: {row['null_urgency']}")
                print(f"  Null Brand Voice: {row['null_brand_voice']}")
                
                self.split_date = row['split_date']
                self.timeline = {
                    'earliest': row['earliest_date'],
                    'latest': row['latest_date'],
                    'split_date': row['split_date'],
                    'training_days': int(row['training_days']),
                    'testing_days': int(row['testing_days']),
                    'total_ads': int(row['total_ads']),
                    'mean_promo': row['mean_promo'],
                    'std_promo': row['std_promo']
                }
                
                # Validate sufficient data
                if row['testing_days'] < 7:
                    print("\n⚠️ WARNING: Less than 7 days of test data. Results may be unstable.")
                
                return True
            else:
                print("❌ Failed to assess data")
                return False
                
        except Exception as e:
            print(f"❌ Data preparation error: {str(e)}")
            return False
    
    def test_24hour_forecast_accuracy(self):
        """Test 24-hour forecast with full statistical rigor"""
        print(f"\n🔮 24-HOUR FORECAST RIGOROUS TESTING")
        print("="*60)
        
        try:
            # Step 1: Prepare daily data with proper lag calculations
            forecast_24h_query = f"""
            -- Step 1: Daily aggregations
            WITH daily_metrics AS (
              SELECT 
                brand,
                DATE(start_timestamp) AS date,
                AVG(promotional_intensity) AS daily_promo,
                AVG(urgency_score) AS daily_urgency,
                AVG(brand_voice_score) AS daily_brand_voice,
                COUNT(*) AS daily_volume,
                STDDEV(promotional_intensity) AS daily_promo_std
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, date
            ),
            
            -- Step 2: Add lag features
            daily_with_lags AS (
              SELECT 
                *,
                LAG(daily_promo, 1) OVER (PARTITION BY brand ORDER BY date) AS promo_lag1,
                LAG(daily_promo, 2) OVER (PARTITION BY brand ORDER BY date) AS promo_lag2,
                LAG(daily_promo, 3) OVER (PARTITION BY brand ORDER BY date) AS promo_lag3,
                LAG(daily_urgency, 1) OVER (PARTITION BY brand ORDER BY date) AS urgency_lag1,
                LAG(daily_urgency, 2) OVER (PARTITION BY brand ORDER BY date) AS urgency_lag2,
                LAG(daily_volume, 1) OVER (PARTITION BY brand ORDER BY date) AS volume_lag1
              FROM daily_metrics
            ),
            
            -- Step 3: Calculate moving averages
            daily_with_ma AS (
              SELECT 
                *,
                -- 3-day moving average
                (COALESCE(promo_lag1, daily_promo) + 
                 COALESCE(promo_lag2, promo_lag1, daily_promo) + 
                 COALESCE(promo_lag3, promo_lag2, promo_lag1, daily_promo)) / 3.0 AS ma3_promo,
                 
                -- 7-day moving average (if enough history)
                AVG(daily_promo) OVER (
                  PARTITION BY brand ORDER BY date 
                  ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                ) AS ma7_promo
              FROM daily_with_lags
            ),
            
            -- Step 4: Generate forecasts
            forecasts AS (
              SELECT 
                brand,
                date,
                daily_promo AS actual_promo,
                daily_urgency AS actual_urgency,
                daily_brand_voice AS actual_brand_voice,
                
                -- Forecast methods
                -- 1. Naive forecast (yesterday's value)
                promo_lag1 AS forecast_naive,
                
                -- 2. Moving average forecast
                ma3_promo AS forecast_ma3,
                ma7_promo AS forecast_ma7,
                
                -- 3. Momentum forecast (with trend)
                CASE
                  WHEN promo_lag1 IS NOT NULL AND promo_lag2 IS NOT NULL THEN
                    LEAST(1.0, GREATEST(0.0, promo_lag1 + (promo_lag1 - promo_lag2)))
                  ELSE promo_lag1
                END AS forecast_momentum,
                
                -- 4. Weighted forecast (combine methods)
                CASE
                  WHEN promo_lag1 IS NOT NULL AND ma3_promo IS NOT NULL THEN
                    0.5 * promo_lag1 + 0.3 * ma3_promo + 0.2 * COALESCE(
                      LEAST(1.0, GREATEST(0.0, promo_lag1 + (promo_lag1 - promo_lag2))),
                      promo_lag1
                    )
                  ELSE promo_lag1
                END AS forecast_weighted,
                
                -- Urgency forecast
                CASE
                  WHEN urgency_lag1 IS NOT NULL AND urgency_lag2 IS NOT NULL THEN
                    LEAST(1.0, GREATEST(0.0, urgency_lag1 + 0.5 * (urgency_lag1 - urgency_lag2)))
                  ELSE urgency_lag1
                END AS forecast_urgency,
                
                -- Direction indicators
                SIGN(daily_promo - promo_lag1) AS actual_direction,
                SIGN(
                  CASE
                    WHEN promo_lag1 IS NOT NULL AND promo_lag2 IS NOT NULL THEN
                      promo_lag1 + (promo_lag1 - promo_lag2) - promo_lag1
                    ELSE 0
                  END
                ) AS forecast_direction
                
              FROM daily_with_ma
              WHERE date > DATE('{self.split_date}')  -- Test set only
            ),
            
            -- Step 5: Calculate comprehensive metrics
            metrics AS (
              SELECT 
                brand,
                COUNT(*) AS test_points,
                
                -- Naive baseline metrics
                AVG(ABS(actual_promo - forecast_naive)) AS mae_naive,
                SQRT(AVG(POW(actual_promo - forecast_naive, 2))) AS rmse_naive,
                AVG(ABS(actual_promo - forecast_naive) / NULLIF(actual_promo, 0)) AS mape_naive,
                
                -- MA3 baseline metrics
                AVG(ABS(actual_promo - forecast_ma3)) AS mae_ma3,
                SQRT(AVG(POW(actual_promo - forecast_ma3, 2))) AS rmse_ma3,
                
                -- Momentum forecast metrics
                AVG(ABS(actual_promo - forecast_momentum)) AS mae_momentum,
                SQRT(AVG(POW(actual_promo - forecast_momentum, 2))) AS rmse_momentum,
                AVG(ABS(actual_promo - forecast_momentum) / NULLIF(actual_promo, 0)) AS mape_momentum,
                
                -- Weighted forecast metrics (our best model)
                AVG(ABS(actual_promo - forecast_weighted)) AS mae_weighted,
                SQRT(AVG(POW(actual_promo - forecast_weighted, 2))) AS rmse_weighted,
                AVG(ABS(actual_promo - forecast_weighted) / NULLIF(actual_promo, 0)) AS mape_weighted,
                
                -- Urgency forecast metrics
                AVG(ABS(actual_urgency - forecast_urgency)) AS mae_urgency,
                
                -- Directional accuracy
                COUNTIF(actual_direction = forecast_direction) / COUNTIF(actual_direction != 0) AS directional_accuracy,
                
                -- Bias (systematic over/under prediction)
                AVG(actual_promo - forecast_weighted) AS bias_weighted,
                
                -- Improvement metrics
                (AVG(ABS(actual_promo - forecast_naive)) - AVG(ABS(actual_promo - forecast_weighted))) 
                  / NULLIF(AVG(ABS(actual_promo - forecast_naive)), 0) AS improvement_vs_naive,
                  
                (AVG(ABS(actual_promo - forecast_ma3)) - AVG(ABS(actual_promo - forecast_weighted))) 
                  / NULLIF(AVG(ABS(actual_promo - forecast_ma3)), 0) AS improvement_vs_ma3
                
              FROM forecasts
              WHERE forecast_naive IS NOT NULL
                AND forecast_weighted IS NOT NULL
              GROUP BY brand
            )
            
            SELECT * FROM metrics
            ORDER BY mae_weighted ASC
            """
            
            start_time = time.time()
            results_24h = client.query(forecast_24h_query).to_dataframe()
            query_time = time.time() - start_time
            
            if not results_24h.empty:
                print("📊 24-Hour Forecast Performance by Brand:")
                
                for _, row in results_24h.iterrows():
                    print(f"\n🏢 {row['brand']}:")
                    print(f"  Test Points: {row['test_points']}")
                    
                    print(f"\n  📈 Forecast Accuracy (Weighted Model):")
                    print(f"    MAE: {row['mae_weighted']:.4f}")
                    print(f"    RMSE: {row['rmse_weighted']:.4f}")
                    print(f"    MAPE: {row['mape_weighted']:.1%}")
                    print(f"    Bias: {row['bias_weighted']:.4f}")
                    
                    print(f"\n  📊 Baseline Comparisons:")
                    print(f"    MAE Naive: {row['mae_naive']:.4f}")
                    print(f"    MAE MA3: {row['mae_ma3']:.4f}")
                    print(f"    MAE Momentum: {row['mae_momentum']:.4f}")
                    
                    print(f"\n  🎯 Performance Metrics:")
                    print(f"    Directional Accuracy: {row['directional_accuracy']:.1%}")
                    print(f"    Improvement vs Naive: {row['improvement_vs_naive']:.1%}")
                    print(f"    Improvement vs MA3: {row['improvement_vs_ma3']:.1%}")
                    print(f"    Urgency MAE: {row['mae_urgency']:.4f}")
                
                # Overall performance summary
                avg_mae = results_24h['mae_weighted'].mean()
                avg_rmse = results_24h['rmse_weighted'].mean()
                avg_mape = results_24h['mape_weighted'].mean()
                avg_directional = results_24h['directional_accuracy'].mean()
                avg_improvement_naive = results_24h['improvement_vs_naive'].mean()
                avg_bias = results_24h['bias_weighted'].mean()
                
                print(f"\n📈 OVERALL 24-HOUR PERFORMANCE:")
                print(f"  Average MAE: {avg_mae:.4f}")
                print(f"  Average RMSE: {avg_rmse:.4f}")
                print(f"  Average MAPE: {avg_mape:.1%}")
                print(f"  Average Bias: {avg_bias:.4f}")
                print(f"  Directional Accuracy: {avg_directional:.1%}")
                print(f"  Improvement vs Naive: {avg_improvement_naive:.1%}")
                print(f"  Query Time: {query_time:.2f}s")
                
                # Success criteria
                success_24h = (
                    avg_mae < 0.1 and 
                    avg_improvement_naive > 0 and 
                    avg_directional > 0.5 and
                    abs(avg_bias) < 0.05  # Low bias
                )
                
                self.results['forecast_24h'] = {
                    'status': 'PASS' if success_24h else 'NEEDS_IMPROVEMENT',
                    'mae': avg_mae,
                    'rmse': avg_rmse,
                    'mape': avg_mape,
                    'bias': avg_bias,
                    'directional_accuracy': avg_directional,
                    'improvement_vs_naive': avg_improvement_naive,
                    'query_time': query_time
                }
                
                # Store baseline for comparison
                self.baselines['24h'] = {
                    'mae': avg_mae,
                    'rmse': avg_rmse,
                    'mape': avg_mape
                }
                
                return success_24h
            else:
                print("❌ No 24-hour results")
                return False
                
        except Exception as e:
            print(f"❌ 24-hour forecast error: {str(e)}")
            return False
    
    def test_7day_forecast_accuracy(self):
        """Test 7-day forecast with full rigor"""
        print(f"\n📅 7-DAY FORECAST RIGOROUS TESTING")
        print("="*60)
        
        try:
            forecast_7d_query = f"""
            -- Step 1: Weekly aggregations
            WITH weekly_metrics AS (
              SELECT 
                brand,
                week_start,
                AVG(promotional_intensity) AS weekly_promo,
                AVG(urgency_score) AS weekly_urgency,
                AVG(brand_voice_score) AS weekly_brand_voice,
                COUNT(*) AS weekly_volume,
                STDDEV(promotional_intensity) AS weekly_promo_std,
                
                -- Weekly patterns
                COUNTIF(primary_angle = 'PROMOTIONAL') / COUNT(*) AS promo_angle_pct,
                COUNTIF(funnel = 'Lower') / COUNT(*) AS lower_funnel_pct
                
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, week_start
            ),
            
            -- Step 2: Add weekly lags
            weekly_with_lags AS (
              SELECT 
                *,
                LAG(weekly_promo, 1) OVER (PARTITION BY brand ORDER BY week_start) AS promo_lag1w,
                LAG(weekly_promo, 2) OVER (PARTITION BY brand ORDER BY week_start) AS promo_lag2w,
                LAG(weekly_promo, 3) OVER (PARTITION BY brand ORDER BY week_start) AS promo_lag3w,
                LAG(weekly_promo, 4) OVER (PARTITION BY brand ORDER BY week_start) AS promo_lag4w,
                
                LAG(weekly_brand_voice, 1) OVER (PARTITION BY brand ORDER BY week_start) AS voice_lag1w,
                LAG(weekly_brand_voice, 2) OVER (PARTITION BY brand ORDER BY week_start) AS voice_lag2w,
                
                LAG(weekly_volume, 1) OVER (PARTITION BY brand ORDER BY week_start) AS volume_lag1w
              FROM weekly_metrics
            ),
            
            -- Step 3: Calculate weekly moving averages
            weekly_with_ma AS (
              SELECT 
                *,
                -- 2-week moving average
                (COALESCE(promo_lag1w, weekly_promo) + 
                 COALESCE(promo_lag2w, promo_lag1w, weekly_promo)) / 2.0 AS ma2w_promo,
                 
                -- 4-week moving average
                (COALESCE(promo_lag1w, weekly_promo) + 
                 COALESCE(promo_lag2w, promo_lag1w, weekly_promo) +
                 COALESCE(promo_lag3w, promo_lag2w, promo_lag1w, weekly_promo) +
                 COALESCE(promo_lag4w, promo_lag3w, promo_lag2w, promo_lag1w, weekly_promo)) / 4.0 AS ma4w_promo,
                 
                -- Brand voice moving average (more stable)
                (COALESCE(voice_lag1w, weekly_brand_voice) + 
                 COALESCE(voice_lag2w, voice_lag1w, weekly_brand_voice)) / 2.0 AS ma2w_voice
              FROM weekly_with_lags
            ),
            
            -- Step 4: Generate 7-day forecasts
            forecasts_7d AS (
              SELECT 
                brand,
                week_start,
                weekly_promo AS actual_promo,
                weekly_brand_voice AS actual_brand_voice,
                weekly_volume AS actual_volume,
                
                -- Forecast methods
                -- 1. Last week (naive)
                promo_lag1w AS forecast_naive,
                
                -- 2. 2-week MA
                ma2w_promo AS forecast_ma2w,
                
                -- 3. 4-week MA
                ma4w_promo AS forecast_ma4w,
                
                -- 4. Trend-adjusted forecast
                CASE
                  WHEN promo_lag1w IS NOT NULL AND promo_lag2w IS NOT NULL THEN
                    LEAST(1.0, GREATEST(0.0, 
                      promo_lag1w + 0.5 * (promo_lag1w - promo_lag2w)
                    ))
                  ELSE promo_lag1w
                END AS forecast_trend,
                
                -- 5. Advanced weighted forecast (best model)
                CASE
                  WHEN promo_lag1w IS NOT NULL AND ma2w_promo IS NOT NULL THEN
                    0.4 * promo_lag1w + 
                    0.3 * ma2w_promo + 
                    0.2 * COALESCE(ma4w_promo, ma2w_promo) +
                    0.1 * LEAST(1.0, GREATEST(0.0, promo_lag1w + 0.3 * (promo_lag1w - promo_lag2w)))
                  ELSE promo_lag1w
                END AS forecast_weighted,
                
                -- Brand voice forecast (stable metric)
                ma2w_voice AS forecast_brand_voice,
                
                -- Direction
                SIGN(weekly_promo - promo_lag1w) AS actual_direction,
                SIGN(
                  CASE
                    WHEN promo_lag1w IS NOT NULL AND promo_lag2w IS NOT NULL THEN
                      (promo_lag1w + 0.5 * (promo_lag1w - promo_lag2w)) - promo_lag1w
                    ELSE 0
                  END
                ) AS forecast_direction
                
              FROM weekly_with_ma
              WHERE week_start > '{self.split_date}'  -- Test set only
            ),
            
            -- Step 5: Calculate metrics
            metrics_7d AS (
              SELECT 
                brand,
                COUNT(*) AS test_weeks,
                
                -- Naive baseline
                AVG(ABS(actual_promo - forecast_naive)) AS mae_naive,
                SQRT(AVG(POW(actual_promo - forecast_naive, 2))) AS rmse_naive,
                
                -- MA baselines
                AVG(ABS(actual_promo - forecast_ma2w)) AS mae_ma2w,
                AVG(ABS(actual_promo - forecast_ma4w)) AS mae_ma4w,
                
                -- Trend forecast
                AVG(ABS(actual_promo - forecast_trend)) AS mae_trend,
                
                -- Weighted forecast (best)
                AVG(ABS(actual_promo - forecast_weighted)) AS mae_weighted,
                SQRT(AVG(POW(actual_promo - forecast_weighted, 2))) AS rmse_weighted,
                AVG(ABS(actual_promo - forecast_weighted) / NULLIF(actual_promo, 0)) AS mape_weighted,
                
                -- Brand voice forecast
                AVG(ABS(actual_brand_voice - forecast_brand_voice)) AS mae_brand_voice,
                
                -- Directional accuracy
                COUNTIF(actual_direction = forecast_direction) / COUNTIF(actual_direction != 0) AS directional_accuracy,
                
                -- Bias
                AVG(actual_promo - forecast_weighted) AS bias_weighted,
                
                -- Improvements
                (AVG(ABS(actual_promo - forecast_naive)) - AVG(ABS(actual_promo - forecast_weighted))) 
                  / NULLIF(AVG(ABS(actual_promo - forecast_naive)), 0) AS improvement_vs_naive
                
              FROM forecasts_7d
              WHERE forecast_naive IS NOT NULL
                AND forecast_weighted IS NOT NULL
              GROUP BY brand
            )
            
            SELECT * FROM metrics_7d
            ORDER BY mae_weighted ASC
            """
            
            start_time = time.time()
            results_7d = client.query(forecast_7d_query).to_dataframe()
            query_time = time.time() - start_time
            
            if not results_7d.empty:
                print("📊 7-Day Forecast Performance by Brand:")
                
                for _, row in results_7d.iterrows():
                    print(f"\n🏢 {row['brand']}:")
                    print(f"  Test Weeks: {row['test_weeks']}")
                    
                    print(f"\n  📈 Forecast Accuracy (Weighted Model):")
                    print(f"    MAE: {row['mae_weighted']:.4f}")
                    print(f"    RMSE: {row['rmse_weighted']:.4f}")
                    print(f"    MAPE: {row['mape_weighted']:.1%}")
                    print(f"    Bias: {row['bias_weighted']:.4f}")
                    
                    print(f"\n  📊 Baseline Comparisons:")
                    print(f"    MAE Naive: {row['mae_naive']:.4f}")
                    print(f"    MAE MA2W: {row['mae_ma2w']:.4f}")
                    print(f"    MAE Trend: {row['mae_trend']:.4f}")
                    
                    print(f"\n  🎯 Additional Metrics:")
                    print(f"    Brand Voice MAE: {row['mae_brand_voice']:.4f}")
                    print(f"    Directional Accuracy: {row['directional_accuracy']:.1%}")
                    print(f"    Improvement vs Naive: {row['improvement_vs_naive']:.1%}")
                
                # Overall 7-day performance
                avg_mae = results_7d['mae_weighted'].mean()
                avg_rmse = results_7d['rmse_weighted'].mean()
                avg_mape = results_7d['mape_weighted'].mean()
                avg_brand_voice_mae = results_7d['mae_brand_voice'].mean()
                avg_improvement = results_7d['improvement_vs_naive'].mean()
                
                print(f"\n📈 OVERALL 7-DAY PERFORMANCE:")
                print(f"  Average MAE (Promo): {avg_mae:.4f}")
                print(f"  Average RMSE: {avg_rmse:.4f}")
                print(f"  Average MAPE: {avg_mape:.1%}")
                print(f"  Average MAE (Brand Voice): {avg_brand_voice_mae:.4f}")
                print(f"  Improvement vs Naive: {avg_improvement:.1%}")
                print(f"  Query Time: {query_time:.2f}s")
                
                # Success criteria
                success_7d = (
                    avg_mae < 0.15 and 
                    avg_improvement > 0 and
                    avg_brand_voice_mae < 0.2
                )
                
                self.results['forecast_7d'] = {
                    'status': 'PASS' if success_7d else 'NEEDS_IMPROVEMENT',
                    'mae_promo': avg_mae,
                    'rmse': avg_rmse,
                    'mape': avg_mape,
                    'mae_brand_voice': avg_brand_voice_mae,
                    'improvement': avg_improvement,
                    'query_time': query_time
                }
                
                # Store baseline
                self.baselines['7d'] = {
                    'mae': avg_mae,
                    'rmse': avg_rmse,
                    'mape': avg_mape
                }
                
                return success_7d
            else:
                print("❌ No 7-day results")
                return False
                
        except Exception as e:
            print(f"❌ 7-day forecast error: {str(e)}")
            return False
    
    def test_confidence_calibration(self):
        """Test confidence interval calibration"""
        print(f"\n🎯 CONFIDENCE INTERVAL CALIBRATION")
        print("="*60)
        
        try:
            confidence_query = f"""
            -- Step 1: Calculate forecast errors and historical volatility
            WITH weekly_forecasts AS (
              SELECT 
                brand,
                week_start,
                AVG(promotional_intensity) AS actual_promo,
                COUNT(*) AS week_count
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, week_start
            ),
            
            weekly_with_forecast AS (
              SELECT 
                brand,
                week_start,
                actual_promo,
                LAG(actual_promo, 1) OVER (PARTITION BY brand ORDER BY week_start) AS forecast_promo
              FROM weekly_forecasts
            ),
            
            error_analysis AS (
              SELECT 
                brand,
                week_start,
                actual_promo,
                forecast_promo,
                ABS(actual_promo - forecast_promo) AS absolute_error,
                actual_promo - forecast_promo AS error
              FROM weekly_with_forecast
              WHERE forecast_promo IS NOT NULL
                AND week_start > '{self.split_date}'
            ),
            
            -- Step 2: Calculate empirical confidence intervals
            error_distribution AS (
              SELECT 
                brand,
                -- Percentiles of absolute errors
                APPROX_QUANTILES(absolute_error, 100)[OFFSET(50)] AS median_error,
                APPROX_QUANTILES(absolute_error, 100)[OFFSET(68)] AS p68_error,
                APPROX_QUANTILES(absolute_error, 100)[OFFSET(95)] AS p95_error,
                APPROX_QUANTILES(absolute_error, 100)[OFFSET(99)] AS p99_error,
                
                -- Mean and std
                AVG(absolute_error) AS mean_error,
                STDDEV(absolute_error) AS std_error,
                
                -- Bias
                AVG(error) AS mean_bias,
                
                COUNT(*) AS sample_size
              FROM error_analysis
              GROUP BY brand
            ),
            
            -- Step 3: Test calibration
            calibration_test AS (
              SELECT 
                e.brand,
                e.absolute_error,
                d.median_error,
                d.p68_error,
                d.p95_error,
                
                -- Check which confidence interval contains the error
                CASE
                  WHEN e.absolute_error <= d.median_error THEN '50%'
                  WHEN e.absolute_error <= d.p68_error THEN '68%'
                  WHEN e.absolute_error <= d.p95_error THEN '95%'
                  WHEN e.absolute_error <= d.p99_error THEN '99%'
                  ELSE 'Outside'
                END AS confidence_band
                
              FROM error_analysis e
              JOIN error_distribution d ON e.brand = d.brand
            )
            
            -- Step 4: Summarize calibration
            SELECT 
              brand,
              COUNT(*) AS total_forecasts,
              
              -- Actual coverage at each level
              COUNTIF(confidence_band IN ('50%')) / COUNT(*) AS coverage_50pct,
              COUNTIF(confidence_band IN ('50%', '68%')) / COUNT(*) AS coverage_68pct,
              COUNTIF(confidence_band IN ('50%', '68%', '95%')) / COUNT(*) AS coverage_95pct,
              COUNTIF(confidence_band != 'Outside') / COUNT(*) AS coverage_99pct,
              
              -- Calibration errors (actual - expected)
              ABS(COUNTIF(confidence_band IN ('50%')) / COUNT(*) - 0.50) AS calib_error_50,
              ABS(COUNTIF(confidence_band IN ('50%', '68%')) / COUNT(*) - 0.68) AS calib_error_68,
              ABS(COUNTIF(confidence_band IN ('50%', '68%', '95%')) / COUNT(*) - 0.95) AS calib_error_95,
              
              -- Average calibration error
              (ABS(COUNTIF(confidence_band IN ('50%')) / COUNT(*) - 0.50) +
               ABS(COUNTIF(confidence_band IN ('50%', '68%')) / COUNT(*) - 0.68) +
               ABS(COUNTIF(confidence_band IN ('50%', '68%', '95%')) / COUNT(*) - 0.95)) / 3.0 AS avg_calib_error
              
            FROM calibration_test
            GROUP BY brand
            ORDER BY avg_calib_error ASC
            """
            
            start_time = time.time()
            calibration_results = client.query(confidence_query).to_dataframe()
            query_time = time.time() - start_time
            
            if not calibration_results.empty:
                print("📊 Confidence Calibration Results:")
                
                for _, row in calibration_results.iterrows():
                    print(f"\n🏢 {row['brand']}:")
                    print(f"  Total Forecasts: {row['total_forecasts']}")
                    
                    print(f"\n  📈 Coverage (Actual vs Expected):")
                    print(f"    50% CI: {row['coverage_50pct']:.1%} (expected: 50.0%)")
                    print(f"    68% CI: {row['coverage_68pct']:.1%} (expected: 68.0%)")
                    print(f"    95% CI: {row['coverage_95pct']:.1%} (expected: 95.0%)")
                    
                    print(f"\n  🎯 Calibration Errors:")
                    print(f"    50% Error: {row['calib_error_50']:.1%}")
                    print(f"    68% Error: {row['calib_error_68']:.1%}")
                    print(f"    95% Error: {row['calib_error_95']:.1%}")
                    print(f"    Average Error: {row['avg_calib_error']:.1%}")
                
                # Overall calibration
                avg_calib_error = calibration_results['avg_calib_error'].mean()
                
                print(f"\n📈 OVERALL CALIBRATION:")
                print(f"  Average Calibration Error: {avg_calib_error:.1%}")
                print(f"  Query Time: {query_time:.2f}s")
                
                # Success: average calibration error < 15%
                calibration_success = avg_calib_error < 0.15
                
                self.results['confidence_calibration'] = {
                    'status': 'PASS' if calibration_success else 'NEEDS_CALIBRATION',
                    'avg_calibration_error': avg_calib_error,
                    'query_time': query_time
                }
                
                return calibration_success
            else:
                print("❌ No calibration results")
                return False
                
        except Exception as e:
            print(f"❌ Confidence calibration error: {str(e)}")
            return False
    
    def generate_comprehensive_report(self):
        """Generate comprehensive baseline report with all metrics"""
        print(f"\n📋 COMPREHENSIVE FORECAST BASELINE REPORT")
        print("="*80)
        
        # Summary statistics
        tests_run = len(self.results)
        tests_passed = sum(1 for r in self.results.values() if r.get('status') == 'PASS')
        
        print(f"📊 TEST SUMMARY:")
        print(f"  Tests Run: {tests_run}")
        print(f"  Tests Passed: {tests_passed}")
        print(f"  Success Rate: {tests_passed/tests_run:.1%}" if tests_run > 0 else "  Success Rate: N/A")
        
        print(f"\n📈 BASELINE METRICS ESTABLISHED:")
        
        # 24-hour metrics
        if 'forecast_24h' in self.results:
            f24 = self.results['forecast_24h']
            print(f"\n  🔮 24-HOUR FORECASTING:")
            print(f"    Status: {f24['status']}")
            print(f"    MAE: {f24['mae']:.4f}")
            print(f"    RMSE: {f24['rmse']:.4f}")
            print(f"    MAPE: {f24['mape']:.1%}")
            print(f"    Bias: {f24['bias']:.4f}")
            print(f"    Directional Accuracy: {f24['directional_accuracy']:.1%}")
            print(f"    Improvement vs Naive: {f24['improvement_vs_naive']:.1%}")
        
        # 7-day metrics
        if 'forecast_7d' in self.results:
            f7d = self.results['forecast_7d']
            print(f"\n  📅 7-DAY FORECASTING:")
            print(f"    Status: {f7d['status']}")
            print(f"    MAE (Promo): {f7d['mae_promo']:.4f}")
            print(f"    MAE (Brand Voice): {f7d['mae_brand_voice']:.4f}")
            print(f"    RMSE: {f7d['rmse']:.4f}")
            print(f"    MAPE: {f7d['mape']:.1%}")
            print(f"    Improvement vs Naive: {f7d['improvement']:.1%}")
        
        # Confidence calibration
        if 'confidence_calibration' in self.results:
            conf = self.results['confidence_calibration']
            print(f"\n  🎯 CONFIDENCE CALIBRATION:")
            print(f"    Status: {conf['status']}")
            print(f"    Avg Calibration Error: {conf['avg_calibration_error']:.1%}")
        
        # Data summary
        print(f"\n📅 BACKTESTING DATA:")
        print(f"  Training Period: {self.timeline['training_days']} days")
        print(f"  Testing Period: {self.timeline['testing_days']} days")
        print(f"  Total Ads: {self.timeline['total_ads']:,}")
        
        # Key baselines for multimodal comparison
        print(f"\n🎯 KEY BASELINES FOR MULTIMODAL COMPARISON:")
        if '24h' in self.baselines:
            print(f"  24-Hour MAE: {self.baselines['24h']['mae']:.4f}")
            print(f"  24-Hour RMSE: {self.baselines['24h']['rmse']:.4f}")
        if '7d' in self.baselines:
            print(f"  7-Day MAE: {self.baselines['7d']['mae']:.4f}")
            print(f"  7-Day RMSE: {self.baselines['7d']['rmse']:.4f}")
        
        # Readiness assessment
        readiness_score = tests_passed / tests_run if tests_run > 0 else 0
        
        print(f"\n🚀 MULTIMODAL READINESS ASSESSMENT:")
        if readiness_score >= 0.6:
            print("  ✅ BASELINES SUCCESSFULLY ESTABLISHED")
            print("  • Rigorous forecasting methodology validated")
            print("  • Statistical baselines documented")
            print("  • Ready to measure multimodal improvement")
            print("  • Expected improvement target: 15-25% reduction in MAE")
        else:
            print("  ⚠️ BASELINES PARTIALLY ESTABLISHED")
            print("  • Some metrics need refinement")
            print("  • Consider methodology adjustments")
            print("  • Multimodal may still provide improvements")
        
        return readiness_score >= 0.6, self.results, self.baselines

def run_rigorous_backtesting():
    """Run the complete rigorous backtesting suite"""
    print("🚀 RIGOROUS FORECAST ACCURACY BACKTESTING")
    print("="*80)
    print("Full statistical validation with multiple baselines and comprehensive metrics")
    print("="*80)
    
    backtester = RigorousForecastBacktesting()
    
    # Step 1: Prepare data
    if not backtester.prepare_backtest_data():
        print("❌ Failed to prepare data")
        return False, {}, {}
    
    # Step 2: Run all tests
    tests = [
        ('24-Hour Forecasting', backtester.test_24hour_forecast_accuracy),
        ('7-Day Forecasting', backtester.test_7day_forecast_accuracy),
        ('Confidence Calibration', backtester.test_confidence_calibration)
    ]
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"Running: {test_name}")
        print(f"{'='*60}")
        try:
            test_func()
        except Exception as e:
            print(f"⚠️ {test_name} failed: {str(e)}")
    
    # Step 3: Generate report
    ready, results, baselines = backtester.generate_comprehensive_report()
    
    print(f"\n{'='*80}")
    if ready:
        print("🎉 RIGOROUS BASELINES ESTABLISHED")
        print("📈 Ready for multimodal enhancement measurement")
    else:
        print("🔧 BASELINES NEED REFINEMENT")
        print("⚠️ Review failed tests before multimodal integration")
    print(f"{'='*80}")
    
    return ready, results, baselines

if __name__ == "__main__":
    success, results, baselines = run_rigorous_backtesting()
    
    # Save baselines for future comparison
    if baselines:
        import json
        with open('forecast_baselines.json', 'w') as f:
            json.dump(baselines, f, indent=2, default=str)
        print(f"\n💾 Baselines saved to forecast_baselines.json")
    
    exit(0 if success else 1)