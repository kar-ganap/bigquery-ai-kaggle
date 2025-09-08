#!/usr/bin/env python3
"""
Forecast Accuracy Backtesting - Establish Baseline Metrics
Uses 80/20 train/test split to validate forecasting methodology
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

class ForecastBacktesting:
    def __init__(self):
        self.results = {}
        self.baselines = {}
        
    def prepare_backtest_data(self):
        """Prepare historical data for backtesting"""
        print("🔍 PREPARING BACKTEST DATA")
        print("="*60)
        
        try:
            # Get data timeline and establish train/test split
            timeline_query = f"""
            SELECT 
              MIN(DATE(start_timestamp)) as earliest_date,
              MAX(DATE(start_timestamp)) as latest_date,
              COUNT(DISTINCT DATE(start_timestamp)) as unique_dates,
              COUNT(DISTINCT week_start) as unique_weeks,
              COUNT(*) as total_ads
            FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
            """
            
            timeline = client.query(timeline_query).to_dataframe()
            
            earliest = timeline['earliest_date'].iloc[0]
            latest = timeline['latest_date'].iloc[0]
            total_days = (latest - earliest).days
            
            # 80/20 split: Use first 80% for training, last 20% for testing
            split_date = earliest + timedelta(days=int(total_days * 0.8))
            
            print(f"📅 Data Timeline:")
            print(f"  Full Range: {earliest} → {latest} ({total_days} days)")
            print(f"  Training Period: {earliest} → {split_date}")
            print(f"  Testing Period: {split_date} → {latest}")
            print(f"  Total Ads: {timeline['total_ads'].iloc[0]:,}")
            print(f"  Unique Weeks: {timeline['unique_weeks'].iloc[0]}")
            
            self.split_date = split_date
            self.timeline = {
                'earliest': earliest,
                'latest': latest,
                'split_date': split_date,
                'training_days': int(total_days * 0.8),
                'testing_days': int(total_days * 0.2),
                'total_ads': timeline['total_ads'].iloc[0]
            }
            
            return True
            
        except Exception as e:
            print(f"❌ Data preparation failed: {str(e)}")
            return False
    
    def test_24hour_forecast_accuracy(self):
        """Test 24-hour forecast accuracy using historical data"""
        print(f"\n🔮 24-HOUR FORECAST BACKTESTING")
        print("="*60)
        
        try:
            # Build and test 24-hour forecasting model
            forecast_24h_query = f"""
            WITH training_data AS (
              SELECT 
                brand,
                DATE(start_timestamp) AS date,
                AVG(promotional_intensity) AS daily_promo,
                AVG(urgency_score) AS daily_urgency,
                COUNT(*) AS daily_volume
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              WHERE DATE(start_timestamp) <= '{self.split_date}'
              GROUP BY brand, date
            ),
            
            testing_data AS (
              SELECT 
                brand,
                DATE(start_timestamp) AS date,
                AVG(promotional_intensity) AS daily_promo,
                AVG(urgency_score) AS daily_urgency,
                COUNT(*) AS daily_volume
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              WHERE DATE(start_timestamp) > '{self.split_date}'
              GROUP BY brand, date
            ),
            
            model_predictions AS (
              SELECT 
                t.brand,
                t.date,
                t.daily_promo AS actual_promo,
                t.daily_urgency AS actual_urgency,
                
                -- 24h forecast using momentum from training data
                LEAST(1.0, GREATEST(0.0, 
                  LAG(t.daily_promo, 1) OVER (PARTITION BY t.brand ORDER BY t.date) + 
                  COALESCE(
                    LAG(t.daily_promo, 1) OVER (PARTITION BY t.brand ORDER BY t.date) - 
                    LAG(t.daily_promo, 2) OVER (PARTITION BY t.brand ORDER BY t.date), 
                    0
                  )
                )) AS forecast_24h_promo,
                
                -- Urgency forecast
                LEAST(1.0, GREATEST(0.0,
                  LAG(t.daily_urgency, 1) OVER (PARTITION BY t.brand ORDER BY t.date) +
                  COALESCE(
                    LAG(t.daily_urgency, 1) OVER (PARTITION BY t.brand ORDER BY t.date) - 
                    LAG(t.daily_urgency, 2) OVER (PARTITION BY t.brand ORDER BY t.date),
                    0
                  )
                )) AS forecast_24h_urgency,
                
                -- Baseline: naive forecast (yesterday = today)
                LAG(t.daily_promo, 1) OVER (PARTITION BY t.brand ORDER BY t.date) AS baseline_naive,
                
                -- Baseline: moving average
                AVG(t.daily_promo) OVER (
                  PARTITION BY t.brand ORDER BY t.date 
                  ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                ) AS baseline_ma3
                
              FROM testing_data t
            )
            
            SELECT 
              brand,
              COUNT(*) AS test_points,
              
              -- 24-hour forecast accuracy
              AVG(ABS(actual_promo - forecast_24h_promo)) AS mae_24h_promo,
              SQRT(AVG(POW(actual_promo - forecast_24h_promo, 2))) AS rmse_24h_promo,
              AVG(ABS(actual_urgency - forecast_24h_urgency)) AS mae_24h_urgency,
              
              -- Baseline comparisons
              AVG(ABS(actual_promo - baseline_naive)) AS mae_baseline_naive,
              AVG(ABS(actual_promo - baseline_ma3)) AS mae_baseline_ma3,
              
              -- Forecast improvement over baselines
              (AVG(ABS(actual_promo - baseline_naive)) - AVG(ABS(actual_promo - forecast_24h_promo))) 
                / NULLIF(AVG(ABS(actual_promo - baseline_naive)), 0) AS improvement_vs_naive,
                
              (AVG(ABS(actual_promo - baseline_ma3)) - AVG(ABS(actual_promo - forecast_24h_promo))) 
                / NULLIF(AVG(ABS(actual_promo - baseline_ma3)), 0) AS improvement_vs_ma3,
              
              -- Directional accuracy (did we predict the right direction?)
              COUNTIF(
                SIGN(actual_promo - LAG(actual_promo) OVER (PARTITION BY brand ORDER BY brand)) = 
                SIGN(forecast_24h_promo - LAG(actual_promo) OVER (PARTITION BY brand ORDER BY brand))
              ) / COUNT(*) AS directional_accuracy
              
            FROM model_predictions
            WHERE forecast_24h_promo IS NOT NULL
              AND baseline_naive IS NOT NULL
              AND baseline_ma3 IS NOT NULL
            GROUP BY brand
            ORDER BY mae_24h_promo ASC
            """
            
            start_time = time.time()
            forecast_results = client.query(forecast_24h_query).to_dataframe()
            query_time = time.time() - start_time
            
            if not forecast_results.empty:
                print("📊 24-Hour Forecast Performance:")
                
                for _, row in forecast_results.iterrows():
                    print(f"\n🏢 {row['brand']}:")
                    print(f"  Test Points: {row['test_points']}")
                    print(f"  MAE (24h): {row['mae_24h_promo']:.4f}")
                    print(f"  RMSE (24h): {row['rmse_24h_promo']:.4f}")
                    print(f"  MAE Urgency: {row['mae_24h_urgency']:.4f}")
                    print(f"  Improvement vs Naive: {row['improvement_vs_naive']:.1%}")
                    print(f"  Improvement vs MA3: {row['improvement_vs_ma3']:.1%}")
                    print(f"  Directional Accuracy: {row['directional_accuracy']:.1%}")
                
                # Overall performance
                avg_mae = forecast_results['mae_24h_promo'].mean()
                avg_improvement_naive = forecast_results['improvement_vs_naive'].mean()
                avg_directional = forecast_results['directional_accuracy'].mean()
                
                print(f"\n📈 OVERALL 24-HOUR PERFORMANCE:")
                print(f"  Average MAE: {avg_mae:.4f}")
                print(f"  Improvement vs Naive: {avg_improvement_naive:.1%}")
                print(f"  Directional Accuracy: {avg_directional:.1%}")
                print(f"  Query Time: {query_time:.2f}s")
                
                # Success criteria: MAE < 0.1 and improvement over baselines
                forecast_24h_success = (avg_mae < 0.1 and 
                                      avg_improvement_naive > 0 and
                                      avg_directional > 0.5)
                
                self.results['forecast_24h'] = {
                    'status': 'PASS' if forecast_24h_success else 'NEEDS_IMPROVEMENT',
                    'mae': avg_mae,
                    'improvement_vs_naive': avg_improvement_naive,
                    'directional_accuracy': avg_directional,
                    'query_time': query_time
                }
                
                return forecast_24h_success
            else:
                print("❌ No 24-hour forecast results")
                return False
                
        except Exception as e:
            print(f"❌ 24-hour forecast test failed: {str(e)}")
            return False
    
    def test_7day_forecast_accuracy(self):
        """Test 7-day forecast accuracy"""
        print(f"\n📅 7-DAY FORECAST BACKTESTING")
        print("="*60)
        
        try:
            forecast_7d_query = f"""
            WITH weekly_data AS (
              SELECT 
                brand,
                week_start,
                AVG(promotional_intensity) AS weekly_promo,
                AVG(urgency_score) AS weekly_urgency,
                AVG(brand_voice_score) AS weekly_brand_voice,
                COUNT(*) AS weekly_volume
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, week_start
              ORDER BY brand, week_start
            ),
            
            training_weeks AS (
              SELECT * FROM weekly_data
              WHERE week_start <= '{self.split_date}'
            ),
            
            testing_weeks AS (
              SELECT * FROM weekly_data  
              WHERE week_start > '{self.split_date}'
            ),
            
            forecast_7d AS (
              SELECT 
                t.brand,
                t.week_start,
                t.weekly_promo AS actual_weekly_promo,
                t.weekly_brand_voice AS actual_brand_voice,
                
                -- 7-day forecast: trend-adjusted moving average
                AVG(LAG(t.weekly_promo, 1) OVER (PARTITION BY t.brand ORDER BY t.week_start)) OVER (
                  PARTITION BY t.brand ORDER BY t.week_start
                  ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING  
                ) + 0.3 * COALESCE(
                  LAG(t.weekly_promo, 1) OVER (PARTITION BY t.brand ORDER BY t.week_start) - 
                  LAG(t.weekly_promo, 2) OVER (PARTITION BY t.brand ORDER BY t.week_start),
                  0
                ) AS forecast_7d_promo,
                
                -- Brand voice forecast (more stable)
                AVG(LAG(t.weekly_brand_voice, 1) OVER (PARTITION BY t.brand ORDER BY t.week_start)) OVER (
                  PARTITION BY t.brand ORDER BY t.week_start
                  ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
                ) AS forecast_7d_brand_voice,
                
                -- Baselines
                LAG(t.weekly_promo, 1) OVER (PARTITION BY t.brand ORDER BY t.week_start) AS baseline_last_week,
                AVG(t.weekly_promo) OVER (
                  PARTITION BY t.brand ORDER BY t.week_start
                  ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                ) AS baseline_ma4
                
              FROM testing_weeks t
            )
            
            SELECT 
              brand,
              COUNT(*) AS test_weeks,
              
              -- 7-day promotional forecast accuracy
              AVG(ABS(actual_weekly_promo - forecast_7d_promo)) AS mae_7d_promo,
              SQRT(AVG(POW(actual_weekly_promo - forecast_7d_promo, 2))) AS rmse_7d_promo,
              
              -- Brand voice forecast accuracy  
              AVG(ABS(actual_brand_voice - forecast_7d_brand_voice)) AS mae_7d_brand_voice,
              
              -- Baseline comparisons
              AVG(ABS(actual_weekly_promo - baseline_last_week)) AS mae_last_week,
              AVG(ABS(actual_weekly_promo - baseline_ma4)) AS mae_ma4,
              
              -- Improvements
              (AVG(ABS(actual_weekly_promo - baseline_last_week)) - AVG(ABS(actual_weekly_promo - forecast_7d_promo)))
                / NULLIF(AVG(ABS(actual_weekly_promo - baseline_last_week)), 0) AS improvement_vs_last_week,
              
              -- Volatility-adjusted accuracy (penalize high volatility periods less)
              AVG(ABS(actual_weekly_promo - forecast_7d_promo) / NULLIF(STDDEV(actual_weekly_promo) OVER (), 1)) AS volatility_adj_mae
              
            FROM forecast_7d  
            WHERE forecast_7d_promo IS NOT NULL
              AND baseline_last_week IS NOT NULL
              AND forecast_7d_brand_voice IS NOT NULL
            GROUP BY brand
            ORDER BY mae_7d_promo ASC
            """
            
            start_time = time.time()
            forecast_7d_results = client.query(forecast_7d_query).to_dataframe()
            query_time = time.time() - start_time
            
            if not forecast_7d_results.empty:
                print("📊 7-Day Forecast Performance:")
                
                for _, row in forecast_7d_results.iterrows():
                    print(f"\n🏢 {row['brand']}:")
                    print(f"  Test Weeks: {row['test_weeks']}")
                    print(f"  MAE (7d Promo): {row['mae_7d_promo']:.4f}")
                    print(f"  MAE (7d Brand Voice): {row['mae_7d_brand_voice']:.4f}")
                    print(f"  RMSE: {row['rmse_7d_promo']:.4f}")
                    print(f"  Improvement vs Last Week: {row['improvement_vs_last_week']:.1%}")
                    print(f"  Volatility-Adjusted MAE: {row['volatility_adj_mae']:.4f}")
                
                # Overall 7-day performance
                avg_mae_7d = forecast_7d_results['mae_7d_promo'].mean()
                avg_brand_voice_mae = forecast_7d_results['mae_7d_brand_voice'].mean()
                avg_improvement = forecast_7d_results['improvement_vs_last_week'].mean()
                
                print(f"\n📈 OVERALL 7-DAY PERFORMANCE:")
                print(f"  Average MAE (Promo): {avg_mae_7d:.4f}")
                print(f"  Average MAE (Brand Voice): {avg_brand_voice_mae:.4f}")
                print(f"  Improvement vs Baseline: {avg_improvement:.1%}")
                print(f"  Query Time: {query_time:.2f}s")
                
                # Success criteria: MAE < 0.15 and improvement over baselines
                forecast_7d_success = (avg_mae_7d < 0.15 and 
                                     avg_improvement > 0 and
                                     avg_brand_voice_mae < 0.2)
                
                self.results['forecast_7d'] = {
                    'status': 'PASS' if forecast_7d_success else 'NEEDS_IMPROVEMENT',
                    'mae_promo': avg_mae_7d,
                    'mae_brand_voice': avg_brand_voice_mae,
                    'improvement': avg_improvement,
                    'query_time': query_time
                }
                
                return forecast_7d_success
            else:
                print("❌ No 7-day forecast results")
                return False
                
        except Exception as e:
            print(f"❌ 7-day forecast test failed: {str(e)}")
            return False
    
    def test_forecast_confidence_calibration(self):
        """Test forecast confidence interval calibration"""
        print(f"\n🎯 FORECAST CONFIDENCE CALIBRATION")
        print("="*60)
        
        try:
            confidence_query = f"""
            WITH forecast_errors AS (
              SELECT 
                brand,
                week_start,
                AVG(promotional_intensity) AS actual_promo,
                
                -- Simple forecast
                LAG(AVG(promotional_intensity), 1) OVER (
                  PARTITION BY brand ORDER BY week_start
                ) AS forecast_promo,
                
                -- Forecast error
                ABS(AVG(promotional_intensity) - LAG(AVG(promotional_intensity), 1) OVER (
                  PARTITION BY brand ORDER BY week_start
                )) AS forecast_error,
                
                -- Historical volatility (for confidence bands)
                STDDEV(promotional_intensity) OVER (
                  PARTITION BY brand ORDER BY week_start
                  ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                ) AS historical_volatility
                
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              WHERE week_start > '{self.split_date}'
              GROUP BY brand, week_start
            ),
            
            confidence_analysis AS (
              SELECT 
                brand,
                forecast_error,
                historical_volatility,
                
                -- Confidence bands (1-sigma, 2-sigma)
                CASE 
                  WHEN forecast_error <= historical_volatility THEN '68%_CONFIDENCE'
                  WHEN forecast_error <= 2 * historical_volatility THEN '95%_CONFIDENCE' 
                  ELSE 'OUTSIDE_CONFIDENCE'
                END AS confidence_band,
                
                -- Error quartiles
                NTILE(4) OVER (ORDER BY forecast_error) AS error_quartile
                
              FROM forecast_errors
              WHERE forecast_error IS NOT NULL 
                AND historical_volatility IS NOT NULL
                AND historical_volatility > 0
            )
            
            SELECT 
              confidence_band,
              COUNT(*) AS forecasts_count,
              AVG(forecast_error) AS avg_error,
              MIN(forecast_error) AS min_error,
              MAX(forecast_error) AS max_error,
              COUNT(*) / SUM(COUNT(*)) OVER () AS actual_coverage
              
            FROM confidence_analysis
            GROUP BY confidence_band
            ORDER BY 
              CASE confidence_band 
                WHEN '68%_CONFIDENCE' THEN 1
                WHEN '95%_CONFIDENCE' THEN 2  
                WHEN 'OUTSIDE_CONFIDENCE' THEN 3
              END
            """
            
            start_time = time.time()
            confidence_results = client.query(confidence_query).to_dataframe()
            query_time = time.time() - start_time
            
            if not confidence_results.empty:
                print("📊 Confidence Calibration Results:")
                
                expected_coverage = {'68%_CONFIDENCE': 0.68, '95%_CONFIDENCE': 0.27, 'OUTSIDE_CONFIDENCE': 0.05}
                
                for _, row in confidence_results.iterrows():
                    band = row['confidence_band']
                    actual = row['actual_coverage']
                    expected = expected_coverage.get(band, 0)
                    calibration_error = abs(actual - expected)
                    
                    print(f"\n📈 {band}:")
                    print(f"  Forecasts: {row['forecasts_count']}")
                    print(f"  Actual Coverage: {actual:.1%}")
                    print(f"  Expected: {expected:.1%}")
                    print(f"  Calibration Error: {calibration_error:.1%}")
                    print(f"  Avg Error: {row['avg_error']:.4f}")
                
                # Overall calibration quality
                total_calibration_error = sum(
                    abs(row['actual_coverage'] - expected_coverage.get(row['confidence_band'], 0))
                    for _, row in confidence_results.iterrows()
                )
                
                print(f"\n🎯 CONFIDENCE CALIBRATION SUMMARY:")
                print(f"  Total Calibration Error: {total_calibration_error:.1%}")
                print(f"  Query Time: {query_time:.2f}s")
                
                # Good calibration: total error < 20%
                confidence_success = total_calibration_error < 0.2
                
                self.results['confidence'] = {
                    'status': 'PASS' if confidence_success else 'NEEDS_CALIBRATION',
                    'total_calibration_error': total_calibration_error,
                    'query_time': query_time
                }
                
                return confidence_success
            else:
                print("❌ No confidence calibration results")
                return False
                
        except Exception as e:
            print(f"❌ Confidence calibration test failed: {str(e)}")
            return False
    
    def generate_baseline_report(self):
        """Generate comprehensive baseline forecast performance report"""
        print(f"\n📋 FORECAST ACCURACY BASELINE REPORT")
        print("="*80)
        
        # Calculate overall scores
        tests_passed = sum(1 for r in self.results.values() if r.get('status') == 'PASS')
        total_tests = len(self.results)
        
        print(f"📊 BASELINE METRICS ESTABLISHED:")
        for test_name, result in self.results.items():
            status_icon = "✅" if result.get('status') == 'PASS' else "⚠️"
            print(f"  {status_icon} {test_name.replace('_', ' ').title()}: {result.get('status', 'UNKNOWN')}")
        
        print(f"\n📈 PERFORMANCE BASELINES:")
        
        if 'forecast_24h' in self.results:
            f24h = self.results['forecast_24h']
            print(f"  24-Hour Forecast MAE: {f24h.get('mae', 0):.4f}")
            print(f"  24-Hour Improvement: {f24h.get('improvement_vs_naive', 0):.1%}")
            print(f"  24-Hour Directional: {f24h.get('directional_accuracy', 0):.1%}")
        
        if 'forecast_7d' in self.results:
            f7d = self.results['forecast_7d']
            print(f"  7-Day Forecast MAE: {f7d.get('mae_promo', 0):.4f}")
            print(f"  7-Day Brand Voice MAE: {f7d.get('mae_brand_voice', 0):.4f}")
            print(f"  7-Day Improvement: {f7d.get('improvement', 0):.1%}")
        
        if 'confidence' in self.results:
            conf = self.results['confidence']
            print(f"  Confidence Calibration Error: {conf.get('total_calibration_error', 0):.1%}")
        
        # Timeline summary
        print(f"\n📅 BACKTESTING TIMELINE:")
        print(f"  Training Period: {self.timeline['earliest']} → {self.timeline['split_date']}")
        print(f"  Testing Period: {self.timeline['split_date']} → {self.timeline['latest']}")
        print(f"  Training Days: {self.timeline['training_days']}")
        print(f"  Testing Days: {self.timeline['testing_days']}")
        
        # Production readiness
        accuracy_ready = tests_passed >= total_tests * 0.6  # At least 60% pass
        
        print(f"\n🚀 FORECAST READINESS FOR MULTIMODAL:")
        if accuracy_ready:
            print("  ✅ BASELINE ESTABLISHED")
            print("  • Current forecasting methodology validated")
            print("  • Performance benchmarks documented") 
            print("  • Ready to measure multimodal improvement")
            print(f"  • Baseline MAE: {self.results.get('forecast_24h', {}).get('mae', 0):.4f} (24h), {self.results.get('forecast_7d', {}).get('mae_promo', 0):.4f} (7d)")
        else:
            print("  ⚠️ BASELINE NEEDS IMPROVEMENT")
            print("  • Forecast accuracy below targets")
            print("  • Consider methodology refinements")
            print("  • May still proceed with multimodal (could improve results)")
        
        return accuracy_ready, self.results

def run_forecast_backtesting():
    """Run complete forecast accuracy backtesting"""
    print("🚀 FORECAST ACCURACY BACKTESTING SUITE")
    print("="*80)
    print("Objective: Establish baseline forecast performance before multimodal enhancement")
    print("="*80)
    
    backtester = ForecastBacktesting()
    
    # Step 1: Prepare backtest data
    if not backtester.prepare_backtest_data():
        print("❌ Failed to prepare backtest data")
        return False, {}
    
    # Step 2: Test forecasting accuracy
    tests = [
        backtester.test_24hour_forecast_accuracy,
        backtester.test_7day_forecast_accuracy,
        backtester.test_forecast_confidence_calibration
    ]
    
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"⚠️ Test failed with exception: {str(e)}")
    
    # Step 3: Generate baseline report
    ready, results = backtester.generate_baseline_report()
    
    print(f"\n{'='*80}")
    if ready:
        print("🎉 FORECAST BASELINE ESTABLISHED")
        print("📈 Ready for multimodal enhancement comparison")
    else:
        print("🔧 FORECAST BASELINE NEEDS REFINEMENT")
        print("⚠️ Consider methodology improvements")
    print(f"{'='*80}")
    
    return ready, results

if __name__ == "__main__":
    success, results = run_forecast_backtesting()
    exit(0 if success else 1)