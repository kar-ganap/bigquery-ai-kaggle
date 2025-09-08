#!/usr/bin/env python3
"""
Comprehensive Validation Suite - End-to-End Testing
Tests all core intelligence tiers and advanced features working together
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import time
from datetime import datetime
import json

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

class ComprehensiveValidationSuite:
    def __init__(self):
        self.results = {}
        self.performance_metrics = {}
        self.errors = []
        
    def test_data_quality(self):
        """Test 1: Data Quality and Availability"""
        print("🔍 TEST 1: DATA QUALITY & AVAILABILITY")
        print("="*60)
        
        try:
            # Check core tables exist and have data
            data_quality_query = f"""
            SELECT 
              'ads_strategic_labels_mock' AS table_name,
              COUNT(*) as row_count,
              COUNT(DISTINCT brand) as brand_count,
              MIN(DATE(start_timestamp)) as earliest_date,
              MAX(DATE(start_timestamp)) as latest_date,
              COUNT(DISTINCT DATE(start_timestamp)) as unique_dates
            FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
            
            UNION ALL
            
            SELECT 
              'validation_data_summary' AS table_name,
              COUNT(*) as row_count,
              COUNT(DISTINCT brand) as brand_count,
              MIN(DATE(start_timestamp)) as earliest_date,
              MAX(DATE(start_timestamp)) as latest_date,
              COUNT(DISTINCT DATE(start_timestamp)) as unique_dates
            FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
            WHERE promotional_intensity IS NOT NULL
              AND urgency_score IS NOT NULL
              AND brand_voice_score IS NOT NULL
            """
            
            start_time = time.time()
            data_quality = client.query(data_quality_query).to_dataframe()
            query_time = time.time() - start_time
            
            self.performance_metrics['data_quality_query_time'] = query_time
            
            print(f"📊 Data Quality Results:")
            for _, row in data_quality.iterrows():
                print(f"  {row['table_name']}:")
                print(f"    Rows: {row['row_count']:,}")
                print(f"    Brands: {row['brand_count']}")
                print(f"    Date Range: {row['earliest_date']} → {row['latest_date']}")
                print(f"    Unique Dates: {row['unique_dates']}")
            
            # Data quality checks
            total_rows = data_quality[data_quality['table_name'] == 'ads_strategic_labels_mock']['row_count'].iloc[0]
            valid_rows = data_quality[data_quality['table_name'] == 'validation_data_summary']['row_count'].iloc[0]
            data_quality_rate = valid_rows / total_rows if total_rows > 0 else 0
            
            print(f"\n✅ Data Quality Rate: {data_quality_rate:.1%}")
            print(f"⏱️ Query Time: {query_time:.2f}s")
            
            self.results['data_quality'] = {
                'status': 'PASS' if data_quality_rate > 0.8 else 'FAIL',
                'quality_rate': data_quality_rate,
                'total_rows': total_rows,
                'valid_rows': valid_rows,
                'query_time': query_time
            }
            
            return data_quality_rate > 0.8
            
        except Exception as e:
            print(f"❌ Data Quality Test Failed: {str(e)}")
            self.errors.append(f"Data Quality: {str(e)}")
            return False

    def test_tier_integration(self):
        """Test 2: All Three Tiers Working Together"""
        print(f"\n🏗️ TEST 2: TIER INTEGRATION")
        print("="*60)
        
        try:
            # Test all tiers producing coherent results
            integration_query = f"""
            WITH tier1_signals AS (
              -- Tier 1: Strategic Goldmine
              SELECT 
                brand,
                DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week,
                AVG(promotional_intensity) AS avg_promo,
                AVG(urgency_score) AS avg_urgency,
                COUNT(*) AS ad_count,
                
                -- Strategic signal detection
                CASE
                  WHEN AVG(promotional_intensity) > 0.7 THEN 'HIGH_PROMO_STRATEGY'
                  WHEN AVG(urgency_score) > 0.7 THEN 'URGENCY_STRATEGY'
                  WHEN AVG(brand_voice_score) > 0.7 THEN 'BRAND_FOCUS_STRATEGY'
                  ELSE 'BALANCED_STRATEGY'
                END AS tier1_signal
                
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, week
              HAVING COUNT(*) >= 2
            ),
            
            tier2_signals AS (
              -- Tier 2: Tactical Intelligence
              SELECT 
                brand,
                week,
                
                -- Media strategy signals (using correct column name)
                CASE
                  WHEN COUNTIF(UPPER(media_type) = 'VIDEO') / COUNT(*) > 0.5 THEN 'VIDEO_HEAVY'
                  WHEN COUNTIF(UPPER(media_type) = 'IMAGE') / COUNT(*) > 0.8 THEN 'IMAGE_FOCUS'
                  ELSE 'MIXED_MEDIA'
                END AS tier2_signal,
                
                -- Volume signals
                ad_count,
                CASE
                  WHEN ad_count > 20 THEN 'HIGH_VOLUME'
                  WHEN ad_count > 10 THEN 'MEDIUM_VOLUME'
                  ELSE 'LOW_VOLUME'
                END AS volume_signal
                
              FROM tier1_signals
            ),
            
            executive_signals AS (
              -- Tier 3: Executive Intelligence (Integration)
              SELECT 
                t1.brand,
                t1.week,
                t1.tier1_signal,
                t2.tier2_signal,
                t2.volume_signal,
                
                -- Business impact scoring
                CASE
                  WHEN t1.tier1_signal IN ('HIGH_PROMO_STRATEGY', 'URGENCY_STRATEGY') 
                    AND t2.volume_signal = 'HIGH_VOLUME'
                    THEN 5  -- Critical business impact
                  WHEN t1.tier1_signal != 'BALANCED_STRATEGY'
                    AND t2.volume_signal IN ('MEDIUM_VOLUME', 'HIGH_VOLUME')
                    THEN 4  -- High business impact
                  WHEN t1.tier1_signal != 'BALANCED_STRATEGY'
                    OR t2.volume_signal != 'LOW_VOLUME'
                    THEN 3  -- Medium business impact
                  ELSE 2  -- Low business impact
                END AS executive_impact_score,
                
                -- Signal coherence check
                CASE
                  WHEN t1.tier1_signal = 'HIGH_PROMO_STRATEGY' AND t2.tier2_signal = 'VIDEO_HEAVY'
                    THEN 'COHERENT_PROMO_VIDEO'
                  WHEN t1.tier1_signal = 'BRAND_FOCUS_STRATEGY' AND t2.tier2_signal = 'IMAGE_FOCUS'
                    THEN 'COHERENT_BRAND_IMAGE'
                  WHEN t1.tier1_signal = 'URGENCY_STRATEGY' AND t2.volume_signal = 'HIGH_VOLUME'
                    THEN 'COHERENT_URGENCY_VOLUME'
                  ELSE 'MIXED_SIGNALS'
                END AS signal_coherence
                
              FROM tier1_signals t1
              JOIN tier2_signals t2 USING (brand, week)
            )
            
            SELECT 
              brand,
              COUNT(*) AS total_weeks,
              COUNT(DISTINCT tier1_signal) AS tier1_signal_variety,
              COUNT(DISTINCT tier2_signal) AS tier2_signal_variety,
              AVG(executive_impact_score) AS avg_impact_score,
              
              -- Integration quality metrics
              COUNTIF(signal_coherence != 'MIXED_SIGNALS') / COUNT(*) AS coherence_rate,
              COUNTIF(executive_impact_score >= 4) / COUNT(*) AS high_impact_rate,
              
              -- Signal distribution
              STRING_AGG(DISTINCT tier1_signal ORDER BY tier1_signal) AS tier1_signals,
              STRING_AGG(DISTINCT signal_coherence ORDER BY signal_coherence) AS coherence_types
              
            FROM executive_signals
            GROUP BY brand
            ORDER BY avg_impact_score DESC
            """
            
            start_time = time.time()
            integration_results = client.query(integration_query).to_dataframe()
            query_time = time.time() - start_time
            
            self.performance_metrics['tier_integration_query_time'] = query_time
            
            if not integration_results.empty:
                print("📊 Tier Integration Results:")
                for _, row in integration_results.iterrows():
                    print(f"\n  🏢 {row['brand']}:")
                    print(f"    Weeks Analyzed: {row['total_weeks']}")
                    print(f"    Tier 1 Signal Variety: {row['tier1_signal_variety']}")
                    print(f"    Tier 2 Signal Variety: {row['tier2_signal_variety']}")
                    print(f"    Avg Impact Score: {row['avg_impact_score']:.1f}/5")
                    print(f"    Signal Coherence: {row['coherence_rate']:.1%}")
                    print(f"    High Impact Rate: {row['high_impact_rate']:.1%}")
                
                # Overall integration health
                avg_coherence = integration_results['coherence_rate'].mean()
                avg_impact = integration_results['avg_impact_score'].mean()
                
                print(f"\n📈 INTEGRATION HEALTH:")
                print(f"  Average Coherence Rate: {avg_coherence:.1%}")
                print(f"  Average Impact Score: {avg_impact:.1f}/5")
                print(f"  Query Time: {query_time:.2f}s")
                
                integration_success = avg_coherence > 0.3 and avg_impact > 2.5
                
                self.results['tier_integration'] = {
                    'status': 'PASS' if integration_success else 'FAIL',
                    'coherence_rate': avg_coherence,
                    'impact_score': avg_impact,
                    'query_time': query_time,
                    'brands_analyzed': len(integration_results)
                }
                
                return integration_success
            else:
                print("❌ No integration results generated")
                return False
                
        except Exception as e:
            print(f"❌ Tier Integration Test Failed: {str(e)}")
            self.errors.append(f"Tier Integration: {str(e)}")
            return False

    def test_advanced_features(self):
        """Test 3: Advanced Features (Multi-Horizon + Cascade Detection)"""
        print(f"\n🚀 TEST 3: ADVANCED FEATURES")
        print("="*60)
        
        advanced_success = True
        
        try:
            # Test Multi-Horizon Forecasting
            print("🔮 Testing Multi-Horizon Forecasting...")
            
            horizon_query = f"""
            WITH daily_metrics AS (
              SELECT 
                brand,
                DATE(start_timestamp) AS date,
                AVG(promotional_intensity) AS daily_promo,
                COUNT(*) AS daily_volume
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, date
            ),
            
            forecasts AS (
              SELECT 
                brand,
                date,
                daily_promo,
                
                -- 24-hour forecast (simple momentum)
                LEAST(1.0, GREATEST(0.0, 
                  daily_promo + COALESCE(daily_promo - LAG(daily_promo, 1) OVER (
                    PARTITION BY brand ORDER BY date
                  ), 0)
                )) AS forecast_24h,
                
                -- 7-day forecast (trend-adjusted)
                AVG(daily_promo) OVER (
                  PARTITION BY brand ORDER BY date 
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS forecast_7d,
                
                -- Horizon divergence
                ABS(daily_promo - AVG(daily_promo) OVER (
                  PARTITION BY brand ORDER BY date 
                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                )) AS horizon_divergence
                
              FROM daily_metrics
            )
            
            SELECT 
              brand,
              COUNT(*) AS forecast_points,
              AVG(ABS(daily_promo - forecast_24h)) AS mae_24h,
              AVG(ABS(daily_promo - forecast_7d)) AS mae_7d,
              AVG(horizon_divergence) AS avg_divergence,
              MAX(horizon_divergence) AS max_divergence
              
            FROM forecasts
            WHERE forecast_24h IS NOT NULL AND forecast_7d IS NOT NULL
            GROUP BY brand
            """
            
            start_time = time.time()
            horizon_results = client.query(horizon_query).to_dataframe()
            horizon_time = time.time() - start_time
            
            if not horizon_results.empty:
                avg_mae_24h = horizon_results['mae_24h'].mean()
                avg_mae_7d = horizon_results['mae_7d'].mean()
                
                print(f"  📊 Forecast Accuracy:")
                print(f"    24-hour MAE: {avg_mae_24h:.3f}")
                print(f"    7-day MAE: {avg_mae_7d:.3f}")
                print(f"    Query Time: {horizon_time:.2f}s")
                
                horizon_success = avg_mae_24h < 0.3 and avg_mae_7d < 0.3
                advanced_success &= horizon_success
            else:
                print("  ❌ Multi-Horizon test failed")
                advanced_success = False
                
        except Exception as e:
            print(f"  ❌ Multi-Horizon error: {str(e)}")
            advanced_success = False
        
        try:
            # Test Cascade Detection
            print("\n🌊 Testing Cascade Detection...")
            
            cascade_query = f"""
            WITH brand_changes AS (
              SELECT 
                brand,
                week_start,
                AVG(promotional_intensity) AS avg_promo,
                AVG(promotional_intensity) - LAG(AVG(promotional_intensity)) OVER (
                  PARTITION BY brand ORDER BY week_start
                ) AS promo_delta
                
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, week_start
            ),
            
            cascades AS (
              SELECT 
                a.brand AS leader,
                a.week_start AS leader_week,
                a.promo_delta AS leader_delta,
                b.brand AS follower,
                b.week_start AS follower_week,
                b.promo_delta AS follower_delta,
                DATE_DIFF(DATE(b.week_start), DATE(a.week_start), WEEK) AS lag_weeks,
                
                CASE
                  WHEN ABS(a.promo_delta) > 0.02 AND ABS(b.promo_delta) > 0.02
                    AND DATE_DIFF(DATE(b.week_start), DATE(a.week_start), WEEK) BETWEEN 1 AND 3
                    THEN 'POTENTIAL_CASCADE'
                  ELSE 'NO_CASCADE'
                END AS cascade_type
                
              FROM brand_changes a
              CROSS JOIN brand_changes b
              WHERE a.brand != b.brand
                AND a.week_start < b.week_start
                AND a.promo_delta IS NOT NULL
                AND b.promo_delta IS NOT NULL
            )
            
            SELECT 
              cascade_type,
              COUNT(*) AS pattern_count,
              AVG(lag_weeks) AS avg_lag_weeks
            FROM cascades
            GROUP BY cascade_type
            """
            
            start_time = time.time()
            cascade_results = client.query(cascade_query).to_dataframe()
            cascade_time = time.time() - start_time
            
            if not cascade_results.empty:
                cascades_detected = cascade_results[
                    cascade_results['cascade_type'] == 'POTENTIAL_CASCADE'
                ]['pattern_count'].sum() if 'POTENTIAL_CASCADE' in cascade_results['cascade_type'].values else 0
                
                print(f"  📊 Cascade Detection:")
                print(f"    Patterns Detected: {cascades_detected}")
                print(f"    Query Time: {cascade_time:.2f}s")
                
                cascade_success = cascades_detected > 0
                advanced_success &= cascade_success
            else:
                print("  ❌ Cascade detection failed")
                advanced_success = False
                
        except Exception as e:
            print(f"  ❌ Cascade detection error: {str(e)}")
            advanced_success = False
        
        self.results['advanced_features'] = {
            'status': 'PASS' if advanced_success else 'FAIL',
            'horizon_success': horizon_success if 'horizon_success' in locals() else False,
            'cascade_success': cascade_success if 'cascade_success' in locals() else False
        }
        
        return advanced_success

    def test_performance_benchmarks(self):
        """Test 4: Performance Benchmarks"""
        print(f"\n⚡ TEST 4: PERFORMANCE BENCHMARKS")
        print("="*60)
        
        try:
            # Test query performance under different loads
            performance_queries = [
                ("Strategic Overview", f"""
                SELECT brand, 
                  AVG(promotional_intensity) as avg_promo,
                  COUNT(*) as total_ads
                FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
                GROUP BY brand
                """),
                
                ("Time Series Analysis", f"""
                SELECT 
                  brand,
                  DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week,
                  AVG(promotional_intensity) AS weekly_promo
                FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
                GROUP BY brand, week
                ORDER BY brand, week
                """),
                
                ("Complex Aggregation", f"""
                SELECT 
                  brand,
                  media_type,
                  primary_angle,
                  COUNT(*) as count,
                  AVG(promotional_intensity) as avg_promo,
                  STDDEV(brand_voice_score) as voice_variation
                FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
                GROUP BY brand, media_type, primary_angle
                HAVING COUNT(*) > 1
                ORDER BY count DESC
                """)
            ]
            
            print("📊 Performance Results:")
            all_queries_fast = True
            
            for query_name, query_sql in performance_queries:
                start_time = time.time()
                result = client.query(query_sql).to_dataframe()
                query_time = time.time() - start_time
                
                rows_returned = len(result)
                is_fast = query_time < 5.0  # 5 second threshold
                all_queries_fast &= is_fast
                
                status = "✅" if is_fast else "⚠️"
                print(f"  {status} {query_name}: {query_time:.2f}s ({rows_returned} rows)")
                
                self.performance_metrics[f"{query_name.lower().replace(' ', '_')}_time"] = query_time
            
            print(f"\n📈 Performance Summary:")
            avg_time = np.mean(list(self.performance_metrics.values()))
            print(f"  Average Query Time: {avg_time:.2f}s")
            print(f"  All Queries <5s: {'✅' if all_queries_fast else '❌'}")
            
            self.results['performance'] = {
                'status': 'PASS' if all_queries_fast else 'FAIL',
                'avg_query_time': avg_time,
                'all_fast': all_queries_fast
            }
            
            return all_queries_fast
            
        except Exception as e:
            print(f"❌ Performance Test Failed: {str(e)}")
            self.errors.append(f"Performance: {str(e)}")
            return False

    def test_edge_cases(self):
        """Test 5: Edge Case Handling"""
        print(f"\n🛡️ TEST 5: EDGE CASE HANDLING")
        print("="*60)
        
        try:
            # Test handling of edge cases
            edge_case_query = f"""
            WITH edge_case_scenarios AS (
              SELECT 
                brand,
                COUNT(*) as total_ads,
                COUNT(DISTINCT DATE(start_timestamp)) as unique_dates,
                MIN(promotional_intensity) as min_promo,
                MAX(promotional_intensity) as max_promo,
                COUNT(CASE WHEN promotional_intensity IS NULL THEN 1 END) as null_promo,
                COUNT(CASE WHEN promotional_intensity < 0 OR promotional_intensity > 1 THEN 1 END) as invalid_promo,
                
                -- Edge case flags
                CASE WHEN COUNT(*) < 5 THEN 'LOW_VOLUME' ELSE 'NORMAL_VOLUME' END as volume_case,
                CASE WHEN COUNT(DISTINCT DATE(start_timestamp)) <= 2 THEN 'LIMITED_DATES' ELSE 'NORMAL_DATES' END as date_case,
                CASE WHEN STDDEV(promotional_intensity) < 0.01 THEN 'NO_VARIATION' ELSE 'NORMAL_VARIATION' END as variation_case
                
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand
            )
            
            SELECT 
              volume_case,
              date_case,
              variation_case,
              COUNT(*) as brand_count,
              AVG(total_ads) as avg_ads_per_brand,
              SUM(null_promo) as total_null_values,
              SUM(invalid_promo) as total_invalid_values
              
            FROM edge_case_scenarios
            GROUP BY volume_case, date_case, variation_case
            ORDER BY brand_count DESC
            """
            
            start_time = time.time()
            edge_results = client.query(edge_case_query).to_dataframe()
            edge_time = time.time() - start_time
            
            if not edge_results.empty:
                print("📊 Edge Case Analysis:")
                for _, row in edge_results.iterrows():
                    print(f"  {row['volume_case']} | {row['date_case']} | {row['variation_case']}: {row['brand_count']} brands")
                
                total_nulls = edge_results['total_null_values'].sum()
                total_invalid = edge_results['total_invalid_values'].sum()
                
                print(f"\n🛡️ Data Quality Issues:")
                print(f"  Null Values: {total_nulls}")
                print(f"  Invalid Values: {total_invalid}")
                print(f"  Query Time: {edge_time:.2f}s")
                
                edge_success = total_nulls < 100 and total_invalid == 0  # Reasonable thresholds
                
                self.results['edge_cases'] = {
                    'status': 'PASS' if edge_success else 'WARN',
                    'null_values': int(total_nulls),
                    'invalid_values': int(total_invalid),
                    'query_time': edge_time
                }
                
                return edge_success
            else:
                print("❌ Edge case analysis failed")
                return False
                
        except Exception as e:
            print(f"❌ Edge Case Test Failed: {str(e)}")
            self.errors.append(f"Edge Cases: {str(e)}")
            return False

    def generate_validation_report(self):
        """Generate comprehensive validation report"""
        print(f"\n📋 COMPREHENSIVE VALIDATION REPORT")
        print("="*80)
        
        total_tests = len(self.results)
        passed_tests = sum(1 for result in self.results.values() if result['status'] == 'PASS')
        
        print(f"📊 TEST SUMMARY:")
        print(f"  Total Tests: {total_tests}")
        print(f"  Passed: {passed_tests}")
        print(f"  Success Rate: {passed_tests/total_tests:.1%}")
        
        print(f"\n📈 DETAILED RESULTS:")
        for test_name, result in self.results.items():
            status_icon = "✅" if result['status'] == 'PASS' else "⚠️" if result['status'] == 'WARN' else "❌"
            print(f"  {status_icon} {test_name.replace('_', ' ').title()}: {result['status']}")
        
        print(f"\n⚡ PERFORMANCE METRICS:")
        for metric_name, time_value in self.performance_metrics.items():
            print(f"  {metric_name.replace('_', ' ').title()}: {time_value:.2f}s")
        
        if self.errors:
            print(f"\n❌ ERRORS ENCOUNTERED:")
            for error in self.errors:
                print(f"  • {error}")
        
        # Overall system health
        overall_success = passed_tests == total_tests and len(self.errors) == 0
        avg_query_time = np.mean(list(self.performance_metrics.values())) if self.performance_metrics else 0
        
        print(f"\n🎯 OVERALL SYSTEM HEALTH:")
        print(f"  Status: {'🟢 HEALTHY' if overall_success else '🟡 NEEDS ATTENTION' if passed_tests >= total_tests * 0.8 else '🔴 CRITICAL ISSUES'}")
        print(f"  Average Query Performance: {avg_query_time:.2f}s")
        print(f"  Ready for Production: {'YES' if overall_success and avg_query_time < 3.0 else 'NEEDS OPTIMIZATION'}")
        
        return overall_success

def run_validation_suite():
    """Run the complete validation suite"""
    print("🚀 STARTING COMPREHENSIVE VALIDATION SUITE")
    print("="*80)
    print("Testing: Data Quality + Tier Integration + Advanced Features + Performance + Edge Cases")
    print("="*80)
    
    suite = ComprehensiveValidationSuite()
    
    # Run all tests
    tests = [
        suite.test_data_quality,
        suite.test_tier_integration,
        suite.test_advanced_features,
        suite.test_performance_benchmarks,
        suite.test_edge_cases
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {str(e)}")
            results.append(False)
    
    # Generate final report
    overall_success = suite.generate_validation_report()
    
    print(f"\n{'='*80}")
    if overall_success:
        print("🎉 ALL VALIDATION TESTS PASSED")
        print("✅ System ready for next phase validation")
    else:
        print("⚠️ SOME TESTS NEED ATTENTION")
        print("🔧 Review failures before proceeding")
    print(f"{'='*80}")
    
    return overall_success

if __name__ == "__main__":
    success = run_validation_suite()
    exit(0 if success else 1)