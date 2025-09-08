#!/usr/bin/env python3
"""
Execute EASY tests for Subgoal 6 validation
Tests core functionality with real data
"""

import os
import time
from google.cloud import bigquery
from datetime import datetime
import pandas as pd

# Configuration
PROJECT_ID = os.environ.get("BQ_PROJECT", "your-project")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def run_test_query(query_name, query, measure_time=False):
    """Execute a test query and return results"""
    print(f"\n{'='*60}")
    print(f"Running: {query_name}")
    print(f"{'='*60}")
    
    try:
        start_time = time.time()
        
        # Execute query
        query_job = client.query(query)
        results = query_job.result()
        
        execution_time = time.time() - start_time
        
        # Convert to dataframe for analysis
        df = results.to_dataframe() if results.total_rows > 0 else pd.DataFrame()
        
        print(f"✅ Query executed successfully")
        print(f"⏱️  Execution time: {execution_time:.2f} seconds")
        
        if measure_time:
            return df, execution_time
        return df
        
    except Exception as e:
        print(f"❌ Error executing query: {str(e)}")
        return None

def test_1_performance():
    """Test query performance for 4 strategic questions"""
    print("\n" + "="*80)
    print("TEST 1: STRATEGIC QUESTION PERFORMANCE")
    print("="*80)
    
    test_results = []
    
    # Question 1: Self-analysis
    q1 = f"""
    SELECT 
      brand,
      week_start,
      upper_funnel_pct,
      promotional_angle_pct,
      avg_promotional_intensity
    FROM `{PROJECT_ID}.{DATASET_ID}.v_strategy_evolution`
    WHERE brand = 'Nike'
      AND week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
    """
    df1, time1 = run_test_query("Q1: Brand Self-Analysis (Nike last 4 weeks)", q1, measure_time=True)
    test_results.append(("Q1_SELF_ANALYSIS", time1, "PASS" if time1 < 30 else "FAIL"))
    
    # Question 2: Competitor analysis
    q2 = f"""
    SELECT 
      brand,
      week_start,
      upper_funnel_pct,
      promotional_angle_pct,
      avg_promotional_intensity
    FROM `{PROJECT_ID}.{DATASET_ID}.v_strategy_evolution`
    WHERE brand IN ('Adidas', 'Under Armour', 'Puma')
      AND week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
    """
    df2, time2 = run_test_query("Q2: Competitor Analysis", q2, measure_time=True)
    test_results.append(("Q2_COMPETITOR_ANALYSIS", time2, "PASS" if time2 < 30 else "FAIL"))
    
    # Question 3: Strategy evolution
    q3 = f"""
    SELECT 
      brand,
      week_start,
      integrated_strategy_change_type,
      strategic_profile
    FROM `{PROJECT_ID}.{DATASET_ID}.v_integrated_strategy_timeseries`
    WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
    LIMIT 100
    """
    df3, time3 = run_test_query("Q3: Strategy Evolution", q3, measure_time=True)
    test_results.append(("Q3_STRATEGY_EVOLUTION", time3, "PASS" if time3 < 30 else "FAIL"))
    
    # Question 4: Forecasting
    q4 = f"""
    SELECT 
      brand,
      forecast_week,
      forecasted_ad_volume,
      forecasted_aggressiveness,
      volume_trend_signal
    FROM `{PROJECT_ID}.{DATASET_ID}.strategic_forecasts`
    WHERE forecast_week > CURRENT_DATE()
    LIMIT 100
    """
    df4, time4 = run_test_query("Q4: Trend Forecasting", q4, measure_time=True)
    test_results.append(("Q4_FORECASTING", time4, "PASS" if time4 < 30 else "FAIL"))
    
    # Summary
    print("\n" + "-"*60)
    print("PERFORMANCE TEST SUMMARY:")
    for test_name, exec_time, result in test_results:
        status_icon = "✅" if result == "PASS" else "❌"
        print(f"{status_icon} {test_name}: {exec_time:.2f}s - {result}")
    
    passed = sum(1 for _, _, r in test_results if r == "PASS")
    print(f"\nOverall: {passed}/4 tests passed")
    
    return passed == 4

def test_2_classification_coverage():
    """Test classification success rate"""
    print("\n" + "="*80)
    print("TEST 2: CLASSIFICATION COVERAGE")
    print("="*80)
    
    query = f"""
    WITH classification_coverage AS (
      SELECT 
        COUNT(*) AS total_ads,
        COUNTIF(funnel IS NOT NULL) AS ads_with_funnel,
        COUNTIF(ARRAY_LENGTH(angles) > 0) AS ads_with_angles,
        COUNTIF(persona IS NOT NULL) AS ads_with_persona,
        COUNTIF(urgency_score IS NOT NULL) AS ads_with_urgency_score,
        COUNTIF(
          funnel IS NOT NULL 
          AND ARRAY_LENGTH(angles) > 0 
          AND persona IS NOT NULL
          AND urgency_score IS NOT NULL
        ) AS fully_classified_ads
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_v2`
      WHERE start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
    )
    SELECT 
      total_ads,
      fully_classified_ads,
      ROUND(fully_classified_ads / total_ads * 100, 2) AS classification_success_rate_pct,
      ROUND(ads_with_funnel / total_ads * 100, 2) AS funnel_success_rate_pct,
      ROUND(ads_with_angles / total_ads * 100, 2) AS angles_success_rate_pct,
      ROUND(ads_with_persona / total_ads * 100, 2) AS persona_success_rate_pct
    FROM classification_coverage
    """
    
    df = run_test_query("Classification Coverage Analysis", query)
    
    if df is not None and not df.empty:
        success_rate = df['classification_success_rate_pct'].iloc[0]
        print(f"\n📊 Classification Metrics:")
        print(f"   Total ads analyzed: {df['total_ads'].iloc[0]}")
        print(f"   Fully classified: {df['fully_classified_ads'].iloc[0]}")
        print(f"   Success rate: {success_rate}%")
        print(f"   Funnel classification: {df['funnel_success_rate_pct'].iloc[0]}%")
        print(f"   Angle extraction: {df['angles_success_rate_pct'].iloc[0]}%")
        print(f"   Persona identification: {df['persona_success_rate_pct'].iloc[0]}%")
        
        if success_rate >= 85:
            print("✅ TEST PASSED: Classification rate >= 85%")
            return True
        else:
            print(f"❌ TEST FAILED: Classification rate {success_rate}% < 85%")
            return False
    return False

def test_3_promotional_detection():
    """Test promotional period detection accuracy"""
    print("\n" + "="*80)
    print("TEST 3: PROMOTIONAL DETECTION")
    print("="*80)
    
    query = f"""
    WITH promotional_validation AS (
      SELECT 
        brand,
        COUNTIF(aggressiveness_tier = 'HIGHLY_AGGRESSIVE') AS highly_aggressive,
        COUNTIF(aggressiveness_tier = 'BRAND_FOCUSED') AS brand_focused,
        COUNTIF(promotional_theme IN ('SEASONAL_MAJOR', 'PROMOTIONAL')) AS promotional_ads,
        COUNTIF(promotional_theme = 'EVERGREEN') AS brand_messaging_ads,
        COUNT(*) AS total_ads,
        AVG(CASE WHEN discount_percentage > 0 THEN discount_percentage END) AS avg_discount
      FROM `{PROJECT_ID}.{DATASET_ID}.cta_aggressiveness_analysis`
      WHERE start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
      GROUP BY brand
    )
    SELECT 
      COUNT(DISTINCT brand) AS brands_analyzed,
      SUM(promotional_ads) AS total_promotional,
      SUM(brand_messaging_ads) AS total_brand_messaging,
      ROUND(SUM(promotional_ads) / SUM(total_ads) * 100, 2) AS promotional_pct,
      ROUND(AVG(avg_discount), 1) AS avg_discount_pct
    FROM promotional_validation
    """
    
    df = run_test_query("Promotional Detection Analysis", query)
    
    if df is not None and not df.empty:
        promotional_pct = df['promotional_pct'].iloc[0]
        print(f"\n📊 Promotional Detection Metrics:")
        print(f"   Brands analyzed: {df['brands_analyzed'].iloc[0]}")
        print(f"   Promotional ads: {df['total_promotional'].iloc[0]}")
        print(f"   Brand messaging ads: {df['total_brand_messaging'].iloc[0]}")
        print(f"   Promotional percentage: {promotional_pct}%")
        print(f"   Average discount when present: {df['avg_discount_pct'].iloc[0]}%")
        
        if 15 <= promotional_pct <= 65:
            print("✅ TEST PASSED: Reasonable promotional/brand split detected")
            return True
        else:
            print(f"⚠️  TEST NEEDS REVIEW: Promotional percentage {promotional_pct}% outside expected range")
            return False
    return False

def test_4_strategy_shifts():
    """Test strategy shift detection"""
    print("\n" + "="*80)
    print("TEST 4: STRATEGY SHIFT DETECTION")
    print("="*80)
    
    query = f"""
    SELECT 
      brand,
      week_start,
      integrated_strategy_change_type,
      promotional_trend_wow,
      funnel_trend_wow
    FROM `{PROJECT_ID}.{DATASET_ID}.v_integrated_strategy_timeseries`
    WHERE integrated_strategy_change_type != 'STABLE_INTEGRATED_STRATEGY'
      AND week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
    ORDER BY week_start DESC
    LIMIT 20
    """
    
    df = run_test_query("Strategy Shift Detection", query)
    
    if df is not None and not df.empty:
        shifts_detected = len(df)
        brands_with_shifts = df['brand'].nunique()
        
        print(f"\n📊 Strategy Shift Metrics:")
        print(f"   Total shifts detected: {shifts_detected}")
        print(f"   Brands with shifts: {brands_with_shifts}")
        print(f"\n   Recent shifts:")
        for _, row in df.head(5).iterrows():
            print(f"   - {row['brand']} ({row['week_start']}): {row['integrated_strategy_change_type']}")
        
        if shifts_detected >= 3:
            print("✅ TEST PASSED: Multiple strategy shifts detected")
            return True
        elif shifts_detected >= 1:
            print("⚠️  PARTIAL PASS: Some shifts detected")
            return True
        else:
            print("❌ TEST FAILED: No strategy shifts detected")
            return False
    return False

def test_5_multi_brand_trends():
    """Test multi-brand trend detection"""
    print("\n" + "="*80)
    print("TEST 5: MULTI-BRAND TREND DETECTION")
    print("="*80)
    
    query = f"""
    WITH brand_trends AS (
      SELECT 
        brand,
        COUNT(DISTINCT week_start) AS weeks_with_data,
        COUNTIF(strategy_change_type != 'STABLE_STRATEGY') AS strategy_changes,
        AVG(upper_funnel_pct) AS avg_upper_funnel,
        STDDEV(promotional_angle_pct) AS promotional_volatility
      FROM `{PROJECT_ID}.{DATASET_ID}.v_strategy_evolution`
      WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
      GROUP BY brand
      HAVING weeks_with_data >= 4
    )
    SELECT 
      COUNT(*) AS brands_analyzed,
      COUNTIF(strategy_changes > 0) AS brands_with_changes,
      AVG(promotional_volatility) AS avg_volatility
    FROM brand_trends
    """
    
    df = run_test_query("Multi-Brand Trend Analysis", query)
    
    if df is not None and not df.empty:
        brands_analyzed = df['brands_analyzed'].iloc[0]
        brands_with_changes = df['brands_with_changes'].iloc[0]
        
        print(f"\n📊 Multi-Brand Trend Metrics:")
        print(f"   Brands analyzed: {brands_analyzed}")
        print(f"   Brands with strategy changes: {brands_with_changes}")
        print(f"   Average volatility: {df['avg_volatility'].iloc[0]:.3f}")
        
        if brands_analyzed >= 3 and brands_with_changes >= 2:
            print("✅ TEST PASSED: Trends detected across multiple brands")
            return True
        elif brands_analyzed >= 2 and brands_with_changes >= 1:
            print("⚠️  PARTIAL PASS: Some multi-brand trends detected")
            return True
        else:
            print("❌ TEST FAILED: Insufficient multi-brand trend detection")
            return False
    return False

def test_6_platform_consistency():
    """Test platform consistency analysis"""
    print("\n" + "="*80)
    print("TEST 6: PLATFORM CONSISTENCY")
    print("="*80)
    
    query = f"""
    SELECT 
      COUNT(DISTINCT brand) AS brands_analyzed,
      AVG(avg_platform_optimization_gap) AS avg_optimization_gap,
      COUNTIF(platform_consistency_tier = 'HIGHLY_CONSISTENT') AS consistent_brands,
      COUNTIF(platform_strategy_classification = 'UNIFIED_CROSS_PLATFORM') AS unified_brands
    FROM `{PROJECT_ID}.{DATASET_ID}.platform_consistency_analysis`
    WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK)
    """
    
    df = run_test_query("Platform Consistency Analysis", query)
    
    if df is not None and not df.empty:
        brands_analyzed = df['brands_analyzed'].iloc[0]
        avg_gap = df['avg_optimization_gap'].iloc[0]
        
        print(f"\n📊 Platform Consistency Metrics:")
        print(f"   Brands analyzed: {brands_analyzed}")
        print(f"   Average optimization gap: {avg_gap:.3f}")
        print(f"   Highly consistent brands: {df['consistent_brands'].iloc[0]}")
        print(f"   Unified cross-platform brands: {df['unified_brands'].iloc[0]}")
        
        if brands_analyzed >= 3 and 0.1 <= avg_gap <= 0.6:
            print("✅ TEST PASSED: Platform consistency patterns detected")
            return True
        else:
            print("⚠️  TEST NEEDS REVIEW: Check platform data availability")
            return False
    return False

def main():
    """Run all EASY tests"""
    print("="*80)
    print("SUBGOAL 6 - EASY TESTS EXECUTION")
    print(f"Timestamp: {datetime.now()}")
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print("="*80)
    
    test_results = {
        "Performance": test_1_performance(),
        "Classification": test_2_classification_coverage(),
        "Promotional": test_3_promotional_detection(),
        "Strategy Shifts": test_4_strategy_shifts(),
        "Multi-Brand": test_5_multi_brand_trends(),
        "Platform": test_6_platform_consistency()
    }
    
    # Final summary
    print("\n" + "="*80)
    print("FINAL TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for v in test_results.values() if v)
    total = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")
    
    print(f"\n📊 Overall Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 SUCCESS: All EASY tests passed! Ready for MODERATE tests.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} tests need attention before proceeding to MODERATE tests.")
        return 1

if __name__ == "__main__":
    exit(main())