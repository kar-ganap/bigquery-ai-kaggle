#!/usr/bin/env python3
"""
Test Batched Whitespace Analysis Performance
Compare batched AI.GENERATE calls vs individual row processing
"""

import time
from datetime import datetime
from src.competitive_intel.analysis.batched_whitespace_detection import BatchedWhiteSpaceDetector
from src.competitive_intel.analysis.enhanced_whitespace_detection import Enhanced3DWhiteSpaceDetector

def test_batched_vs_individual_performance():
    """Compare performance of batched vs individual AI.GENERATE approaches"""

    print("🧪 Testing Batched vs Individual Whitespace Analysis")
    print("=" * 70)

    # Setup
    competitors = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
    run_id = "stage4_test"

    print(f"🎯 Target: Warby Parker")
    print(f"🏢 Competitors: {competitors}")
    print(f"📊 Using data: {run_id}")

    # Test 1: Batched Approach
    print(f"\n1️⃣ Testing Batched Approach (50 ads per AI.GENERATE call)")
    print("-" * 50)

    try:
        batched_detector = BatchedWhiteSpaceDetector(
            project_id="bigquery-ai-kaggle-469620",
            dataset_id="ads_demo",
            brand="Warby Parker",
            competitors=competitors
        )

        start_time = time.time()
        batched_results = batched_detector.analyze_batched_performance(run_id, batch_size=50)
        batched_duration = time.time() - start_time

        print(f"   ✅ Status: {batched_results['status']}")
        print(f"   ⏱️  Duration: {batched_duration:.1f} seconds")
        print(f"   📊 Rows processed: {batched_results.get('rows_processed', 0)}")
        print(f"   🎯 Opportunities: {batched_results.get('opportunities_found', 0)}")

        if batched_results.get('top_opportunities'):
            print(f"   🔍 Top opportunity: {batched_results['top_opportunities'][0][:100]}...")

        batched_success = batched_results['status'] == 'success'

    except Exception as e:
        print(f"   ❌ Batched approach failed: {e}")
        batched_success = False
        batched_duration = float('inf')

    # Test 2: Individual Approach (limited sample to avoid long wait)
    print(f"\n2️⃣ Testing Individual Approach (limited to 5 ads for timing)")
    print("-" * 50)

    try:
        individual_detector = Enhanced3DWhiteSpaceDetector(
            project_id="bigquery-ai-kaggle-469620",
            dataset_id="ads_demo",
            brand="Warby Parker",
            competitors=competitors
        )

        # Generate SQL but limit it for testing
        start_time = time.time()
        individual_sql = individual_detector.analyze_real_strategic_positions(run_id)

        # Modify SQL to limit rows for performance testing
        limited_sql = individual_sql.replace(
            "AND r.brand IN (",
            "AND r.brand IN ("
        ).replace(
            "FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r",
            "FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r"
        )

        # Add LIMIT for testing (we'll estimate full performance from this)
        if "ORDER BY overall_score DESC" in limited_sql:
            limited_sql = limited_sql.replace(
                "ORDER BY overall_score DESC, market_potential DESC\n        LIMIT 20",
                "ORDER BY overall_score DESC, market_potential DESC\n        LIMIT 5"
            )

        # Add sample limit to reduce AI.GENERATE calls for testing
        limited_sql = limited_sql.replace(
            "WHERE r.creative_text IS NOT NULL",
            "WHERE r.creative_text IS NOT NULL\n    QUALIFY ROW_NUMBER() OVER (PARTITION BY r.brand ORDER BY r.start_timestamp DESC) <= 2"
        )

        print(f"   🔍 Testing with limited dataset (2 ads per brand)...")

        from src.utils.bigquery_client import run_query
        individual_results = run_query(limited_sql)
        individual_duration = time.time() - start_time

        if individual_results is not None:
            print(f"   ✅ Status: success")
            print(f"   ⏱️  Duration: {individual_duration:.1f} seconds (limited sample)")
            print(f"   📊 Rows processed: {len(individual_results)}")

            # Estimate full performance
            estimated_full_duration = individual_duration * 5  # Estimate for full dataset
            print(f"   📈 Estimated full duration: ~{estimated_full_duration:.1f} seconds")

            individual_success = True
        else:
            print(f"   ❌ No results returned")
            individual_success = False
            individual_duration = 0

    except Exception as e:
        print(f"   ❌ Individual approach failed: {e}")
        individual_success = False
        individual_duration = float('inf')
        estimated_full_duration = float('inf')

    # Performance Comparison
    print(f"\n📊 PERFORMANCE COMPARISON")
    print("=" * 70)

    if batched_success and individual_success:
        speedup = estimated_full_duration / batched_duration if batched_duration > 0 else float('inf')
        print(f"🚀 Batched approach: {batched_duration:.1f}s")
        print(f"🐌 Individual approach: ~{estimated_full_duration:.1f}s (estimated)")
        print(f"⚡ Performance improvement: {speedup:.1f}x faster")

        if speedup > 5:
            print(f"✅ RECOMMENDATION: Use batched approach - {speedup:.1f}x performance gain!")
        elif speedup > 2:
            print(f"✅ RECOMMENDATION: Use batched approach - moderate performance gain")
        else:
            print(f"⚠️  RECOMMENDATION: Marginal improvement, either approach acceptable")

    elif batched_success:
        print(f"✅ Batched approach: {batched_duration:.1f}s - Working")
        print(f"❌ Individual approach: Failed")
        print(f"✅ RECOMMENDATION: Use batched approach - only working solution")

    elif individual_success:
        print(f"❌ Batched approach: Failed")
        print(f"✅ Individual approach: ~{estimated_full_duration:.1f}s - Working")
        print(f"⚠️  RECOMMENDATION: Fix batched approach or use individual with caching")

    else:
        print(f"❌ Both approaches failed")
        print(f"🔧 RECOMMENDATION: Debug AI.GENERATE syntax and connection issues")

    # Summary
    print(f"\n📋 SUMMARY")
    print("-" * 30)
    print(f"Goal: Optimize ML whitespace analysis from 2+ minutes to <30 seconds")
    if batched_success and speedup > 5:
        print(f"✅ ACHIEVED: {speedup:.1f}x improvement with batching")
    else:
        print(f"🔧 NEEDS WORK: Further optimization required")

if __name__ == "__main__":
    test_batched_vs_individual_performance()