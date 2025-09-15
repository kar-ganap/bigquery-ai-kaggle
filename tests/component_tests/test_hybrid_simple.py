#!/usr/bin/env python3
"""
Test Hybrid Whitespace Detection - Simple Version
Fix SQL issues and test the hybrid approach
"""

import time
from src.competitive_intel.analysis.hybrid_whitespace_detection import HybridWhiteSpaceDetector

def test_hybrid_sql_generation():
    """Test just the SQL generation without execution"""

    print("🧪 Testing Hybrid Whitespace SQL Generation")
    print("=" * 50)

    # Initialize detector
    competitors = ['LensCrafters', 'EyeBuyDirect']
    detector = HybridWhiteSpaceDetector(
        project_id="bigquery-ai-kaggle-469620",
        dataset_id="ads_demo",
        brand="Warby Parker",
        competitors=competitors
    )

    print(f"🎯 Generating hybrid SQL...")

    try:
        sql = detector.analyze_hybrid_strategic_positions("hybrid_test")
        print(f"✅ SQL generated successfully ({len(sql)} chars)")
        print(f"📊 Approach: Chunked processing with campaign intelligence")

        # Show SQL snippet
        print(f"\n🔍 SQL Preview (first 500 chars):")
        print(sql[:500] + "...")

        return True, len(sql)
    except Exception as e:
        print(f"❌ SQL generation failed: {e}")
        return False, 0

def test_hybrid_fallback():
    """Test hybrid with fallback to parallel if needed"""

    print(f"\n🔄 Testing Hybrid with Parallel Fallback")
    print("-" * 40)

    # If hybrid fails, we should use parallel
    try:
        from src.competitive_intel.analysis.parallel_whitespace_detection import ParallelWhiteSpaceDetector

        competitors = ['LensCrafters', 'EyeBuyDirect']
        parallel_detector = ParallelWhiteSpaceDetector(
            project_id="bigquery-ai-kaggle-469620",
            dataset_id="ads_demo",
            brand="Warby Parker",
            competitors=competitors
        )

        start_time = time.time()
        results = parallel_detector.analyze_parallel_performance("hybrid_fallback_test")
        duration = time.time() - start_time

        print(f"✅ Parallel fallback working: {duration:.1f}s")
        print(f"📊 Status: {results.get('status')}")

        if results.get('status') == 'success':
            opportunities = results.get('strategic_opportunities', [])
            print(f"🎯 Fallback opportunities: {len(opportunities)}")
            return True
        else:
            print(f"❌ Fallback error: {results.get('error')}")
            return False

    except Exception as e:
        print(f"❌ Fallback failed: {e}")
        return False

if __name__ == "__main__":
    print("🎯 HYBRID WHITESPACE TESTING STRATEGY")
    print("=" * 60)

    # Test 1: SQL Generation
    sql_success, sql_length = test_hybrid_sql_generation()

    # Test 2: Fallback approach
    fallback_success = test_hybrid_fallback()

    print(f"\n📊 HYBRID TESTING RESULTS")
    print("-" * 30)

    if sql_success and sql_length > 1000:
        print(f"✅ Hybrid SQL: Ready for execution ({sql_length} chars)")
    else:
        print(f"⚠️  Hybrid SQL: Needs debugging")

    if fallback_success:
        print(f"✅ Parallel Fallback: Working perfectly")
    else:
        print(f"❌ Parallel Fallback: Needs attention")

    # Overall recommendation
    print(f"\n🎯 RECOMMENDATION")
    print("-" * 20)

    if sql_success and fallback_success:
        print(f"✅ Use hybrid with parallel fallback")
    elif fallback_success:
        print(f"✅ Use parallel approach (proven)")
    else:
        print(f"❌ Both approaches need work")