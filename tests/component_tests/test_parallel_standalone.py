#!/usr/bin/env python3
"""
Test Parallel Whitespace Detection Standalone
"""

import time
from src.competitive_intel.analysis.parallel_whitespace_detection import ParallelWhiteSpaceDetector

def test_parallel_standalone():
    """Test the complete parallel whitespace analysis"""

    print("🧪 Testing Parallel Whitespace Detection (Standalone)")
    print("=" * 60)

    # Initialize detector
    competitors = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
    detector = ParallelWhiteSpaceDetector(
        project_id="bigquery-ai-kaggle-469620",
        dataset_id="ads_demo",
        brand="Warby Parker",
        competitors=competitors
    )

    print(f"🎯 Brands: Warby Parker + {len(competitors)} competitors")
    print(f"📊 Full dataset processing")

    # Test the full analysis
    start_time = time.time()
    results = detector.analyze_parallel_performance("parallel_test_standalone")
    duration = time.time() - start_time

    print(f"\n✅ ANALYSIS COMPLETE")
    print(f"   ⏱️  Duration: {duration:.1f}s")
    print(f"   📊 Status: {results.get('status')}")

    if results.get('status') == 'success':
        opportunities = results.get('strategic_opportunities', [])
        print(f"   🎯 Opportunities: {len(opportunities)}")
        print(f"   📈 Performance: {results.get('performance_category')}")
        print(f"   🔍 Coverage: {results.get('coverage')}")

        # Show sample opportunities
        print(f"\n🔍 Sample Strategic Opportunities:")
        for i, opp in enumerate(opportunities[:3], 1):
            print(f"   {i}. {opp[:80]}...")

        return True, duration, len(opportunities)
    else:
        print(f"   ❌ Error: {results.get('error')}")
        return False, duration, 0

if __name__ == "__main__":
    success, duration, count = test_parallel_standalone()

    print(f"\n🎯 PARALLEL WHITESPACE ASSESSMENT")
    print("-" * 40)

    if success:
        if duration <= 10:
            print(f"✅ EXCELLENT: {duration:.1f}s execution, {count} opportunities")
        elif duration <= 30:
            print(f"✅ GOOD: {duration:.1f}s execution, {count} opportunities")
        else:
            print(f"⚠️  SLOW: {duration:.1f}s execution")

        print(f"📊 Ready for Stage 8 integration")
    else:
        print(f"❌ FAILED: Needs debugging before Stage 8")