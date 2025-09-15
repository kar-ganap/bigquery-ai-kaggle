#!/usr/bin/env python3
"""
Test optimized whitespace analysis with performance limits
"""

import time
from src.competitive_intel.analysis.enhanced_whitespace_detection import Enhanced3DWhiteSpaceDetector
from src.utils.bigquery_client import run_query

def test_optimized_whitespace_performance():
    """Test the optimized whitespace analysis with practical limits"""

    print("🧪 Testing Optimized Whitespace Analysis")
    print("=" * 60)

    print("🎯 Configuration:")
    print("   - 3 ads per brand (5 brands = 15 total)")
    print("   - Expected time: ~2 minutes (15 * 8.5s)")
    print("   - Performance target: <3 minutes")

    try:
        # Initialize optimized detector
        competitors = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
        detector = Enhanced3DWhiteSpaceDetector(
            project_id="bigquery-ai-kaggle-469620",
            dataset_id="ads_demo",
            brand="Warby Parker",
            competitors=competitors
        )

        print(f"\n🚀 Starting optimized whitespace analysis...")
        print(f"   Brands: Warby Parker + {len(competitors)} competitors")

        # Execute optimized analysis
        start_time = time.time()
        whitespace_sql = detector.analyze_real_strategic_positions("stage4_test")

        print(f"   📝 SQL generated ({len(whitespace_sql)} chars)")
        print(f"   🔍 Executing BigQuery analysis...")

        results = run_query(whitespace_sql)
        duration = time.time() - start_time

        print(f"\n✅ ANALYSIS COMPLETE")
        print(f"   ⏱️  Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"   📊 Results: {results.shape if results is not None else 'None'}")

        if results is not None and not results.empty:
            print(f"   🎯 Opportunities found: {len(results)}")

            # Show sample opportunities
            print(f"\n🔍 Top Strategic Opportunities:")
            for i, (_, row) in enumerate(results.head(3).iterrows(), 1):
                space_type = row.get('space_type', 'UNKNOWN')
                messaging = row.get('messaging_angle', 'UNKNOWN')
                funnel = row.get('funnel_stage', 'UNKNOWN')
                score = row.get('overall_score', 0)
                print(f"   {i}. {space_type}: {messaging} messaging in {funnel} (Score: {score:.2f})")

            # Generate strategic recommendations
            opportunities = detector.generate_strategic_opportunities(results)
            print(f"\n📋 Strategic Recommendations:")
            for i, opp in enumerate(opportunities[:3], 1):
                print(f"   {i}. {opp}")

        else:
            print(f"   ⚠️  No opportunities identified")

        # Performance assessment
        print(f"\n📊 PERFORMANCE ASSESSMENT")
        print("-" * 40)

        target_time = 180  # 3 minutes
        if duration <= target_time:
            print(f"✅ PERFORMANCE: Excellent ({duration:.1f}s ≤ {target_time}s target)")
            print(f"   Ready for production use")
        elif duration <= target_time * 1.5:
            print(f"⚠️  PERFORMANCE: Acceptable ({duration:.1f}s, slightly over target)")
            print(f"   Usable but consider further optimization")
        else:
            print(f"❌ PERFORMANCE: Too slow ({duration:.1f}s > {target_time*1.5}s)")
            print(f"   Needs further optimization or should use fallback")

        # Integration assessment
        total_pipeline_time = 392  # From our earlier analysis
        whitespace_percentage = (duration / total_pipeline_time) * 100

        print(f"\n🔗 PIPELINE INTEGRATION")
        print("-" * 30)
        print(f"Total pipeline time: {total_pipeline_time}s")
        print(f"Whitespace analysis: {duration:.1f}s ({whitespace_percentage:.1f}% of pipeline)")

        if whitespace_percentage <= 15:
            print(f"✅ INTEGRATION: Excellent - {whitespace_percentage:.1f}% impact")
        elif whitespace_percentage <= 25:
            print(f"⚠️  INTEGRATION: Acceptable - {whitespace_percentage:.1f}% impact")
        else:
            print(f"❌ INTEGRATION: Too expensive - {whitespace_percentage:.1f}% impact")

        return True, duration, len(results) if results is not None else 0

    except Exception as e:
        print(f"❌ Optimized whitespace analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False, float('inf'), 0

def compare_with_fallback():
    """Compare optimized ML approach with basic fallback"""

    print(f"\n" + "=" * 60)
    print("🔄 COMPARING ML vs FALLBACK APPROACHES")
    print("=" * 60)

    # We know fallback is very fast from earlier tests
    fallback_time = 0.7  # seconds from pipeline logs
    fallback_quality = "Basic strategic insights"

    print(f"💨 Fallback approach: {fallback_time}s")
    print(f"   Quality: {fallback_quality}")

    # Test ML approach
    ml_success, ml_time, ml_opportunities = test_optimized_whitespace_performance()

    if ml_success:
        speedup_cost = ml_time / fallback_time
        print(f"\n📊 COMPARISON SUMMARY")
        print("-" * 30)
        print(f"ML approach: {ml_time:.1f}s, {ml_opportunities} opportunities")
        print(f"Fallback: {fallback_time}s, basic insights")
        print(f"Trade-off: {speedup_cost:.0f}x slower but {ml_opportunities}x more insights")

        if speedup_cost <= 30 and ml_opportunities >= 3:
            print(f"✅ RECOMMENDATION: Use ML approach - good value")
        elif speedup_cost <= 60:
            print(f"⚠️  RECOMMENDATION: ML acceptable for deep analysis")
        else:
            print(f"❌ RECOMMENDATION: Use fallback - ML too expensive")
    else:
        print(f"❌ RECOMMENDATION: Use fallback - ML approach failed")

if __name__ == "__main__":
    compare_with_fallback()