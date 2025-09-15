#!/usr/bin/env python3
"""
Test Hybrid Whitespace Detection Performance
Goal: Full dataset + rich campaign intelligence in <3 minutes
"""

import time
from src.competitive_intel.analysis.hybrid_whitespace_detection import HybridWhiteSpaceDetector

def test_hybrid_whitespace_performance():
    """Test the hybrid approach: full dataset + campaign intelligence in <3 minutes"""

    print("🧪 Testing Hybrid Chunked-Enhanced Whitespace Analysis")
    print("=" * 70)

    print("🎯 Goals:")
    print("   ✅ Full dataset processing (no sampling)")
    print("   ✅ Rich campaign intelligence (headlines, CTAs, investment guidance)")
    print("   ✅ Target execution time: <3 minutes")
    print("   ✅ Campaign-ready templates")

    try:
        # Initialize hybrid detector
        competitors = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
        detector = HybridWhiteSpaceDetector(
            project_id="bigquery-ai-kaggle-469620",
            dataset_id="ads_demo",
            brand="Warby Parker",
            competitors=competitors
        )

        print(f"\n🚀 Starting hybrid analysis...")
        print(f"   Brands: Warby Parker + {len(competitors)} competitors")
        print(f"   Approach: Single AI call per chunk with campaign intelligence")

        # Execute hybrid analysis with performance tracking
        start_time = time.time()
        results = detector.analyze_hybrid_performance("stage8_hybrid_test")
        duration = time.time() - start_time

        print(f"\n✅ HYBRID ANALYSIS COMPLETE")
        print(f"   ⏱️  Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
        print(f"   🎯 Target met: {'✅' if duration <= 180 else '❌'} (<3 minutes)")

        if results.get('status') == 'success':
            opportunities = results.get('opportunities', [])
            print(f"   📊 Opportunities found: {len(opportunities)}")
            print(f"   🎪 Campaign-ready: {results.get('campaign_ready_opportunities', 0)}")
            print(f"   🏆 High-confidence: {results.get('high_confidence_opportunities', 0)}")
            print(f"   💼 Business value: {results.get('business_value', 'UNKNOWN')}")

            # Show sample campaign intelligence
            print(f"\n🔍 Sample Campaign Intelligence:")
            for i, opp in enumerate(opportunities[:3], 1):
                space_type = opp.get('space_type', 'UNKNOWN')
                score = opp.get('overall_score', 0)
                investment = opp.get('investment_recommendation', 'Unknown')[:50]
                readiness = opp.get('campaign_brief', {}).get('readiness_level', 'UNKNOWN')

                print(f"   {i}. {space_type} (Score: {score:.2f}) - {readiness}")
                print(f"      Investment: {investment}...")

                headlines = opp.get('campaign_brief', {}).get('sample_headlines', '')
                if headlines:
                    print(f"      Headlines: {headlines[:80]}...")

        else:
            print(f"   ❌ Analysis failed: {results.get('error')}")
            opportunities = []  # Initialize for scope

        # Performance comparison
        print(f"\n📊 PERFORMANCE COMPARISON")
        print("-" * 50)
        print(f"Enhanced3D (individual):  ~120+ seconds, rich templates")
        print(f"Parallel (chunked):       ~6 seconds, basic summaries")
        print(f"Hybrid (chunked+rich):    ~{duration:.0f} seconds, campaign intelligence")

        target_time = 180  # 3 minutes
        if duration <= target_time:
            print(f"✅ SUCCESS: Hybrid approach meets <3 minute target!")
            print(f"   Gets Enhanced3D intelligence at Parallel speed")
        elif duration <= target_time * 1.5:
            print(f"⚠️  ACCEPTABLE: Slightly over target but good value")
        else:
            print(f"❌ OPTIMIZATION NEEDED: Exceeds target significantly")

        # Business value assessment
        campaign_ready = results.get('campaign_ready_opportunities', 0)
        if campaign_ready >= 3:
            print(f"🏆 HIGH BUSINESS VALUE: {campaign_ready} campaign-ready opportunities")
        elif campaign_ready >= 1:
            print(f"📈 MEDIUM BUSINESS VALUE: {campaign_ready} campaign-ready opportunities")
        else:
            print(f"📋 BASIC BUSINESS VALUE: Analysis-only insights")

        return True, duration, len(opportunities) if opportunities else 0

    except Exception as e:
        print(f"❌ Hybrid analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False, float('inf'), 0

def compare_all_approaches():
    """Compare all three whitespace approaches"""

    print(f"\n" + "=" * 70)
    print("🔄 COMPREHENSIVE APPROACH COMPARISON")
    print("=" * 70)

    print("📊 Approach Comparison Matrix:")
    print("┌─────────────────┬─────────────┬──────────────┬─────────────────┐")
    print("│ Approach        │ Time        │ Intelligence │ Business Value  │")
    print("├─────────────────┼─────────────┼──────────────┼─────────────────┤")
    print("│ Enhanced3D      │ 120+ sec    │ Campaign     │ Ready-to-launch │")
    print("│ Parallel        │ 6 sec       │ Discovery    │ Strategic guide │")
    print("│ Hybrid          │ ?-? sec     │ Campaign+    │ Best of both    │")
    print("└─────────────────┴─────────────┴──────────────┴─────────────────┘")

    # Test hybrid approach
    success, duration, opportunities = test_hybrid_whitespace_performance()

    if success:
        print(f"\n🎯 FINAL RECOMMENDATION")
        print("-" * 30)

        if duration <= 60 and opportunities >= 5:
            print(f"✅ HYBRID OPTIMAL: Best performance + intelligence")
        elif duration <= 180 and opportunities >= 3:
            print(f"✅ HYBRID RECOMMENDED: Good balance of speed + value")
        elif duration <= 180:
            print(f"⚠️  HYBRID ACCEPTABLE: Meets time target")
        else:
            print(f"❌ HYBRID NEEDS WORK: Fallback to Parallel for speed")

        print(f"\n💡 Key Innovation: Single AI call with comprehensive JSON")
        print(f"📈 Business Impact: Campaign-ready intelligence at scale")

    else:
        print(f"❌ RECOMMENDATION: Use Parallel approach (fallback)")

if __name__ == "__main__":
    compare_all_approaches()