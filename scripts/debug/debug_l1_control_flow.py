#!/usr/bin/env python3
"""
Debug L1 control flow to understand why temporal enhancements aren't appearing
"""

from src.intelligence.framework import (
    ProgressiveDisclosureFramework,
    IntelligenceLevel,
    create_creative_intelligence_signals
)

def debug_l1_control_flow():
    """Debug the L1 control flow issue step by step"""

    print("🔍 L1 CONTROL FLOW DEBUGGING")
    print("=" * 50)

    framework = ProgressiveDisclosureFramework()

    # Check default thresholds
    print(f"\n📊 L1 THRESHOLDS:")
    print(f"   • Min Confidence: {framework.thresholds.l1_min_confidence}")
    print(f"   • Min Business Impact: {framework.thresholds.l1_min_business_impact}")
    print(f"   • Max Signals: {framework.thresholds.l1_max_signals}")

    # Add test data that should trigger temporal enhancement
    creative_data = {
        'avg_text_length': 25,          # Should trigger temporal enhancement
        'avg_brand_mentions': 0.3       # Should trigger temporal enhancement
    }

    print(f"\n🎨 CREATIVE INTELLIGENCE SIGNAL GENERATION:")
    print(f"   • Input: avg_text_length={creative_data['avg_text_length']} (< 30 threshold)")
    print(f"   • Input: avg_brand_mentions={creative_data['avg_brand_mentions']} (< 0.5 threshold)")

    # Generate signals and examine them
    before_count = len(framework.signals)
    create_creative_intelligence_signals(framework, creative_data)
    after_count = len(framework.signals)

    print(f"   • Signals generated: {after_count - before_count}")

    # Examine each signal in detail
    for i, signal in enumerate(framework.signals, 1):
        print(f"\n🔍 SIGNAL {i} ANALYSIS:")
        print(f"   • Insight: {signal.insight}")
        print(f"   • Confidence: {signal.confidence}")
        print(f"   • Business Impact: {signal.business_impact}")
        print(f"   • Actionability: {signal.actionability}")
        print(f"   • Source Module: {signal.source_module}")
        print(f"   • Recommended Levels: {[level.name for level in signal.recommended_levels]}")

        # Check L1 eligibility
        l1_eligible = IntelligenceLevel.L1_EXECUTIVE in signal.recommended_levels
        print(f"   • L1 Eligible: {l1_eligible}")

        if not l1_eligible:
            # Calculate why it's not L1 eligible
            meets_confidence = signal.confidence >= framework.thresholds.l1_min_confidence
            meets_impact = signal.business_impact >= framework.thresholds.l1_min_business_impact
            print(f"     - Meets Confidence Threshold: {meets_confidence} ({signal.confidence} >= {framework.thresholds.l1_min_confidence})")
            print(f"     - Meets Business Impact Threshold: {meets_impact} ({signal.business_impact} >= {framework.thresholds.l1_min_business_impact})")

        # Check for temporal indicators
        temporal_indicators = ['over', 'weeks', 'accelerating', 'trend', 'increasing', 'declining', 'improving', 'concerning']
        has_temporal = any(indicator in signal.insight.lower() for indicator in temporal_indicators)
        print(f"   • Has Temporal Language: {has_temporal}")

        if has_temporal:
            found_indicators = [indicator for indicator in temporal_indicators if indicator in signal.insight.lower()]
            print(f"     - Found indicators: {found_indicators}")

    # Test L1 generation
    print(f"\n📈 L1 GENERATION:")
    l1_result = framework.generate_level_1_executive()
    l1_insights = l1_result['executive_insights']

    print(f"   • L1 insights generated: {len(l1_insights)}")

    for i, insight in enumerate(l1_insights, 1):
        temporal_indicators = ['over', 'weeks', 'accelerating', 'trend', 'increasing', 'declining', 'improving', 'concerning']
        has_temporal = any(indicator in insight.lower() for indicator in temporal_indicators)
        status = "✅ TEMPORAL" if has_temporal else "⚪ STATIC"
        print(f"   {i}. {status} {insight}")

    # Test with manually adjusted signals for L1 eligibility
    print(f"\n🔧 TESTING HIGH-PRIORITY TEMPORAL SIGNAL:")

    # Add a high-priority temporal signal manually
    framework.add_signal(
        insight="Creative text length optimization needed - content is too brief for engagement - declining performance over 4 weeks, immediate action required",
        value=25,
        confidence=0.85,  # High confidence for L1
        business_impact=0.90,  # High business impact for L1
        actionability=0.85,
        source_module="Creative Intelligence - Temporal"
    )

    # Test L1 generation again
    l1_result_enhanced = framework.generate_level_1_executive()
    l1_insights_enhanced = l1_result_enhanced['executive_insights']

    print(f"   • L1 insights with enhanced signal: {len(l1_insights_enhanced)}")

    for i, insight in enumerate(l1_insights_enhanced, 1):
        temporal_indicators = ['over', 'weeks', 'accelerating', 'trend', 'increasing', 'declining', 'improving', 'concerning']
        has_temporal = any(indicator in insight.lower() for indicator in temporal_indicators)
        status = "✅ TEMPORAL" if has_temporal else "⚪ STATIC"
        print(f"   {i}. {status} {insight}")

    print(f"\n🎯 DIAGNOSIS:")

    # Count temporal signals by eligibility
    temporal_signals = [s for s in framework.signals if any(indicator in s.insight.lower() for indicator in temporal_indicators)]
    l1_temporal_signals = [s for s in temporal_signals if IntelligenceLevel.L1_EXECUTIVE in s.recommended_levels]

    print(f"   • Total signals: {len(framework.signals)}")
    print(f"   • Temporal signals: {len(temporal_signals)}")
    print(f"   • L1-eligible temporal signals: {len(l1_temporal_signals)}")

    if len(temporal_signals) > 0 and len(l1_temporal_signals) == 0:
        print(f"   🔴 ISSUE: Temporal signals exist but don't meet L1 thresholds")
        print(f"   💡 SOLUTION: Either lower L1 thresholds or increase signal confidence/impact scores")
    elif len(l1_temporal_signals) > 0:
        print(f"   ✅ SUCCESS: Temporal signals are flowing to L1")
    else:
        print(f"   🔴 ISSUE: No temporal signals being generated")

if __name__ == "__main__":
    debug_l1_control_flow()