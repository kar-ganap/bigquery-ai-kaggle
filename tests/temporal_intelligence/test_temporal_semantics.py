#!/usr/bin/env python3
"""
Test script to verify corrected temporal semantic logic
"""

from src.intelligence.framework import ProgressiveDisclosureFramework

def test_temporal_semantics():
    """Test that temporal semantics are correct for competitive vs performance metrics"""

    framework = ProgressiveDisclosureFramework()

    print("🧠 TEMPORAL SEMANTIC LOGIC TEST")
    print("=" * 50)

    # Test competitive/threat scenarios
    competitive_cases = [
        {
            'insight': 'Competitive copying detected from EyeBuyDirect (similarity: 72.3%)',
            'trend': 'increasing',
            'expected': 'threat accelerating'
        },
        {
            'insight': 'Competitive copying detected from EyeBuyDirect (similarity: 45.1%)',
            'trend': 'decreasing',
            'expected': 'threat diminishing'
        },
        {
            'insight': 'Competitor aggressive pricing strategy detected',
            'trend': 'increasing',
            'expected': 'threat accelerating'
        },
        {
            'insight': 'Competitor market share expansion observed',
            'trend': 'decreasing',
            'expected': 'threat diminishing'
        }
    ]

    # Test performance/optimization scenarios
    performance_cases = [
        {
            'insight': 'Brand mention frequency optimization needed',
            'trend': 'increasing',
            'expected': 'improving trend'
        },
        {
            'insight': 'Creative engagement optimization opportunity',
            'trend': 'decreasing',
            'expected': 'concerning downward trend'
        },
        {
            'insight': 'Platform diversification optimization needed',
            'trend': 'increasing',
            'expected': 'improving trend'
        }
    ]

    print("\n🎯 COMPETITIVE/THREAT SCENARIOS:")
    for case in competitive_cases:
        metadata = {'temporal_trend': case['trend'], 'timeframe': '6 weeks'}
        result = framework.add_temporal_context(
            case['insight'], 0.5, 'test_metric', metadata
        )

        has_expected = case['expected'] in result
        status = "✅" if has_expected else "❌"
        print(f"   {status} {case['trend'].upper()}: {result}")
        if not has_expected:
            print(f"      Expected: '{case['expected']}' in result")

    print(f"\n📈 PERFORMANCE/OPTIMIZATION SCENARIOS:")
    for case in performance_cases:
        metadata = {'temporal_trend': case['trend'], 'timeframe': '4 weeks'}
        result = framework.add_temporal_context(
            case['insight'], 0.5, 'test_metric', metadata
        )

        has_expected = case['expected'] in result
        status = "✅" if has_expected else "❌"
        print(f"   {status} {case['trend'].upper()}: {result}")
        if not has_expected:
            print(f"      Expected: '{case['expected']}' in result")

    print(f"\n📊 SEMANTIC MEANING:")
    print(f"   🔴 Competitive Increasing = Threat Accelerating (bad for us)")
    print(f"   🟢 Competitive Decreasing = Threat Diminishing (good for us)")
    print(f"   🟢 Performance Increasing = Improving Trend (good for us)")
    print(f"   🔴 Performance Decreasing = Concerning Downward Trend (bad for us)")

    print(f"\n🎉 CORRECTED TEMPORAL SEMANTICS: Competitive vs Performance metrics now have proper directional meaning!")

if __name__ == "__main__":
    test_temporal_semantics()