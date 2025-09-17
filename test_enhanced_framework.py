#!/usr/bin/env python3
"""
Test Enhanced L1-L4 Framework with Visual Intelligence
"""
from src.intelligence.framework import (
    ProgressiveDisclosureFramework,
    create_creative_intelligence_signals,
    create_channel_intelligence_signals,
    create_visual_intelligence_signals
)

def test_enhanced_framework():
    """Test the enhanced L1-L4 framework with Visual Intelligence integration"""

    print("🧠 Testing Enhanced L1-L4 Progressive Disclosure Framework")
    print("=" * 70)

    # Initialize framework
    framework = ProgressiveDisclosureFramework()

    # Mock data for all three intelligence modules
    creative_data = {
        'avg_text_length': 25,  # Too short - will trigger signal
        'avg_brand_mentions': 0.3,  # Low brand mentions
        'avg_emotional_keywords': 0.8,
        'avg_creative_density': 8.5  # Low density
    }

    channel_data = {
        'avg_platform_diversification': 1.2,  # Low diversification
        'cross_platform_synergy_rate': 25.0,  # Low synergy
        'platform_optimization_rate': 45.0,
        'platform_concentration': 'CONCENTRATED'  # High risk
    }

    visual_data = {
        'avg_visual_text_alignment': 0.5,  # Poor alignment
        'avg_brand_consistency': 0.65,  # Inconsistent branding
        'avg_luxury_positioning': 0.8,  # Luxury positioning
        'avg_boldness': 0.3,  # Subtle approach
        'avg_visual_differentiation': 0.4,  # Low differentiation
        'avg_creative_pattern_risk': 0.75,  # High fatigue risk
        'dominant_target_demographic': 'young_professional',
        'demographic_targeting_confidence': 0.85  # Clear targeting
    }

    print("📊 Generating intelligence signals...")

    # Generate signals from all modules
    create_creative_intelligence_signals(framework, creative_data)
    create_channel_intelligence_signals(framework, channel_data)
    create_visual_intelligence_signals(framework, visual_data)

    # Get framework statistics
    stats = framework.get_framework_stats()
    print(f"\n📈 Framework Statistics:")
    print(f"   Total signals: {stats['total_signals']}")
    print(f"   By module: {stats['by_module']}")
    print(f"   By strength: {stats['by_strength']}")
    print(f"   Framework efficiency: {stats['framework_efficiency']:.2%}")

    # Generate L1 Executive Summary
    print(f"\n🎯 L1 EXECUTIVE SUMMARY:")
    print("=" * 40)
    l1_output = framework.generate_level_1_executive()

    print(f"Threat Level: {l1_output['threat_level']}")
    print(f"Confidence: {l1_output['confidence_score']:.2%}")
    print(f"Signals: {l1_output['signal_count']} (filtered: {l1_output['filtered_signals']})")

    print("\nCritical Insights:")
    for i, insight in enumerate(l1_output['executive_insights'], 1):
        print(f"  {i}. {insight}")

    # Generate L2 Strategic Dashboard
    print(f"\n📊 L2 STRATEGIC DASHBOARD:")
    print("=" * 40)
    l2_output = framework.generate_level_2_strategic()

    print(f"Active modules: {l2_output['modules_active']}")
    print(f"Total signals: {l2_output['total_signals']}")

    print("\nCross-module patterns:")
    for pattern in l2_output['cross_module_patterns']:
        print(f"  • {pattern}")

    print("\nStrategic priorities:")
    for i, priority in enumerate(l2_output['strategic_priorities'], 1):
        print(f"  {i}. {priority}")

    # Test module-specific insights
    print(f"\nModule Breakdown:")
    for module, data in l2_output['strategic_intelligence'].items():
        print(f"  📍 {module}: {data['signal_count']} signals, confidence: {data['confidence_avg']:.2%}")
        for insight in data['insights'][:2]:  # Show top 2 insights per module
            print(f"     • {insight}")

    # Generate L3 Interventions
    print(f"\n🛠️  L3 ACTIONABLE INTERVENTIONS:")
    print("=" * 40)
    l3_output = framework.generate_level_3_interventions()

    summary = l3_output['intervention_summary']
    print(f"Total interventions: {summary['total_interventions']}")
    print(f"Immediate: {summary['immediate_count']} | Short-term: {summary['short_term_count']} | Monitoring: {summary['monitoring_count']}")
    print(f"Avg actionability: {summary['avg_actionability']:.2%}")

    # Show immediate actions
    if l3_output['immediate_actions']:
        print(f"\n🚨 IMMEDIATE ACTIONS:")
        for action in l3_output['immediate_actions'][:3]:
            print(f"  • {action['action']}")
            print(f"    Confidence: {action['confidence']:.2%} | Impact: {action['business_impact']:.2%}")
            print(f"    Timeline: {action['timeline']}")

    # Show sample short-term tactics
    if l3_output['short_term_tactics']:
        print(f"\n📋 SHORT-TERM TACTICS (sample):")
        for action in l3_output['short_term_tactics'][:2]:
            print(f"  • {action['action']}")
            print(f"    Success metrics: {', '.join(action['success_metrics'][:2])}")

    # Generate L4 Dashboard queries
    print(f"\n🔍 L4 DASHBOARD QUERIES:")
    print("=" * 40)
    l4_output = framework.generate_level_4_dashboards("bigquery-ai-kaggle-469620", "ads_demo")

    print(f"Available dashboards: {len(l4_output['dashboard_queries'])}")
    for dashboard_name in l4_output['dashboard_queries'].keys():
        print(f"  📊 {dashboard_name}")

    print(f"\nModule performance:")
    for module, perf in l4_output['module_performance_metrics'].items():
        print(f"  {module}: {perf['signal_count']} signals, confidence: {perf['avg_confidence']:.3f}")

    print(f"\n✅ Enhanced L1-L4 Framework with Visual Intelligence completed!")
    print(f"   🎨 Visual Intelligence successfully integrated")
    print(f"   🔄 Multi-dimensional intelligence active")
    print(f"   📈 {stats['total_signals']} actionable signals generated")

    return True

if __name__ == "__main__":
    test_enhanced_framework()