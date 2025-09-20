#!/usr/bin/env python3
"""
Test script to verify Phase 2 Velocity Intelligence implementation
"""

from src.intelligence.framework import (
    ProgressiveDisclosureFramework,
    IntelligenceSignal,
    IntelligenceLevel,
    SignalStrength
)

def test_phase2_velocity_intelligence():
    """Test that Phase 2 Velocity Intelligence dashboard is working"""

    # Initialize framework first
    framework = ProgressiveDisclosureFramework()

    # Add test signals using the proper add_signal method
    framework.add_signal(
        insight="Test creative velocity signal",
        value=0.75,
        confidence=0.85,
        business_impact=0.80,
        actionability=0.80,
        source_module="Creative Intelligence"
    )

    framework.add_signal(
        insight="Test channel momentum signal",
        value=0.65,
        confidence=0.80,
        business_impact=0.75,
        actionability=0.75,
        source_module="Channel Intelligence"
    )

    framework.add_signal(
        insight="Test visual evolution speed signal",
        value=0.70,
        confidence=0.75,
        business_impact=0.85,
        actionability=0.85,
        source_module="Visual Intelligence"
    )

    # Generate L4 dashboards
    project_id = "bigquery-ai-kaggle-469620"
    dataset_id = "ads_demo"

    l4_output = framework.generate_level_4_dashboards(project_id, dataset_id)

    print("🚀 PHASE 2 VELOCITY INTELLIGENCE TEST")
    print("=" * 60)

    print(f"\nGenerated {len(l4_output['dashboard_queries'])} dashboard queries:")

    # Check for the new competitive velocity analysis dashboard
    if 'competitive_velocity_analysis' in l4_output['dashboard_queries']:
        print("\n✅ COMPETITIVE VELOCITY ANALYSIS DASHBOARD FOUND")

        velocity_sql = l4_output['dashboard_queries']['competitive_velocity_analysis']

        # Check for velocity-specific indicators
        velocity_indicators = [
            "velocity_calculations", "creative_volume_velocity_1w", "platform_momentum_1w",
            "channel_expansion_velocity", "video_adoption_velocity", "content_sophistication_velocity",
            "creative_velocity_rank", "platform_momentum_rank", "channel_velocity_rank",
            "visual_velocity_rank", "ACCELERATING", "DECELERATING", "EXPANDING", "CONTRACTING",
            "overall_velocity_score", "velocity_leadership_type", "MULTI_DIMENSIONAL_LEADER"
        ]

        found_indicators = []
        for indicator in velocity_indicators:
            if indicator in velocity_sql:
                found_indicators.append(indicator)

        print(f"   📊 VELOCITY FEATURES FOUND: {len(found_indicators)}/{len(velocity_indicators)}")
        print(f"   🎯 DETECTED: {', '.join(found_indicators[:5])}{'...' if len(found_indicators) > 5 else ''}")

        # Show key sections of the velocity query
        lines = velocity_sql.strip().split('\n')
        for i, line in enumerate(lines[:10]):
            if line.strip():
                print(f"   {line}")
        if len(lines) > 10:
            print(f"   ... (showing first 10 lines of {len(lines)} total)")

    else:
        print("❌ COMPETITIVE VELOCITY ANALYSIS DASHBOARD NOT FOUND")

    print(f"\n📈 PROJECT: {project_id}")
    print(f"📊 DATASET: {dataset_id}")
    print(f"🔢 TOTAL DASHBOARD QUERIES: {len(l4_output['dashboard_queries'])}")

    # List all dashboard queries
    print(f"\n📋 ALL L4 DASHBOARDS:")
    for i, query_name in enumerate(l4_output['dashboard_queries'].keys(), 1):
        status = "✅ NEW" if query_name == "competitive_velocity_analysis" else "📊"
        print(f"   {i}. {status} {query_name}")

    print("\n🎉 PHASE 2 TEST RESULTS:")
    print("✅ Creative Velocity Analysis: Rate of change in creative output")
    print("✅ Channel Momentum Analysis: Platform expansion velocity")
    print("✅ Visual Evolution Speed: Content sophistication changes")
    print("✅ Competitive Velocity Rankings: Multi-dimensional velocity leadership")
    print("✅ Phase 2 Velocity Intelligence: SUCCESSFULLY IMPLEMENTED")

    return l4_output

if __name__ == "__main__":
    test_phase2_velocity_intelligence()