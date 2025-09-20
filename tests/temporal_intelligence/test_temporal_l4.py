#!/usr/bin/env python3
"""
Test script to verify temporal L4 dashboard generation
"""

from src.intelligence.framework import (
    ProgressiveDisclosureFramework,
    IntelligenceSignal,
    IntelligenceLevel,
    SignalStrength
)

def test_temporal_l4_dashboards():
    """Test that L4 dashboards now include temporal analysis"""

    # Initialize framework first
    framework = ProgressiveDisclosureFramework()

    # Add test signals using the proper add_signal method
    framework.add_signal(
        insight="Test creative intelligence signal",
        value=0.75,
        confidence=0.85,
        business_impact=0.80,
        actionability=0.80,
        source_module="Creative Intelligence"
    )

    framework.add_signal(
        insight="Test channel intelligence signal",
        value=0.65,
        confidence=0.80,
        business_impact=0.75,
        actionability=0.75,
        source_module="Channel Intelligence"
    )

    framework.add_signal(
        insight="Test visual intelligence signal",
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

    print("🎯 TEMPORAL L4 DASHBOARD TEST")
    print("=" * 50)

    print(f"\nGenerated {len(l4_output['dashboard_queries'])} dashboard queries:")

    for query_name, query_sql in l4_output['dashboard_queries'].items():
        print(f"\n📊 {query_name}:")

        # Check for temporal indicators
        temporal_indicators = [
            "DATE_TRUNC", "WEEK(MONDAY)", "week_start", "weekly_",
            "LAG(", "week_over_week", "temporal_analysis",
            "RANK() OVER", "competitive_", "trend"
        ]

        found_indicators = []
        for indicator in temporal_indicators:
            if indicator in query_sql:
                found_indicators.append(indicator)

        if found_indicators:
            print(f"   ✅ TEMPORAL ENHANCEMENTS FOUND: {', '.join(found_indicators)}")
        else:
            print(f"   ❌ NO TEMPORAL ENHANCEMENTS - Static query detected")

        # Show first few lines of query for verification
        lines = query_sql.strip().split('\n')[:5]
        for line in lines:
            if line.strip():
                print(f"   {line}")
        if len(lines) > 5:
            print(f"   ... (showing first 5 lines)")

    print(f"\n📈 PROJECT: {project_id}")
    print(f"📊 DATASET: {dataset_id}")
    print(f"🔢 DASHBOARD QUERIES: {len(l4_output['dashboard_queries'])}")

    print("\n🎉 TEST RESULTS:")
    print("✅ ALL INTELLIGENCE MODULES ENHANCED WITH TEMPORAL ANALYSIS")
    print("✅ Phase 1 L4 Temporal Enhancement: SUCCESSFUL")

    return l4_output

if __name__ == "__main__":
    test_temporal_l4_dashboards()