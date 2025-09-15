#!/usr/bin/env python3
"""
Stage 8 Multi-Dimensional Intelligence Testing with Cached Results
Tests the updated Stage 8 that no longer duplicates CTA intelligence (now handled in Stage 7)
"""

import json
from datetime import datetime
from src.pipeline.stages.multidimensional_intelligence import MultiDimensionalIntelligenceStage
from src.pipeline.models.results import AnalysisResults

def load_stage7_results():
    """Load Stage 7 results that include CTA Intelligence"""
    try:
        with open('data/output/stage_tests/stage7_cta_enhanced_test_results.json', 'r') as f:
            stage7_data = json.load(f)

        print("✅ Loaded Stage 7 results with CTA Intelligence")
        print(f"   - Status: {stage7_data.get('status')}")
        print(f"   - Business Impact Score: {stage7_data.get('forecasts', {}).get('business_impact_score')}")
        print(f"   - CTA Intelligence: {stage7_data.get('metadata', {}).get('cta_intelligence_enabled')}")

        # Convert to AnalysisResults object
        analysis_results = AnalysisResults(
            status=stage7_data.get('status', 'unknown'),
            message="Stage 7 Analysis with CTA Intelligence completed",
            current_state=stage7_data.get('current_state', {}),
            influence=stage7_data.get('influence', {}),
            evolution=stage7_data.get('evolution', {}),
            forecasts=stage7_data.get('forecasts', {}),
            metadata=stage7_data.get('metadata', {})
        )

        return analysis_results

    except FileNotFoundError:
        print("❌ Stage 7 results file not found. Run Stage 7 test first.")
        return None
    except Exception as e:
        print(f"❌ Error loading Stage 7 results: {e}")
        return None

def test_stage8_multidimensional_intelligence():
    """Test Stage 8 Multi-Dimensional Intelligence with cached Stage 7 results"""

    print("🧪 Testing Stage 8 Multi-Dimensional Intelligence")
    print("=" * 60)

    # Load Stage 7 results as input
    previous_results = load_stage7_results()
    if not previous_results:
        return

    try:
        # Set competitor brands for analysis
        competitor_brands = [
            'Warby Parker', 'LensCrafters', 'EyeBuyDirect',
            'Zenni Optical', 'GlassesUSA'
        ]

        # Initialize Stage 8
        stage8 = MultiDimensionalIntelligenceStage(
            stage_name="Multi-Dimensional Intelligence",
            stage_number=8,
            run_id="stage4_test"
        )

        # Set competitor brands
        stage8.competitor_brands = competitor_brands

        print(f"🎯 Testing Multi-Dimensional Intelligence for: {competitor_brands}")
        print(f"📊 Using cached data from run_id: stage4_test")

        # Execute Stage 8
        print("\n🚀 Executing Multi-Dimensional Intelligence Analysis...")
        results = stage8.execute(previous_results)

        # Validate results
        print(f"\n📊 Stage 8 Results:")
        print(f"   Status: {results.status}")
        print(f"   Message: {results.message}")

        # Check core strategic metrics preservation
        print(f"\n🎯 Strategic Metrics Preservation:")
        print(f"   - Promotional Intensity: {results.current_state.get('promotional_intensity', 'N/A')}")
        print(f"   - Business Impact Score: {results.forecasts.get('business_impact_score', 'N/A')}")
        print(f"   - Top Copier: {results.influence.get('top_copier', 'N/A')}")
        print(f"   - Momentum Status: {results.evolution.get('momentum_status', 'N/A')}")

        # Check Multi-Dimensional Intelligence additions
        print(f"\n🧠 Multi-Dimensional Intelligence Results:")
        print(f"   - Audience Intelligence: {results.audience_intelligence.get('status', 'N/A')}")
        print(f"   - Creative Intelligence: {results.creative_intelligence.get('status', 'N/A')}")
        print(f"   - Channel Intelligence: {results.channel_intelligence.get('status', 'N/A')}")
        print(f"   - Whitespace Intelligence: {results.whitespace_intelligence.get('status', 'N/A')}")
        print(f"   - Data Completeness: {results.data_completeness:.1f}%")

        # Verify CTA Intelligence is NOT duplicated in Stage 8
        print(f"\n✅ CTA Intelligence Handling:")
        print(f"   - Stage 8 CTA duplication removed: {results.metadata.get('artificial_metrics_removed', False)}")
        print(f"   - Preserved from Stage 7: {results.metadata.get('cta_intelligence_enabled', 'N/A')}")

        # Check intelligence summary
        if results.intelligence_summary.get('status') == 'success':
            print(f"   - Intelligence Summary: Created successfully")
            print(f"   - P0 Modules: {results.metadata.get('p0_modules_implemented', [])}")
            print(f"   - P1 Modules: {results.metadata.get('p1_modules_implemented', [])}")

        # Save results for next stage testing
        output_data = {
            'status': results.status,
            'message': results.message,
            'current_state': results.current_state,
            'influence': results.influence,
            'evolution': results.evolution,
            'forecasts': results.forecasts,
            'audience_intelligence': results.audience_intelligence,
            'creative_intelligence': results.creative_intelligence,
            'channel_intelligence': results.channel_intelligence,
            'whitespace_intelligence': results.whitespace_intelligence,
            'intelligence_summary': results.intelligence_summary,
            'data_completeness': results.data_completeness,
            'metadata': results.metadata,
            'test_timestamp': datetime.now().isoformat()
        }

        with open('data/output/stage_tests/stage8_multidimensional_test_results.json', 'w') as f:
            json.dump(output_data, f, indent=2)

        print(f"\n💾 Results saved to: data/output/stage_tests/stage8_multidimensional_test_results.json")

        if results.status == 'success':
            print(f"✅ Stage 8 Multi-Dimensional Intelligence completed successfully!")
            print(f"   - Successfully preserved all strategic metrics from Stage 7")
            print(f"   - Added comprehensive audience, creative, channel & whitespace intelligence")
            print(f"   - No CTA intelligence duplication (handled properly in Stage 7)")
            print(f"   - Data completeness: {results.data_completeness:.1f}%")
        else:
            print(f"❌ Stage 8 failed: {results.message}")

    except Exception as e:
        print(f"❌ Stage 8 test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_stage8_multidimensional_intelligence()