#!/usr/bin/env python3
"""
Stage 8 Hybrid Detector Verification Test
Ensures Hybrid whitespace detector is used, not fallback
"""

import json
from datetime import datetime
from src.pipeline.stages.analysis import AnalysisStage
from src.pipeline.stages.multidimensional_intelligence import MultiDimensionalIntelligenceStage
from src.pipeline.models.results import AnalysisResults
import logging

# Set up detailed logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

def load_stage7_results():
    """Load existing Stage 7 results that include CTA Intelligence"""
    try:
        with open('data/output/stage_tests/stage7_cta_enhanced_test_results.json', 'r') as f:
            stage7_data = json.load(f)

        print("🔧 Step 1: Loading existing Stage 7 results with CTA table...")
        print(f"   ✅ Status: {stage7_data.get('status')}")
        print(f"   📊 Business Impact Score: {stage7_data.get('forecasts', {}).get('business_impact_score')}")
        print(f"   🎯 CTA Intelligence: {stage7_data.get('metadata', {}).get('cta_intelligence_enabled')}")

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
        print("   ❌ Stage 7 results file not found. Run Stage 7 test first.")
        return None
    except Exception as e:
        print(f"   ❌ Error loading Stage 7 results: {e}")
        return None

def test_stage8_hybrid_explicit():
    """Test Stage 8 with explicit verification that Hybrid detector is used"""

    print("\n🧪 Step 2: Testing Stage 8 with Hybrid Detector Verification")
    print("=" * 70)

    # Get Stage 7 results first
    stage7_results = load_stage7_results()
    if not stage7_results:
        print("❌ Cannot proceed without Stage 7 results")
        return

    try:
        # Initialize Stage 8 with logging
        stage8 = MultiDimensionalIntelligenceStage(
            stage_name="Multi-Dimensional Intelligence",
            stage_number=8,
            run_id="stage4_test"
        )

        # Set competitor brands
        stage8.competitor_brands = [
            'Warby Parker', 'LensCrafters', 'EyeBuyDirect',
            'Zenni Optical', 'GlassesUSA'
        ]

        print(f"\n🎯 Testing with brands: {stage8.competitor_brands}")
        print(f"📊 Using run_id: stage4_test")

        # Execute Stage 8 with detailed logging
        print(f"\n🚀 Executing Stage 8 Multi-Dimensional Intelligence...")

        # Add explicit logging to capture which detector is used
        original_logger = stage8.logger

        class DetectorLogger:
            def info(self, msg):
                print(f"   🔍 {msg}")
                original_logger.info(msg)
            def warning(self, msg):
                print(f"   ⚠️  {msg}")
                original_logger.warning(msg)
            def error(self, msg):
                print(f"   ❌ {msg}")
                original_logger.error(msg)

        stage8.logger = DetectorLogger()

        results = stage8.execute(stage7_results)

        # Verify results
        print(f"\n📊 Stage 8 Results:")
        print(f"   Status: {results.status}")
        print(f"   Message: {results.message}")

        # Check whitespace intelligence specifically
        whitespace = results.whitespace_intelligence
        print(f"\n🎯 Whitespace Intelligence Verification:")
        print(f"   Status: {whitespace.get('status', 'N/A')}")
        print(f"   Opportunities: {whitespace.get('opportunities_found', 0)}")
        print(f"   Approach: {whitespace.get('performance_metrics', {}).get('approach', 'N/A')}")
        print(f"   Duration: {whitespace.get('performance_metrics', {}).get('duration_seconds', 0):.2f}s")

        # Check if campaign intelligence is present (indicates Hybrid)
        opportunities = whitespace.get('strategic_opportunities', [])
        if opportunities and isinstance(opportunities[0], dict):
            first_opp = opportunities[0]
            has_campaign_brief = 'campaign_brief' in first_opp
            print(f"   Campaign Intelligence: {'✅ Present' if has_campaign_brief else '❌ Missing'}")

            if has_campaign_brief:
                brief = first_opp['campaign_brief']
                print(f"   Sample Headlines: {brief.get('sample_headlines', 'N/A')[:50]}...")
                print(f"   Readiness Level: {brief.get('readiness_level', 'N/A')}")
                print(f"   🎉 CONFIRMED: Hybrid detector successfully used!")
            else:
                print(f"   ⚠️  WARNING: Fallback detector used (no campaign intelligence)")
        else:
            print(f"   ⚠️  WARNING: No strategic opportunities detected")

        # Save results with timestamp
        output_data = {
            'test_type': 'hybrid_verification',
            'stage7_cta_enabled': stage7_results.metadata.get('cta_intelligence_enabled', False),
            'stage8_status': results.status,
            'whitespace_approach': whitespace.get('performance_metrics', {}).get('approach', 'unknown'),
            'whitespace_duration': whitespace.get('performance_metrics', {}).get('duration_seconds', 0),
            'opportunities_found': whitespace.get('opportunities_found', 0),
            'hybrid_indicators': {
                'has_campaign_briefs': len([o for o in opportunities if isinstance(o, dict) and 'campaign_brief' in o]),
                'has_enhanced_metrics': 'enhanced_market_potential' in str(whitespace),
                'approach_indicates_hybrid': 'hybrid' in whitespace.get('performance_metrics', {}).get('approach', '').lower()
            },
            'full_results': {
                'status': results.status,
                'message': results.message,
                'whitespace_intelligence': whitespace,
                'metadata': results.metadata
            },
            'test_timestamp': datetime.now().isoformat()
        }

        with open('data/output/stage_tests/stage8_hybrid_verification_results.json', 'w') as f:
            json.dump(output_data, f, indent=2)

        print(f"\n💾 Detailed results saved to: data/output/stage_tests/stage8_hybrid_verification_results.json")

        # Final verdict
        approach = whitespace.get('performance_metrics', {}).get('approach', 'unknown')
        duration = whitespace.get('performance_metrics', {}).get('duration_seconds', 0)

        print(f"\n🏁 FINAL VERIFICATION:")
        print(f"   Detector Used: {approach}")
        print(f"   Execution Time: {duration:.2f}s")
        print(f"   Target Met (<180s): {'✅ Yes' if duration < 180 else '❌ No'}")

        if 'hybrid' in approach.lower() and duration < 180:
            print(f"   🎉 SUCCESS: Hybrid detector is working as intended!")
        elif duration < 180:
            print(f"   ⚠️  PARTIAL: Fast execution but approach unclear")
        else:
            print(f"   ❌ FAILED: Either wrong detector or performance issue")

    except Exception as e:
        print(f"❌ Stage 8 test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_stage8_hybrid_explicit()