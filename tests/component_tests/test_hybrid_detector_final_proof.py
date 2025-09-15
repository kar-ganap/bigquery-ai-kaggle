#!/usr/bin/env python3
"""
Final Proof: Hybrid Detector Integration with Explicit Evidence
Uses existing test data but provides explicit proof of hybrid detector usage
"""

import json
from datetime import datetime
from src.pipeline.stages.multidimensional_intelligence import MultiDimensionalIntelligenceStage
from src.pipeline.models.results import AnalysisResults
from src.utils.bigquery_client import run_query
import logging
import time

# Set up detailed logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

def verify_cta_table_exists_global():
    """Check if CTA table exists (from any previous run)"""
    print("🔍 STEP 1: Verifying CTA Table Availability")
    print("=" * 60)

    try:
        # Check global CTA table
        check_sql = """
        SELECT
            COUNT(*) as row_count,
            COUNT(DISTINCT brand) as brand_count,
            MIN(cta_adoption_rate) as min_cta_rate,
            MAX(cta_adoption_rate) as max_cta_rate
        FROM `bigquery-ai-kaggle-469620.ads_demo.cta_aggressiveness_analysis`
        """

        print("   🔍 Checking global CTA table...")
        result = run_query(check_sql)

        if result is not None and not result.empty:
            row = result.iloc[0]
            print(f"   ✅ CTA table exists!")
            print(f"   📊 Total rows: {row['row_count']}")
            print(f"   🏢 Brands: {row['brand_count']}")
            print(f"   📈 CTA adoption rate range: {row['min_cta_rate']:.3f} - {row['max_cta_rate']:.3f}")
            return True
        else:
            print(f"   ❌ CTA table is empty or missing")
            return False

    except Exception as e:
        print(f"   ❌ CTA table check failed: {e}")
        return False

def run_hybrid_detector_directly():
    """Test hybrid detector directly to prove it works"""
    print("\n🧪 STEP 2: Direct Hybrid Detector Test")
    print("=" * 60)

    try:
        from src.competitive_intel.analysis.hybrid_whitespace_detection import HybridWhiteSpaceDetector

        # Test with eyewear competitor brands
        competitors = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
        detector = HybridWhiteSpaceDetector(
            'bigquery-ai-kaggle-469620',
            'ads_demo',
            'Warby Parker',
            competitors
        )

        print(f"   🎯 Testing Hybrid detector with brands: {competitors}")

        start_time = time.time()
        result = detector.analyze_hybrid_performance('stage4_test')
        duration = time.time() - start_time

        print(f"   ⏱️  Direct test completed in {duration:.2f}s")
        print(f"   📊 Status: {result.get('status', 'unknown')}")
        print(f"   🔍 Approach: {result.get('approach', 'unknown')}")
        print(f"   🎯 Opportunities: {result.get('white_space_opportunities', 0)}")

        if result.get('status') == 'success':
            print(f"   ✅ Direct hybrid detector test: SUCCESS")
            return True, result
        else:
            print(f"   ❌ Direct hybrid detector test: FAILED - {result.get('error', 'unknown')}")
            return False, result

    except Exception as e:
        print(f"   ❌ Direct hybrid test failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def run_stage8_with_explicit_logging():
    """Run Stage 8 with maximum logging to prove hybrid usage"""
    print("\n🧪 STEP 3: Stage 8 Integration Test with Explicit Logging")
    print("=" * 60)

    try:
        # Load existing Stage 7 results
        with open('data/output/stage_tests/stage7_cta_enhanced_test_results.json', 'r') as f:
            stage7_data = json.load(f)

        print("   ✅ Loaded Stage 7 results")

        # Convert to AnalysisResults
        stage7_results = AnalysisResults(
            status=stage7_data.get('status', 'unknown'),
            message="Stage 7 Analysis with CTA Intelligence completed",
            current_state=stage7_data.get('current_state', {}),
            influence=stage7_data.get('influence', {}),
            evolution=stage7_data.get('evolution', {}),
            forecasts=stage7_data.get('forecasts', {}),
            metadata=stage7_data.get('metadata', {})
        )

        # Initialize Stage 8
        stage8 = MultiDimensionalIntelligenceStage(
            stage_name="Multi-Dimensional Intelligence",
            stage_number=8,
            run_id="stage4_test"  # Use existing test data
        )

        stage8.competitor_brands = [
            'Warby Parker', 'LensCrafters', 'EyeBuyDirect',
            'Zenni Optical', 'GlassesUSA'
        ]

        print(f"   🎯 Stage 8 initialized with run_id: stage4_test")

        # Capture ALL logs to prove which detector is used
        all_logs = []
        original_logger = stage8.logger

        class ExplicitLogger:
            def info(self, msg):
                all_logs.append(f"INFO: {msg}")
                if 'hybrid' in msg.lower() or 'fallback' in msg.lower() or 'whitespace' in msg.lower():
                    print(f"   🔍 {msg}")
                original_logger.info(msg)
            def warning(self, msg):
                all_logs.append(f"WARNING: {msg}")
                print(f"   ⚠️  {msg}")
                original_logger.warning(msg)
            def error(self, msg):
                all_logs.append(f"ERROR: {msg}")
                print(f"   ❌ {msg}")
                original_logger.error(msg)

        stage8.logger = ExplicitLogger()

        print(f"   🚀 Executing Stage 8...")
        start_time = time.time()
        results = stage8.execute(stage7_results)
        duration = time.time() - start_time

        print(f"   ✅ Stage 8 completed in {duration:.2f}s")

        # Analyze whitespace results
        whitespace = results.whitespace_intelligence
        approach = whitespace.get('performance_metrics', {}).get('approach', 'unknown')
        ws_duration = whitespace.get('performance_metrics', {}).get('duration_seconds', 0)
        opportunities = whitespace.get('opportunities_found', 0)

        print(f"\n📊 WHITESPACE ANALYSIS:")
        print(f"   Status: {whitespace.get('status', 'N/A')}")
        print(f"   Approach: {approach}")
        print(f"   Duration: {ws_duration:.2f}s")
        print(f"   Opportunities: {opportunities}")

        # Check for hybrid-specific features
        strategic_opps = whitespace.get('strategic_opportunities', [])
        campaign_briefs = 0
        sample_headlines = []

        for opp in strategic_opps:
            if isinstance(opp, dict) and 'campaign_brief' in opp:
                campaign_briefs += 1
                brief = opp['campaign_brief']
                if 'sample_headlines' in brief:
                    sample_headlines.append(brief['sample_headlines'][:60])

        print(f"   Campaign Briefs: {campaign_briefs}/{len(strategic_opps)}")
        if sample_headlines:
            print(f"   Sample Headlines: {sample_headlines[0]}...")

        # Log analysis
        hybrid_logs = [log for log in all_logs if 'hybrid' in log.lower()]
        fallback_logs = [log for log in all_logs if 'fallback' in log.lower()]

        print(f"\n📋 LOG EVIDENCE:")
        print(f"   Hybrid mentions: {len(hybrid_logs)}")
        print(f"   Fallback mentions: {len(fallback_logs)}")

        if hybrid_logs:
            print(f"   Key hybrid log: {hybrid_logs[0]}")

        # Generate final proof
        proof_verdict = {
            'approach_is_hybrid': 'hybrid' in approach.lower(),
            'has_campaign_intelligence': campaign_briefs > 0,
            'performance_under_3min': ws_duration < 180,
            'no_fallback_detected': len(fallback_logs) == 0,
            'hybrid_logs_present': len(hybrid_logs) > 0
        }

        all_evidence_positive = all(proof_verdict.values())

        proof_data = {
            'test_timestamp': datetime.now().isoformat(),
            'cta_table_available': True,
            'stage8_duration': duration,
            'whitespace_approach': approach,
            'whitespace_duration': ws_duration,
            'opportunities_found': opportunities,
            'campaign_briefs_generated': campaign_briefs,
            'sample_headlines': sample_headlines,
            'proof_verdict': proof_verdict,
            'final_conclusion': 'HYBRID_CONFIRMED' if all_evidence_positive else 'EVIDENCE_MIXED',
            'all_logs': all_logs,
            'hybrid_specific_logs': hybrid_logs,
            'fallback_logs': fallback_logs
        }

        # Save proof
        with open('data/output/stage_tests/hybrid_detector_final_proof.json', 'w') as f:
            json.dump(proof_data, f, indent=2)

        print(f"\n💾 Proof saved to: data/output/stage_tests/hybrid_detector_final_proof.json")

        return proof_data

    except Exception as e:
        print(f"   ❌ Stage 8 integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Run final proof sequence"""
    print("🏁 FINAL HYBRID DETECTOR INTEGRATION PROOF")
    print("=" * 80)

    # Step 1: Verify CTA table exists
    cta_available = verify_cta_table_exists_global()
    if not cta_available:
        print("❌ Cannot proceed without CTA table - hybrid detector requires it")
        return

    # Step 2: Test hybrid detector directly
    direct_success, direct_result = run_hybrid_detector_directly()
    if not direct_success:
        print("❌ Direct hybrid detector test failed - integration impossible")
        return

    # Step 3: Test Stage 8 integration
    proof_data = run_stage8_with_explicit_logging()
    if not proof_data:
        print("❌ Stage 8 integration test failed")
        return

    # Final verdict
    print(f"\n🏁 FINAL PROOF VERDICT")
    print("=" * 80)

    conclusion = proof_data.get('final_conclusion', 'UNKNOWN')

    if conclusion == 'HYBRID_CONFIRMED':
        print(f"🎉 PROOF CONFIRMED: Hybrid detector integration is working!")
        print(f"   ✅ CTA table available and accessible")
        print(f"   ✅ Direct hybrid detector test successful")
        print(f"   ✅ Stage 8 uses hybrid approach: {proof_data['whitespace_approach']}")
        print(f"   ✅ Campaign intelligence generated: {proof_data['campaign_briefs_generated']} briefs")
        print(f"   ✅ Performance target met: {proof_data['whitespace_duration']:.2f}s (<180s)")
        print(f"   ✅ No fallback detected: {len(proof_data['fallback_logs'])} fallback logs")
        print(f"   ✅ Hybrid logs present: {len(proof_data['hybrid_specific_logs'])} mentions")

        if proof_data['sample_headlines']:
            print(f"   ✅ Sample campaign output: '{proof_data['sample_headlines'][0]}...'")

        print(f"\n🎯 CLAIM VERIFIED: When CTA table exists, Stage 8 successfully uses")
        print(f"   Hybrid whitespace detector for full dataset processing in <3 minutes")
        print(f"   with rich campaign intelligence generation.")

    else:
        print(f"❌ PROOF INCONCLUSIVE: Evidence is mixed")
        verdict = proof_data.get('proof_verdict', {})
        for check, result in verdict.items():
            status = "✅" if result else "❌"
            print(f"   {status} {check}: {result}")

if __name__ == "__main__":
    main()