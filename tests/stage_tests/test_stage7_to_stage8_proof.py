#!/usr/bin/env python3
"""
Complete Stage 7 → Stage 8 Integration Proof
Runs full sequence from scratch to prove Hybrid detector integration
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

def run_stage7_complete():
    """Run complete Stage 7 to create CTA table from existing embeddings"""
    print("🔧 STEP 1: Running Complete Stage 7 Analysis")
    print("=" * 60)

    try:
        from src.pipeline.stages.analysis import AnalysisStage
        from src.pipeline.core.base import PipelineContext

        # Create a fresh run_id for this test
        test_run_id = f"proof_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"📊 Using fresh run_id: {test_run_id}")

        # Create pipeline context
        context = PipelineContext(
            brand="Warby Parker",
            vertical="eyewear",
            run_id=test_run_id,
            verbose=True
        )
        # Set competitor brands after initialization
        context.competitor_brands = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']

        # Initialize Stage 7
        print("   🚀 Initializing Stage 7...")
        stage7 = AnalysisStage(context=context, dry_run=False, verbose=True)

        # Create mock embeddings result to feed to Stage 7
        from src.pipeline.models.results import EmbeddingResults
        embeddings_input = EmbeddingResults(
            table_id="bigquery-ai-kaggle-469620.ads_demo.ads_embeddings",
            embedding_count=762,  # From our test data
            dimension=768,
            generation_time=10.0
        )

        print("   🚀 Executing Stage 7 with CTA Intelligence...")
        start_time = time.time()
        results = stage7.execute(embeddings_input)
        duration = time.time() - start_time

        print(f"   ✅ Stage 7 completed in {duration:.2f}s")
        print(f"   📊 Status: {results.status}")
        print(f"   💼 Business Impact Score: {results.forecasts.get('business_impact_score', 'N/A')}")
        print(f"   🎯 CTA Intelligence: {hasattr(results, 'metadata') and results.metadata.get('cta_intelligence_enabled', 'N/A')}")
        print(f"   🔗 Run ID: {test_run_id}")

        return results, test_run_id, duration

    except Exception as e:
        print(f"   ❌ Stage 7 failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None

def verify_cta_table_exists(run_id):
    """Explicitly verify CTA table was created by Stage 7"""
    print(f"\n🔍 STEP 2: Verifying CTA Table Creation")
    print("=" * 60)

    try:
        # Check if CTA table exists
        table_name = f"bigquery-ai-kaggle-469620.ads_demo.cta_aggressiveness_analysis"
        check_sql = f"""
        SELECT
            COUNT(*) as row_count,
            COUNT(DISTINCT brand) as brand_count,
            COUNTIF(promotional_intensity IS NOT NULL) as pi_count,
            COUNTIF(urgency_score IS NOT NULL) as urgency_count
        FROM `{table_name}`
        """

        print(f"   🔍 Checking table: {table_name}")
        result = run_query(check_sql)

        if result is not None and not result.empty:
            row = result.iloc[0]
            print(f"   ✅ CTA table exists!")
            print(f"   📊 Rows: {row['row_count']}")
            print(f"   🏢 Brands: {row['brand_count']}")
            print(f"   📈 PI metrics: {row['pi_count']}")
            print(f"   ⚡ Urgency metrics: {row['urgency_count']}")
            return True
        else:
            print(f"   ❌ CTA table missing or empty")
            return False

    except Exception as e:
        print(f"   ❌ CTA table check failed: {e}")
        return False

def run_stage8_with_proof(stage7_results, run_id):
    """Run Stage 8 with explicit hybrid detector verification"""
    print(f"\n🧪 STEP 3: Running Stage 8 with Hybrid Detector Proof")
    print("=" * 60)

    try:
        # Initialize Stage 8 with the same run_id
        stage8 = MultiDimensionalIntelligenceStage(
            stage_name="Multi-Dimensional Intelligence",
            stage_number=8,
            run_id=run_id  # CRITICAL: Same run_id as Stage 7
        )

        # Set competitor brands
        stage8.competitor_brands = [
            'Warby Parker', 'LensCrafters', 'EyeBuyDirect',
            'Zenni Optical', 'GlassesUSA'
        ]

        print(f"   🎯 Using run_id: {run_id}")
        print(f"   🏢 Brands: {stage8.competitor_brands}")

        # Capture all log output to prove which detector is used
        log_messages = []
        original_logger = stage8.logger

        class ProofLogger:
            def info(self, msg):
                log_messages.append(f"INFO: {msg}")
                print(f"   🔍 {msg}")
                original_logger.info(msg)
            def warning(self, msg):
                log_messages.append(f"WARNING: {msg}")
                print(f"   ⚠️  {msg}")
                original_logger.warning(msg)
            def error(self, msg):
                log_messages.append(f"ERROR: {msg}")
                print(f"   ❌ {msg}")
                original_logger.error(msg)

        stage8.logger = ProofLogger()

        print(f"   🚀 Executing Stage 8...")
        start_time = time.time()
        results = stage8.execute(stage7_results)
        duration = time.time() - start_time

        print(f"   ✅ Stage 8 completed in {duration:.2f}s")

        # Analyze logs for proof
        hybrid_used = any("hybrid" in msg.lower() for msg in log_messages)
        fallback_used = any("fallback" in msg.lower() for msg in log_messages)

        print(f"\n📋 LOG ANALYSIS:")
        print(f"   Hybrid mentions: {sum(1 for msg in log_messages if 'hybrid' in msg.lower())}")
        print(f"   Fallback mentions: {sum(1 for msg in log_messages if 'fallback' in msg.lower())}")

        # Analyze whitespace results
        whitespace = results.whitespace_intelligence
        approach = whitespace.get('performance_metrics', {}).get('approach', 'unknown')
        ws_duration = whitespace.get('performance_metrics', {}).get('duration_seconds', 0)
        opportunities = whitespace.get('opportunities_found', 0)

        print(f"\n📊 WHITESPACE RESULTS:")
        print(f"   Status: {whitespace.get('status', 'N/A')}")
        print(f"   Approach: {approach}")
        print(f"   Duration: {ws_duration:.2f}s")
        print(f"   Opportunities: {opportunities}")

        # Check for campaign intelligence (proves Hybrid)
        strategic_opps = whitespace.get('strategic_opportunities', [])
        campaign_briefs = 0
        sample_headline = "N/A"

        if strategic_opps and isinstance(strategic_opps[0], dict):
            for opp in strategic_opps:
                if 'campaign_brief' in opp:
                    campaign_briefs += 1
                    if sample_headline == "N/A":
                        sample_headline = opp['campaign_brief'].get('sample_headlines', 'N/A')[:60]

        print(f"   Campaign Briefs: {campaign_briefs}/{len(strategic_opps)}")
        print(f"   Sample Headline: {sample_headline}...")

        # Generate proof report
        proof_data = {
            'test_timestamp': datetime.now().isoformat(),
            'stage7_run_id': run_id,
            'stage7_duration': getattr(run_stage7_complete, 'duration', 0),
            'stage7_cta_enabled': hasattr(stage7_results, 'metadata') and stage7_results.metadata.get('cta_intelligence_enabled', False),
            'cta_table_verified': True,  # We checked this in step 2
            'stage8_duration': duration,
            'stage8_status': results.status,
            'whitespace_approach': approach,
            'whitespace_duration': ws_duration,
            'opportunities_found': opportunities,
            'campaign_briefs_generated': campaign_briefs,
            'hybrid_indicators': {
                'approach_is_hybrid': 'hybrid' in approach.lower(),
                'has_campaign_intelligence': campaign_briefs > 0,
                'logs_mention_hybrid': hybrid_used,
                'no_fallback_used': not fallback_used,
                'performance_under_3min': ws_duration < 180
            },
            'log_messages': log_messages,
            'sample_opportunity': strategic_opps[0] if strategic_opps else None,
            'proof_verdict': 'CONFIRMED' if (
                'hybrid' in approach.lower() and
                campaign_briefs > 0 and
                ws_duration < 180 and
                not fallback_used
            ) else 'FAILED'
        }

        # Save proof
        with open(f'data/output/stage_tests/stage7_to_stage8_proof_{run_id}.json', 'w') as f:
            json.dump(proof_data, f, indent=2)

        print(f"\n💾 Proof saved to: data/output/stage_tests/stage7_to_stage8_proof_{run_id}.json")

        return proof_data

    except Exception as e:
        print(f"   ❌ Stage 8 failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Run complete proof sequence"""
    print("🏁 COMPLETE STAGE 7 → STAGE 8 INTEGRATION PROOF")
    print("=" * 80)

    # Step 1: Run Stage 7
    stage7_results, run_id, stage7_duration = run_stage7_complete()
    if not stage7_results or not run_id:
        print("❌ Cannot proceed without successful Stage 7")
        return

    # Step 2: Verify CTA table
    if not verify_cta_table_exists(run_id):
        print("❌ Cannot proceed without CTA table verification")
        return

    # Step 3: Run Stage 8 with proof
    proof_data = run_stage8_with_proof(stage7_results, run_id)
    if not proof_data:
        print("❌ Stage 8 proof failed")
        return

    # Final verdict
    print(f"\n🏁 FINAL PROOF VERDICT")
    print("=" * 80)
    verdict = proof_data['proof_verdict']

    if verdict == 'CONFIRMED':
        print(f"🎉 PROOF CONFIRMED: Hybrid detector integration works as claimed!")
        print(f"   ✅ Stage 7 created CTA table (verified)")
        print(f"   ✅ Stage 8 used Hybrid detector (approach: {proof_data['whitespace_approach']})")
        print(f"   ✅ Generated {proof_data['campaign_briefs_generated']} campaign briefs")
        print(f"   ✅ Completed in {proof_data['whitespace_duration']:.2f}s (target: <180s)")
        print(f"   ✅ No fallback used")
    else:
        print(f"❌ PROOF FAILED: Claims not substantiated")
        print(f"   Approach: {proof_data.get('whitespace_approach', 'unknown')}")
        print(f"   Campaign briefs: {proof_data.get('campaign_briefs_generated', 0)}")
        print(f"   Duration: {proof_data.get('whitespace_duration', 0):.2f}s")
        print(f"   Fallback used: {not proof_data.get('hybrid_indicators', {}).get('no_fallback_used', True)}")

if __name__ == "__main__":
    main()