#!/usr/bin/env python3
"""
API Variability Experiment: Test if differences are external API or systematic code issues
Runs 4 back-to-back Stage 1-4 tests to isolate ingestion differences
"""

import time
import json
from datetime import datetime
from pathlib import Path

# Import both approaches
from src.pipeline.orchestrator import CompetitiveIntelligencePipeline
from tests.stage_testing_framework import StageTestingFramework
from src.pipeline.core.base import PipelineContext

def run_pipeline_to_stage4(run_id: str):
    """Run regular pipeline up to Stage 4 (ingestion) only"""
    print(f"\n🚀 REGULAR PIPELINE TEST - {run_id}")
    print("=" * 50)

    start_time = time.time()

    try:
        # Initialize pipeline
        pipeline = CompetitiveIntelligencePipeline("Warby Parker", "eyewear", verbose=True)

        # Stage 1: Discovery
        print("🔍 Stage 1: Discovery")
        from src.pipeline.stages.discovery import DiscoveryStage
        discovery_stage = DiscoveryStage(pipeline.context, False)
        raw_candidates = discovery_stage.run(pipeline.context, pipeline.progress)
        print(f"   Found {len(raw_candidates)} candidates")

        # Stage 2: Curation
        print("🤖 Stage 2: AI Curation")
        from src.pipeline.stages.curation import CurationStage
        curation_stage = CurationStage(pipeline.context, False)
        validated_competitors = curation_stage.run(raw_candidates, pipeline.progress)
        print(f"   Validated {len(validated_competitors)} competitors")

        # Stage 3: Ranking
        print("📊 Stage 3: Meta Ad Ranking")
        from src.pipeline.stages.ranking import RankingStage
        ranking_stage = RankingStage(pipeline.context, False, True)
        ranked_competitors = ranking_stage.run(validated_competitors, pipeline.progress)
        print(f"   Ranked {len(ranked_competitors)} Meta-active competitors")

        # Set competitor brands in context (critical for Stage 4)
        pipeline.context.competitor_brands = [comp.company_name for comp in ranked_competitors]

        # Stage 4: Ingestion
        print("📱 Stage 4: Meta Ads Ingestion")
        from src.pipeline.stages.ingestion import IngestionStage
        ingestion_stage = IngestionStage(pipeline.context, False, True)
        ingestion_results = ingestion_stage.run(ranked_competitors, pipeline.progress)

        duration = time.time() - start_time

        result = {
            'approach': 'pipeline',
            'run_id': run_id,
            'duration': duration,
            'total_ads': ingestion_results.total_ads if hasattr(ingestion_results, 'total_ads') else len(ingestion_results.ads),
            'brands': len(ingestion_results.brands) if hasattr(ingestion_results, 'brands') else 'unknown',
            'competitors_found': len(ranked_competitors),
            'success': True,
            'timestamp': datetime.now().isoformat()
        }

        print(f"✅ Pipeline Stage 4 Complete: {result['total_ads']} ads from {result['brands']} brands")
        return result

    except Exception as e:
        print(f"❌ Pipeline Stage 4 Failed: {e}")
        return {
            'approach': 'pipeline',
            'run_id': run_id,
            'duration': time.time() - start_time,
            'total_ads': 0,
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

def run_staged_to_stage4(run_id: str):
    """Run staged testing up to Stage 4 (ingestion) only"""
    print(f"\n🧪 STAGED TESTING TEST - {run_id}")
    print("=" * 50)

    start_time = time.time()

    try:
        # Initialize staged testing framework
        framework = StageTestingFramework("Warby Parker", "eyewear", run_id)

        # Stage 1: Discovery
        print("🔍 Stage 1: Discovery")
        discovery_results = framework.test_stage_1_discovery()
        print(f"   Found {len(discovery_results)} candidates")

        # Stage 2: Curation
        print("🤖 Stage 2: AI Curation")
        curation_results = framework.test_stage_2_curation(discovery_results)
        print(f"   Validated {len(curation_results)} competitors")

        # Stage 3: Ranking
        print("📊 Stage 3: Meta Ad Ranking")
        ranking_results = framework.test_stage_3_ranking(curation_results)
        print(f"   Ranked {len(ranking_results)} Meta-active competitors")

        # Stage 4: Ingestion
        print("📱 Stage 4: Meta Ads Ingestion")
        ingestion_results = framework.test_stage_4_ingestion(ranking_results)

        duration = time.time() - start_time

        result = {
            'approach': 'staged',
            'run_id': run_id,
            'duration': duration,
            'total_ads': ingestion_results.total_ads if hasattr(ingestion_results, 'total_ads') else len(ingestion_results.ads),
            'brands': len(ingestion_results.brands) if hasattr(ingestion_results, 'brands') else 'unknown',
            'competitors_found': len(ranking_results),
            'success': True,
            'timestamp': datetime.now().isoformat()
        }

        print(f"✅ Staged Stage 4 Complete: {result['total_ads']} ads from {result['brands']} brands")
        return result

    except Exception as e:
        print(f"❌ Staged Stage 4 Failed: {e}")
        return {
            'approach': 'staged',
            'run_id': run_id,
            'duration': time.time() - start_time,
            'total_ads': 0,
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

def main():
    """Run the 4-test API variability experiment"""

    print("🧪 API VARIABILITY EXPERIMENT")
    print("=" * 60)
    print("Testing if ingestion differences are API variability or systematic")
    print("Running 4 back-to-back Stage 1-4 tests...")
    print()

    # Generate base timestamp for all tests
    base_time = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Run 4 tests in sequence
    tests = [
        ('pipeline', f'exp1_{base_time}'),
        ('staged', f'exp2_{base_time}'),
        ('pipeline', f'exp3_{base_time}'),
        ('staged', f'exp4_{base_time}')
    ]

    results = []

    for i, (approach, run_id) in enumerate(tests, 1):
        print(f"\n📊 TEST {i}/4: {approach.upper()} APPROACH")
        print(f"Run ID: {run_id}")

        if approach == 'pipeline':
            result = run_pipeline_to_stage4(run_id)
        else:
            result = run_staged_to_stage4(run_id)

        results.append(result)

        # Brief pause between tests
        if i < 4:
            print(f"\n⏳ Pausing 10 seconds before next test...")
            time.sleep(10)

    # Analyze results
    print("\n" + "=" * 60)
    print("🔬 EXPERIMENT RESULTS ANALYSIS")
    print("=" * 60)

    print("\n📊 RAW RESULTS:")
    for i, result in enumerate(results, 1):
        status = "✅" if result['success'] else "❌"
        ads = result.get('total_ads', 0)
        approach = result['approach'].upper()
        print(f"  Test {i} ({approach:8}): {status} {ads:3d} ads")

    # Statistical analysis
    pipeline_ads = [r['total_ads'] for r in results if r['approach'] == 'pipeline' and r['success']]
    staged_ads = [r['total_ads'] for r in results if r['approach'] == 'staged' and r['success']]

    print(f"\n📈 PATTERN ANALYSIS:")
    print(f"  Pipeline results: {pipeline_ads}")
    print(f"  Staged results:   {staged_ads}")

    if len(pipeline_ads) >= 2 and len(staged_ads) >= 2:
        pipeline_consistent = max(pipeline_ads) - min(pipeline_ads) < 100  # <100 ad difference
        staged_consistent = max(staged_ads) - min(staged_ads) < 100
        approaches_different = abs(sum(pipeline_ads)/len(pipeline_ads) - sum(staged_ads)/len(staged_ads)) > 200

        print(f"\n🧠 HYPOTHESIS TEST:")
        print(f"  Pipeline consistency: {'✅ Consistent' if pipeline_consistent else '❌ Variable'}")
        print(f"  Staged consistency:   {'✅ Consistent' if staged_consistent else '❌ Variable'}")
        print(f"  Approach difference:  {'✅ Significant' if approaches_different else '❌ Minimal'}")

        if pipeline_consistent and staged_consistent and approaches_different:
            print(f"\n🎯 CONCLUSION: SYSTEMATIC DIFFERENCE (H1)")
            print(f"  Each approach is internally consistent but they differ significantly")
            print(f"  This suggests a CODE ISSUE, not API variability")
        elif not pipeline_consistent and not staged_consistent:
            print(f"\n🎯 CONCLUSION: API VARIABILITY (H0)")
            print(f"  Both approaches show high variability")
            print(f"  This suggests EXTERNAL API CHANGES are the cause")
        else:
            print(f"\n🤔 CONCLUSION: MIXED PATTERN")
            print(f"  Results are inconclusive - may need more data")

    # Save detailed results
    output_file = f"data/output/api_variability_experiment_{base_time}.json"
    Path("data/output").mkdir(exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump({
            'experiment': 'api_variability_test',
            'timestamp': datetime.now().isoformat(),
            'hypothesis': {
                'h0': 'Differences due to external API variability',
                'h1': 'Differences due to systematic code issues'
            },
            'tests': results,
            'analysis': {
                'pipeline_ads': pipeline_ads,
                'staged_ads': staged_ads,
                'pipeline_consistent': pipeline_consistent if 'pipeline_consistent' in locals() else None,
                'staged_consistent': staged_consistent if 'staged_consistent' in locals() else None,
                'approaches_different': approaches_different if 'approaches_different' in locals() else None
            }
        }, f, indent=2)

    print(f"\n💾 Detailed results saved: {output_file}")

if __name__ == "__main__":
    main()