#!/usr/bin/env python3
"""
Test Stage 7 Analysis using cached Stage 5 and 6 results
"""
import sys
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.pipeline.core.base import PipelineContext
from src.pipeline.core.progress import ProgressTracker
from src.pipeline.stages.analysis import AnalysisStage
from src.pipeline.models.candidates import EmbeddingResults

def load_previous_results():
    """Load cached Stage 5 and 6 results"""
    # Load Stage 6 embeddings results (primary input)
    stage6_cache = Path("data/output/stage_tests/stage5_test_results.json")  # Note: stage5_test_results.json contains embeddings

    if not stage6_cache.exists():
        print(f"❌ Stage 6 embeddings cache file not found: {stage6_cache}")
        return None

    with open(stage6_cache, 'r') as f:
        stage6_data = json.load(f)

    # Convert to EmbeddingResults object
    embedding_results = EmbeddingResults(
        table_id=stage6_data['embedding_table'],
        embedding_count=stage6_data['embedding_count'],
        dimension=stage6_data['embedding_dimension'],
        generation_time=stage6_data['generation_time']
    )

    return embedding_results, stage6_data

def main():
    print("🧠 TESTING STAGE 7 STRATEGIC ANALYSIS WITH CACHED RESULTS")
    print("=" * 70)

    # Load cached results
    results = load_previous_results()
    if not results:
        return

    embedding_results, stage6_data = results

    print(f"📊 Loaded previous stage results:")
    print(f"   Embeddings Table: {embedding_results.table_id}")
    print(f"   Total Embeddings: {embedding_results.embedding_count}")
    print(f"   Embedding Dimension: {embedding_results.dimension}")
    print(f"   Input Brands: {', '.join(stage6_data['input_brands'])}")

    # Initialize context and progress
    context = PipelineContext("Warby Parker", "eyewear", "stage7_analysis_test", verbose=True)
    # Set competitor brands for analysis stage
    context.competitor_brands = [brand for brand in stage6_data['input_brands'] if brand != "Warby Parker"]
    progress = ProgressTracker(total_stages=9)

    # Initialize and run Stage 7
    print("\n🚀 Running Stage 7 Strategic Analysis...")
    print("📋 Expected outputs:")
    print("   • Current state analysis (promotional intensity, urgency, brand voice)")
    print("   • Competitive copying detection (who's copying whom)")
    print("   • Temporal evolution analysis (momentum, velocity changes)")
    print("   • White space detection (market gaps)")
    print("   • Business impact forecasts")
    print()

    analysis_stage = AnalysisStage(context, dry_run=False, verbose=True)

    try:
        analysis_results = analysis_stage.run(embedding_results, progress)

        print(f"\n✅ Stage 7 Complete - Strategic analysis complete")

        # Show results
        print("\n🎯 STRATEGIC ANALYSIS RESULTS:")
        print("=" * 60)

        # 1. Current State Analysis
        print("\n📊 CURRENT STATE ANALYSIS:")
        if analysis_results.current_state:
            for key, value in analysis_results.current_state.items():
                if isinstance(value, (int, float)):
                    print(f"   • {key}: {value:.3f}")
                else:
                    print(f"   • {key}: {value}")

        # 2. Competitive Influence/Copying
        print("\n🔍 COMPETITIVE INFLUENCE ANALYSIS:")
        if analysis_results.influence:
            for key, value in analysis_results.influence.items():
                if isinstance(value, (int, float)):
                    print(f"   • {key}: {value:.3f}")
                else:
                    print(f"   • {key}: {value}")

        # 3. Evolution Analysis
        print("\n📈 EVOLUTION & MOMENTUM:")
        if analysis_results.evolution:
            for key, value in analysis_results.evolution.items():
                if isinstance(value, (int, float)):
                    print(f"   • {key}: {value:.3f}")
                else:
                    print(f"   • {key}: {value}")

        # 4. Forecasts
        print("\n🔮 BUSINESS IMPACT FORECASTS:")
        if analysis_results.forecasts:
            for key, value in analysis_results.forecasts.items():
                if isinstance(value, (int, float)):
                    print(f"   • {key}: {value:.3f}")
                else:
                    print(f"   • {key}: {value}")

        # 5. White Space Detection (if available)
        if hasattr(analysis_results, 'white_spaces') and analysis_results.white_spaces:
            print("\n💡 WHITE SPACE OPPORTUNITIES:")
            if isinstance(analysis_results.white_spaces, dict):
                for key, value in analysis_results.white_spaces.items():
                    print(f"   • {key}: {value}")
            elif isinstance(analysis_results.white_spaces, list):
                for i, space in enumerate(analysis_results.white_spaces[:5], 1):
                    print(f"   {i}. {space}")

        # 6. Velocity Metrics (if available)
        if hasattr(analysis_results, 'velocity') and analysis_results.velocity:
            print("\n⚡ VELOCITY METRICS:")
            for key, value in analysis_results.velocity.items():
                if isinstance(value, (int, float)):
                    print(f"   • {key}: {value:.3f}")
                else:
                    print(f"   • {key}: {value}")

        # Save results
        output_file = Path("data/output/stage_tests/stage7_analysis_test_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Convert analysis results to serializable format
        results_dict = {
            'status': analysis_results.status,
            'current_state': analysis_results.current_state,
            'influence': analysis_results.influence,
            'evolution': analysis_results.evolution,
            'forecasts': analysis_results.forecasts,
            'metadata': {
                'input_embeddings': int(embedding_results.embedding_count),
                'embedding_table': embedding_results.table_id,
                'brands_analyzed': stage6_data['input_brands']
            }
        }

        # Add optional fields if they exist
        if hasattr(analysis_results, 'white_spaces') and analysis_results.white_spaces:
            results_dict['white_spaces'] = analysis_results.white_spaces
        if hasattr(analysis_results, 'velocity') and analysis_results.velocity:
            results_dict['velocity'] = analysis_results.velocity
        if hasattr(analysis_results, 'patterns') and analysis_results.patterns:
            results_dict['patterns'] = analysis_results.patterns

        with open(output_file, 'w') as f:
            json.dump(results_dict, f, indent=2, default=str)

        print(f"\n💾 Results saved to: {output_file}")

        # Additional analysis verification
        print("\n📊 ANALYSIS QUALITY CHECK:")
        print(f"   ✅ Status: {analysis_results.status}")
        print(f"   ✅ Current state metrics: {len(analysis_results.current_state) if analysis_results.current_state else 0}")
        print(f"   ✅ Influence metrics: {len(analysis_results.influence) if analysis_results.influence else 0}")
        print(f"   ✅ Evolution metrics: {len(analysis_results.evolution) if analysis_results.evolution else 0}")
        print(f"   ✅ Forecast metrics: {len(analysis_results.forecasts) if analysis_results.forecasts else 0}")

    except Exception as e:
        print(f"❌ Stage 7 Failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()