#!/usr/bin/env python3
"""
Test Stage 5 using cached Stage 4 results
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
from src.pipeline.stages.embeddings import EmbeddingsStage
from src.pipeline.models.candidates import IngestionResults

def load_stage4_results():
    """Load cached Stage 4 results and convert to IngestionResults object"""
    stage4_cache = Path("data/output/stage_tests/stage4_test_results.json")

    if not stage4_cache.exists():
        print(f"❌ Cache file not found: {stage4_cache}")
        return None

    with open(stage4_cache, 'r') as f:
        stage4_data = json.load(f)

    # Convert to IngestionResults object
    ingestion_results = IngestionResults(
        ads=[],  # We don't need actual ads for embeddings stage, just metadata
        brands=stage4_data['brands'],
        total_ads=stage4_data['total_ads'],
        ingestion_time=stage4_data['ingestion_time'],
        ads_table_id=stage4_data['ads_table_id']
    )

    return ingestion_results

def main():
    print("🧠 TESTING STAGE 5 WITH CACHED STAGE 4 RESULTS")
    print("=" * 60)

    # Load cached Stage 4 results
    ingestion_results = load_stage4_results()
    if not ingestion_results:
        return

    print(f"📊 Loaded Stage 4 results:")
    print(f"   Total Ads: {ingestion_results.total_ads}")
    print(f"   Brands: {', '.join(ingestion_results.brands)}")
    print(f"   BigQuery Table: {ingestion_results.ads_table_id}")

    # Initialize context and progress
    context = PipelineContext("Warby Parker", "eyewear", "stage5_test", verbose=True)
    # Set competitor brands for embeddings stage
    context.competitor_brands = [brand for brand in ingestion_results.brands if brand != "Warby Parker"]
    progress = ProgressTracker(total_stages=9)

    # Initialize and run Stage 5
    print("\n🚀 Running Stage 5 Embeddings Generation...")
    embeddings_stage = EmbeddingsStage(context, dry_run=False, verbose=True)

    try:
        embedding_results = embeddings_stage.run(ingestion_results, progress)

        print(f"\n✅ Stage 5 Complete - Generated embeddings")

        # Show results
        print("\n🧠 EMBEDDINGS RESULTS:")
        print(f"   Embedding Table: {embedding_results.table_id}")
        print(f"   Total Embeddings: {embedding_results.embedding_count}")
        print(f"   Embedding Dimension: {embedding_results.dimension}")
        print(f"   Generation Time: {embedding_results.generation_time:.2f}s")

        # Save results
        output_file = Path("data/output/stage_tests/stage5_test_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        results = {
            'input_ads': int(ingestion_results.total_ads),
            'input_brands': ingestion_results.brands,
            'input_table': ingestion_results.ads_table_id,
            'embedding_table': embedding_results.table_id,
            'embedding_count': int(embedding_results.embedding_count),
            'embedding_dimension': int(embedding_results.dimension),
            'generation_time': float(embedding_results.generation_time)
        }

        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\n💾 Results saved to: {output_file}")

        # Check if we can query the embeddings table
        print("\n📊 CHECKING EMBEDDING TABLE:")
        try:
            from src.utils.bigquery_client import run_query

            sample_query = f"""
            SELECT
                brand,
                COUNT(*) as embedding_count,
                AVG(content_length_chars) as avg_content_length
            FROM `{embedding_results.table_id}`
            GROUP BY brand
            ORDER BY embedding_count DESC
            """

            sample_result = run_query(sample_query)
            print("   ✅ Embedding distribution by brand:")
            for _, row in sample_result.iterrows():
                print(f"      {row['brand']}: {row['embedding_count']} embeddings (avg {row['avg_content_length']:.0f} chars)")

        except Exception as e:
            print(f"   ⚠️  Could not query embedding table: {e}")

    except Exception as e:
        print(f"❌ Stage 5 Failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()