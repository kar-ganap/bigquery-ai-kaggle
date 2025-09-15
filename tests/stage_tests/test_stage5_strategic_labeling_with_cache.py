#!/usr/bin/env python3
"""
Test Stage 5 Strategic Labeling using cached Stage 4 results
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
from src.pipeline.stages.strategic_labeling import StrategicLabelingStage
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
        ads=[],  # We don't need actual ads for strategic labeling, just metadata
        brands=stage4_data['brands'],
        total_ads=stage4_data['total_ads'],
        ingestion_time=stage4_data['ingestion_time'],
        ads_table_id=stage4_data['ads_table_id']
    )

    return ingestion_results

def main():
    print("🏷️  TESTING STAGE 5 STRATEGIC LABELING WITH CACHED STAGE 4 RESULTS")
    print("=" * 70)

    # Load cached Stage 4 results
    ingestion_results = load_stage4_results()
    if not ingestion_results:
        return

    print(f"📊 Loaded Stage 4 results:")
    print(f"   Total Ads: {ingestion_results.total_ads}")
    print(f"   Brands: {', '.join(ingestion_results.brands)}")
    print(f"   BigQuery Table: {ingestion_results.ads_table_id}")

    # Initialize context and progress
    context = PipelineContext("Warby Parker", "eyewear", "stage5_strategic_test", verbose=True)
    # Set competitor brands for strategic labeling stage
    context.competitor_brands = [brand for brand in ingestion_results.brands if brand != "Warby Parker"]
    progress = ProgressTracker(total_stages=9)

    # Initialize and run Stage 5
    print("\n🚀 Running Stage 5 Strategic Labeling...")
    print("📋 Expected: Generate strategic labels using AI.GENERATE_TABLE")
    print("🎯 Output: ads_with_dates table with promotional_intensity, funnel, angles fields")

    strategic_labeling_stage = StrategicLabelingStage(context, dry_run=False, verbose=True)

    try:
        strategic_results = strategic_labeling_stage.run(ingestion_results, progress)

        print(f"\n✅ Stage 5 Complete - Generated strategic labels")

        # Show results
        print("\n🏷️  STRATEGIC LABELING RESULTS:")
        print(f"   Labels Table: {strategic_results.table_id}")
        print(f"   Labeled Ads: {strategic_results.labeled_ads}")
        print(f"   Generation Time: {strategic_results.generation_time:.2f}s")

        # Save results
        output_file = Path("data/output/stage_tests/stage5_strategic_labeling_test_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        results = {
            'input_ads': int(ingestion_results.total_ads),
            'input_brands': ingestion_results.brands,
            'input_table': ingestion_results.ads_table_id,
            'labels_table': strategic_results.table_id,
            'labeled_ads': int(strategic_results.labeled_ads),
            'generation_time': float(strategic_results.generation_time)
        }

        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\n💾 Results saved to: {output_file}")

        # Check if we can query the strategic labels table
        print("\n📊 CHECKING STRATEGIC LABELS TABLE:")
        try:
            from src.utils.bigquery_client import run_query

            # First check the schema to understand data types
            schema_query = f"""
            SELECT column_name, data_type
            FROM `{strategic_results.table_id.split('.')[0]}.{strategic_results.table_id.split('.')[1]}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{strategic_results.table_id.split('.')[2]}'
            AND column_name IN ('promotional_intensity', 'funnel', 'angles')
            ORDER BY column_name
            """

            try:
                schema_result = run_query(schema_query)
                print("   📋 Strategic label field types:")
                for _, row in schema_result.iterrows():
                    print(f"      {row['column_name']}: {row['data_type']}")
            except:
                print("   📋 Could not fetch schema, using safe queries")

            # Sample the strategic labels with safe queries
            sample_query = f"""
            SELECT
                brand,
                COUNT(*) as labeled_ads,
                COUNT(promotional_intensity) as with_promotional_labels,
                COUNT(funnel) as with_funnel_labels,
                COUNT(angles) as with_angle_labels
            FROM `{strategic_results.table_id}`
            GROUP BY brand
            ORDER BY labeled_ads DESC
            """

            sample_result = run_query(sample_query)
            print("   ✅ Strategic labels by brand:")
            for _, row in sample_result.iterrows():
                print(f"      {row['brand']}: {row['labeled_ads']} ads")
                print(f"         • {row['with_promotional_labels']} with promotional labels")
                print(f"         • {row['with_funnel_labels']} with funnel labels")
                print(f"         • {row['with_angle_labels']} with angle labels")
                print()

            # Show sample strategic labels
            print("   📋 Sample strategic labels:")
            sample_labels_query = f"""
            SELECT
                brand,
                creative_text,
                promotional_intensity,
                funnel,
                angles
            FROM `{strategic_results.table_id}`
            WHERE creative_text IS NOT NULL
                AND LENGTH(creative_text) > 20
                AND promotional_intensity IS NOT NULL
            LIMIT 3
            """

            sample_labels = run_query(sample_labels_query)
            for _, row in sample_labels.iterrows():
                print(f"      {row['brand']}: \"{row['creative_text'][:60]}...\"")
                print(f"         → Promo: {row['promotional_intensity']}, Funnel: {row['funnel']}, Angles: {row['angles']}")
                print()

        except Exception as e:
            print(f"   ⚠️  Could not query strategic labels table: {e}")

    except Exception as e:
        print(f"❌ Stage 5 Failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()