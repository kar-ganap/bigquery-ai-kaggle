#!/usr/bin/env python3
"""
Test Stage 4 using cached Stage 3 results
"""
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.append('src')

from src.pipeline.core.base import PipelineContext
from src.pipeline.core.progress import ProgressTracker
from src.pipeline.stages.ingestion import IngestionStage
from src.pipeline.models.candidates import ValidatedCompetitor

def load_stage3_results():
    """Load cached Stage 3 results and convert to ValidatedCompetitor objects"""
    stage3_cache = Path("data/output/stage_tests/stage3_test_results.json")

    if not stage3_cache.exists():
        print(f"❌ Cache file not found: {stage3_cache}")
        return []

    with open(stage3_cache, 'r') as f:
        stage3_data = json.load(f)

    # Convert JSON data to ValidatedCompetitor objects
    competitors = []
    for comp_data in stage3_data['competitors']:
        competitor = ValidatedCompetitor(
            company_name=comp_data['company_name'],
            is_competitor=True,
            tier="Meta-Active",  # From Stage 3 ranking
            market_overlap_pct=80,  # Default for Meta-active competitors
            customer_substitution_ease="High",
            confidence=comp_data['confidence'],
            reasoning="Meta-active competitor validated through Stage 3 ranking",
            evidence_sources="Meta Ad Library Activity",
            quality_score=0.9,  # High quality from Meta filtering
            # Meta fields from Stage 3
            meta_tier=1,  # Major Player tier
            estimated_ad_count="20+",  # All had 20+ ads
            meta_classification="Major Player"
        )
        competitors.append(competitor)

    return competitors

def main():
    print("📥 TESTING STAGE 4 WITH CACHED STAGE 3 RESULTS")
    print("=" * 60)

    # Load cached Stage 3 results
    competitors = load_stage3_results()
    if not competitors:
        return

    print(f"📁 Loaded {len(competitors)} Meta-active competitors from Stage 3")

    for i, comp in enumerate(competitors, 1):
        print(f"   {i}. {comp.company_name} ({comp.meta_classification} - {comp.estimated_ad_count} ads)")

    # Initialize context and progress
    context = PipelineContext("Warby Parker", "eyewear", "stage4_test", verbose=True)
    progress = ProgressTracker(total_stages=9)

    # Initialize and run Stage 4
    print("\n🚀 Running Stage 4 Ingestion...")
    ingestion_stage = IngestionStage(context, dry_run=False)

    try:
        ingestion_results = ingestion_stage.run(competitors, progress)

        print(f"\n✅ Stage 4 Complete - Ingested data for {len(competitors)} competitors")

        # Show results
        print("\n📥 INGESTION RESULTS:")
        print(f"   Total Ads Ingested: {ingestion_results.total_ads}")
        print(f"   Unique Brands: {len(ingestion_results.brands)}")
        print(f"   Ingestion Time: {ingestion_results.ingestion_time:.2f} seconds")
        if ingestion_results.ads_table_id:
            print(f"   BigQuery Table: {ingestion_results.ads_table_id}")

        print(f"\n   Brands Processed:")
        for brand in ingestion_results.brands[:10]:  # Show first 10
            print(f"   • {brand}")
        if len(ingestion_results.brands) > 10:
            print(f"   ... and {len(ingestion_results.brands) - 10} more brands")

        # Show sample ads
        if ingestion_results.ads and len(ingestion_results.ads) > 0:
            print(f"\n📋 SAMPLE ADS ({min(5, len(ingestion_results.ads))} of {len(ingestion_results.ads)}):")
            for i, ad in enumerate(ingestion_results.ads[:5], 1):
                print(f"   {i}. {ad.get('page_name', 'Unknown')} - {ad.get('creative_text', 'No text')[:100]}...")

        # Save results
        output_file = Path("data/output/stage_tests/stage4_test_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        results = {
            'input_competitors': len(competitors),
            'total_ads': ingestion_results.total_ads,
            'unique_brands': len(ingestion_results.brands),
            'ingestion_time': ingestion_results.ingestion_time,
            'ads_table_id': ingestion_results.ads_table_id,
            'brands': ingestion_results.brands,
            'sample_ads': ingestion_results.ads[:10] if ingestion_results.ads else []
        }

        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n💾 Results saved to: {output_file}")

    except Exception as e:
        print(f"❌ Stage 4 Failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()