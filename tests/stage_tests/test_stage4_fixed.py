#!/usr/bin/env python3
"""
Test Stage 4 with fixed creative content extraction (using correct ads_fetcher)
"""
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.append('src')

from pipeline.core.base import PipelineContext
from pipeline.core.progress import ProgressTracker
from pipeline.stages.ingestion import IngestionStage
from pipeline.models.candidates import ValidatedCompetitor

def main():
    print("📥 TESTING STAGE 4 WITH FIXED CREATIVE CONTENT EXTRACTION")
    print("=" * 70)

    # Create test competitors (simplified for testing)
    competitors = [
        ValidatedCompetitor(
            company_name="EyeBuyDirect",
            is_competitor=True,
            tier="Meta-Active",
            market_overlap_pct=80,
            customer_substitution_ease="High",
            confidence=0.9,
            reasoning="Test competitor",
            evidence_sources="Test",
            quality_score=0.9
        )
    ]

    print(f"📁 Testing with {len(competitors)} competitor:")
    for comp in competitors:
        print(f"   • {comp.company_name}")

    # Initialize context and progress
    context = PipelineContext("Warby Parker", "eyewear", "stage4_fixed_test", verbose=True)
    progress = ProgressTracker(total_stages=9)

    # Initialize and run Stage 4
    print("\n🚀 Running Stage 4 with Fixed Content Extraction...")
    ingestion_stage = IngestionStage(context, dry_run=False)

    try:
        ingestion_results = ingestion_stage.run(competitors, progress)

        print(f"\n✅ Stage 4 Complete!")
        print(f"   Total Ads: {ingestion_results.total_ads}")
        print(f"   Brands: {len(ingestion_results.brands)}")
        print(f"   BigQuery Table: {ingestion_results.ads_table_id}")

    except Exception as e:
        print(f"❌ Stage 4 Failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()