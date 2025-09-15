#!/usr/bin/env python3
"""
Test Stage 3 using cached Stage 2 results
"""
import sys
import json
from pathlib import Path

# Add project root to path
sys.path.append('src')

from src.pipeline.core.base import PipelineContext
from src.pipeline.core.progress import ProgressTracker
from src.pipeline.stages.ranking import RankingStage
from src.pipeline.models.candidates import ValidatedCompetitor

def load_stage2_results():
    """Load cached Stage 2 results and convert to ValidatedCompetitor objects"""
    stage2_cache = Path("data/output/stage_tests/stage2_test_results.json")

    if not stage2_cache.exists():
        print(f"❌ Cache file not found: {stage2_cache}")
        return []

    with open(stage2_cache, 'r') as f:
        stage2_data = json.load(f)

    # Convert JSON data to ValidatedCompetitor objects
    competitors = []
    for comp_data in stage2_data['competitors']:
        competitor = ValidatedCompetitor(
            company_name=comp_data['company_name'],
            is_competitor=True,  # All Stage 2 results are validated competitors
            tier="Unknown",  # Will be determined by Stage 3
            market_overlap_pct=comp_data['market_overlap_pct'],
            customer_substitution_ease="High",  # Default value
            confidence=comp_data['confidence'],
            reasoning=f"AI-validated competitor with {comp_data['quality_score']:.2f} quality score",
            evidence_sources="Stage 2 AI Consensus Validation",  # Required field
            quality_score=comp_data['quality_score']
        )
        competitors.append(competitor)

    return competitors

def main():
    print("🏆 TESTING STAGE 3 WITH CACHED STAGE 2 RESULTS")
    print("=" * 60)

    # Load cached Stage 2 results
    competitors = load_stage2_results()
    if not competitors:
        return

    print(f"📁 Loaded {len(competitors)} validated competitors from Stage 2")

    for i, comp in enumerate(competitors, 1):
        print(f"   {i}. {comp.company_name} (confidence: {comp.confidence:.3f})")

    # Initialize context and progress
    context = PipelineContext("Warby Parker", "eyewear", "stage3_test", verbose=True)
    progress = ProgressTracker(total_stages=9)

    # Initialize and run Stage 3
    print("\n🚀 Running Stage 3 Ranking...")
    ranking_stage = RankingStage(context, dry_run=False)

    try:
        ranked_competitors = ranking_stage.run(competitors, progress)

        print(f"\n✅ Stage 3 Complete - {len(ranked_competitors)} ranked competitors")

        # Show results
        print("\n🏆 RANKING RESULTS:")
        for i, competitor in enumerate(ranked_competitors, 1):
            print(f"   {i}. {competitor.company_name}")
            print(f"      Tier: {getattr(competitor, 'tier', 'N/A')}")
            print(f"      Threat Score: {getattr(competitor, 'threat_score', 'N/A')}")
            print(f"      Priority Rank: {getattr(competitor, 'priority_rank', i)}")
            print(f"      Strategic Priority: {getattr(competitor, 'strategic_priority', 'N/A')}")
            print()

        # Check BigQuery tables created
        print("\n📊 CHECKING BIGQUERY TABLES CREATED:")

        # Save results
        output_file = Path("data/output/stage_tests/stage3_test_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        results = {
            'input_competitors': len(competitors),
            'ranked_competitors': len(ranked_competitors),
            'competitors': [
                {
                    'company_name': comp.company_name,
                    'tier': getattr(comp, 'tier', 'N/A'),
                    'threat_score': getattr(comp, 'threat_score', None),
                    'priority_rank': i,
                    'strategic_priority': getattr(comp, 'strategic_priority', 'N/A'),
                    'confidence': getattr(comp, 'confidence', None)
                } for i, comp in enumerate(ranked_competitors, 1)
            ]
        }

        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\n💾 Results saved to: {output_file}")

    except Exception as e:
        print(f"❌ Stage 3 Failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()