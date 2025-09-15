#!/usr/bin/env python3
"""
Test Stage 2 using cached Stage 1 results
"""
import sys
import json
import re
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.pipeline.core.base import PipelineContext
from src.pipeline.core.progress import ProgressTracker
from src.pipeline.stages.curation import CurationStage
from src.pipeline.models.candidates import CompetitorCandidate

def parse_competitor_candidate(candidate_str: str) -> CompetitorCandidate:
    """Parse string representation back to CompetitorCandidate object"""
    # Extract values using regex
    pattern = r"CompetitorCandidate\(company_name='([^']*)', source_url='([^']*)', source_title='([^']*)', query_used='([^']*)', raw_score=([0-9.]+), found_in='([^']*)', discovery_method='([^']*)'\)"
    match = re.match(pattern, candidate_str)

    if match:
        return CompetitorCandidate(
            company_name=match.group(1),
            source_url=match.group(2),
            source_title=match.group(3),
            query_used=match.group(4),
            raw_score=float(match.group(5)),
            found_in=match.group(6),
            discovery_method=match.group(7)
        )
    else:
        # Fallback - try to extract at least the company name
        company_match = re.search(r"company_name='([^']*)'", candidate_str)
        if company_match:
            return CompetitorCandidate(
                company_name=company_match.group(1),
                source_url="unknown",
                source_title="unknown",
                query_used="unknown",
                raw_score=0.0,
                found_in="unknown",
                discovery_method="unknown"
            )
        else:
            raise ValueError(f"Could not parse candidate: {candidate_str[:100]}...")

def main():
    print("🤖 TESTING STAGE 2 WITH CACHED STAGE 1 RESULTS")
    print("=" * 60)

    # Load cached Stage 1 results
    stage1_cache = Path("data/output/stage_tests/test_warby_parker_20250915_025522/stage_1_discovery_result.json")

    if not stage1_cache.exists():
        print(f"❌ Cache file not found: {stage1_cache}")
        return

    with open(stage1_cache, 'r') as f:
        stage1_data = json.load(f)

    candidate_strings = stage1_data['full_output']
    print(f"📁 Loaded {len(candidate_strings)} candidate strings from Stage 1")

    # Parse string representations back to objects
    print("🔄 Parsing candidate strings to objects...")
    candidates = []
    for i, candidate_str in enumerate(candidate_strings):
        try:
            candidate = parse_competitor_candidate(candidate_str)
            candidates.append(candidate)
        except Exception as e:
            print(f"⚠️ Failed to parse candidate {i+1}: {str(e)}")

    print(f"✅ Successfully parsed {len(candidates)} candidates")

    # Initialize context and progress
    context = PipelineContext("Warby Parker", "eyewear", "stage2_test", verbose=True)
    progress = ProgressTracker(total_stages=9)

    # Initialize and run Stage 2
    print("\n🚀 Running Stage 2 Curation...")
    curation_stage = CurationStage(context, dry_run=False)

    try:
        validated_competitors = curation_stage.run(candidates, progress)

        print(f"\n✅ Stage 2 Complete - {len(validated_competitors)} validated competitors")

        # Show results
        print("\n📋 CURATION RESULTS:")
        for i, competitor in enumerate(validated_competitors[:10], 1):
            print(f"   {i}. {competitor.company_name}")
            print(f"      Confidence: {competitor.confidence:.3f}")
            print(f"      Quality Score: {competitor.quality_score:.3f}")
            print(f"      Market Overlap: {getattr(competitor, 'market_overlap_pct', 'N/A')}")
            print()

        if len(validated_competitors) > 10:
            print(f"   ... and {len(validated_competitors) - 10} more competitors")

        # Check BigQuery tables created
        print("\n📊 CHECKING BIGQUERY TABLES CREATED:")

        # Save results
        output_file = Path("data/output/stage_tests/stage2_test_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        results = {
            'input_candidates': len(candidates),
            'validated_competitors': len(validated_competitors),
            'competitors': [
                {
                    'company_name': comp.company_name,
                    'confidence': comp.confidence,
                    'quality_score': comp.quality_score,
                    'market_overlap_pct': getattr(comp, 'market_overlap_pct', None)
                } for comp in validated_competitors
            ]
        }

        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\n💾 Results saved to: {output_file}")

    except Exception as e:
        print(f"❌ Stage 2 Failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()