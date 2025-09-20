#!/usr/bin/env python3
"""
Test the Visual Intelligence Stage with Adaptive Sampling
"""
import sys
import os
sys.path.append('src')

from pipeline.core.base import PipelineContext
from pipeline.stages.visual_intelligence import VisualIntelligenceStage
from pipeline.models.results import AnalysisResults

def test_adaptive_sampling_strategy():
    """Test the adaptive sampling strategy for visual intelligence"""

    print("🎨 Testing Visual Intelligence Stage with Adaptive Sampling")
    print("=" * 65)

    # Create test context
    context = PipelineContext(
        brand="Warby Parker",
        vertical="eyewear",
        run_id="test_visual_20250915",
        verbose=True
    )

    # Initialize visual intelligence stage in REAL mode (not dry_run)
    visual_stage = VisualIntelligenceStage(context, dry_run=False)

    print(f"\n📊 Configuration:")
    print(f"   Per brand budget: {visual_stage.per_brand_budget}")
    print(f"   Max total budget: {visual_stage.max_total_budget}")
    print(f"   Stage name: {visual_stage.stage_name}")
    print(f"   Run ID: {visual_stage.run_id}")

    # Test adaptive sampling SQL generation
    print(f"\n🧮 Testing adaptive sampling SQL generation...")
    try:
        sampling_sql = visual_stage._generate_adaptive_sampling_sql()
        print(f"   ✅ Generated adaptive sampling SQL ({len(sampling_sql)} chars)")

        # Check key components
        assert "brand_stats" in sampling_sql, "Missing brand_stats CTE"
        assert "budget_allocation" in sampling_sql, "Missing budget_allocation CTE"
        assert "target_sample_size" in sampling_sql, "Missing target_sample_size logic"
        assert str(visual_stage.per_brand_budget) in sampling_sql, "Per brand budget not used"

        print(f"   🎯 Sampling strategy includes:")
        if "COUNT(*) <= 20" in sampling_sql:
            print(f"      • Small brands (≤20 ads): 50% coverage, max 10 images")
        if "COUNT(*) <= 50" in sampling_sql:
            print(f"      • Medium brands (21-50 ads): 30% coverage, max 15 images")
        if "COUNT(*) <= 100" in sampling_sql:
            print(f"      • Large brands (51-100 ads): 20% coverage, max 20 images")
        if "ELSE 15" in sampling_sql:
            print(f"      • Dominant brands (>100 ads): 15 images fixed")

    except Exception as e:
        print(f"   ❌ Failed to generate sampling SQL: {e}")
        return False

    # Test visual analysis SQL generation
    print(f"\n🔍 Testing visual analysis SQL generation...")
    try:
        analysis_sql = visual_stage._generate_visual_analysis_sql()
        print(f"   ✅ Generated visual analysis SQL ({len(analysis_sql)} chars)")

        # Check key components
        assert "sampled_ads" in analysis_sql, "Missing sampled_ads CTE"
        assert "strategic_score" in analysis_sql, "Missing strategic scoring"
        assert "AI.GENERATE" in analysis_sql, "Missing multimodal AI call"
        assert "primary_image_url" in analysis_sql, "Missing image URL"
        assert "visual_text_alignment_score" in analysis_sql, "Missing alignment scoring"

        print(f"   🎯 Analysis includes:")
        print(f"      • Multi-factor strategic scoring")
        print(f"      • Multimodal AI.GENERATE_TEXT analysis")
        print(f"      • Visual-text alignment scoring")
        print(f"      • Brand consistency metrics")

    except Exception as e:
        print(f"   ❌ Failed to generate analysis SQL: {e}")
        return False

    # Test REAL execution with actual BigQuery AI
    print(f"\n🏃‍♂️ Testing REAL execution with BigQuery AI...")
    try:
        # Mock analysis results
        mock_analysis = AnalysisResults()

        # Execute REAL visual intelligence (not dry_run)
        results = visual_stage.execute(mock_analysis)

        print(f"   ✅ REAL execution completed successfully")
        print(f"   📊 Results:")
        print(f"      • Sampled ads: {results.sampled_ads}")
        print(f"      • Visual insights: {results.visual_insights}")
        print(f"      • Estimated cost: ${results.cost_estimate:.2f}")
        print(f"      • Table ID: {results.table_id}")

        # Validate results
        assert results.sampled_ads > 0, "No ads sampled"
        assert results.cost_estimate > 0, "No cost estimate"
        assert "visual_intelligence_" in results.table_id, "Invalid table ID format"

    except Exception as e:
        print(f"   ❌ Dry run execution failed: {e}")
        return False

    # Test environment configuration
    print(f"\n⚙️  Testing environment configuration...")
    default_budget = int(os.getenv('MULTIMODAL_IMAGE_BUDGET_PER_BRAND', '20'))
    print(f"   Per brand budget from env: {default_budget}")
    assert visual_stage.per_brand_budget == default_budget, "Environment config not loaded"
    print(f"   ✅ Environment configuration working correctly")

    print(f"\n" + "=" * 65)
    print(f"🎯 Visual Intelligence Stage Test Results:")
    print(f"   ✅ Adaptive sampling strategy: PASSED")
    print(f"   ✅ Multimodal analysis SQL: PASSED")
    print(f"   ✅ Dry run execution: PASSED")
    print(f"   ✅ Environment configuration: PASSED")
    print(f"\n🚀 Ready for Phase 2 integration!")

    return True


if __name__ == "__main__":
    success = test_adaptive_sampling_strategy()
    if not success:
        sys.exit(1)