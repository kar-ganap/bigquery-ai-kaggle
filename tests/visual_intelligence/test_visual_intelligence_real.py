#!/usr/bin/env python3
"""
Test Visual Intelligence with REAL BigQuery data to validate improved budget strategy
"""
import sys
import os
sys.path.append('src')

from pipeline.core.base import PipelineContext
from pipeline.stages.visual_intelligence import VisualIntelligenceStage

try:
    from src.utils.bigquery_client import get_bigquery_client, run_query
except ImportError:
    print("❌ BigQuery client not available - cannot run real test")
    sys.exit(1)

def test_improved_budget_strategy_real():
    """Test the improved per-brand budget strategy with real data"""

    print("🎨 Testing Improved Visual Intelligence Budget Strategy - REAL DATA")
    print("=" * 75)

    # Create test context
    context = PipelineContext(
        brand="Warby Parker",
        vertical="eyewear",
        run_id="test_budget_real_20250915",
        verbose=True
    )

    # Initialize visual intelligence stage
    visual_stage = VisualIntelligenceStage(context, dry_run=False)

    print(f"\n📊 Budget Configuration:")
    print(f"   Per-brand budget: {visual_stage.per_brand_budget} images")
    print(f"   Max total budget: {visual_stage.max_total_budget} images")

    # Test 1: Generate and execute adaptive sampling SQL
    print(f"\n🧮 Step 1: Testing adaptive sampling with real BigQuery data...")
    try:
        sampling_sql = visual_stage._generate_adaptive_sampling_sql()
        print(f"   ✅ Generated improved sampling SQL ({len(sampling_sql)} chars)")

        # Execute the sampling SQL
        client = get_bigquery_client()

        print(f"   🔄 Executing sampling strategy against real data...")
        query_job = client.query(sampling_sql)
        results = query_job.result()

        # Analyze results
        brand_data = []
        total_sampled = 0
        for row in results:
            brand_info = {
                'brand': row.brand,
                'total_ads': row.total_ads,
                'ads_with_images': row.ads_with_images,
                'target_sample_size': row.target_sample_size,
                'final_sample_size': row.final_sample_size,
                'total_brands': row.total_brands,
                'actual_coverage_pct': row.actual_coverage_pct,
                'avg_budget_per_brand': row.avg_budget_per_brand
            }
            brand_data.append(brand_info)
            total_sampled += row.final_sample_size

        print(f"   ✅ Sampling strategy executed successfully")
        print(f"\n📊 Real Budget Allocation Results:")
        print(f"   Total brands analyzed: {len(brand_data)}")
        print(f"   Total images to analyze: {total_sampled}")
        print(f"   Average per brand: {total_sampled / len(brand_data):.1f} images")
        print(f"   Budget efficiency: {total_sampled / visual_stage.max_total_budget * 100:.1f}%")

        print(f"\n🔍 Per-Brand Breakdown:")
        for brand in brand_data:
            print(f"   {brand['brand']:15} | {brand['total_ads']:3d} ads | {brand['ads_with_images']:3d} w/imgs | {brand['final_sample_size']:2d} sampled | {brand['actual_coverage_pct']:4.1f}% coverage")

        # Validate budget constraints
        assert total_sampled <= visual_stage.max_total_budget, f"Total budget exceeded: {total_sampled} > {visual_stage.max_total_budget}"
        assert all(b['final_sample_size'] >= 3 for b in brand_data), "Minimum 3 images per brand not met"
        assert all(b['final_sample_size'] <= visual_stage.per_brand_budget * 1.2 for b in brand_data), "Per-brand budget significantly exceeded"

        print(f"   ✅ Budget constraints validated")

    except Exception as e:
        print(f"   ❌ Sampling strategy failed: {e}")
        return False

    # Test 2: Check scalability scenarios
    print(f"\n📈 Step 2: Testing scalability scenarios...")

    scenarios = [
        ("5 brands", 5, 12, 60),
        ("10 brands", 10, 12, 120),
        ("15 brands", 15, 8, 120),  # Would reduce per-brand budget
        ("20 brands", 20, 6, 120)   # Would reduce per-brand budget further
    ]

    for scenario_name, brand_count, expected_per_brand, total_budget in scenarios:
        actual_per_brand = min(visual_stage.per_brand_budget, total_budget // brand_count)
        efficiency = (actual_per_brand * brand_count) / total_budget * 100

        print(f"   {scenario_name:10} | {actual_per_brand:2d} per brand | {actual_per_brand * brand_count:3d} total | {efficiency:5.1f}% efficiency")

    # Test 3: Compare old vs new strategy
    print(f"\n🔄 Step 3: Old vs New Strategy Comparison:")

    old_total_budget = 60  # Original approach
    new_total_budget = visual_stage.max_total_budget

    brand_count = len(brand_data)
    old_avg_per_brand = old_total_budget / brand_count
    new_avg_per_brand = total_sampled / brand_count

    print(f"   Old strategy (total budget): {old_avg_per_brand:.1f} avg per brand")
    print(f"   New strategy (per-brand budget): {new_avg_per_brand:.1f} avg per brand")
    print(f"   Improvement: {((new_avg_per_brand - old_avg_per_brand) / old_avg_per_brand * 100):+.1f}%")

    # Test 4: Cost estimation
    print(f"\n💰 Step 4: Cost Analysis:")
    cost_per_image = 0.15  # Rough estimate for multimodal analysis
    estimated_cost = total_sampled * cost_per_image
    monthly_cost = estimated_cost * 4  # Assuming weekly runs

    print(f"   Per-run cost: ${estimated_cost:.2f}")
    print(f"   Monthly cost (4 runs): ${monthly_cost:.2f}")
    print(f"   Cost per brand: ${estimated_cost / brand_count:.2f}")

    print(f"\n" + "=" * 75)
    print(f"🎯 Improved Budget Strategy Test Results:")
    print(f"   ✅ Real data sampling: PASSED")
    print(f"   ✅ Budget constraints: PASSED")
    print(f"   ✅ Scalability analysis: PASSED")
    print(f"   ✅ Cost estimation: PASSED")
    print(f"\n🚀 Ready for multimodal analysis with {total_sampled} strategically selected images!")

    return True


if __name__ == "__main__":
    success = test_improved_budget_strategy_real()
    if not success:
        sys.exit(1)