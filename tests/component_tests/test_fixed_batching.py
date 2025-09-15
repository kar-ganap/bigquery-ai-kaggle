#!/usr/bin/env python3
"""
Test fixed batching approach with corrected SQL syntax
"""

import time
from src.utils.bigquery_client import run_query

def test_fixed_batching():
    """Test corrected batching SQL to compare with individual approach"""

    print("🧪 Testing Fixed AI.GENERATE Batching")
    print("=" * 60)

    # Test 1: Individual approach (3 rows for comparison)
    print("\n1️⃣ Individual AI.GENERATE (3 rows)")
    print("-" * 40)

    individual_sql = """
    SELECT
      brand,
      ad_archive_id,
      SUBSTR(creative_text, 1, 100) as creative_sample,
      AI.GENERATE(
        CONCAT('Classify this eyewear ad messaging angle: "', SUBSTR(creative_text, 1, 100),
               '". Return only: EMOTIONAL, FUNCTIONAL, ASPIRATIONAL, SOCIAL_PROOF, or PROBLEM_SOLUTION'),
        connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
      ).result as messaging_angle
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE creative_text IS NOT NULL
      AND brand = 'Warby Parker'
    ORDER BY start_timestamp DESC
    LIMIT 3
    """

    try:
        start_time = time.time()
        individual_result = run_query(individual_sql)
        individual_duration = time.time() - start_time

        print(f"   ✅ Duration: {individual_duration:.1f} seconds")
        print(f"   📊 Result: {individual_result.shape if individual_result is not None else 'None'}")
        print(f"   📈 Per-row time: {individual_duration/3:.1f} seconds")

        if individual_result is not None and not individual_result.empty:
            print(f"   🔍 Sample classification: {individual_result.iloc[0]['messaging_angle'][:50]}...")

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        individual_duration = float('inf')

    # Test 2: Fixed batching approach
    print("\n2️⃣ Fixed Batched AI.GENERATE (3 ads in 1 call)")
    print("-" * 40)

    # Fixed SQL without analytic functions in aggregates
    batched_sql = """
    WITH numbered_ads AS (
      SELECT
        brand,
        ad_archive_id,
        creative_text,
        ROW_NUMBER() OVER (ORDER BY start_timestamp DESC) as ad_num
      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
      WHERE creative_text IS NOT NULL
        AND brand = 'Warby Parker'
      ORDER BY start_timestamp DESC
      LIMIT 3
    ),
    combined_ads AS (
      SELECT
        STRING_AGG(
          CONCAT('Ad_', CAST(ad_num AS STRING), ': "',
                 SUBSTR(creative_text, 1, 100), '"'),
          ' | '
          ORDER BY ad_num
        ) as combined_text,
        COUNT(*) as total_ads
      FROM numbered_ads
    )
    SELECT
      combined_text,
      total_ads,
      AI.GENERATE(
        CONCAT(
          'Classify the messaging angle for each eyewear ad. ',
          'Return JSON like: {"Ad_1": "EMOTIONAL", "Ad_2": "FUNCTIONAL", "Ad_3": "ASPIRATIONAL"}. ',
          'Use only: EMOTIONAL, FUNCTIONAL, ASPIRATIONAL, SOCIAL_PROOF, PROBLEM_SOLUTION. ',
          'Ads: ', combined_text
        ),
        connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai',
        model_params => STRUCT(
          0.1 AS temperature,
          300 AS max_output_tokens,
          TRUE AS flatten_json_output
        )
      ).result as batch_classifications
    FROM combined_ads
    """

    try:
        start_time = time.time()
        batched_result = run_query(batched_sql)
        batched_duration = time.time() - start_time

        print(f"   ✅ Duration: {batched_duration:.1f} seconds")
        print(f"   📊 Result: {batched_result.shape if batched_result is not None else 'None'}")
        print(f"   📈 Per-ad time: {batched_duration/3:.1f} seconds")

        if batched_result is not None and not batched_result.empty:
            print(f"   🔍 Batch result: {batched_result.iloc[0]['batch_classifications'][:100]}...")

        batched_success = True

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        batched_duration = float('inf')
        batched_success = False

    # Performance Comparison
    print(f"\n📊 PERFORMANCE COMPARISON")
    print("=" * 60)

    if individual_duration < float('inf') and batched_duration < float('inf'):
        speedup = individual_duration / batched_duration
        individual_per_row = individual_duration / 3
        batched_per_row = batched_duration / 3

        print(f"Individual approach: {individual_duration:.1f}s ({individual_per_row:.1f}s per row)")
        print(f"Batched approach: {batched_duration:.1f}s ({batched_per_row:.1f}s per row)")
        print(f"Speedup: {speedup:.1f}x")

        # Estimate for larger datasets
        print(f"\n🔮 Projections for 50 ads:")
        individual_50 = individual_per_row * 50
        batched_50 = (batched_duration / 3) * (50 / 10)  # Estimate 10 ads per batch
        print(f"Individual: ~{individual_50:.1f} seconds")
        print(f"Batched (10 ads/batch): ~{batched_50:.1f} seconds")

        if batched_50 < individual_50:
            improvement = individual_50 / batched_50
            print(f"✅ Batching would be {improvement:.1f}x faster for 50 ads")
        else:
            print(f"⚠️  Individual might still be competitive")

    elif individual_duration < float('inf'):
        print(f"✅ Individual approach works: {individual_duration:.1f}s")
        print(f"❌ Batched approach needs more work")

        # Calculate reasonable limits
        max_ads_1min = int(60 / (individual_duration / 3))
        max_ads_2min = int(120 / (individual_duration / 3))
        print(f"💡 With individual calls:")
        print(f"   - Max ~{max_ads_1min} ads in 1 minute")
        print(f"   - Max ~{max_ads_2min} ads in 2 minutes")

    # Recommendations
    print(f"\n🎯 IMPLEMENTATION RECOMMENDATIONS")
    print("-" * 40)

    if batched_success and batched_duration < individual_duration:
        print(f"✅ USE BATCHED APPROACH")
        print(f"   - Process 10-20 ads per AI.GENERATE call")
        print(f"   - Expected performance: {speedup:.1f}x improvement")
        print(f"   - Implement JSON parsing for batch results")
    else:
        print(f"✅ USE INDIVIDUAL APPROACH WITH LIMITS")
        print(f"   - Limit to 30-50 ads for reasonable performance")
        print(f"   - Expected time: ~{(individual_duration/3)*50:.0f}s for 50 ads")
        print(f"   - Simple implementation, reliable results")

if __name__ == "__main__":
    test_fixed_batching()