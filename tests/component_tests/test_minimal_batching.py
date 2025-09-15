#!/usr/bin/env python3
"""
Test minimal batching approach to isolate AI.GENERATE performance bottleneck
"""

import time
from src.utils.bigquery_client import run_query

def test_minimal_ai_generate_batching():
    """Test the simplest possible batching to understand AI.GENERATE performance"""

    print("🧪 Testing Minimal AI.GENERATE Batching")
    print("=" * 60)

    # Test 1: Single row AI.GENERATE
    print("\n1️⃣ Single Row AI.GENERATE (baseline)")
    print("-" * 40)

    single_sql = """
    SELECT
      brand,
      creative_text,
      AI.GENERATE(
        CONCAT('Classify this eyewear ad messaging angle: "', SUBSTR(creative_text, 1, 100),
               '". Return only: EMOTIONAL, FUNCTIONAL, ASPIRATIONAL, SOCIAL_PROOF, or PROBLEM_SOLUTION'),
        connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
      ) as messaging_angle
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE creative_text IS NOT NULL
      AND brand = 'Warby Parker'
    LIMIT 1
    """

    try:
        start_time = time.time()
        result = run_query(single_sql)
        single_duration = time.time() - start_time

        print(f"   ✅ Duration: {single_duration:.1f} seconds")
        print(f"   📊 Result: {result.shape if result is not None else 'None'}")

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        single_duration = float('inf')

    # Test 2: Multiple rows individually (2 rows)
    print("\n2️⃣ Multiple Rows Individual AI.GENERATE (2 rows)")
    print("-" * 40)

    multi_individual_sql = """
    SELECT
      brand,
      creative_text,
      AI.GENERATE(
        CONCAT('Classify this eyewear ad messaging angle: "', SUBSTR(creative_text, 1, 100),
               '". Return only: EMOTIONAL, FUNCTIONAL, ASPIRATIONAL, SOCIAL_PROOF, or PROBLEM_SOLUTION'),
        connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
      ) as messaging_angle
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE creative_text IS NOT NULL
      AND brand = 'Warby Parker'
    LIMIT 2
    """

    try:
        start_time = time.time()
        result = run_query(multi_individual_sql)
        multi_duration = time.time() - start_time

        print(f"   ✅ Duration: {multi_duration:.1f} seconds")
        print(f"   📊 Result: {result.shape if result is not None else 'None'}")
        print(f"   📈 Per-row time: {multi_duration/2:.1f} seconds")

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        multi_duration = float('inf')

    # Test 3: Batched approach (multiple ads in single AI.GENERATE)
    print("\n3️⃣ Batched AI.GENERATE (2 ads in 1 call)")
    print("-" * 40)

    batched_sql = """
    WITH ads_batch AS (
      SELECT
        STRING_AGG(
          CONCAT('Ad_', CAST(ROW_NUMBER() OVER() AS STRING), ': "',
                 SUBSTR(creative_text, 1, 100), '"'),
          ' | '
        ) as combined_ads
      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
      WHERE creative_text IS NOT NULL
        AND brand = 'Warby Parker'
      LIMIT 2
    )
    SELECT
      combined_ads,
      AI.GENERATE(
        CONCAT('Classify the messaging angle for each of these eyewear ads. ',
               'Return JSON format like: {"Ad_1": "EMOTIONAL", "Ad_2": "FUNCTIONAL"}. ',
               'Use only: EMOTIONAL, FUNCTIONAL, ASPIRATIONAL, SOCIAL_PROOF, PROBLEM_SOLUTION. ',
               'Ads: ', combined_ads),
        connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai',
        model_params => STRUCT(
          0.1 AS temperature,
          200 AS max_output_tokens,
          TRUE AS flatten_json_output
        )
      ) as batch_result
    FROM ads_batch
    """

    try:
        start_time = time.time()
        result = run_query(batched_sql)
        batched_duration = time.time() - start_time

        print(f"   ✅ Duration: {batched_duration:.1f} seconds")
        print(f"   📊 Result: {result.shape if result is not None else 'None'}")
        print(f"   📈 Per-ad time: {batched_duration/2:.1f} seconds")

        if result is not None and not result.empty:
            print(f"   🔍 Sample result: {str(result.iloc[0]['batch_result'])[:100]}...")

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        batched_duration = float('inf')

    # Performance Analysis
    print(f"\n📊 PERFORMANCE ANALYSIS")
    print("=" * 60)

    if single_duration < float('inf'):
        print(f"Single row: {single_duration:.1f}s")
        estimated_10_rows = single_duration * 10
        print(f"Estimated 10 rows individually: {estimated_10_rows:.1f}s")

    if multi_duration < float('inf'):
        print(f"2 rows individually: {multi_duration:.1f}s ({multi_duration/2:.1f}s per row)")

    if batched_duration < float('inf'):
        print(f"2 rows batched: {batched_duration:.1f}s ({batched_duration/2:.1f}s per row)")

    # Recommendations
    print(f"\n🎯 RECOMMENDATIONS")
    print("-" * 30)

    if single_duration < float('inf') and batched_duration < float('inf'):
        if batched_duration < multi_duration:
            speedup = multi_duration / batched_duration
            print(f"✅ Batching is {speedup:.1f}x faster - USE BATCHED APPROACH")
        else:
            print(f"⚠️  Individual processing is faster - optimize batching logic")
    elif single_duration < float('inf'):
        print(f"⚠️  Individual calls work, batching needs debugging")
        print(f"💡 Consider limiting to {int(60/single_duration)} ads max for 1-minute limit")
    else:
        print(f"❌ AI.GENERATE calls are failing - check connection setup")

if __name__ == "__main__":
    test_minimal_ai_generate_batching()