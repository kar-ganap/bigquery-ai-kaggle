#!/usr/bin/env python3
"""
Test parallel approaches for whitespace analysis without sampling
"""

import time
import concurrent.futures
from typing import List, Dict
from src.utils.bigquery_client import run_query

def parallel_brand_analysis(brand: str, competitors: List[str], run_id: str) -> Dict:
    """Analyze whitespace for a single brand in parallel"""

    print(f"   🔍 Analyzing {brand}...")
    start_time = time.time()

    # Single-brand optimized SQL with full data
    brand_sql = f"""
    WITH brand_ads AS (
      SELECT
        brand,
        ad_archive_id,
        creative_text,
        publisher_platforms,
        DATE(start_timestamp) as campaign_date,
        AI.GENERATE(
          CONCAT(
            'Analyze this ', brand, ' eyewear ad and return JSON with 3 classifications: ',
            '{{"messaging_angle": "EMOTIONAL|FUNCTIONAL|ASPIRATIONAL|SOCIAL_PROOF|PROBLEM_SOLUTION", ',
            '"funnel_stage": "AWARENESS|CONSIDERATION|DECISION|RETENTION", ',
            '"target_persona": "2-3 words like Young Professionals"}}. ',
            'Ad text: "', SUBSTR(creative_text, 1, 200), '"'
          ),
          connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai',
          model_params => JSON_OBJECT(
            'temperature', 0.1,
            'max_output_tokens', 150
          )
        ).result as classification_json
      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
      WHERE brand = '{brand}'
        AND creative_text IS NOT NULL
        AND LENGTH(creative_text) > 20
        AND DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    ),
    parsed_classifications AS (
      SELECT
        brand,
        ad_archive_id,
        campaign_date,
        -- Extract from JSON (simplified parsing)
        REGEXP_EXTRACT(classification_json, r'"messaging_angle":\\s*"([^"]+)"') as messaging_angle,
        REGEXP_EXTRACT(classification_json, r'"funnel_stage":\\s*"([^"]+)"') as funnel_stage,
        REGEXP_EXTRACT(classification_json, r'"target_persona":\\s*"([^"]+)"') as target_persona
      FROM brand_ads
      WHERE classification_json IS NOT NULL
    )
    SELECT
      '{brand}' as brand,
      messaging_angle,
      funnel_stage,
      target_persona,
      COUNT(*) as ad_count,
      COUNT(CASE WHEN campaign_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN 1 END) as recent_ads
    FROM parsed_classifications
    WHERE messaging_angle IS NOT NULL
      AND funnel_stage IS NOT NULL
      AND target_persona IS NOT NULL
    GROUP BY messaging_angle, funnel_stage, target_persona
    ORDER BY ad_count DESC
    """

    try:
        result = run_query(brand_sql)
        duration = time.time() - start_time

        return {
            'brand': brand,
            'success': True,
            'duration': duration,
            'positions': len(result) if result is not None else 0,
            'data': result
        }
    except Exception as e:
        duration = time.time() - start_time
        return {
            'brand': brand,
            'success': False,
            'duration': duration,
            'error': str(e),
            'data': None
        }

def test_parallel_brand_processing():
    """Test parallel processing of brands for whitespace analysis"""

    print("🧪 Testing Parallel Brand Processing for Whitespace Analysis")
    print("=" * 70)

    brands = ['Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
    run_id = "stage4_test"

    print(f"🎯 Processing {len(brands)} brands in parallel")
    print(f"📊 Using full dataset (no sampling)")
    print(f"⚡ Each brand analyzed independently")

    # Test 1: Sequential processing (baseline)
    print(f"\n1️⃣ Sequential Processing (Baseline)")
    print("-" * 40)

    sequential_start = time.time()
    sequential_results = []

    for brand in brands[:2]:  # Test with 2 brands first
        result = parallel_brand_analysis(brand, brands, run_id)
        sequential_results.append(result)
        print(f"   {brand}: {result['duration']:.1f}s ({'✅' if result['success'] else '❌'})")

    sequential_duration = time.time() - sequential_start
    print(f"   Total sequential time: {sequential_duration:.1f}s")

    # Test 2: Parallel processing
    print(f"\n2️⃣ Parallel Processing")
    print("-" * 40)

    parallel_start = time.time()
    parallel_results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Submit jobs for first 2 brands
        future_to_brand = {
            executor.submit(parallel_brand_analysis, brand, brands, run_id): brand
            for brand in brands[:2]
        }

        for future in concurrent.futures.as_completed(future_to_brand):
            brand = future_to_brand[future]
            try:
                result = future.result()
                parallel_results.append(result)
                print(f"   {brand}: {result['duration']:.1f}s ({'✅' if result['success'] else '❌'})")
            except Exception as e:
                print(f"   {brand}: Failed - {e}")

    parallel_duration = time.time() - parallel_start
    print(f"   Total parallel time: {parallel_duration:.1f}s")

    # Performance comparison
    if sequential_duration > 0 and parallel_duration > 0:
        speedup = sequential_duration / parallel_duration
        print(f"\n📊 Performance Improvement: {speedup:.1f}x faster")

        if speedup > 1.5:
            print(f"✅ Significant parallel speedup achieved!")
        else:
            print(f"⚠️  Limited parallel benefit (BigQuery may be bottleneck)")

    return parallel_results

def test_chunked_processing():
    """Test processing ads in chunks for better parallelization"""

    print(f"\n" + "=" * 70)
    print("🧪 Testing Chunked Processing Approach")
    print("=" * 70)

    # Strategy: Process ads in time-based chunks
    chunk_sql = """
    WITH time_chunks AS (
      SELECT
        'recent' as chunk_name,
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) as start_date,
        CURRENT_DATE() as end_date
      UNION ALL
      SELECT
        'medium_term' as chunk_name,
        DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) as start_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) as end_date
    ),
    chunked_analysis AS (
      SELECT
        tc.chunk_name,
        r.brand,
        COUNT(*) as ads_in_chunk,
        -- Batch AI.GENERATE for chunk
        AI.GENERATE(
          CONCAT(
            'Analyze these ', COUNT(*), ' eyewear ads from ', r.brand,
            ' and identify the top 3 messaging patterns. Return JSON: ',
            '{"patterns": [{"messaging_angle": "...", "frequency": "HIGH|MEDIUM|LOW"}]}. ',
            'Ads: ', STRING_AGG(SUBSTR(r.creative_text, 1, 100), ' | ' LIMIT 20)
          ),
          connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
        ).result as chunk_patterns
      FROM time_chunks tc
      JOIN `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r
        ON DATE(r.start_timestamp) BETWEEN tc.start_date AND tc.end_date
      WHERE r.brand IN ('Warby Parker', 'LensCrafters')
        AND r.creative_text IS NOT NULL
      GROUP BY tc.chunk_name, r.brand
      HAVING COUNT(*) >= 3
    )
    SELECT * FROM chunked_analysis
    """

    print(f"🔍 Testing time-based chunking approach...")

    try:
        start_time = time.time()
        result = run_query(chunk_sql)
        duration = time.time() - start_time

        print(f"✅ Chunked analysis: {duration:.1f}s")
        print(f"📊 Results: {result.shape if result is not None else 'None'}")

        if result is not None and not result.empty:
            for _, row in result.iterrows():
                chunk = row['chunk_name']
                brand = row['brand']
                ads = row['ads_in_chunk']
                print(f"   {chunk} - {brand}: {ads} ads processed")

        return True, duration

    except Exception as e:
        print(f"❌ Chunked analysis failed: {e}")
        return False, float('inf')

def main():
    """Main test function"""

    print("🎯 GOAL: Full whitespace analysis without sampling")
    print("🚀 APPROACH: Parallel processing to maintain coverage")
    print("=" * 70)

    # Test parallel brand processing
    parallel_results = test_parallel_brand_processing()

    # Test chunked processing
    chunked_success, chunked_time = test_chunked_processing()

    # Final recommendations
    print(f"\n🎯 FINAL RECOMMENDATIONS")
    print("=" * 40)

    successful_parallel = sum(1 for r in parallel_results if r['success'])

    if successful_parallel > 0:
        avg_parallel_time = sum(r['duration'] for r in parallel_results if r['success']) / successful_parallel
        estimated_full_time = avg_parallel_time  # Parallel execution

        print(f"✅ Parallel brand processing: ~{estimated_full_time:.1f}s per brand")
        print(f"📊 Estimated 5 brands: ~{estimated_full_time:.1f}s (parallel)")

        if estimated_full_time <= 60:
            print(f"✅ RECOMMENDED: Use parallel brand processing")
        else:
            print(f"⚠️  Parallel helps but still slow")

    if chunked_success and chunked_time <= 30:
        print(f"✅ ALTERNATIVE: Use chunked processing ({chunked_time:.1f}s)")

    print(f"\n💡 Key insight: Avoid sampling - use parallelization instead")

if __name__ == "__main__":
    main()