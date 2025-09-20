#!/usr/bin/env python3
"""
Debug Visual Intelligence sampling strategy
"""
import os
from src.utils.bigquery_client import get_bigquery_client, run_query

BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

def debug_visual_intelligence():
    """Debug the Visual Intelligence sampling strategy creation"""

    print("🔍 DEBUGGING Visual Intelligence Sampling Strategy")
    print("=" * 60)

    try:
        client = get_bigquery_client()

        # First, check if ads_with_dates table exists
        print("1️⃣ Checking if ads_with_dates table exists...")
        tables = client.list_tables(f"{BQ_PROJECT}.{BQ_DATASET}")
        table_names = [table.table_id for table in tables]

        if "ads_with_dates" in table_names:
            print("   ✅ ads_with_dates table exists")

            # Check row count
            count_query = f"SELECT COUNT(*) as count FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`"
            count_result = run_query(count_query)
            count_val = count_result.iloc[0]['count']
            print(f"   📊 Row count: {count_val}")

            # Check for image URLs
            image_query = f"""
            SELECT
                COUNT(*) as total_ads,
                COUNT(CASE WHEN image_urls IS NOT NULL AND ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as ads_with_images
            FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
            """
            image_result = run_query(image_query)
            total_ads = image_result.iloc[0]['total_ads']
            ads_with_images = image_result.iloc[0]['ads_with_images']
            print(f"   🖼️  Total ads: {total_ads}, Ads with images: {ads_with_images}")
            if ads_with_images == 0:
                print("   ⚠️  NO IMAGES FOUND - Visual Intelligence will have no data to analyze")

        else:
            print("   ❌ ads_with_dates table does NOT exist")
            print(f"   📋 Available tables: {table_names}")
            return False

        # Now test the sampling strategy SQL
        print("\n2️⃣ Testing sampling strategy SQL...")

        per_brand_budget = 20
        max_total_budget = 200

        sampling_sql = f"""
        -- Improved Visual Intelligence Sampling Strategy
        -- Per-brand budget: {per_brand_budget} images per brand
        -- Max total budget: {max_total_budget} images across all competitors

        CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.visual_sampling_strategy` AS
        WITH brand_stats AS (
          SELECT
            brand,
            COUNT(*) as total_ads,
            COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as ads_with_images,
            -- Calculate adaptive sample size based on brand portfolio
            CASE
              WHEN COUNT(*) <= 20 THEN LEAST(CAST(COUNT(*) * 0.5 AS INT64), {per_brand_budget})  -- 50% coverage, max per-brand budget
              WHEN COUNT(*) <= 50 THEN LEAST(CAST(COUNT(*) * 0.3 AS INT64), {per_brand_budget})  -- 30% coverage, max per-brand budget
              WHEN COUNT(*) <= 100 THEN LEAST(CAST(COUNT(*) * 0.2 AS INT64), {per_brand_budget}) -- 20% coverage, max per-brand budget
              ELSE LEAST({per_brand_budget}, 15)  -- Fixed per-brand budget, but capped at 15 for very large brands
            END as target_sample_size
          FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
          WHERE image_urls IS NOT NULL
            AND ARRAY_LENGTH(image_urls) > 0
          GROUP BY brand
        ),
        budget_allocation AS (
          SELECT
            *,
            -- Ensure total doesn't exceed maximum budget across all brands
            CASE
              WHEN SUM(target_sample_size) OVER() <= {max_total_budget} THEN target_sample_size
              ELSE GREATEST(
                CAST(target_sample_size * {max_total_budget} / SUM(target_sample_size) OVER() AS INT64),
                3  -- Minimum 3 images per brand for meaningful analysis
              )
            END as final_sample_size,
            COUNT(*) OVER() as total_brands
          FROM brand_stats
        )
        SELECT
          brand,
          total_ads,
          ads_with_images,
          target_sample_size,
          final_sample_size,
          total_brands,
          ROUND(final_sample_size * 100.0 / total_ads, 1) as actual_coverage_pct,
          ROUND({max_total_budget} / total_brands, 1) as avg_budget_per_brand
        FROM budget_allocation
        ORDER BY total_ads DESC
        """

        print("   🔨 Executing sampling strategy creation...")
        sampling_job = client.query(sampling_sql)
        sampling_job.result()  # Wait for completion
        print("   ✅ Sampling strategy table created successfully")

        # Verify the sampling strategy results
        print("\n3️⃣ Verifying sampling strategy results...")
        verify_query = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.visual_sampling_strategy`"
        verify_result = run_query(verify_query)

        total_budget_used = 0
        for _, row in verify_result.iterrows():
            print(f"   📊 {row['brand']}: {row['final_sample_size']} images ({row['actual_coverage_pct']}% coverage)")
            total_budget_used += row['final_sample_size']

        print(f"\n   💰 Total budget used: {total_budget_used} images")

        return True

    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_visual_intelligence()