#!/usr/bin/env python3
"""
Fix data consistency investigation with correct run_id
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.utils.bigquery_client import run_query

def investigate_data_consistency():
    """Check data consistency across intelligence modules"""

    print("🔍 INVESTIGATING DATA CONSISTENCY ISSUES")
    print("=" * 60)

    # Get the actual run_id from recent ads_raw tables
    tables_query = """
    SELECT table_name, creation_time
    FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'ads_raw_%'
    ORDER BY creation_time DESC
    LIMIT 5
    """

    print("📋 Recent ads_raw tables:")
    tables_result = run_query(tables_query)
    if not tables_result.empty:
        for _, row in tables_result.iterrows():
            print(f"   • {row['table_name']} ({row['creation_time']})")

    # Extract the correct run_id from the most recent ads_raw table
    recent_ads_table = tables_result.iloc[0]['table_name'] if not tables_result.empty else None
    if recent_ads_table and recent_ads_table.startswith('ads_raw_'):
        run_id = recent_ads_table[8:]  # Remove 'ads_raw_' prefix
        print(f"\n🎯 Using run_id: {run_id}")
    else:
        print("❌ Could not determine run_id")
        return

    print(f"\n📊 DATA CONSISTENCY ANALYSIS FOR: {run_id}")
    print("-" * 60)

    # 1. Check main data source
    main_data_query = f"""
    SELECT
        COUNT(*) as total_ads,
        COUNT(DISTINCT brand) as unique_brands,
        COUNT(CASE WHEN creative_text IS NOT NULL AND LENGTH(TRIM(creative_text)) > 0 THEN 1 END) as has_creative_text,
        COUNT(CASE WHEN title IS NOT NULL AND LENGTH(TRIM(title)) > 0 THEN 1 END) as has_title,
        COUNT(CASE WHEN cta_text IS NOT NULL AND LENGTH(TRIM(cta_text)) > 0 THEN 1 END) as has_cta_text,
        COUNT(CASE WHEN publisher_platforms IS NOT NULL THEN 1 END) as has_publisher_platforms
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw_{run_id}`
    """

    print("1. 📁 MAIN DATA SOURCE (ads_raw):")
    main_result = run_query(main_data_query)
    if not main_result.empty:
        row = main_result.iloc[0]
        print(f"   ✅ Total ads: {row['total_ads']}")
        print(f"   👥 Unique brands: {row['unique_brands']}")
        print(f"   📝 Has creative_text: {row['has_creative_text']}")
        print(f"   📋 Has title: {row['has_title']}")
        print(f"   🎯 Has cta_text: {row['has_cta_text']}")
        print(f"   📱 Has publisher_platforms: {row['has_publisher_platforms']}")

    # 2. Check brand distribution
    print(f"\n2. 🏷️ BRAND DISTRIBUTION:")

    brand_dist_query = f"""
    SELECT
        brand,
        COUNT(*) as ad_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw_{run_id}`
    GROUP BY brand
    ORDER BY ad_count DESC
    """

    brand_result = run_query(brand_dist_query)
    if not brand_result.empty:
        for _, row in brand_result.iterrows():
            print(f"   • {row['brand']}: {row['ad_count']} ads ({row['percentage']}%)")

    # 3. Check intelligence modules
    print(f"\n3. 🧠 INTELLIGENCE MODULE CONSISTENCY:")

    intelligence_checks = [
        ('audience_intelligence', 'WHERE brand IS NOT NULL AND creative_text IS NOT NULL'),
        ('creative_intelligence', 'WHERE (creative_text IS NOT NULL OR title IS NOT NULL)'),
        ('channel_intelligence', 'WHERE brand IS NOT NULL AND publisher_platforms IS NOT NULL')
    ]

    for table_suffix, filter_condition in intelligence_checks:
        table_name = f"{table_suffix}_{run_id}"

        # Check if table exists and get count
        try:
            table_query = f"""
            SELECT
                COUNT(*) as records,
                COUNT(DISTINCT brand) as brands,
                MIN(total_ads) as min_total_ads,
                MAX(total_ads) as max_total_ads
            FROM `bigquery-ai-kaggle-469620.ads_demo.{table_name}`
            """

            result = run_query(table_query)
            if not result.empty:
                row = result.iloc[0]
                print(f"   📊 {table_suffix.replace('_', ' ').title()}:")
                print(f"      Records: {row['records']}")
                print(f"      Brands: {row['brands']}")
                if 'total_ads' in table_name:
                    print(f"      Total ads range: {row['min_total_ads']}-{row['max_total_ads']}")

                # Check what the source filter would produce
                source_check_query = f"""
                SELECT COUNT(*) as source_count
                FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw_{run_id}`
                {filter_condition}
                """

                source_result = run_query(source_check_query)
                if not source_result.empty:
                    source_count = source_result.iloc[0]['source_count']
                    print(f"      Expected from source: {source_count}")

        except Exception as e:
            print(f"   ❌ {table_suffix.replace('_', ' ').title()}: {str(e)}")

    # 4. Check creative intelligence discrepancy
    print(f"\n4. 🔍 CREATIVE INTELLIGENCE DISCREPANCY INVESTIGATION:")

    try:
        creative_detail_query = f"""
        SELECT
            brand,
            total_ads,
            'from_creative_table' as source
        FROM `bigquery-ai-kaggle-469620.ads_demo.creative_intelligence_{run_id}`

        UNION ALL

        SELECT
            brand,
            COUNT(*) as total_ads,
            'from_source_table' as source
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw_{run_id}`
        WHERE brand IN ('Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA')
          AND (creative_text IS NOT NULL OR title IS NOT NULL)
        GROUP BY brand
        ORDER BY brand, source
        """

        creative_detail_result = run_query(creative_detail_query)
        if not creative_detail_result.empty:
            print("   Creative Intelligence vs Source Comparison:")
            for _, row in creative_detail_result.iterrows():
                print(f"   • {row['brand']} ({row['source']}): {row['total_ads']} ads")

    except Exception as e:
        print(f"   ❌ Creative intelligence check failed: {e}")

    # 5. Solution recommendations
    print(f"\n5. 💡 RECOMMENDED FIXES:")
    print("   1. Ensure all intelligence modules use the same brand filter")
    print("   2. Use consistent data quality filters across modules")
    print("   3. Add data validation checks to Stage 9 execution")
    print("   4. Create a unified data preparation step before intelligence analysis")

if __name__ == "__main__":
    investigate_data_consistency()