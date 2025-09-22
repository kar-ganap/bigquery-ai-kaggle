#!/usr/bin/env python3
"""
Investigate data consistency issues in Stage 9 intelligence modules
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.utils.bigquery_client import run_query

def investigate_data_sources():
    """Check what data sources each intelligence module is using"""

    print("🔍 INVESTIGATING DATA CONSISTENCY ISSUES")
    print("=" * 60)

    # Get the run_id from recent runs
    tables_query = """
    SELECT table_name, creation_time
    FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE '%warby_parker%'
    ORDER BY creation_time DESC
    LIMIT 10
    """

    print("📋 Recent tables:")
    tables_result = run_query(tables_query)
    if not tables_result.empty:
        for _, row in tables_result.iterrows():
            print(f"   • {row['table_name']} ({row['creation_time']})")

    # Extract the most recent run_id
    recent_table = tables_result.iloc[0]['table_name'] if not tables_result.empty else None
    if recent_table and '_' in recent_table:
        run_id = recent_table.split('_')[-1]
        print(f"\n🎯 Using run_id: {run_id}")
    else:
        print("❌ Could not determine run_id")
        return

    print(f"\n📊 CHECKING DATA COUNTS FOR RUN: {run_id}")
    print("-" * 50)

    # Check main data source
    main_data_query = f"""
    SELECT
        COUNT(*) as total_ads,
        COUNT(DISTINCT brand) as unique_brands,
        COUNT(CASE WHEN creative_text IS NOT NULL THEN 1 END) as has_creative_text,
        COUNT(CASE WHEN title IS NOT NULL THEN 1 END) as has_title,
        COUNT(CASE WHEN cta_text IS NOT NULL THEN 1 END) as has_cta_text,
        COUNT(CASE WHEN publisher_platforms IS NOT NULL THEN 1 END) as has_publisher_platforms
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw_{run_id}`
    """

    print("1. 📁 Main Data Source (ads_raw):")
    main_result = run_query(main_data_query)
    if not main_result.empty:
        row = main_result.iloc[0]
        print(f"   Total ads: {row['total_ads']}")
        print(f"   Unique brands: {row['unique_brands']}")
        print(f"   Has creative_text: {row['has_creative_text']}")
        print(f"   Has title: {row['has_title']}")
        print(f"   Has cta_text: {row['has_cta_text']}")
        print(f"   Has publisher_platforms: {row['has_publisher_platforms']}")

    # Check intelligence module tables
    intelligence_tables = [
        ('audience_intelligence', 'Audience Intelligence'),
        ('creative_intelligence', 'Creative Intelligence'),
        ('channel_intelligence', 'Channel Intelligence'),
        ('visual_intelligence', 'Visual Intelligence')
    ]

    print(f"\n2. 🧠 Intelligence Module Data Counts:")

    for table_suffix, display_name in intelligence_tables:
        table_name = f"{table_suffix}_{run_id}"

        count_query = f"""
        SELECT COUNT(*) as count, COUNT(DISTINCT brand) as brands
        FROM `bigquery-ai-kaggle-469620.ads_demo.{table_name}`
        """

        try:
            result = run_query(count_query)
            if not result.empty:
                count = result.iloc[0]['count']
                brands = result.iloc[0]['brands']
                print(f"   {display_name}: {count} ads, {brands} brands")
            else:
                print(f"   {display_name}: No data")
        except Exception as e:
            print(f"   {display_name}: Error - {str(e)}")

    # Check for brand filtering issues
    print(f"\n3. 🏷️ Brand Distribution Analysis:")

    brand_dist_query = f"""
    SELECT
        brand,
        COUNT(*) as ad_count
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw_{run_id}`
    GROUP BY brand
    ORDER BY ad_count DESC
    """

    brand_result = run_query(brand_dist_query)
    if not brand_result.empty:
        print("   Brand distribution in main data:")
        for _, row in brand_result.iterrows():
            print(f"     • {row['brand']}: {row['ad_count']} ads")

    # Check what filters are being applied in audience intelligence
    print(f"\n4. 🔍 Filter Analysis - Audience Intelligence:")

    audience_filter_query = f"""
    SELECT
        COUNT(*) as total_in_source,
        COUNT(CASE WHEN brand IN ('Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA') THEN 1 END) as after_brand_filter,
        COUNT(CASE WHEN brand IN ('Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA') AND creative_text IS NOT NULL THEN 1 END) as after_creative_filter
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw_{run_id}`
    """

    filter_result = run_query(audience_filter_query)
    if not filter_result.empty:
        row = filter_result.iloc[0]
        print(f"   Total in source: {row['total_in_source']}")
        print(f"   After brand filter: {row['after_brand_filter']}")
        print(f"   After creative_text filter: {row['after_creative_filter']}")

    print(f"\n5. 🔍 Creative Intelligence Large Count Investigation:")

    # Check if creative intelligence is reading from wrong table or aggregating wrong
    creative_source_query = f"""
    SELECT
        'ads_raw' as source,
        COUNT(*) as count
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw_{run_id}`
    WHERE brand IN ('Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA')

    UNION ALL

    SELECT
        'creative_intelligence' as source,
        SUM(total_ads) as count
    FROM `bigquery-ai-kaggle-469620.ads_demo.creative_intelligence_{run_id}`
    """

    try:
        creative_source_result = run_query(creative_source_query)
        if not creative_source_result.empty:
            print("   Source comparison:")
            for _, row in creative_source_result.iterrows():
                print(f"     • {row['source']}: {row['count']} ads")
    except Exception as e:
        print(f"   Error checking creative intelligence source: {e}")

if __name__ == "__main__":
    investigate_data_sources()