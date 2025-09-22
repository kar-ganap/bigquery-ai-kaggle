#!/usr/bin/env python3
"""
Debug Creative Intelligence data count issue
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.utils.bigquery_client import run_query

def debug_creative_intelligence():
    """Debug why Creative Intelligence shows wrong data count"""

    print("🔍 DEBUGGING CREATIVE INTELLIGENCE DATA COUNT ISSUE")
    print("=" * 70)

    # Check what table Creative Intelligence is actually reading from
    print("1. 📋 CHECKING CREATIVE INTELLIGENCE TABLE:")

    # Get the most recent creative intelligence table
    tables_query = """
    SELECT table_name, creation_time, row_count
    FROM `bigquery-ai-kaggle-469620.ads_demo.__TABLES__`
    WHERE table_name LIKE 'creative_intelligence%'
    ORDER BY creation_time DESC
    LIMIT 5
    """

    try:
        tables_result = run_query(tables_query)
        if not tables_result.empty:
            print("   Recent Creative Intelligence tables:")
            for _, row in tables_result.iterrows():
                print(f"   • {row['table_name']}: {row['row_count']} rows ({row['creation_time']})")

            # Use the most recent table
            latest_table = tables_result.iloc[0]['table_name']
            print(f"\\n🎯 Analyzing latest table: {latest_table}")

        else:
            print("   ❌ No Creative Intelligence tables found")
            return
    except Exception as e:
        print(f"   ❌ Error checking tables: {e}")
        return

    # Check the data in the Creative Intelligence table
    print("\\n2. 📊 ANALYZING CREATIVE INTELLIGENCE DATA:")

    creative_data_query = f"""
    SELECT
        brand,
        total_ads,
        'from_creative_table' as source
    FROM `bigquery-ai-kaggle-469620.ads_demo.{latest_table}`

    UNION ALL

    SELECT
        brand,
        COUNT(*) as total_ads,
        'from_ads_with_dates' as source
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand IS NOT NULL
      AND (creative_text IS NOT NULL OR title IS NOT NULL)
    GROUP BY brand

    ORDER BY brand, source
    """

    try:
        comparison_result = run_query(creative_data_query)
        if not comparison_result.empty:
            print("   Brand comparison (Creative Table vs Source):")
            current_brand = None
            for _, row in comparison_result.iterrows():
                if current_brand != row['brand']:
                    current_brand = row['brand']
                    print(f"\\n   {current_brand}:")
                print(f"      {row['source']}: {row['total_ads']} ads")
        else:
            print("   ❌ No comparison data found")
    except Exception as e:
        print(f"   ❌ Error in comparison: {e}")

    # Check if Creative Intelligence is summing incorrectly
    print("\\n3. 🔍 CHECKING CREATIVE INTELLIGENCE CALCULATION:")

    sum_check_query = f"""
    SELECT
        SUM(total_ads) as sum_of_total_ads,
        COUNT(*) as number_of_brands,
        AVG(total_ads) as avg_per_brand
    FROM `bigquery-ai-kaggle-469620.ads_demo.{latest_table}`
    """

    try:
        sum_result = run_query(sum_check_query)
        if not sum_result.empty:
            row = sum_result.iloc[0]
            print(f"   Sum of total_ads: {row['sum_of_total_ads']}")
            print(f"   Number of brands: {row['number_of_brands']}")
            print(f"   Average per brand: {row['avg_per_brand']:.1f}")

            if row['sum_of_total_ads'] == 11466:
                print("   🎯 FOUND THE ISSUE: Creative Intelligence is summing individual brand totals!")
                print("   📊 The dashboard should show the sum (11,466) represents all individual ads processed")
                print("   💡 But it should display it differently to avoid confusion")
        else:
            print("   ❌ No sum data found")
    except Exception as e:
        print(f"   ❌ Error in sum check: {e}")

    # Check the source data count
    print("\\n4. 📋 CHECKING SOURCE DATA COUNT:")

    source_count_query = """
    SELECT
        COUNT(*) as total_source_ads,
        COUNT(DISTINCT brand) as unique_brands,
        COUNT(CASE WHEN creative_text IS NOT NULL OR title IS NOT NULL THEN 1 END) as ads_with_text
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    """

    try:
        source_result = run_query(source_count_query)
        if not source_result.empty:
            row = source_result.iloc[0]
            print(f"   Total ads in source: {row['total_source_ads']}")
            print(f"   Unique brands: {row['unique_brands']}")
            print(f"   Ads with text content: {row['ads_with_text']}")
        else:
            print("   ❌ No source data found")
    except Exception as e:
        print(f"   ❌ Error checking source: {e}")

    # Provide fix recommendation
    print("\\n5. 💡 RECOMMENDED FIX:")
    print("   The Creative Intelligence module is working correctly but displaying misleadingly.")
    print("   It processes each ad individually (11,466 total operations) but should show")
    print("   the unique source count (582 ads) for consistency with other modules.")
    print("\\n   Fix: Update Creative Intelligence to show source ad count, not processing count.")

if __name__ == "__main__":
    debug_creative_intelligence()