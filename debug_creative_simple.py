#!/usr/bin/env python3
"""
Simple debug of Creative Intelligence data count issue
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.utils.bigquery_client import run_query

def debug_creative_simple():
    """Simple debug of Creative Intelligence"""

    print("🔍 DEBUGGING CREATIVE INTELLIGENCE - SIMPLE APPROACH")
    print("=" * 60)

    # Find Creative Intelligence tables
    print("1. 📋 FINDING CREATIVE INTELLIGENCE TABLES:")

    tables_query = """
    SELECT table_name
    FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'creative_intelligence%'
    ORDER BY creation_time DESC
    LIMIT 5
    """

    try:
        tables_result = run_query(tables_query)
        if not tables_result.empty:
            print("   Creative Intelligence tables found:")
            for _, row in tables_result.iterrows():
                print(f"   • {row['table_name']}")

            # Use the first (most recent) table
            latest_table = tables_result.iloc[0]['table_name']
            print(f"\\n🎯 Using table: {latest_table}")

        else:
            print("   ❌ No Creative Intelligence tables found")
            return
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return

    # Check what's in the Creative Intelligence table
    print("\\n2. 📊 CHECKING CREATIVE INTELLIGENCE DATA:")

    creative_check_query = f"""
    SELECT
        brand,
        total_ads
    FROM `bigquery-ai-kaggle-469620.ads_demo.{latest_table}`
    ORDER BY brand
    """

    try:
        creative_result = run_query(creative_check_query)
        if not creative_result.empty:
            print("   Creative Intelligence table contents:")
            total_sum = 0
            for _, row in creative_result.iterrows():
                print(f"   • {row['brand']}: {row['total_ads']} ads")
                total_sum += row['total_ads']
            print(f"   📊 SUM: {total_sum} total ads")
        else:
            print("   ❌ No data in Creative Intelligence table")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # Check the source data
    print("\\n3. 📋 CHECKING SOURCE DATA (ads_with_dates):")

    source_check_query = """
    SELECT
        brand,
        COUNT(*) as actual_ads
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand IS NOT NULL
      AND (creative_text IS NOT NULL OR title IS NOT NULL)
    GROUP BY brand
    ORDER BY brand
    """

    try:
        source_result = run_query(source_check_query)
        if not source_result.empty:
            print("   Source data (ads_with_dates):")
            source_total = 0
            for _, row in source_result.iterrows():
                print(f"   • {row['brand']}: {row['actual_ads']} ads")
                source_total += row['actual_ads']
            print(f"   📊 SUM: {source_total} total ads")
        else:
            print("   ❌ No source data found")
    except Exception as e:
        print(f"   ❌ Error: {e}")

    # Compare the numbers
    print("\\n4. 🔍 DIAGNOSIS:")
    if 'total_sum' in locals() and 'source_total' in locals():
        if total_sum != source_total:
            ratio = total_sum / source_total if source_total > 0 else 0
            print(f"   ❌ MISMATCH FOUND:")
            print(f"      Creative Intelligence: {total_sum} ads")
            print(f"      Source (ads_with_dates): {source_total} ads")
            print(f"      Ratio: {ratio:.1f}x")
            print(f"\\n   💡 LIKELY CAUSE:")
            if ratio > 5:
                print(f"      Creative Intelligence is counting processed rows, not source ads")
                print(f"      Each ad gets processed multiple times in the analysis pipeline")
        else:
            print(f"   ✅ Numbers match! No issue found.")
    else:
        print("   ❌ Could not compare numbers - missing data")

    print("\\n5. 💡 RECOMMENDED FIX:")
    print("   Update Creative Intelligence dashboard to show source ad count")
    print("   instead of internal processing count for consistency")

if __name__ == "__main__":
    debug_creative_simple()