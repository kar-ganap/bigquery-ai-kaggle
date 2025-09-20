#!/usr/bin/env python3
"""
Simple test of Visual Intelligence with direct ad selection
"""
import os
from src.utils.bigquery_client import run_query

def test_simple_visual_intelligence():
    """Test Visual Intelligence with a direct, simple query"""

    print("🎨 Testing Simple Visual Intelligence")
    print("=" * 50)

    # Simple query to analyze just a few ads directly
    simple_sql = """
    WITH sample_ads AS (
      SELECT
        brand,
        ad_archive_id,
        creative_text,
        image_urls[OFFSET(0)] as primary_image_url,
        ARRAY_LENGTH(image_urls) as image_count
      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
      WHERE brand = 'Warby Parker'
        AND image_urls IS NOT NULL
        AND ARRAY_LENGTH(image_urls) > 0
        AND image_urls[OFFSET(0)] IS NOT NULL
        AND LENGTH(creative_text) > 20
      LIMIT 2
    ),
    visual_analyzed AS (
      SELECT
        *,
        AI.GENERATE(
          CONCAT(
            'Analyze this eyewear ad for visual-text alignment. ',
            'TEXT: "', SUBSTR(creative_text, 1, 200), '". ',
            'Provide JSON with: visual_text_alignment_score (0-1), brand_consistency_score (0-1), key_insights (string)'
          ),
          connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
        ) as visual_analysis
      FROM sample_ads
    )
    SELECT
      brand,
      ad_archive_id,
      SUBSTR(creative_text, 1, 100) as creative_preview,
      primary_image_url,
      visual_analysis.result as analysis_result,
      visual_analysis.status as analysis_status
    FROM visual_analyzed
    """

    print("📝 Testing direct visual analysis:")
    print("   - 2 Warby Parker ads with images")
    print("   - Simple AI.GENERATE call")
    print("   - Direct connection to gemini-connection")

    try:
        print("\n🚀 Executing simplified visual intelligence...")
        result = run_query(simple_sql)

        if len(result) > 0:
            print(f"✅ SUCCESS! Analyzed {len(result)} ads")
            for idx, row in result.iterrows():
                print(f"\n📊 Ad {idx + 1}:")
                print(f"   Brand: {row['brand']}")
                print(f"   Creative: {row['creative_preview']}...")
                print(f"   Image URL: {row['primary_image_url'][:60] if row['primary_image_url'] else 'None'}...")
                print(f"   Status: {row['analysis_status']}")
                print(f"   Result: {str(row['analysis_result'])[:100] if row['analysis_result'] else 'None'}...")
        else:
            print("❌ No results returned")

        return True

    except Exception as e:
        print(f"❌ Simple visual intelligence failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_simple_visual_intelligence()