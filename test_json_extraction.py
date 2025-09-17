#!/usr/bin/env python3
"""
Test JSON extraction from Visual Intelligence results
"""
import os
from src.utils.bigquery_client import run_query

def test_json_extraction():
    """Test extracting JSON from AI.GENERATE results"""

    print("🧪 Testing JSON Extraction from Visual Intelligence")
    print("=" * 60)

    # Test extraction with different regex patterns
    query = """
    SELECT
      brand,
      SUBSTR(creative_text, 1, 50) as creative_preview,
      visual_analysis.result as full_result,
      -- Try to extract JSON from markdown blocks
      REGEXP_EXTRACT(visual_analysis.result, r'```json\\s*({[\\s\\S]*?})\\s*```') as extracted_json,
      -- Check if it contains valid JSON structure
      REGEXP_CONTAINS(visual_analysis.result, r'```json') as has_markdown_json,
      REGEXP_CONTAINS(visual_analysis.result, r'"visual_text_alignment_score"') as has_score_field
    FROM `bigquery-ai-kaggle-469620.ads_demo.visual_intelligence_test_visual_20250915`
    WHERE brand = 'Warby Parker'
    LIMIT 3
    """

    try:
        result = run_query(query)

        for idx, row in result.iterrows():
            print(f"\n📊 Ad {idx + 1}:")
            print(f"   Creative: {row['creative_preview']}...")
            print(f"   Has Markdown JSON: {row['has_markdown_json']}")
            print(f"   Has Score Field: {row['has_score_field']}")

            if row['extracted_json']:
                print(f"   Extracted JSON: {row['extracted_json'][:100]}...")

                # Try to parse the JSON
                try:
                    import json
                    parsed = json.loads(row['extracted_json'])
                    print(f"   ✅ JSON PARSED SUCCESSFULLY!")
                    print(f"   Alignment Score: {parsed.get('visual_text_alignment_score')}")
                    print(f"   Brand Score: {parsed.get('brand_consistency_score')}")
                    print(f"   Fatigue Risk: {parsed.get('creative_fatigue_risk')}")
                except Exception as e:
                    print(f"   ❌ JSON parsing failed: {e}")
            else:
                print("   No JSON extracted")
                print(f"   Raw result: {row['full_result'][:200]}...")

        return True

    except Exception as e:
        print(f"❌ Query failed: {e}")
        return False

def test_updated_extraction_sql():
    """Test the updated SQL with proper JSON extraction"""

    print("\n" + "=" * 60)
    print("🔧 Testing Updated SQL with JSON Extraction")
    print("=" * 60)

    # Create a view with proper JSON extraction
    create_view_sql = """
    CREATE OR REPLACE VIEW `bigquery-ai-kaggle-469620.ads_demo.visual_intelligence_with_scores` AS
    WITH extracted_json AS (
      SELECT
        *,
        REGEXP_EXTRACT(visual_analysis.result, r'```json\\s*({[\\s\\S]*?})\\s*```') as clean_json
      FROM `bigquery-ai-kaggle-469620.ads_demo.visual_intelligence_test_visual_20250915`
    )
    SELECT
      brand,
      ad_archive_id,
      creative_text,
      primary_image_url,
      visual_analysis,
      clean_json,
      -- Extract scores from the clean JSON
      CAST(JSON_VALUE(clean_json, '$.visual_text_alignment_score') AS FLOAT64) as visual_text_alignment_score,
      CAST(JSON_VALUE(clean_json, '$.brand_consistency_score') AS FLOAT64) as brand_consistency_score,
      CAST(JSON_VALUE(clean_json, '$.creative_fatigue_risk') AS FLOAT64) as creative_fatigue_risk,
      JSON_VALUE(clean_json, '$.messaging_tone') as messaging_tone,
      JSON_VALUE(clean_json, '$.visual_tone') as visual_tone
    FROM extracted_json
    WHERE clean_json IS NOT NULL
    """

    try:
        print("Creating view with proper JSON extraction...")
        run_query(create_view_sql)
        print("✅ View created successfully")

        # Test the view
        test_query = """
        SELECT
          brand,
          COUNT(*) as total_ads,
          COUNT(visual_text_alignment_score) as ads_with_alignment_score,
          AVG(visual_text_alignment_score) as avg_alignment_score,
          AVG(brand_consistency_score) as avg_brand_score
        FROM `bigquery-ai-kaggle-469620.ads_demo.visual_intelligence_with_scores`
        GROUP BY brand
        ORDER BY total_ads DESC
        """

        result = run_query(test_query)

        print("\n📊 EXTRACTION RESULTS:")
        for idx, row in result.iterrows():
            print(f"   {row['brand']:<20} | Total: {row['total_ads']:>2} | With Scores: {row['ads_with_alignment_score']:>2} | Avg Alignment: {row['avg_alignment_score']:>4.2f} | Avg Brand: {row['avg_brand_score']:>4.2f}")

        return True

    except Exception as e:
        print(f"❌ View creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success1 = test_json_extraction()
    if success1:
        success2 = test_updated_extraction_sql()
    else:
        print("Skipping view test due to extraction test failure")