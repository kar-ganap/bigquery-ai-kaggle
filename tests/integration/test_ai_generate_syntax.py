#!/usr/bin/env python3
"""
Test AI.GENERATE syntax to verify the connection_id format is correct
"""

from src.utils.bigquery_client import run_query

def test_ai_generate_syntax():
    """Test if our AI.GENERATE syntax actually works"""

    print("🧪 Testing AI.GENERATE Syntax")
    print("=" * 60)

    # Test basic AI.GENERATE call with minimal data
    test_sql = """
    SELECT
      'test prompt' as input_text,
      AI.GENERATE(
        'Classify this text as POSITIVE or NEGATIVE',
        connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
      ) as result
    LIMIT 1
    """

    print("📝 Testing basic AI.GENERATE syntax:")
    print(test_sql)

    try:
        print("\n🚀 Executing test query...")
        result = run_query(test_sql)
        print("✅ AI.GENERATE syntax is CORRECT!")
        print(f"   Result type: {type(result)}")
        if hasattr(result, 'shape'):
            print(f"   Result shape: {result.shape}")
        print(f"   First row: {result.iloc[0].to_dict() if not result.empty else 'No data'}")

        return True

    except Exception as e:
        print(f"❌ AI.GENERATE syntax failed: {e}")

        # Check if it's a connection issue vs syntax issue
        if "connection_id" in str(e):
            print("   🔍 This appears to be a connection_id syntax issue")
        elif "Syntax error" in str(e):
            print("   🔍 This appears to be a SQL syntax error")
        elif "connection" in str(e).lower():
            print("   🔍 This appears to be a connection setup issue")
        else:
            print("   🔍 This appears to be a different issue")

        import traceback
        traceback.print_exc()
        return False

def test_simplified_whitespace_sql():
    """Test a simplified version of the whitespace SQL to isolate the issue"""

    print("\n" + "=" * 60)
    print("🧪 Testing Simplified Whitespace SQL")
    print("=" * 60)

    # Just test the first AI.GENERATE call without complex structure
    simplified_sql = """
    SELECT
      brand,
      creative_text,
      AI.GENERATE(
        CONCAT(
          'Analyze this eyewear ad and classify the primary messaging angle. ',
          'Text: "', SUBSTR(creative_text, 1, 100), '". ',
          'Return ONLY one of: EMOTIONAL, FUNCTIONAL, ASPIRATIONAL, SOCIAL_PROOF, PROBLEM_SOLUTION'
        ),
        connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
      ) as messaging_angle_raw
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE creative_text IS NOT NULL
      AND LENGTH(creative_text) > 20
      AND brand = 'Warby Parker'
    LIMIT 2
    """

    print("📝 Testing simplified whitespace SQL:")
    print(simplified_sql[:400] + "...")

    try:
        print("\n🚀 Executing simplified query...")
        result = run_query(simplified_sql)
        print("✅ Simplified whitespace SQL works!")
        print(f"   Result shape: {result.shape if hasattr(result, 'shape') else 'Unknown'}")
        return True

    except Exception as e:
        print(f"❌ Simplified whitespace SQL failed: {e}")
        return False

if __name__ == "__main__":
    basic_works = test_ai_generate_syntax()

    if basic_works:
        test_simplified_whitespace_sql()
    else:
        print("\n⚠️  Basic AI.GENERATE syntax fails, need to fix connection setup first")