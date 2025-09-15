#!/usr/bin/env python3
"""
Test the whitespace analysis SQL directly to see actual errors
"""

from src.competitive_intel.analysis.enhanced_whitespace_detection import Enhanced3DWhiteSpaceDetector
from src.utils.bigquery_client import run_query

def test_whitespace_sql():
    """Test the actual SQL generation and execution"""

    print("🧪 Testing Enhanced Whitespace Detection SQL")
    print("=" * 60)

    try:
        # Initialize detector
        competitors = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
        detector = Enhanced3DWhiteSpaceDetector(
            project_id="bigquery-ai-kaggle-469620",
            dataset_id="ads_demo",
            brand="Warby Parker",
            competitors=competitors
        )

        print("✅ Detector initialized")
        print(f"   - Main brand: Warby Parker")
        print(f"   - Competitors: {competitors}")

        # Generate the SQL
        print("\n📝 Generating whitespace analysis SQL...")
        whitespace_sql = detector.analyze_real_strategic_positions("stage4_test")

        print("✅ SQL generated successfully")
        print(f"   - SQL length: {len(whitespace_sql)} characters")

        # Show first part of SQL for debugging
        print(f"\n🔍 SQL Preview (first 500 chars):")
        print("-" * 40)
        print(whitespace_sql[:500] + "...")
        print("-" * 40)

        # Try to execute the SQL with a smaller dataset first
        print("\n🚀 Attempting to execute SQL...")
        print("   (This may take a while due to AI.GENERATE_TEXT calls)")

        # Let's try just the first part to see where it fails
        first_cte = whitespace_sql.split("cleaned_positions AS (")[0] + "SELECT 1 as test_column"

        print("\n🔍 Testing first CTE...")
        try:
            test_result = run_query(first_cte)
            print("✅ First CTE executes successfully")
        except Exception as e:
            print(f"❌ First CTE failed: {e}")
            return

        # Now try the full query with LIMIT to avoid timeout
        limited_sql = whitespace_sql.replace(
            "FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r",
            "FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r LIMIT 50"
        )

        print("\n🔍 Testing limited query (50 rows)...")
        try:
            limited_result = run_query(limited_sql)
            print("✅ Limited query executes successfully")
            print(f"   - Result type: {type(limited_result)}")
            if hasattr(limited_result, 'shape'):
                print(f"   - Result shape: {limited_result.shape}")
        except Exception as e:
            print(f"❌ Limited query failed: {e}")
            print(f"   - Error type: {type(e).__name__}")
            import traceback
            traceback.print_exc()

    except Exception as e:
        print(f"❌ Whitespace SQL test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_whitespace_sql()