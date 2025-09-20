#!/usr/bin/env python3
"""
Test Enhanced Visual Intelligence with Competitive Insights
"""
import os
from src.utils.bigquery_client import run_query

def test_enhanced_visual_intelligence():
    """Test the enhanced visual intelligence with competitive matrix analysis"""

    print("🎨 Testing Enhanced Visual Intelligence with Competitive Insights")
    print("=" * 70)

    # Test the new competitive analysis fields
    test_sql = """
    WITH recent_visual AS (
      SELECT
        brand,
        COUNT(*) as total_ads,
        AVG(visual_text_alignment_score) as avg_visual_alignment,
        AVG(brand_consistency_score) as avg_brand_consistency,
        AVG(luxury_positioning_score) as avg_luxury_positioning,
        AVG(boldness_score) as avg_boldness,
        AVG(visual_differentiation_level) as avg_differentiation,
        STRING_AGG(DISTINCT target_demographic, ', ') as target_demographics,
        AVG(creative_pattern_risk) as avg_pattern_risk
      FROM `bigquery-ai-kaggle-469620.ads_demo.visual_intelligence_test_visual_20250915`
      WHERE luxury_positioning_score IS NOT NULL
        AND boldness_score IS NOT NULL
      GROUP BY brand
    )
    SELECT
      brand,
      total_ads,
      ROUND(avg_visual_alignment, 3) as visual_alignment,
      ROUND(avg_brand_consistency, 3) as brand_consistency,
      ROUND(avg_luxury_positioning, 3) as luxury_pos,
      ROUND(avg_boldness, 3) as boldness,
      ROUND(avg_differentiation, 3) as differentiation,
      target_demographics,
      ROUND(avg_pattern_risk, 3) as pattern_risk,
      -- Create competitive positioning coordinates
      CONCAT(
        CASE
          WHEN avg_luxury_positioning > 0.7 THEN 'Luxury-'
          WHEN avg_luxury_positioning > 0.4 THEN 'Mid-'
          ELSE 'Accessible-'
        END,
        CASE
          WHEN avg_boldness > 0.7 THEN 'Bold'
          WHEN avg_boldness > 0.4 THEN 'Balanced'
          ELSE 'Subtle'
        END
      ) as positioning_quadrant
    FROM recent_visual
    ORDER BY total_ads DESC
    """

    try:
        print("🔍 Running competitive positioning analysis...")
        result = run_query(test_sql)

        if len(result) > 0:
            print(f"✅ SUCCESS! Found competitive insights for {len(result)} brands")
            print("\n📊 COMPETITIVE VISUAL POSITIONING MATRIX:")
            print("-" * 70)

            for idx, row in result.iterrows():
                print(f"\n🏢 {row['brand']}")
                print(f"   📈 Visual Alignment: {row['visual_alignment']}")
                print(f"   🎯 Brand Consistency: {row['brand_consistency']}")
                print(f"   💎 Luxury Position: {row['luxury_pos']} | 🎨 Boldness: {row['boldness']}")
                print(f"   🎭 Differentiation: {row['differentiation']} | ⚠️  Pattern Risk: {row['pattern_risk']}")
                print(f"   👥 Target Demographics: {row['target_demographics']}")
                print(f"   📍 Positioning: {row['positioning_quadrant']}")

            # Create visual matrix
            print("\n🗺️  VISUAL COMPETITIVE MATRIX:")
            print("   Luxury/Accessible (Y) vs Bold/Subtle (X)")
            print("-" * 50)
            for _, row in result.iterrows():
                x_pos = "Right" if row['boldness'] > 0.5 else "Left"
                y_pos = "Top" if row['luxury_pos'] > 0.5 else "Bottom"
                print(f"   {row['brand']:<15} -> {y_pos}-{x_pos} ({row['positioning_quadrant']})")

        else:
            print("❌ No competitive insights found - may need to run enhanced analysis first")

        return True

    except Exception as e:
        print(f"❌ Enhanced visual intelligence test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_enhanced_visual_intelligence()