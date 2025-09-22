#!/usr/bin/env python3
"""
Completely rewrite Stage 8 Deep Dive cell with correct indentation
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# The complete corrected cell content with proper indentation
new_cell_content = '''# === STAGE 8 DEEP DIVE: COMPETITIVE POSITIONING ANALYSIS ===

print(f"🔍 === COMPREHENSIVE COMPETITIVE INTELLIGENCE ANALYSIS ===")
print("=" * 70)

if 'cta_df' in locals() and not cta_df.empty:
    print(f"\\n📊 1. COMPETITIVE POSITIONING MATRIX")
    print("=" * 50)

    # Get comprehensive brand comparison from CTA analysis
    try:
        brand_positioning_query = f"""
        SELECT
            brand,
            total_ads,
            avg_cta_aggressiveness,
            aggressiveness_rank,
            correct_category as market_position,
            urgency_driven_ctas as high_pressure_ads,
            action_focused_ctas as medium_engagement_ads,
            exploratory_ctas as consultative_ads,
            soft_sell_ctas as low_pressure_ads
        FROM `bigquery-ai-kaggle-469620.ads_demo.{cta_table}`
        WHERE brand IS NOT NULL
        ORDER BY avg_cta_aggressiveness DESC
        """

        positioning_df = run_query(brand_positioning_query)

        if not positioning_df.empty:
            print("🏆 COMPETITIVE POSITIONING MATRIX")
            print("CTA strategy analysis across all competitors:")
            print()

            for _, row in positioning_df.iterrows():
                brand_marker = "🎯" if row['brand'] == context.brand else "🔸"
                rank_display = f"#{int(row['aggressiveness_rank'])}"

                print(f"{brand_marker} {rank_display} {row['brand']}: {row['avg_cta_aggressiveness']:.1f}/10 ({row['market_position']})")
                print(f"   📊 Ads: {row['total_ads']} | Strategy Mix:")
                print(f"   🔥 High Pressure: {row['high_pressure_ads']}")
                print(f"   ⚡ Action-Focused: {row['medium_engagement_ads']}")
                print(f"   🤔 Exploratory: {row['consultative_ads']}")
                print(f"   💬 Soft Sell: {row['low_pressure_ads']}")
                print()

        else:
            print("❌ No positioning data found")

    except Exception as e:
        print(f"⚠️ Error in positioning analysis: {e}")

else:
    print("❌ CTA analysis data not available")
    print("   Run Stage 8 CTA Analysis first to see competitive positioning")
'''

# Find and replace the problematic cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the problematic Deep Dive cell
        if 'COMPETITIVE POSITIONING MATRIX' in source_text and 'brand_positioning_query' in source_text:
            print("Found and replacing the problematic Stage 8 Deep Dive cell...")

            # Replace the entire cell content
            cell['source'] = new_cell_content.splitlines(keepends=True)
            print("Cell rewritten with proper indentation!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")