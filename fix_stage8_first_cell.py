#!/usr/bin/env python3
"""
Fix the FIRST Stage 8 Deep Dive cell (the actual CODE, not output)
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the FIRST (actual CODE) cell with Stage 8 Deep Dive
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        if "=== STAGE 8 DEEP DIVE:" in source_text:
            print(f"Found the FIRST Stage 8 Deep Dive cell (actual CODE cell #{i})...")

            # Replace with clean, working code
            new_content = '''# === STAGE 8 DEEP DIVE: COMPETITIVE POSITIONING ANALYSIS ===

print("🔍 === COMPREHENSIVE COMPETITIVE INTELLIGENCE ANALYSIS ===")
print("=" * 70)

if 'cta_df' in locals() and not cta_df.empty:
    print("\\n📊 1. COMPETITIVE POSITIONING MATRIX")
    print("=" * 50)

    try:
        from src.utils.bigquery_client import run_query
        import os

        BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
        BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

        positioning_query = f"""
        SELECT
            brand,
            total_ads,
            avg_cta_aggressiveness,
            urgency_driven_ctas,
            action_focused_ctas,
            exploratory_ctas,
            soft_sell_ctas,
            RANK() OVER (ORDER BY avg_cta_aggressiveness DESC) as aggressiveness_rank
        FROM `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis`
        ORDER BY avg_cta_aggressiveness DESC
        """

        print("🚀 Running positioning analysis...")
        positioning_df = run_query(positioning_query)

        if not positioning_df.empty:
            print("\\n🏆 COMPETITIVE POSITIONING MATRIX")
            print("CTA strategy analysis across all competitors:")
            print()

            # Display as DataFrame
            from IPython.display import display
            display(positioning_df)
            print()

        else:
            print("❌ No positioning data available")

    except Exception as e:
        print(f"❌ Error in positioning analysis: {str(e)}")

else:
    print("❌ CTA analysis data not available")
    print("   Run Stage 8 CTA Analysis first")'''

            # Replace the cell content
            cell['source'] = new_content.splitlines(keepends=True)
            print("✅ Fixed the FIRST (actual CODE) cell!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated!")
print("🎯 Fixed the actual Stage 8 Deep Dive CODE cell (first occurrence)")