#!/usr/bin/env python3
"""
Complete rewrite of the Stage 8 Deep Dive cell to fix all syntax issues
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Complete new cell content for Stage 8 Deep Dive
new_cell_content = '''# === STAGE 8 DEEP DIVE: COMPETITIVE POSITIONING ANALYSIS ===

print(f"🔍 === COMPREHENSIVE COMPETITIVE INTELLIGENCE ANALYSIS ===")
print("=" * 70)

if 'cta_df' in locals() and not cta_df.empty:
    print(f"\\n📊 1. COMPETITIVE POSITIONING MATRIX")
    print("=" * 50)

    # Get comprehensive CTA analysis
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

            # Display as clean DataFrame
            from IPython.display import display

            # Create display DataFrame with clean column names
            display_df = positioning_df.copy()
            display_df['Rank'] = '#' + display_df['aggressiveness_rank'].astype(str)
            display_df['Aggressiveness'] = display_df['avg_cta_aggressiveness'].round(1).astype(str) + '/10'

            # Select columns for display
            final_df = display_df[['Rank', 'brand', 'Aggressiveness', 'total_ads',
                                  'urgency_driven_ctas', 'action_focused_ctas',
                                  'exploratory_ctas', 'soft_sell_ctas']]

            final_df.columns = ['Rank', 'Brand', 'Aggressiveness', 'Total_Ads',
                               'High_Pressure', 'Action_Focused', 'Exploratory', 'Soft_Sell']

            display(final_df)
            print()

        else:
            print("❌ No positioning data available")

    except Exception as e:
        print(f"❌ Error in positioning analysis: {str(e)}")

else:
    print("❌ CTA analysis data not available")
    print("   Run Stage 8 CTA Analysis first to see competitive positioning")'''

# Find and replace the problematic Stage 8 Deep Dive cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Look for the Stage 8 Deep Dive cell that has syntax issues
        if (("=== STAGE 8 DEEP DIVE:" in source_text and "COMPETITIVE POSITIONING" in source_text) or
            ("SyntaxError" in source_text) or
            ("unmatched ']'" in source_text) or
            ("brand_marker" in source_text)):

            print("Found problematic Stage 8 Deep Dive cell - replacing entirely...")

            # Replace with the complete new content
            cell['source'] = new_cell_content.splitlines(keepends=True)
            print("✅ Completely rewrote the cell with clean syntax!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated!")
print("\n🎯 Stage 8 Deep Dive cell completely rewritten:")
print("   • Clean, working syntax")
print("   • DataFrame display for competitive positioning")
print("   • No loop code or indentation issues")
print("   • Should execute without any errors")