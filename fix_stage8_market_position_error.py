#!/usr/bin/env python3
"""
Fix Stage 8 Strategic Dashboard market_position KeyError
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the Strategic Dashboard cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Strategic Dashboard cell with market_position error
        if ("target_category = target_data.iloc[0]['market_position']" in source_text):
            print("Found Strategic Dashboard cell with market_position error...")

            # Replace the problematic field access with a calculated category
            fixed_source = source_text.replace(
                "target_category = target_data.iloc[0]['market_position']",
                """# Calculate market position category from aggressiveness score
    aggressiveness_score = target_data.iloc[0]['avg_cta_aggressiveness']
    if aggressiveness_score >= 8.0:
        target_category = 'ULTRA_AGGRESSIVE'
    elif aggressiveness_score >= 6.0:
        target_category = 'AGGRESSIVE'
    elif aggressiveness_score >= 4.0:
        target_category = 'MODERATE'
    else:
        target_category = 'CONSERVATIVE'"""
            )

            # Also add debug info to show available columns
            fixed_source = fixed_source.replace(
                "if not target_data.empty:",
                """# Debug: Show available columns
    print(f"   📊 Available columns: {list(positioning_df.columns)}")

    if not target_data.empty:"""
            )

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("✅ Fixed market_position field reference!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Fixed Stage 8 Strategic Dashboard:")
print("   • Replaced 'market_position' field access with calculated category")
print("   • Added debug info to show available columns")
print("   • Category calculated from avg_cta_aggressiveness score")
print("   • Should now work without KeyError")