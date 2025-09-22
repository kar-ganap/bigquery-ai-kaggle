#!/usr/bin/env python3
"""
Fix Stage 8 Strategic Dashboard rank_position KeyError
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

        # Check if this is the Strategic Dashboard cell with rank_position error
        if ("target_rank = int(target_data.iloc[0]['rank_position'])" in source_text):
            print("Found Strategic Dashboard cell with rank_position error...")

            # Fix the field reference - use 'aggressiveness_rank' instead of 'rank_position'
            fixed_source = source_text.replace(
                "target_rank = int(target_data.iloc[0]['rank_position'])",
                "target_rank = int(target_data.iloc[0]['aggressiveness_rank'])"
            )

            # Also check for any other references to rank_position and fix them
            fixed_source = fixed_source.replace("'rank_position'", "'aggressiveness_rank'")
            fixed_source = fixed_source.replace('"rank_position"', '"aggressiveness_rank"')

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("✅ Fixed rank_position field reference!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Fixed Stage 8 Strategic Dashboard:")
print("   • Changed 'rank_position' to 'aggressiveness_rank'")
print("   • This matches the field name from the CTA analysis query")
print("   • Dashboard should now work without KeyError")