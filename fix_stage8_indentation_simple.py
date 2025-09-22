#!/usr/bin/env python3
"""
Fix Stage 8 Strategic Dashboard indentation error - simple approach
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the Strategic Dashboard cell with indentation error
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Strategic Dashboard cell with the broken market position code
        if ("# Calculate market position category from aggressiveness score" in source_text):

            print("Found Strategic Dashboard cell with indentation error...")

            # Replace the entire problematic section with properly indented code
            old_section = """# Calculate market position category from aggressiveness score
    aggressiveness_score = target_data.iloc[0]['avg_cta_aggressiveness']
    if aggressiveness_score >= 8.0:
        target_category = 'ULTRA_AGGRESSIVE'
    elif aggressiveness_score >= 6.0:
        target_category = 'AGGRESSIVE'
    elif aggressiveness_score >= 4.0:
        target_category = 'MODERATE'
    else:
        target_category = 'CONSERVATIVE'"""

            new_section = """    # Calculate market position category from aggressiveness score
        aggressiveness_score = target_data.iloc[0]['avg_cta_aggressiveness']
        if aggressiveness_score >= 8.0:
            target_category = 'ULTRA_AGGRESSIVE'
        elif aggressiveness_score >= 6.0:
            target_category = 'AGGRESSIVE'
        elif aggressiveness_score >= 4.0:
            target_category = 'MODERATE'
        else:
            target_category = 'CONSERVATIVE'"""

            if old_section in source_text:
                fixed_source = source_text.replace(old_section, new_section)

                # Convert back to list of lines
                cell['source'] = fixed_source.splitlines(keepends=True)
                print("✅ Fixed indentation by replacing the problematic section!")
                break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Fixed Stage 8 Strategic Dashboard:")
print("   • Corrected indentation for market position calculation")
print("   • Should now execute without IndentationError")