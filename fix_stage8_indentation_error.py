#!/usr/bin/env python3
"""
Fix Stage 8 Strategic Dashboard indentation error
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

        # Check if this is the Strategic Dashboard cell with indentation issues
        if ("Calculate market position category from aggressiveness score" in source_text and
            "IndentationError" in source_text or "unexpected indent" in source_text or
            "print(f\"🎯 {context.brand} Current Position:\")" in source_text):

            print("Found Strategic Dashboard cell with indentation error...")

            # Fix the indentation by properly aligning the code
            lines = source_text.split('\n')
            fixed_lines = []

            for line in lines:
                # Fix common indentation issues
                if line.strip().startswith('# Calculate market position category'):
                    fixed_lines.append('    # Calculate market position category from aggressiveness score')
                elif line.strip().startswith('aggressiveness_score = target_data'):
                    fixed_lines.append('    aggressiveness_score = target_data.iloc[0][\'avg_cta_aggressiveness\']')
                elif line.strip().startswith('if aggressiveness_score >= 8.0:'):
                    fixed_lines.append('    if aggressiveness_score >= 8.0:')
                elif line.strip().startswith('target_category = \'ULTRA_AGGRESSIVE\''):
                    fixed_lines.append('        target_category = \'ULTRA_AGGRESSIVE\'')
                elif line.strip().startswith('elif aggressiveness_score >= 6.0:'):
                    fixed_lines.append('    elif aggressiveness_score >= 6.0:')
                elif line.strip().startswith('target_category = \'AGGRESSIVE\''):
                    fixed_lines.append('        target_category = \'AGGRESSIVE\'')
                elif line.strip().startswith('elif aggressiveness_score >= 4.0:'):
                    fixed_lines.append('    elif aggressiveness_score >= 4.0:')
                elif line.strip().startswith('target_category = \'MODERATE\''):
                    fixed_lines.append('        target_category = \'MODERATE\'')
                elif line.strip().startswith('else:') and 'target_category' in lines[lines.index(line)+1] if lines.index(line)+1 < len(lines) else False:
                    fixed_lines.append('    else:')
                elif line.strip().startswith('target_category = \'CONSERVATIVE\''):
                    fixed_lines.append('        target_category = \'CONSERVATIVE\'')
                else:
                    fixed_lines.append(line)

            fixed_source = '\n'.join(fixed_lines)

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("✅ Fixed indentation issues!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Fixed Stage 8 Strategic Dashboard:")
print("   • Corrected indentation for market position calculation")
print("   • Properly aligned if/elif/else blocks")
print("   • Should now execute without IndentationError")