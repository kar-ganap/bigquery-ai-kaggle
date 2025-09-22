#!/usr/bin/env python3
"""
Completely remove all loop remnants causing indentation errors
"""

import json
import re

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and completely clean the cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Look for the cell with the DataFrame display and loop remnants
        if ("display(positioning_df.sort_values" in source_text and
            "brand_marker" in source_text):

            print("Found cell with loop remnants...")

            # Use regex to remove everything from the loop onwards until we hit else: or except:
            pattern = r'# for _, row in positioning_df\.iterrows\(\):.*?(?=\n\s*(?:else:|except:|$))'
            cleaned = re.sub(pattern, '# Loop removed', source_text, flags=re.DOTALL)

            # Also remove any remaining indented loop code
            lines = cleaned.split('\n')
            final_lines = []
            in_loop_section = False

            for line in lines:
                # Skip lines that are clearly loop remnants
                if ('brand_marker =' in line or
                    'display_parts' in line or
                    'print(f"{brand_marker}' in line or
                    'strategy_fields =' in line):
                    continue

                # Skip heavily indented lines that are loop remnants
                if line.startswith('                ') and ('print(' in line or 'for field' in line):
                    continue

                final_lines.append(line)

            fixed_source = '\n'.join(final_lines)

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("✅ Completely removed all loop remnants!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated!")
print("\n🎯 All loop code and indentation issues should be resolved")