#!/usr/bin/env python3
"""
Fix the indentation error by properly removing the loop
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the indentation error
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Look for the problematic loop
        if ("brand_marker = \"🎯\" if row['brand'] == context.brand else \"🔸\"" in source_text and
            "IndentationError" in source_text or "for _, row in positioning_df.iterrows():" in source_text):

            print("Found cell with indentation error...")

            # Find the loop section and remove it entirely
            lines = source_text.split('\n')
            fixed_lines = []
            skip_loop = False

            for line in lines:
                # Start skipping when we hit the loop
                if 'for _, row in positioning_df.iterrows():' in line:
                    skip_loop = True
                    fixed_lines.append('            # Loop removed - using DataFrame display above instead')
                    continue

                # Stop skipping when we reach the next section (else block or try/except)
                if skip_loop and (line.strip().startswith('else:') or
                                line.strip().startswith('except') or
                                line.strip().startswith('print(') and 'else:' not in line):
                    skip_loop = False

                # Add line if we're not skipping
                if not skip_loop:
                    fixed_lines.append(line)

            fixed_source = '\n'.join(fixed_lines)

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("✅ Removed problematic loop section!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated!")
print("\n🎯 Fixed indentation error by removing the problematic loop")