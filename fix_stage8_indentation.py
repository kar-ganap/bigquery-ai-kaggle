#!/usr/bin/env python3
"""
Fix indentation error in Stage 8 Deep Dive cell
"""

import json
import re

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the cell with indentation issues
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_lines = cell['source']
        source_text = ''.join(source_lines)

        # Check if this contains the positioning query with potential indentation issues
        if 'brand_positioning_query' in source_text and 'COMPETITIVE POSITIONING MATRIX' in source_text:
            print("Found cell with potential indentation issues...")

            # Split into lines and fix indentation
            lines = source_text.splitlines()
            fixed_lines = []

            for line in lines:
                # If line starts with positioning_df = run_query, ensure proper indentation
                if 'positioning_df = run_query' in line:
                    # Make sure it has proper indentation (8 spaces to match try block)
                    fixed_lines.append('        positioning_df = run_query(brand_positioning_query)')
                elif 'if not positioning_df.empty:' in line:
                    fixed_lines.append('        if not positioning_df.empty:')
                else:
                    fixed_lines.append(line)

            # Join back with newlines and ensure proper line endings
            fixed_source = '\n'.join(fixed_lines) + '\n'

            # Convert back to list of lines with proper endings
            cell['source'] = [line + '\n' for line in fixed_lines]

            print("Fixed indentation!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")