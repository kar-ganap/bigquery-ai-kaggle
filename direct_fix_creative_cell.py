#!/usr/bin/env python3
"""
Directly fix the creative fatigue cell by finding and replacing the exact content
"""

import json
import re

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the cell with the SQL query containing ad_creation_date
cells_fixed = 0

for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Look for the cell with the problematic query
        if 'DATE_TRUNC(ad_creation_date, WEEK)' in source_text:
            print(f"Found problematic cell #{i} with ad_creation_date...")

            # Get the source lines
            source_lines = cell['source']
            fixed_lines = []

            for line in source_lines:
                # Replace all occurrences of ad_creation_date with first_seen
                fixed_line = line.replace('ad_creation_date', 'first_seen')
                fixed_lines.append(fixed_line)

            # Update the cell
            cell['source'] = fixed_lines
            cells_fixed += 1
            print(f"✅ Fixed cell #{i}")

            # Show what was changed
            original_refs = source_text.count('ad_creation_date')
            print(f"   • Replaced {original_refs} references to 'ad_creation_date' with 'first_seen'")

print(f"\n📊 Summary: Fixed {cells_fixed} cells")

# Also clear any existing output from the problematic cell
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'outputs' in cell:
        for output in cell['outputs']:
            if 'text' in output and isinstance(output['text'], list):
                output_text = ''.join(output['text'])
                if 'Unrecognized name: ad_creation_date' in output_text:
                    print(f"Clearing error output from cell #{i}")
                    cell['outputs'] = []
                    break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("\n✅ Notebook updated!")
print("🎯 All references to 'ad_creation_date' should now be 'first_seen'")
print("🧹 Cleared any cached error outputs")
print("💡 Try running the creative fatigue cell again!")