#!/usr/bin/env python3
"""
Simple fix for the date field in creative fatigue analysis
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the creative fatigue cell and fix the date field
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        if "CREATIVE FATIGUE FORECASTING WITH CONFIDENCE INTERVALS" in source_text:
            print(f"Fixing date field in creative fatigue cell #{i}...")

            # Replace ad_creation_date with first_seen
            source_lines = cell['source']
            fixed_lines = []

            for line in source_lines:
                # Replace the problematic date field references
                if 'ad_creation_date' in line:
                    line = line.replace('ad_creation_date', 'first_seen')
                fixed_lines.append(line)

            cell['source'] = fixed_lines
            print("✅ Fixed date field references!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("\n✅ Date field fixed!")
print("🎯 Changed 'ad_creation_date' to 'first_seen' (DATE field)")
print("💡 The creative fatigue visualization should now work with real data!")