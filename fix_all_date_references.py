#!/usr/bin/env python3
"""
Fix ALL references to ad_creation_date in the creative fatigue cell
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the creative fatigue cell and replace ALL ad_creation_date references
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        if "CREATIVE FATIGUE FORECASTING WITH CONFIDENCE INTERVALS" in source_text:
            print(f"Fixing ALL date field references in cell #{i}...")

            # Get the source as a single string
            source_code = ''.join(cell['source'])

            # Replace ALL occurrences of ad_creation_date with first_seen
            fixed_source = source_code.replace('ad_creation_date', 'first_seen')

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)

            print("✅ Fixed ALL date field references!")
            print("🔍 Replacements made:")

            # Count replacements
            original_count = source_code.count('ad_creation_date')
            if original_count > 0:
                print(f"   • Replaced {original_count} occurrences of 'ad_creation_date' with 'first_seen'")
            else:
                print("   • No more 'ad_creation_date' references found")

            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("\n✅ All date field references fixed!")
print("🎯 The query should now use 'first_seen' consistently")
print("💡 Try running the creative fatigue cell again!")