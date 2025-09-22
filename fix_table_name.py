#!/usr/bin/env python3
"""
Fix table name in Stage 8 Deep Dive cell
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the table name
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Deep Dive cell with wrong table reference
        if 'COMPETITIVE POSITIONING MATRIX' in source_text and '{cta_table}' in source_text:
            print("Found cell with incorrect table reference...")

            # Fix the table name to use the correct one
            fixed_source = source_text.replace(
                'FROM `bigquery-ai-kaggle-469620.ads_demo.{cta_table}`',
                'FROM `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis`'
            )

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("Fixed table reference!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")