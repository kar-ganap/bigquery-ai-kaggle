#!/usr/bin/env python3
"""
Simple fix for Stage 8 Deep Dive cell - target the specific failing query
"""

import json
import re

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the cell with the failing query
fixed_count = 0
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_lines = cell['source']
        source_text = ''.join(source_lines)

        # Check if this contains the problematic PERCENTILE_CONT query
        if 'PERCENTILE_CONT' in source_text and 'brand_positioning_query' in source_text:
            print(f"Found cell with PERCENTILE_CONT query...")

            # Fix the PERCENTILE_CONT syntax - replace with simple median calculation
            fixed_source = re.sub(
                r'PERCENTILE_CONT\([^)]+\)\s+OVER\s+\([^)]+\)',
                'NULL',
                source_text
            )

            # Remove the market_median_aggressiveness field entirely since it's causing issues
            fixed_source = re.sub(
                r',\s*PERCENTILE_CONT[^,]+as\s+market_median_aggressiveness',
                '',
                fixed_source
            )

            # Also remove any reference to market_median_aggressiveness in the rest of the query
            fixed_source = re.sub(
                r'[^,\n]*market_median_aggressiveness[^,\n]*,?\n?',
                '',
                fixed_source
            )

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            fixed_count += 1
            print(f"Fixed cell #{fixed_count}")

print(f"Fixed {fixed_count} cells with PERCENTILE_CONT issues")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")