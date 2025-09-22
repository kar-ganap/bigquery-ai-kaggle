#!/usr/bin/env python3
"""
Fix SQL syntax error in Stage 8 Deep Dive cell
"""

import json
import re

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# The correct query without syntax errors
correct_query = '''        brand_positioning_query = f"""
        SELECT
            brand,
            total_ads,
            avg_cta_aggressiveness,
            aggressiveness_rank,
            correct_category as market_position,
            urgency_driven_ctas as high_pressure_ads,
            action_focused_ctas as medium_engagement_ads,
            exploratory_ctas as consultative_ads,
            soft_sell_ctas as low_pressure_ads

        FROM `bigquery-ai-kaggle-469620.ads_demo.{cta_table}`
        WHERE brand IS NOT NULL
        ORDER BY avg_cta_aggressiveness DESC
        """'''

# Find and fix the cell with the broken SQL
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_lines = cell['source']
        source_text = ''.join(source_lines)

        # Check if this contains the broken SQL syntax
        if ('NULL / GREATEST(total_ads,' in source_text or
            'brand_positioning_query' in source_text and 'COMPETITIVE POSITIONING MATRIX' in source_text):

            print("Found cell with broken SQL syntax...")

            # Replace the entire broken query with the correct one
            pattern = r'brand_positioning_query = f""".*?"""'
            fixed_source = re.sub(pattern, correct_query, source_text, flags=re.DOTALL)

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("Fixed SQL syntax!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")