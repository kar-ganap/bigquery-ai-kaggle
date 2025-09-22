#!/usr/bin/env python3
"""
Fix Stage 8 Deep Dive cell by updating old field names to new ones
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the cell with the failing query
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_lines = cell['source']
        source_text = ''.join(source_lines)

        # Check if this is the failing Deep Dive cell
        if 'high_urgency_ctas' in source_text and 'COMPETITIVE POSITIONING MATRIX' in source_text:
            print("Found the failing Stage 8 Deep Dive cell!")

            # Fix all the old field names
            fixed_source = source_text

            # Replace old field names with new ones
            replacements = {
                'high_urgency_ctas': 'urgency_driven_ctas',
                'medium_engagement_ctas': 'action_focused_ctas',
                'consultative_ctas': 'exploratory_ctas',
                'low_pressure_ctas': 'soft_sell_ctas',
                'cta_adoption_rate': 'avg_cta_aggressiveness'
            }

            for old_field, new_field in replacements.items():
                fixed_source = fixed_source.replace(old_field, new_field)

            # Also fix the calculation logic to use the new approach
            old_calc = "ROUND((urgency_driven_ctas * 10.0 + action_focused_ctas * 6.0 + exploratory_ctas * 3.0 + soft_sell_ctas * 1.0) / GREATEST(total_ads, 1), 2) as avg_cta_aggressiveness"
            new_calc = "avg_cta_aggressiveness"

            fixed_source = fixed_source.replace(old_calc, new_calc)

            # Fix PERCENTILE_CONT syntax - BigQuery requires 2 arguments
            import re

            # Replace all variations of PERCENTILE_CONT usage
            percentile_patterns = [
                (r"PERCENTILE_CONT\(0\.5\) OVER \(PARTITION BY 1 ORDER BY avg_cta_aggressiveness\)",
                 "PERCENTILE_CONT(avg_cta_aggressiveness, 0.5) OVER ()"),
                (r"PERCENTILE_CONT\(0\.5\) OVER \(PARTITION BY 1 ORDER BY \([^)]+\)\)",
                 "PERCENTILE_CONT(avg_cta_aggressiveness, 0.5) OVER ()"),
                (r"PERCENTILE_CONT\(0\.5\) OVER \([^)]+ORDER BY[^)]+\)",
                 "PERCENTILE_CONT(avg_cta_aggressiveness, 0.5) OVER ()")
            ]

            for pattern, replacement in percentile_patterns:
                fixed_source = re.sub(pattern, replacement, fixed_source)

            # Add correct_category field
            if 'correct_category' not in fixed_source:
                fixed_source = fixed_source.replace(
                    "avg_cta_aggressiveness,",
                    "avg_cta_aggressiveness,\n            correct_category as market_position,"
                )

            # Remove the old CASE statement for market position since we now have correct_category
            case_pattern = r"""CASE \s*
                WHEN.*?> 8\.0 THEN 'ULTRA_AGGRESSIVE'.*?
                WHEN.*?> 6\.0 THEN 'AGGRESSIVE'.*?
                WHEN.*?> 4\.0 THEN 'MODERATE'.*?
                ELSE 'CONSERVATIVE'.*?
            END as market_position"""

            import re
            fixed_source = re.sub(case_pattern, "correct_category as market_position", fixed_source, flags=re.DOTALL)

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)

            print("Fixed the cell successfully!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")