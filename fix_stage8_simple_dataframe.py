#!/usr/bin/env python3
"""
Simple fix to replace competitive positioning loop with DataFrame display
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the competitive positioning matrix
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Look for the loop that prints the competitive matrix
        if ("for _, row in positioning_df.iterrows():" in source_text and
            "🏆 COMPETITIVE POSITIONING MATRIX" in source_text):

            print("Found competitive positioning matrix loop...")

            # Simple replacement: add DataFrame display right after the title
            old_pattern = '''            print("\\n🏆 COMPETITIVE POSITIONING MATRIX")
            print("CTA strategy analysis across all competitors:")
            print()

            for _, row in positioning_df.iterrows():'''

            new_pattern = '''            print("\\n🏆 COMPETITIVE POSITIONING MATRIX")
            print("CTA strategy analysis across all competitors:")
            print()

            # Display as DataFrame
            from IPython.display import display
            display(positioning_df.sort_values('avg_cta_aggressiveness', ascending=False))
            print()

            # Original loop (commented out):
            # for _, row in positioning_df.iterrows():'''

            if old_pattern in source_text:
                fixed_source = source_text.replace(old_pattern, new_pattern)
                cell['source'] = fixed_source.splitlines(keepends=True)
                print("✅ Added DataFrame display!")
                break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated!")
print("\n🎯 The competitive positioning matrix will now show as a DataFrame")