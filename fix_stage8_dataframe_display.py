#!/usr/bin/env python3
"""
Fix Stage 8 Deep Dive to display competitive analysis as proper DataFrame
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the Stage 8 Deep Dive cell and replace the text display with DataFrame
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Deep Dive cell with the competitive positioning matrix
        if ("COMPETITIVE POSITIONING MATRIX" in source_text and
            "🔸 #1" in source_text or
            "print(f\"   {brand_indicator}" in source_text):

            print("Found Deep Dive cell with text-based competitive matrix...")

            # Replace the text-based display with DataFrame display
            # Find the section that prints individual brand rankings
            if "for _, row in positioning_df.iterrows():" in source_text:
                # Replace the loop that prints individual rows with DataFrame display
                old_pattern = '''for _, row in positioning_df.iterrows():
                brand_indicator = "🎯" if row['brand'] == context.brand else "🔸"
                print(f"   {brand_indicator} #{row['rank']:0.0f} {row['brand']}: {row['avg_cta_aggressiveness']:.1f}/10 ({row['category']})")
                print(f"      📊 Ads: {row['total_ads']} | Strategy Mix:")
                print(f"      🔥 High Pressure: {row.get('urgency_driven_ctas', 0)}")
                print(f"      ⚡ Action-Focused: {row.get('action_focused_ctas', 0)}")
                print(f"      🤔 Exploratory: {row.get('exploratory_ctas', 0)}")
                print(f"      💬 Soft Sell: {row.get('soft_sell_ctas', 0)}")
                print()'''

                new_pattern = '''# Create a formatted DataFrame for display
            display_df = positioning_df.copy()

            # Add target brand indicator
            display_df['Indicator'] = display_df['brand'].apply(lambda x: '🎯' if x == context.brand else '🔸')

            # Create formatted columns
            display_df['Rank'] = '#' + display_df['rank'].astype(str)
            display_df['Brand'] = display_df['Indicator'] + ' ' + display_df['brand']
            display_df['Aggressiveness'] = display_df['avg_cta_aggressiveness'].round(1).astype(str) + '/10'
            display_df['Total Ads'] = display_df['total_ads']
            display_df['High Pressure'] = display_df.get('urgency_driven_ctas', 0)
            display_df['Action-Focused'] = display_df.get('action_focused_ctas', 0)
            display_df['Exploratory'] = display_df.get('exploratory_ctas', 0)
            display_df['Soft Sell'] = display_df.get('soft_sell_ctas', 0)
            display_df['Category'] = display_df['category']

            # Select and reorder columns for display
            final_df = display_df[['Rank', 'Brand', 'Aggressiveness', 'Category', 'Total Ads',
                                  'High Pressure', 'Action-Focused', 'Exploratory', 'Soft Sell']]

            print("\\n📊 COMPETITIVE POSITIONING MATRIX (DataFrame)")
            print("=" * 80)
            from IPython.display import display
            display(final_df)
            print()'''

                if old_pattern in source_text:
                    fixed_source = source_text.replace(old_pattern, new_pattern)
                else:
                    # If exact pattern not found, try a more flexible approach
                    # Look for the loop and replace it
                    import re
                    loop_pattern = r'for _, row in positioning_df\.iterrows\(\):.*?print\(\)'
                    fixed_source = re.sub(loop_pattern, new_pattern, source_text, flags=re.DOTALL)

                # Convert back to list of lines
                cell['source'] = fixed_source.splitlines(keepends=True)
                print("✅ Replaced text display with DataFrame display!")
                break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Enhanced Stage 8 Deep Dive:")
print("   • Replaced text-based competitive matrix with proper DataFrame")
print("   • Includes target brand indicator (🎯)")
print("   • Clean tabular format with all key metrics")
print("   • Easier to read and analyze competitive positioning")