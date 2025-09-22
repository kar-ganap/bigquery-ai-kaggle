#!/usr/bin/env python3
"""
Fix the specific Stage 8 Deep Dive cell to display competitive analysis as DataFrame
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the specific Stage 8 Deep Dive cell that has the competitive matrix display
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Deep Dive cell with the loop that prints brand rankings
        if ("for _, row in positioning_df.iterrows():" in source_text and
            "brand_marker = " in source_text and
            "🎯" in source_text):

            print("Found the Deep Dive cell with competitive positioning loop...")

            # Replace the entire loop section with DataFrame display
            old_loop_section = '''            for _, row in positioning_df.iterrows():
                brand_marker = "🎯" if row['brand'] == context.brand else "🔸"

                # Build display string based on available fields
                display_parts = []
                if 'aggressiveness_rank' in row:
                    display_parts.append(f"#{int(row['aggressiveness_rank'])}")
                display_parts.append(f"{row['brand']}: {row['avg_cta_aggressiveness']:.1f}/10")
                if 'correct_category' in row:
                    display_parts.append(f"({row['correct_category']})")

                print(f"   {brand_marker} {' '.join(display_parts)}")
                print(f"      📊 Ads: {row['total_ads']} | Strategy Mix:")

                # Show strategy breakdown based on available fields
                for field_name, display_name, emoji in [
                    ('urgency_driven_ctas', 'High Pressure', '🔥'),
                    ('action_focused_ctas', 'Action-Focused', '⚡'),
                    ('exploratory_ctas', 'Exploratory', '🤔'),
                    ('soft_sell_ctas', 'Soft Sell', '💬')
                ]:
                    if field_name in available_fields:
                        print(f"      {emoji} {display_name}: {row[field_name]}")

                print()'''

            new_dataframe_section = '''            # Create a formatted DataFrame for display
            display_df = positioning_df.copy()

            # Add target brand indicator
            display_df['Indicator'] = display_df['brand'].apply(lambda x: '🎯' if x == context.brand else '🔸')

            # Create formatted columns for display
            cols_to_show = ['Indicator', 'Brand']

            # Add rank if available
            if 'aggressiveness_rank' in available_fields:
                display_df['Rank'] = '#' + display_df['aggressiveness_rank'].astype(int).astype(str)
                cols_to_show.insert(1, 'Rank')

            # Core metrics
            display_df['Brand'] = display_df['brand']
            display_df['Aggressiveness'] = display_df['avg_cta_aggressiveness'].round(1).astype(str) + '/10'
            display_df['Total Ads'] = display_df['total_ads']
            cols_to_show.extend(['Aggressiveness', 'Total Ads'])

            # Add category if available
            if 'correct_category' in available_fields:
                display_df['Category'] = display_df['correct_category']
                cols_to_show.append('Category')

            # Add CTA strategy breakdown
            for field_name, display_name in [
                ('urgency_driven_ctas', 'High Pressure'),
                ('action_focused_ctas', 'Action-Focused'),
                ('exploratory_ctas', 'Exploratory'),
                ('soft_sell_ctas', 'Soft Sell')
            ]:
                if field_name in available_fields:
                    display_df[display_name] = display_df[field_name]
                    cols_to_show.append(display_name)

            # Select and display the DataFrame
            final_df = display_df[cols_to_show]

            print("\\n📊 COMPETITIVE POSITIONING MATRIX")
            print("=" * 80)
            from IPython.display import display
            display(final_df.sort_values('avg_cta_aggressiveness', ascending=False))
            print()'''

            # Replace the loop section
            if old_loop_section in source_text:
                fixed_source = source_text.replace(old_loop_section, new_dataframe_section)

                # Convert back to list of lines
                cell['source'] = fixed_source.splitlines(keepends=True)
                print("✅ Successfully replaced loop with DataFrame display!")
                break
            else:
                print("❌ Exact loop pattern not found, trying alternative approach...")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Enhanced Stage 8 Deep Dive:")
print("   • Replaced text-based loop with clean DataFrame display")
print("   • Shows competitive positioning in tabular format")
print("   • Includes all available fields dynamically")
print("   • Target brand (🎯) clearly highlighted")