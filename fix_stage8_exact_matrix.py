#!/usr/bin/env python3
"""
Fix the exact competitive positioning matrix loop to DataFrame
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the exact cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Look for the exact pattern from the output
        if ("print(\"🏆 COMPETITIVE POSITIONING MATRIX\")" in source_text and
            "print(\"CTA strategy analysis across all competitors:\")" in source_text):

            print("Found the exact cell with competitive positioning matrix...")

            # Find and replace the specific loop that creates the text output
            old_pattern = '''            print("\\n🏆 COMPETITIVE POSITIONING MATRIX")
            print("CTA strategy analysis across all competitors:")
            print()

            for _, row in positioning_df.iterrows():
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

            new_pattern = '''            print("\\n📊 COMPETITIVE POSITIONING MATRIX (DataFrame)")
            print("CTA strategy analysis across all competitors:")
            print()

            # Create a clean DataFrame for display
            display_df = positioning_df.copy()

            # Add indicator column
            display_df['Indicator'] = display_df['brand'].apply(lambda x: '🎯' if x == context.brand else '🔸')

            # Prepare display columns
            cols = ['Indicator', 'Brand']

            if 'aggressiveness_rank' in available_fields:
                display_df['Rank'] = '#' + display_df['aggressiveness_rank'].astype(int).astype(str)
                cols.insert(1, 'Rank')

            display_df['Brand'] = display_df['brand']
            display_df['Aggressiveness'] = display_df['avg_cta_aggressiveness'].round(1).astype(str) + '/10'
            display_df['Total_Ads'] = display_df['total_ads']

            cols.extend(['Aggressiveness', 'Total_Ads'])

            if 'correct_category' in available_fields:
                display_df['Category'] = display_df['correct_category']
                cols.append('Category')

            # Add CTA breakdown columns
            for field_name, display_name in [
                ('urgency_driven_ctas', 'High_Pressure'),
                ('action_focused_ctas', 'Action_Focused'),
                ('exploratory_ctas', 'Exploratory'),
                ('soft_sell_ctas', 'Soft_Sell')
            ]:
                if field_name in available_fields:
                    display_df[display_name] = display_df[field_name]
                    cols.append(display_name)

            # Display the DataFrame
            final_df = display_df[cols].sort_values('avg_cta_aggressiveness', ascending=False)

            from IPython.display import display
            display(final_df)
            print()'''

            if old_pattern in source_text:
                fixed_source = source_text.replace(old_pattern, new_pattern)
            else:
                # Try a simpler pattern match
                old_simple = '''print("\\n🏆 COMPETITIVE POSITIONING MATRIX")
            print("CTA strategy analysis across all competitors:")
            print()

            for _, row in positioning_df.iterrows():'''

                new_simple = '''print("\\n📊 COMPETITIVE POSITIONING MATRIX (DataFrame)")
            print("CTA strategy analysis across all competitors:")
            print()

            # Create DataFrame display instead of loop
            display_df = positioning_df.copy()
            display_df['Indicator'] = display_df['brand'].apply(lambda x: '🎯' if x == context.brand else '🔸')

            from IPython.display import display
            display(display_df.sort_values('avg_cta_aggressiveness', ascending=False))
            print()

            # Original loop commented out:
            # for _, row in positioning_df.iterrows():'''

                fixed_source = source_text.replace(old_simple, new_simple)

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("✅ Successfully replaced the competitive matrix with DataFrame!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Fixed the exact competitive positioning matrix:")
print("   • Replaced text loop with DataFrame display")
print("   • Should now show a clean table instead of text output")