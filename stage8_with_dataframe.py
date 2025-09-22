#!/usr/bin/env python3
"""
Update Stage 8 Deep Dive to display competitive positioning as a clean DataFrame
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Updated cell content with DataFrame display
dataframe_cell = '''# === STAGE 8 DEEP DIVE: COMPETITIVE POSITIONING ANALYSIS ===

print(f"🔍 === COMPREHENSIVE COMPETITIVE INTELLIGENCE ANALYSIS ===")
print("=" * 70)

if 'cta_df' in locals() and not cta_df.empty:
    print(f"\\n📊 1. COMPETITIVE POSITIONING MATRIX")
    print("=" * 50)

    try:
        # Get comprehensive positioning data
        brand_positioning_query = f"""
        SELECT
            brand,
            total_ads,
            avg_cta_aggressiveness,
            correct_category as market_position,
            urgency_driven_ctas as high_pressure_ads,
            action_focused_ctas as action_focused_ads,
            exploratory_ctas as exploratory_ads,
            soft_sell_ctas as soft_sell_ads,
            RANK() OVER (ORDER BY avg_cta_aggressiveness DESC) as rank_position
        FROM `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis`
        WHERE brand IS NOT NULL
        ORDER BY avg_cta_aggressiveness DESC
        """

        positioning_df = run_query(brand_positioning_query)

        if not positioning_df.empty:
            print("🏆 COMPETITIVE POSITIONING MATRIX")
            print()

            # Create a clean display DataFrame
            display_df = positioning_df.copy()

            # Format the aggressiveness score
            display_df['Aggressiveness'] = display_df['avg_cta_aggressiveness'].apply(lambda x: f"{x:.1f}/10")

            # Add target brand indicator
            display_df['Brand'] = display_df.apply(
                lambda row: f"🎯 {row['brand']}" if row['brand'] == context.brand else f"🔸 {row['brand']}",
                axis=1
            )

            # Calculate strategy mix percentages
            display_df['High Pressure %'] = (display_df['high_pressure_ads'] * 100 / display_df['total_ads']).round(1)
            display_df['Action Focus %'] = (display_df['action_focused_ads'] * 100 / display_df['total_ads']).round(1)
            display_df['Exploratory %'] = (display_df['exploratory_ads'] * 100 / display_df['total_ads']).round(1)
            display_df['Soft Sell %'] = (display_df['soft_sell_ads'] * 100 / display_df['total_ads']).round(1)

            # Select and reorder columns for clean display
            final_columns = [
                'rank_position',
                'Brand',
                'Aggressiveness',
                'market_position',
                'total_ads',
                'High Pressure %',
                'Action Focus %',
                'Exploratory %',
                'Soft Sell %'
            ]

            clean_df = display_df[final_columns].copy()
            clean_df.columns = [
                'Rank',
                'Brand',
                'Aggressiveness',
                'Category',
                'Total Ads',
                'High Pressure %',
                'Action Focus %',
                'Exploratory %',
                'Soft Sell %'
            ]

            # Display the clean DataFrame
            print(clean_df.to_string(index=False))

            print()
            print("=" * 70)

            # Show competitive insights
            print("\\n🧠 COMPETITIVE INSIGHTS")
            print("=" * 30)

            target_data = positioning_df[positioning_df['brand'] == context.brand]
            competitor_data = positioning_df[positioning_df['brand'] != context.brand]

            if not target_data.empty and not competitor_data.empty:
                target_score = target_data.iloc[0]['avg_cta_aggressiveness']
                target_rank = int(target_data.iloc[0]['rank_position'])
                market_median = competitor_data['avg_cta_aggressiveness'].median()

                print(f"🎯 {context.brand}:")
                print(f"   • Rank: #{target_rank} of {len(positioning_df)}")
                print(f"   • Score: {target_score:.1f}/10")
                print(f"   • vs Market Median: {target_score - market_median:+.1f} points")
                print(f"   • Category: {target_data.iloc[0]['market_position']}")

                # Show competitive threats (higher scores)
                threats = competitor_data[competitor_data['avg_cta_aggressiveness'] > target_score]
                if not threats.empty:
                    print(f"\\n🚨 More Aggressive Competitors ({len(threats)}):")
                    for _, comp in threats.head(3).iterrows():
                        gap = comp['avg_cta_aggressiveness'] - target_score
                        print(f"   • {comp['brand']}: {comp['avg_cta_aggressiveness']:.1f}/10 (+{gap:.1f})")

                # Show opportunities (lower scores)
                opportunities = competitor_data[competitor_data['avg_cta_aggressiveness'] < target_score]
                if not opportunities.empty:
                    print(f"\\n💡 Less Aggressive Competitors ({len(opportunities)}):")
                    for _, comp in opportunities.head(3).iterrows():
                        gap = target_score - comp['avg_cta_aggressiveness']
                        print(f"   • {comp['brand']}: {comp['avg_cta_aggressiveness']:.1f}/10 (-{gap:.1f})")

        else:
            print("❌ No positioning data found")

    except Exception as e:
        print(f"⚠️ Error in positioning analysis: {e}")

else:
    print("❌ CTA analysis data not available")
    print("   Run Stage 8 CTA Analysis first to see competitive positioning")
'''

# Find and replace the Deep Dive cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Deep Dive cell
        if 'COMPETITIVE POSITIONING MATRIX' in source_text and ('schema_query' in source_text or 'brand_positioning_query' in source_text):
            print("Found and updating Stage 8 Deep Dive cell with DataFrame display...")

            # Replace the entire cell content
            cell['source'] = dataframe_cell.splitlines(keepends=True)
            print("Cell updated with clean DataFrame display!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\\n🎯 The cell now displays:")
print("   • Clean DataFrame with competitive positioning matrix")
print("   • Formatted aggressiveness scores")
print("   • Strategy mix percentages")
print("   • Target brand highlighting")
print("   • Comprehensive competitive insights")