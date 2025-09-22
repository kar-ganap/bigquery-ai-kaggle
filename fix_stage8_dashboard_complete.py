#!/usr/bin/env python3
"""
Complete rewrite of Stage 8 Strategic Dashboard to fix all issues
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Complete Strategic Dashboard cell content
new_dashboard_cell = '''# 🎯 STRATEGIC ANALYSIS DASHBOARD
print("🎯 STRATEGIC ANALYSIS DASHBOARD")
print("=" * 50)

if stage8_results is None:
    print("❌ No strategic analysis results found")
    print("   Make sure you ran Stage 8 Strategic Analysis first")
else:
    print(f"✅ Strategic Analysis Status: {stage8_results.status}")
    print(f"📋 Analysis completed successfully")
    print()

    # Import required libraries
    import pandas as pd
    from src.utils.bigquery_client import run_query
    import os

    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

    print("🔍 1. CURRENT COMPETITIVE STATE")
    print("-" * 40)

    try:
        # Get CTA positioning data
        positioning_query = f"""
        SELECT
            brand,
            total_ads,
            avg_cta_aggressiveness,
            RANK() OVER (ORDER BY avg_cta_aggressiveness DESC) as aggressiveness_rank
        FROM `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis`
        ORDER BY avg_cta_aggressiveness DESC
        """

        positioning_df = run_query(positioning_query)

        if not positioning_df.empty:
            print(f"📊 Available columns: {list(positioning_df.columns)}")

            # Filter for target brand
            target_data = positioning_df[positioning_df['brand'] == context.brand]
            total_competitors = len(positioning_df)

            if not target_data.empty:
                target_rank = int(target_data.iloc[0]['aggressiveness_rank'])
                target_score = target_data.iloc[0]['avg_cta_aggressiveness']

                # Calculate market position category from aggressiveness score
                if target_score >= 8.0:
                    target_category = 'ULTRA_AGGRESSIVE'
                elif target_score >= 6.0:
                    target_category = 'AGGRESSIVE'
                elif target_score >= 4.0:
                    target_category = 'MODERATE'
                else:
                    target_category = 'CONSERVATIVE'

                print(f"🎯 {context.brand} Current Position:")
                print(f"   • Market Rank: #{target_rank} of {total_competitors} brands")
                print(f"   • CTA Aggressiveness: {target_score:.1f}/10")
                print(f"   • Market Category: {target_category}")
                print()

                # Competitive analysis
                print("🏆 2. COMPETITIVE LANDSCAPE")
                print("-" * 40)

                for _, row in positioning_df.head(5).iterrows():
                    indicator = "🎯" if row['brand'] == context.brand else "🔸"
                    print(f"   {indicator} #{int(row['aggressiveness_rank'])} {row['brand']}: {row['avg_cta_aggressiveness']:.1f}/10")

                print()

                # Strategic recommendations
                print("💡 3. STRATEGIC RECOMMENDATIONS")
                print("-" * 40)

                if target_rank <= 2:
                    print("   ✅ Strong market position - maintain aggressive strategy")
                    print("   🎯 Focus on differentiation to stay ahead")
                elif target_rank <= total_competitors // 2:
                    print("   ⚡ Moderate position - opportunity to increase aggressiveness")
                    print("   🎯 Consider more direct CTAs and urgency tactics")
                else:
                    print("   🚀 Low market position - significant opportunity for improvement")
                    print("   🎯 Implement more aggressive CTA strategies immediately")

            else:
                print(f"❌ No data found for {context.brand}")
        else:
            print("❌ No CTA positioning data available")

    except Exception as e:
        print(f"❌ Error in strategic analysis: {str(e)}")
        print("   Check that CTA analysis has been completed successfully")

print()
print("📊 Strategic Analysis Dashboard Complete")'''

# Find and replace the Strategic Dashboard cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Strategic Dashboard cell (multiple possible identifiers)
        if (("STRATEGIC ANALYSIS DASHBOARD" in source_text and "stage8_results" in source_text) or
            ("target_rank = int(target_data.iloc[0]" in source_text) or
            ("Calculate market position category" in source_text)):

            print("Found Strategic Dashboard cell - replacing with complete rewrite...")

            # Replace with the complete new dashboard
            cell['source'] = new_dashboard_cell.splitlines(keepends=True)
            print("✅ Completely rewrote Strategic Dashboard cell!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Complete Strategic Dashboard Rewrite:")
print("   • Clean, properly indented code")
print("   • Correct field references (aggressiveness_rank)")
print("   • Calculated market position categories")
print("   • Comprehensive error handling")
print("   • Debug info showing available columns")
print("   • Strategic recommendations based on market position")
print("   • Should execute without any errors")