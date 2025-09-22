#!/usr/bin/env python3
"""
Restore complete Stage 8 Deep Dive with DataFrame AND additional intelligence analysis
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Complete Stage 8 Deep Dive with both matrix and additional intelligence
complete_content = '''# === STAGE 8 DEEP DIVE: COMPETITIVE POSITIONING ANALYSIS ===

print("🔍 === COMPREHENSIVE COMPETITIVE INTELLIGENCE ANALYSIS ===")
print("=" * 70)

if 'cta_df' in locals() and not cta_df.empty:
    print("\\n📊 1. COMPETITIVE POSITIONING MATRIX")
    print("=" * 50)

    try:
        from src.utils.bigquery_client import run_query
        import os

        BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
        BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

        # Get table schema first
        schema_query = f"""
        SELECT column_name, data_type
        FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'cta_aggressiveness_analysis'
        ORDER BY ordinal_position
        """

        print("🔍 Discovering available fields in CTA analysis table...")
        schema_result = run_query(schema_query)
        available_fields = set(schema_result['column_name'].tolist()) if not schema_result.empty else set()
        print(f"✅ Found {len(available_fields)} fields in table")

        # Build positioning query with available fields
        positioning_query = f"""
        SELECT
            brand,
            total_ads,
            avg_cta_aggressiveness,
            urgency_driven_ctas,
            action_focused_ctas,
            exploratory_ctas,
            soft_sell_ctas,
            RANK() OVER (ORDER BY avg_cta_aggressiveness DESC) as aggressiveness_rank
        FROM `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis`
        ORDER BY avg_cta_aggressiveness DESC
        """

        print("🚀 Running positioning analysis...")
        positioning_df = run_query(positioning_query)

        if not positioning_df.empty:
            print("\\n🏆 COMPETITIVE POSITIONING MATRIX")
            print("CTA strategy analysis across all competitors:")
            print()

            # Display as DataFrame
            from IPython.display import display
            display(positioning_df)
            print()

            # Additional Competitive Intelligence Analysis
            print("\\n🧠 COMPETITIVE INSIGHTS")
            print("=" * 30)

            target_data = positioning_df[positioning_df['brand'] == context.brand]
            competitor_data = positioning_df[positioning_df['brand'] != context.brand]

            if not target_data.empty and not competitor_data.empty:
                target_score = target_data.iloc[0]['avg_cta_aggressiveness']
                market_median = competitor_data['avg_cta_aggressiveness'].median()

                print(f"🎯 {context.brand}: {target_score:.1f}/10")
                print(f"📊 Market Median: {market_median:.1f}/10")
                print(f"📈 Gap: {target_score - market_median:+.1f} points")

                # Show competitive threats (higher scores)
                threats = competitor_data[competitor_data['avg_cta_aggressiveness'] > target_score]
                if not threats.empty:
                    print("\\n🚨 More Aggressive Competitors:")
                    for _, comp in threats.head(3).iterrows():
                        gap = comp['avg_cta_aggressiveness'] - target_score
                        print(f"   • {comp['brand']}: +{gap:.1f} points")

                # Show opportunities (lower scores)
                opportunities = competitor_data[competitor_data['avg_cta_aggressiveness'] < target_score]
                if not opportunities.empty:
                    print("\\n💡 Less Aggressive Competitors:")
                    for _, comp in opportunities.head(3).iterrows():
                        gap = target_score - comp['avg_cta_aggressiveness']
                        print(f"   • {comp['brand']}: -{gap:.1f} points")

                # Strategic recommendations
                print("\\n📋 STRATEGIC RECOMMENDATIONS")
                print("=" * 30)

                if target_score > market_median + 1:
                    print("✅ Strong aggressive positioning")
                    print("🎯 Focus: Maintain leadership, test premium messaging")
                elif target_score < market_median - 1:
                    print("⚡ Opportunity: Increase CTA aggressiveness")
                    print("🎯 Focus: More urgent/direct call-to-actions")
                else:
                    print("📊 Moderate positioning")
                    print("🎯 Focus: Differentiate through unique value props")

        else:
            print("❌ No positioning data available")

    except Exception as e:
        print(f"❌ Error in positioning analysis: {str(e)}")

else:
    print("❌ CTA analysis data not available")
    print("   Run Stage 8 CTA Analysis first")'''

# Find and replace the Stage 8 Deep Dive cell
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        if "=== STAGE 8 DEEP DIVE:" in source_text:
            print(f"Found Stage 8 Deep Dive CODE cell (#{i}) - restoring complete content...")

            # Replace with complete content
            cell['source'] = complete_content.splitlines(keepends=True)
            print("✅ Restored complete Stage 8 Deep Dive with DataFrame AND intelligence analysis!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated!")
print("\\n🎯 Restored complete Stage 8 Deep Dive:")
print("   • DataFrame display for competitive positioning")
print("   • Competitive insights analysis")
print("   • Strategic recommendations")
print("   • Clean syntax without errors")