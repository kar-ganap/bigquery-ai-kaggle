#!/usr/bin/env python3
"""
Comprehensive fix for Stage 8 Deep Dive - analyze table schema and create proper query
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# First, let's create a cell that discovers what fields are actually available
discovery_and_analysis_cell = '''# === STAGE 8 DEEP DIVE: COMPETITIVE POSITIONING ANALYSIS ===

print(f"🔍 === COMPREHENSIVE COMPETITIVE INTELLIGENCE ANALYSIS ===")
print("=" * 70)

if 'cta_df' in locals() and not cta_df.empty:
    print(f"\\n📊 1. COMPETITIVE POSITIONING MATRIX")
    print("=" * 50)

    # First, let's discover what fields are actually available in the CTA table
    try:
        # Get table schema
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

        # Use only fields that actually exist
        select_fields = []
        field_mapping = {
            'brand': 'brand',
            'total_ads': 'total_ads',
            'avg_cta_aggressiveness': 'avg_cta_aggressiveness',
            'correct_category': 'correct_category',
            'urgency_driven_ctas': 'urgency_driven_ctas',
            'action_focused_ctas': 'action_focused_ctas',
            'exploratory_ctas': 'exploratory_ctas',
            'soft_sell_ctas': 'soft_sell_ctas'
        }

        for alias, field in field_mapping.items():
            if field in available_fields:
                if alias != field:
                    select_fields.append(f"{field} as {alias}")
                else:
                    select_fields.append(field)
            else:
                print(f"⚠️ Field '{field}' not found in table")

        # Add rank calculation only if we have avg_cta_aggressiveness
        if 'avg_cta_aggressiveness' in available_fields:
            select_fields.append("RANK() OVER (ORDER BY avg_cta_aggressiveness DESC) as aggressiveness_rank")

        # Build the query with available fields
        brand_positioning_query = f"""
        SELECT
            {', '.join(select_fields)}
        FROM `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis`
        WHERE brand IS NOT NULL
        ORDER BY avg_cta_aggressiveness DESC
        """

        print("🚀 Running positioning analysis...")
        positioning_df = run_query(brand_positioning_query)

        if not positioning_df.empty:
            print("\\n🏆 COMPETITIVE POSITIONING MATRIX")
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

                print(f"{brand_marker} {' '.join(display_parts)}")

                if 'total_ads' in row:
                    print(f"   📊 Ads: {row['total_ads']} | Strategy Mix:")

                # Show strategy mix if fields are available
                strategy_fields = [
                    ('urgency_driven_ctas', '🔥 High Pressure'),
                    ('action_focused_ctas', '⚡ Action-Focused'),
                    ('exploratory_ctas', '🤔 Exploratory'),
                    ('soft_sell_ctas', '💬 Soft Sell')
                ]

                for field, label in strategy_fields:
                    if field in row:
                        print(f"   {label}: {row[field]}")

                print()

            # Show competitive insights
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
                    print(f"\\n🚨 More Aggressive Competitors:")
                    for _, comp in threats.head(3).iterrows():
                        gap = comp['avg_cta_aggressiveness'] - target_score
                        print(f"   • {comp['brand']}: +{gap:.1f} points")

                # Show opportunities (lower scores)
                opportunities = competitor_data[competitor_data['avg_cta_aggressiveness'] < target_score]
                if not opportunities.empty:
                    print(f"\\n💡 Less Aggressive Competitors:")
                    for _, comp in opportunities.head(3).iterrows():
                        gap = target_score - comp['avg_cta_aggressiveness']
                        print(f"   • {comp['brand']}: -{gap:.1f} points")

        else:
            print("❌ No positioning data found")

    except Exception as e:
        print(f"⚠️ Error in positioning analysis: {e}")
        print("\\n🔍 Available fields for debugging:")
        if 'schema_result' in locals() and not schema_result.empty:
            for _, field in schema_result.iterrows():
                print(f"   • {field['column_name']} ({field['data_type']})")

else:
    print("❌ CTA analysis data not available")
    print("   Run Stage 8 CTA Analysis first to see competitive positioning")
'''

# Find and replace the problematic cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the problematic Deep Dive cell
        if 'COMPETITIVE POSITIONING MATRIX' in source_text and 'brand_positioning_query' in source_text:
            print("Found and replacing the problematic Stage 8 Deep Dive cell...")

            # Replace the entire cell content
            cell['source'] = discovery_and_analysis_cell.splitlines(keepends=True)
            print("Cell completely rewritten with schema discovery and robust error handling!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\\n🎯 This new cell will:")
print("   • Discover what fields actually exist in the table")
print("   • Build queries using only available fields")
print("   • Handle missing fields gracefully")
print("   • Provide debugging information if errors occur")
print("   • Show comprehensive competitive intelligence analysis")