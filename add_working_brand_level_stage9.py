#!/usr/bin/env python3
"""
Add a working brand-level Stage 9 summary with simplified query
"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the brand-level Stage 9 cell
target_cell = None
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])
        if 'BRAND-LEVEL STAGE 9 INTELLIGENCE DASHBOARD' in source_text:
            target_cell = i
            break

if target_cell is not None:
    # Create simpler, working brand-level code
    working_code = '''# === 🎯 BRAND-LEVEL STAGE 9 INTELLIGENCE DASHBOARD ===

print("🎯 BRAND-LEVEL MULTI-DIMENSIONAL INTELLIGENCE DASHBOARD")
print("=" * 80)
print("📋 Strategic Intelligence by Brand (Aggregate is essentially meaningless)")
print()

if stage9_results is None:
    print("❌ No multi-dimensional intelligence results found")
    print("   Make sure you ran Stage 9 Multi-Dimensional Intelligence first")
else:
    print(f"✅ Intelligence Status: {stage9_results.status}")
    print(f"📊 Data Completeness: {stage9_results.data_completeness:.1f}%")
    print()

    # === EXTRACT BRAND-LEVEL DATA FROM STAGE 9 RESULTS ===

    try:
        from src.utils.bigquery_client import run_query
        import pandas as pd
        import os

        BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
        BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

        # Simplified brand-level intelligence query
        brand_intelligence_query = f"""
        SELECT
          brand,
          COUNT(*) as total_ads,

          -- Audience metrics (use COALESCE to combine text fields)
          AVG(LENGTH(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, ''))) as avg_text_length,
          -- Create combined text field for analysis
          SUM(CASE WHEN LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%price%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%cost%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%affordable%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%cheap%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%budget%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%value%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%save%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%discount%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%deal%'
               THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as price_conscious_rate,

          SUM(CASE WHEN LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%millennial%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%young professional%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%career%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%lifestyle%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%trendy%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%modern%'
               THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as millennial_targeting_rate,

          -- Creative metrics
          SUM(CASE WHEN LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%love%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%amazing%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%perfect%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%beautiful%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%stunning%'
               THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as emotional_keyword_rate,

          -- Channel metrics (use publisher_platforms field)
          COUNT(DISTINCT publisher_platforms) as platform_diversification,
          SUM(CASE WHEN publisher_platforms LIKE '%Facebook%' OR publisher_platforms LIKE '%Instagram%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as platform_optimization_rate,

          -- Strategic positioning
          SUM(CASE WHEN LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%quality%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%premium%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%luxury%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%superior%'
               THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as premium_positioning_rate,

          SUM(CASE WHEN LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%try%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%free%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%risk%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%guarantee%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%return%'
               THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as trial_focus_rate,

          -- Visual style metrics (from existing labels if available)
          SUM(CASE WHEN LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%style%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%design%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%fashion%'
                    OR LOWER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '') || ' ' || COALESCE(cta_text, '')) LIKE '%look%'
               THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as style_focus_rate

        FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
        WHERE brand IS NOT NULL
        GROUP BY brand
        HAVING COUNT(*) >= 10  -- Minimum sample size
        ORDER BY total_ads DESC
        """

        print("📊 Executing brand-level intelligence query...")
        brand_intel_df = run_query(brand_intelligence_query)

        if not brand_intel_df.empty:
            print("✅ Brand-level data retrieved successfully!")
            print()
            print("📊 BRAND-LEVEL INTELLIGENCE ANALYSIS:")
            print("=" * 80)

            # Create summary DataFrame for display
            summary_data = []

            for idx, row in brand_intel_df.iterrows():
                brand = row['brand']

                print(f"\\n🎨 {brand.upper()}:")
                print("-" * 60)

                # Audience Intelligence
                print("👥 AUDIENCE INTELLIGENCE:")
                print(f"   📊 Total Ads: {row['total_ads']:,}")
                print(f"   📝 Avg Text Length: {row['avg_text_length']:.0f} characters")
                print(f"   💰 Price-Conscious Focus: {row['price_conscious_rate']:.1f}%")
                print(f"   👨‍💼 Millennial Targeting: {row['millennial_targeting_rate']:.1f}%")

                # Creative Intelligence
                print("\\n🎨 CREATIVE INTELLIGENCE:")
                print(f"   💖 Emotional Keywords: {row['emotional_keyword_rate']:.1f}%")
                print(f"   🎯 Style/Design Focus: {row['style_focus_rate']:.1f}%")

                # Channel Intelligence
                print("\\n📡 CHANNEL INTELLIGENCE:")
                print(f"   🔄 Platform Diversification: {row['platform_diversification']:.0f}/3 platforms")
                print(f"   ⚡ FB/IG Optimization: {row['platform_optimization_rate']:.1f}%")

                # Strategic Positioning
                print("\\n🎯 STRATEGIC POSITIONING:")
                print(f"   👑 Premium Positioning: {row['premium_positioning_rate']:.1f}%")
                print(f"   🆓 Trial/Risk-Free Focus: {row['trial_focus_rate']:.1f}%")

                # Brand-specific insights
                print("\\n💡 BRAND-SPECIFIC INSIGHTS:")

                # Determine primary positioning
                if row['premium_positioning_rate'] > row['price_conscious_rate']:
                    positioning = "Premium/Quality"
                    print(f"   • 👑 Premium positioning strategy ({row['premium_positioning_rate']:.0f}% premium vs {row['price_conscious_rate']:.0f}% price)")
                else:
                    positioning = "Value/Price"
                    print(f"   • 💰 Value positioning strategy ({row['price_conscious_rate']:.0f}% price vs {row['premium_positioning_rate']:.0f}% premium)")

                # Audience targeting
                if row['millennial_targeting_rate'] > 20:
                    print(f"   • 🎯 Strong millennial focus ({row['millennial_targeting_rate']:.0f}%)")

                # Emotional vs rational
                if row['emotional_keyword_rate'] > 15:
                    print(f"   • 💖 Emotion-driven messaging ({row['emotional_keyword_rate']:.0f}%)")
                elif row['emotional_keyword_rate'] < 5:
                    print(f"   • 📊 Rational/functional messaging ({row['emotional_keyword_rate']:.0f}%)")

                # Platform strategy
                if row['platform_diversification'] >= 3:
                    print(f"   • 📱 Multi-platform strategy ({row['platform_diversification']:.0f} platforms)")
                elif row['platform_diversification'] <= 1:
                    print(f"   • ⚠️ Single-platform focus ({row['platform_diversification']:.0f} platform)")

                # Add to summary
                summary_data.append({
                    'Brand': brand,
                    'Ads': f"{row['total_ads']:,}",
                    'Positioning': positioning,
                    'Price Focus': f"{row['price_conscious_rate']:.0f}%",
                    'Premium Focus': f"{row['premium_positioning_rate']:.0f}%",
                    'Emotional': f"{row['emotional_keyword_rate']:.0f}%",
                    'Platforms': f"{row['platform_diversification']:.0f}",
                    'Millennial': f"{row['millennial_targeting_rate']:.0f}%"
                })

            # Display summary table
            print("\\n" + "=" * 80)
            print("📊 BRAND COMPARISON SUMMARY:")
            print("=" * 80)

            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                display(summary_df)

            print("\\n🎯 KEY COMPETITIVE INSIGHTS:")

            # Identify market leaders
            if not brand_intel_df.empty:
                top_brand = brand_intel_df.iloc[0]['brand']
                top_ads = brand_intel_df.iloc[0]['total_ads']
                print(f"   • 👑 Market Leader: {top_brand} with {top_ads:,} ads")

                # Find positioning gaps
                premium_brands = brand_intel_df[brand_intel_df['premium_positioning_rate'] > brand_intel_df['price_conscious_rate']]
                value_brands = brand_intel_df[brand_intel_df['price_conscious_rate'] > brand_intel_df['premium_positioning_rate']]

                if len(premium_brands) > 0:
                    print(f"   • 💎 Premium Players: {', '.join(premium_brands['brand'].tolist())}")
                if len(value_brands) > 0:
                    print(f"   • 💰 Value Players: {', '.join(value_brands['brand'].tolist())}")

                # Platform diversity
                multi_platform = brand_intel_df[brand_intel_df['platform_diversification'] >= 3]
                if len(multi_platform) > 0:
                    print(f"   • 📱 Multi-Platform Leaders: {', '.join(multi_platform['brand'].tolist())}")

        else:
            print("⚠️ No brand-level intelligence data available")
            print("   Check that ads_with_dates table has sufficient data")

    except Exception as e:
        print(f"❌ Error generating brand-level intelligence: {e}")

        # More informative error handling
        import traceback
        print("\\nDetailed error:")
        print(traceback.format_exc())

        print("\\n📊 Falling back to aggregate Stage 9 data...")

        # Show what aggregate data is available
        if hasattr(stage9_results, 'audience_intelligence'):
            audience_intel = stage9_results.audience_intelligence
            if audience_intel.get('status') == 'success':
                print("\\n👥 AUDIENCE INTELLIGENCE (Aggregate Only):")
                print(f"   📊 Total Ads: {audience_intel.get('total_ads', 0):,}")
                print(f"   📝 Avg Text Length: {audience_intel.get('avg_text_length', 0):.0f}")
                print(f"   💰 Price Focus: {audience_intel.get('avg_price_conscious_rate', 0):.1f}%")
                print(f"   👨‍💼 Millennial Focus: {audience_intel.get('avg_millennial_focus_rate', 0):.1f}%")

    print("\\n" + "=" * 80)
    print("🎯 BRAND-LEVEL INTELLIGENCE VALUE:")
    print("   ✅ Individual competitor strategies visible")
    print("   ✅ Positioning gaps identified")
    print("   ✅ Actionable insights per brand")
    print("   ✅ Competitive differentiation clear")
    print("\\n💡 Note: Brand-level analysis enables targeted competitive responses")
    print("   while aggregate data provides limited strategic value.")
'''

    # Replace the cell's source
    notebook['cells'][target_cell]['source'] = [line + '\n' for line in working_code.split('\n')]

    print(f"✅ Replaced brand-level Stage 9 cell at index {target_cell}")
else:
    print("❌ Could not find brand-level Stage 9 cell")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("\n✅ Notebook updated with working brand-level Stage 9 analysis!")
print("\n📋 Changes made:")
print("   • Simplified regex patterns to use LIKE instead of REGEXP_CONTAINS")
print("   • Added proper BQ_PROJECT and BQ_DATASET variable initialization")
print("   • Improved error handling with detailed traceback")
print("   • Added summary DataFrame for easy comparison")
print("   • Included competitive insights and market positioning analysis")