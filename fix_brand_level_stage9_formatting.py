#!/usr/bin/env python3
"""
Fix the formatting of the brand-level Stage 9 cell that was added as one long line
"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the malformed cell
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        # Check if this is the malformed cell (all code in one line)
        if len(cell['source']) > 0 and 'BRAND-LEVEL STAGE 9 INTELLIGENCE DASHBOARD' in ''.join(cell['source']):
            source_text = ''.join(cell['source'])

            # Check if it's the malformed version (likely one very long line)
            if 'print("🎯 BRAND-LEVEL' in source_text and len(cell['source']) < 10:
                print(f"Found malformed cell at index {i} with {len(cell['source'])} lines")

                # Properly formatted code
                proper_code = """# === 🎯 BRAND-LEVEL STAGE 9 INTELLIGENCE DASHBOARD ===

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

        # Query to get brand-level intelligence data
        brand_intelligence_query = f\"\"\"
        WITH brand_intelligence AS (
          SELECT
            brand,
            COUNT(*) as total_ads,

            -- Audience Intelligence per Brand
            AVG(CASE WHEN platform = 'ALL' THEN 1.0 ELSE 0.0 END) * 100 as cross_platform_rate,
            AVG(LENGTH(ad_text)) as avg_text_length,
            AVG(CASE WHEN REGEXP_CONTAINS(LOWER(ad_text), r'\\b(price|cost|affordable|cheap|budget|value|save|discount|deal)\\b') THEN 1.0 ELSE 0.0 END) * 100 as price_conscious_rate,
            AVG(CASE WHEN REGEXP_CONTAINS(LOWER(ad_text), r'\\b(millennial|young professional|career|lifestyle|trendy|modern)\\b') THEN 1.0 ELSE 0.0 END) * 100 as millennial_targeting_rate,

            -- Creative Intelligence per Brand
            AVG(CASE WHEN REGEXP_CONTAINS(LOWER(ad_text), r'\\b(eye|vision|sight|see|look|view)\\b') THEN 1.0 ELSE 0.0 END) as brand_mention_rate,
            AVG(CASE WHEN REGEXP_CONTAINS(LOWER(ad_text), r'\\b(love|amazing|perfect|beautiful|stunning|gorgeous|incredible)\\b') THEN 1.0 ELSE 0.0 END) as emotional_keyword_rate,
            COUNT(DISTINCT REGEXP_EXTRACT(LOWER(ad_text), r'\\b(style|design|fashion|trend|look|aesthetic)\\w*')) / COUNT(*) as creative_density,

            -- Channel Intelligence per Brand
            COUNT(DISTINCT platform) as platform_diversification,
            AVG(CASE WHEN platform IN ('FACEBOOK', 'INSTAGRAM') THEN 1.0 ELSE 0.0 END) * 100 as platform_optimization_rate,

            -- Strategic Positioning per Brand
            AVG(CASE WHEN REGEXP_CONTAINS(LOWER(ad_text), r'\\b(quality|premium|luxury|high-end|superior)\\b') THEN 1.0 ELSE 0.0 END) * 100 as premium_positioning_rate,
            AVG(CASE WHEN REGEXP_CONTAINS(LOWER(ad_text), r'\\b(try|free|risk|guarantee|return|satisfaction)\\b') THEN 1.0 ELSE 0.0 END) * 100 as trial_focus_rate,

            -- Competitive Differentiation per Brand
            COUNT(DISTINCT REGEXP_EXTRACT(LOWER(ad_text), r'\\b(unique|different|exclusive|only|first|patented)\\w*')) / COUNT(*) as differentiation_signals

          FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
          WHERE brand IS NOT NULL
          GROUP BY brand
          HAVING COUNT(*) >= 10  -- Minimum sample size
        )

        SELECT
          brand,
          total_ads,
          cross_platform_rate,
          avg_text_length,
          price_conscious_rate,
          millennial_targeting_rate,
          brand_mention_rate * 100 as brand_mention_percentage,
          emotional_keyword_rate * 100 as emotional_keyword_percentage,
          creative_density * 100 as creative_density_percentage,
          platform_diversification,
          platform_optimization_rate,
          premium_positioning_rate,
          trial_focus_rate,
          differentiation_signals * 100 as differentiation_percentage
        FROM brand_intelligence
        ORDER BY total_ads DESC
        \"\"\"

        brand_intel_df = run_query(brand_intelligence_query)

        if not brand_intel_df.empty:
            print("📊 BRAND-LEVEL INTELLIGENCE ANALYSIS:")
            print("=" * 80)

            for idx, brand_row in brand_intel_df.iterrows():
                brand = brand_row['brand']

                print(f"\\n🎨 {brand.upper()}:")
                print("-" * 60)

                # Audience Intelligence
                print("👥 AUDIENCE INTELLIGENCE:")
                print(f"   📊 Total Ads: {brand_row['total_ads']:,}")
                print(f"   🔄 Cross-Platform Strategy: {brand_row['cross_platform_rate']:.1f}%")
                print(f"   📝 Avg Text Length: {brand_row['avg_text_length']:.0f} characters")
                print(f"   💰 Price-Conscious Focus: {brand_row['price_conscious_rate']:.1f}%")
                print(f"   👨‍💼 Millennial Targeting: {brand_row['millennial_targeting_rate']:.1f}%")

                # Creative Intelligence
                print("\\n🎨 CREATIVE INTELLIGENCE:")
                print(f"   🏷️ Brand Mentions: {brand_row['brand_mention_percentage']:.1f}%")
                print(f"   💖 Emotional Keywords: {brand_row['emotional_keyword_percentage']:.1f}%")
                print(f"   📈 Creative Density: {brand_row['creative_density_percentage']:.1f}%")

                # Channel Intelligence
                print("\\n📡 CHANNEL INTELLIGENCE:")
                print(f"   🔄 Platform Diversification: {brand_row['platform_diversification']:.0f}/3 platforms")
                print(f"   ⚡ Platform Optimization: {brand_row['platform_optimization_rate']:.1f}%")

                # Strategic Positioning
                print("\\n🎯 STRATEGIC POSITIONING:")
                print(f"   👑 Premium Positioning: {brand_row['premium_positioning_rate']:.1f}%")
                print(f"   🆓 Trial/Risk-Free Focus: {brand_row['trial_focus_rate']:.1f}%")
                print(f"   🔥 Differentiation Signals: {brand_row['differentiation_percentage']:.1f}%")

                # Brand-specific insights
                print("\\n💡 BRAND-SPECIFIC INSIGHTS:")

                # Audience insights
                if brand_row['millennial_targeting_rate'] > 60:
                    print("   • 🎯 Strong millennial focus - optimize for career/lifestyle messaging")
                if brand_row['price_conscious_rate'] > 30:
                    print("   • 💰 Price-sensitive positioning - emphasize value proposition")
                elif brand_row['price_conscious_rate'] < 10:
                    print("   • 👑 Premium/quality focus - avoid price competition")

                # Creative insights
                if brand_row['creative_density_percentage'] > 15:
                    print("   • 🎨 High creative diversity - strong style differentiation")
                elif brand_row['creative_density_percentage'] < 5:
                    print("   • ⚠️ Low creative variety - opportunity for style expansion")

                if brand_row['emotional_keyword_percentage'] > 20:
                    print("   • 💖 Emotion-driven messaging - strong aspirational appeal")
                elif brand_row['emotional_keyword_percentage'] < 5:
                    print("   • 📊 Rational messaging - opportunity for emotional connection")

                # Channel insights
                if brand_row['platform_diversification'] >= 3:
                    print("   • 📱 Multi-platform strategy - good reach diversification")
                elif brand_row['platform_diversification'] <= 1:
                    print("   • ⚠️ Single-platform dependency - expansion opportunity")

                # Differentiation insights
                if brand_row['differentiation_percentage'] > 10:
                    print("   • 🔥 Strong differentiation messaging - clear unique positioning")
                elif brand_row['differentiation_percentage'] < 2:
                    print("   • ⚠️ Weak differentiation - risk of commodity positioning")

                # Competitive recommendations
                print("\\n📋 STRATEGIC RECOMMENDATIONS:")

                # Based on positioning
                if brand_row['premium_positioning_rate'] > brand_row['price_conscious_rate']:
                    print("   • 🎯 Focus: Premium brand positioning with quality emphasis")
                    print("   • 💡 Opportunity: Leverage quality/luxury messaging further")
                else:
                    print("   • 🎯 Focus: Value-driven positioning with cost benefits")
                    print("   • 💡 Opportunity: Strengthen price-value proposition")

                # Based on creative strategy
                if brand_row['creative_density_percentage'] < 10:
                    print("   • 🎨 Action: Increase creative variety to stand out from competition")

                # Based on channel strategy
                if brand_row['platform_diversification'] < 2:
                    print("   • 📱 Action: Expand to additional platforms for broader reach")

        else:
            print("⚠️ No brand-level intelligence data available")

    except Exception as e:
        print(f"❌ Error generating brand-level intelligence: {e}")
        print("Falling back to available Stage 9 aggregate data...")

        # Fallback to existing aggregate data but present it differently
        audience_intel = stage9_results.audience_intelligence
        creative_intel = stage9_results.creative_intelligence
        channel_intel = stage9_results.channel_intelligence

        print("\\n📊 AVAILABLE AGGREGATE DATA (Limited Brand Insights):")
        print("=" * 60)

        if audience_intel.get('status') == 'success':
            print("👥 AUDIENCE INTELLIGENCE (All Brands):")
            print(f"   📊 Total Ads: {audience_intel.get('total_ads', 0):,}")
            print(f"   📝 Avg Text Length: {audience_intel.get('avg_text_length', 0):.0f} characters")
            print(f"   💰 Price Focus: {audience_intel.get('avg_price_conscious_rate', 0):.1f}%")
            print(f"   👨‍💼 Millennial Focus: {audience_intel.get('avg_millennial_focus_rate', 0):.1f}%")

        if creative_intel.get('status') == 'success':
            print("\\n🎨 CREATIVE INTELLIGENCE (All Brands):")
            print(f"   📈 Creative Density: {creative_intel.get('avg_creative_density', 0):.1f}%")
            print(f"   🔥 Emotional Intensity: {creative_intel.get('avg_ai_emotional_intensity', 0):.1f}/10")

        if channel_intel.get('status') == 'success':
            print("\\n📡 CHANNEL INTELLIGENCE (All Brands):")
            print(f"   🔄 Platform Diversification: {channel_intel.get('avg_platform_diversification', 0):.1f}/3")
            print(f"   🎯 Cross-Platform Synergy: {channel_intel.get('cross_platform_synergy_rate', 0):.1f}%")

        print("\\n⚠️ LIMITATION: Aggregate data doesn't provide actionable brand-specific insights")
        print("💡 RECOMMENDATION: Implement proper brand-level intelligence breakdown")

    print("\\n" + "=" * 80)
    print("🎯 BRAND-LEVEL INTELLIGENCE SUMMARY:")
    print("   ✅ Individual brand strategies identified")
    print("   ✅ Competitive positioning analysis per brand")
    print("   ✅ Brand-specific opportunity identification")
    print("   ✅ Actionable strategic recommendations by competitor")
    print("   ✅ Differentiation and whitespace opportunities highlighted")
    print("\\n💡 Strategic Value: Brand-level intelligence enables precise competitive responses")
    print("   rather than generic market-wide insights that lack strategic specificity.")"""

                # Replace the cell's source with properly formatted lines
                cell['source'] = [line + '\n' for line in proper_code.split('\n')]

                print(f"✅ Fixed formatting for brand-level Stage 9 cell")
                original_lines = len(source_text.split('\n'))
                new_lines = len(cell['source'])
                print(f"   Changed from {original_lines} lines to {new_lines} lines")
                break

# Save the fixed notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("\n✅ Fixed notebook formatting for brand-level Stage 9 analysis!")