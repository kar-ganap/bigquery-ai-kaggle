#!/usr/bin/env python3
"""
Fix Stage 9 dashboard cell replacement
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Comprehensive Stage 9 dashboard cell content
comprehensive_dashboard = '''# 🎯 MULTI-DIMENSIONAL INTELLIGENCE - COMPREHENSIVE COMPETITIVE DASHBOARD

print("🎯 MULTI-DIMENSIONAL INTELLIGENCE - COMPREHENSIVE COMPETITIVE DASHBOARD")
print("=" * 80)

if stage9_results is None:
    print("❌ No multi-dimensional intelligence results found")
    print("   Make sure you ran Stage 9 Multi-Dimensional Intelligence first")
    print("   Check the output above for any errors")
else:
    print(f"✅ Intelligence Status: {stage9_results.status}")
    print(f"📊 Data Completeness: {stage9_results.data_completeness:.1f}%")
    print()

    # === 1. AUDIENCE INTELLIGENCE DASHBOARD ===
    print("👥 === AUDIENCE INTELLIGENCE ANALYSIS ===")
    print("-" * 50)

    audience_intel = stage9_results.audience_intelligence
    if audience_intel.get('status') == 'success':
        print(f"📊 Total Ads Analyzed: {audience_intel.get('total_ads', 0):,}")
        print(f"🔄 Cross-Platform Strategy: {audience_intel.get('avg_cross_platform_rate', 0):.1f}%")
        print(f"📝 Average Text Length: {audience_intel.get('avg_text_length', 0):.0f} characters")
        print(f"💰 Price-Conscious Focus: {audience_intel.get('avg_price_conscious_rate', 0):.1f}%")
        print(f"👨‍💼 Millennial Targeting: {audience_intel.get('avg_millennial_focus_rate', 0):.1f}%")
        print()
        print(f"🎯 Dominant Strategies:")
        print(f"   📱 Platform: {audience_intel.get('most_common_platform_strategy', 'Unknown')}")
        print(f"   💬 Communication: {audience_intel.get('most_common_communication_style', 'Unknown')}")
        print(f"   🧠 Psychographic: {audience_intel.get('most_common_psychographic', 'Unknown')}")
        print(f"   👥 Age Group: {audience_intel.get('most_common_age_group', 'Unknown')}")
    else:
        print(f"⚠️ Audience Intelligence: {audience_intel.get('error', 'Analysis incomplete')}")

    print()

    # === 2. CREATIVE INTELLIGENCE DASHBOARD ===
    print("🎨 === CREATIVE INTELLIGENCE ANALYSIS ===")
    print("-" * 50)

    creative_intel = stage9_results.creative_intelligence
    if creative_intel.get('status') == 'success':
        print(f"📊 Total Ads Analyzed: {creative_intel.get('total_ads', 0):,}")
        print(f"📝 Average Text Length: {creative_intel.get('avg_text_length', 0):.0f} characters")
        print(f"🏷️ Brand Mentions/Ad: {creative_intel.get('avg_brand_mentions', 0):.1f}")
        print(f"💖 Emotional Keywords/Ad: {creative_intel.get('avg_emotional_keywords', 0):.1f}")
        print(f"📈 Creative Density: {creative_intel.get('avg_creative_density', 0):.1f}%")

        # AI-Enhanced Sentiment Metrics
        if 'avg_ai_emotional_intensity' in creative_intel:
            print()
            print(f"🤖 AI-Enhanced Sentiment Analysis:")
            print(f"   🔥 Emotional Intensity: {creative_intel.get('avg_ai_emotional_intensity', 0):.1f}/10")
            print(f"   🎯 Industry Relevance: {creative_intel.get('avg_ai_industry_relevance', 0):.2f}")
            print(f"   😊 Positive Sentiment: {creative_intel.get('ai_positive_sentiment_rate', 0):.1f}%")
            print(f"   ✨ Aspirational Content: {creative_intel.get('ai_aspirational_sentiment_rate', 0):.1f}%")
            print(f"   🏖️ Lifestyle Approach: {creative_intel.get('ai_lifestyle_style_rate', 0):.1f}%")
            print(f"   💎 Premium Positioning: {creative_intel.get('ai_premium_style_rate', 0):.1f}%")

        print()
        print(f"🎯 Dominant Creative Strategies:")
        print(f"   📢 Messaging Theme: {creative_intel.get('dominant_messaging_theme', 'Unknown')}")
        print(f"   💭 Emotional Tone: {creative_intel.get('dominant_emotional_tone', 'Unknown')}")

        # Fix the TypeError by checking if brands_analyzed is int or list
        brands_count = creative_intel.get('brands_analyzed', 0)
        if isinstance(brands_count, list):
            brands_display = len(brands_count)
        else:
            brands_display = brands_count
        print(f"   👥 Brands Analyzed: {brands_display}")
    else:
        print(f"⚠️ Creative Intelligence: {creative_intel.get('error', 'Analysis incomplete')}")

    print()

    # === 3. CHANNEL INTELLIGENCE DASHBOARD ===
    print("📡 === CHANNEL INTELLIGENCE ANALYSIS ===")
    print("-" * 50)

    channel_intel = stage9_results.channel_intelligence
    if channel_intel.get('status') == 'success':
        print(f"📊 Total Ads Analyzed: {channel_intel.get('total_ads', 0):,}")
        print(f"🔄 Platform Diversification: {channel_intel.get('avg_platform_diversification', 0):.1f}/3")
        print(f"🎯 Cross-Platform Synergy: {channel_intel.get('cross_platform_synergy_rate', 0):.1f}%")
        print(f"⚡ Platform Optimization: {channel_intel.get('platform_optimization_rate', 0):.1f}%")
        print()
        print(f"🎯 Dominant Channel Strategies:")
        print(f"   📱 Platform Strategy: {channel_intel.get('dominant_platform_strategy', 'Unknown')}")
        print(f"   🎨 Channel Focus: {channel_intel.get('dominant_channel_focus', 'Unknown')}")
    else:
        print(f"⚠️ Channel Intelligence: {channel_intel.get('error', 'Analysis incomplete')}")

    print()

    # === 4. VISUAL INTELLIGENCE DASHBOARD ===
    print("🎨 === VISUAL INTELLIGENCE METRICS ===")
    print("-" * 50)

    visual_intel = stage9_results.visual_intelligence
    if visual_intel.get('status') == 'success':
        print(f"📊 Visual Ads Analyzed: {visual_intel.get('total_visual_ads', 0):,}")
        print(f"🎯 Visual-Text Alignment: {visual_intel.get('avg_visual_text_alignment', 0):.1f}/10")
        print(f"🏷️ Brand Consistency: {visual_intel.get('avg_brand_consistency', 0):.1f}/10")
        print(f"⚠️ Creative Fatigue Risk: {visual_intel.get('avg_creative_fatigue_risk', 0):.1f}/10")
        print(f"💎 Luxury Positioning: {visual_intel.get('avg_luxury_positioning', 0):.1f}/10")
        print(f"💪 Visual Boldness: {visual_intel.get('avg_boldness', 0):.1f}/10")
        print(f"🔄 Visual Differentiation: {visual_intel.get('avg_visual_differentiation', 0):.1f}/10")
        print(f"🎭 Pattern Risk Score: {visual_intel.get('avg_creative_pattern_risk', 0):.1f}/10")
    else:
        print(f"⚠️ Visual Intelligence: {visual_intel.get('error', 'Analysis incomplete')}")

    print()

    # === 5. WHITESPACE INTELLIGENCE DASHBOARD ===
    print("🎯 === WHITESPACE INTELLIGENCE ANALYSIS ===")
    print("-" * 50)

    whitespace_intel = stage9_results.whitespace_intelligence
    if whitespace_intel.get('status') == 'success':
        opportunities_found = whitespace_intel.get('opportunities_found', 0)
        print(f"🔍 Strategic Opportunities Found: {opportunities_found}")

        if opportunities_found > 0:
            print(f"📈 Analysis Summary: {whitespace_intel.get('analysis_summary', 'No summary available')}")
            print()
            print(f"🏆 Top Strategic Opportunities:")

            top_opportunities = whitespace_intel.get('top_opportunities', [])
            for i, opportunity in enumerate(top_opportunities[:5], 1):
                if isinstance(opportunity, dict):
                    summary = opportunity.get('strategic_summary', str(opportunity)[:80])
                    print(f"   {i}. {summary}")
                else:
                    print(f"   {i}. {opportunity}")

            print()
            print(f"💡 Strategic Recommendations:")
            recommendations = whitespace_intel.get('strategic_recommendations', [])
            for i, rec in enumerate(recommendations[:3], 1):
                if isinstance(rec, dict):
                    rec_text = rec.get('recommendation', str(rec)[:80])
                else:
                    rec_text = str(rec)
                print(f"   {i}. {rec_text}")

            # Performance metrics
            performance = whitespace_intel.get('performance_metrics', {})
            if performance:
                duration = performance.get('duration_seconds', 0)
                approach = performance.get('approach', 'Unknown')
                print()
                print(f"⚡ Performance Metrics:")
                print(f"   ⏱️ Analysis Duration: {duration:.1f}s")
                print(f"   🔧 Detection Method: {approach.replace('_', ' ').title()}")
                print(f"   📊 Coverage: {performance.get('coverage', 'Unknown')}")
        else:
            print(f"📊 Market appears well-covered by competitors")
            print(f"💡 Focus on differentiation and execution quality")

    else:
        print(f"⚠️ Whitespace Intelligence: {whitespace_intel.get('error', 'Analysis incomplete')}")

    print()

    # === 6. STRATEGIC SUMMARY ===
    print("🧠 === STRATEGIC INTELLIGENCE SUMMARY ===")
    print("-" * 50)

    # Calculate overall intelligence score
    intelligence_modules = [
        ('Audience', audience_intel.get('status') == 'success'),
        ('Creative', creative_intel.get('status') == 'success'),
        ('Channel', channel_intel.get('status') == 'success'),
        ('Visual', visual_intel.get('status') == 'success'),
        ('Whitespace', whitespace_intel.get('status') == 'success')
    ]

    successful_modules = sum(1 for _, success in intelligence_modules if success)
    intelligence_coverage = (successful_modules / len(intelligence_modules)) * 100

    print(f"📊 Intelligence Coverage: {intelligence_coverage:.0f}% ({successful_modules}/{len(intelligence_modules)} modules)")
    print(f"📈 Data Quality: {stage9_results.data_completeness:.1f}% complete")
    print()

    print(f"🎯 Key Strategic Insights:")

    # Audience insights
    if audience_intel.get('status') == 'success':
        cross_platform = audience_intel.get('avg_cross_platform_rate', 0)
        if cross_platform > 70:
            print(f"   📱 High cross-platform adoption ({cross_platform:.0f}%) indicates mature multi-channel strategies")
        elif cross_platform < 30:
            print(f"   📱 Low cross-platform adoption ({cross_platform:.0f}%) suggests single-channel focus opportunities")

    # Creative insights
    if creative_intel.get('status') == 'success':
        emotional_intensity = creative_intel.get('avg_ai_emotional_intensity', 0)
        if emotional_intensity > 7:
            print(f"   💖 High emotional intensity ({emotional_intensity:.1f}/10) indicates emotion-driven market")
        elif emotional_intensity < 4:
            print(f"   💖 Low emotional intensity ({emotional_intensity:.1f}/10) suggests rational/functional messaging")

    # Channel insights
    if channel_intel.get('status') == 'success':
        diversification = channel_intel.get('avg_platform_diversification', 0)
        if diversification > 2:
            print(f"   🔄 High platform diversification ({diversification:.1f}/3) shows sophisticated channel strategy")
        elif diversification < 1:
            print(f"   🔄 Low platform diversification ({diversification:.1f}/3) indicates focused channel approach")

    # Whitespace insights
    if whitespace_intel.get('status') == 'success':
        opportunities = whitespace_intel.get('opportunities_found', 0)
        if opportunities > 10:
            print(f"   🎯 Many opportunities found ({opportunities}) suggests fragmented market with gaps")
        elif opportunities < 3:
            print(f"   🎯 Few opportunities found ({opportunities}) indicates saturated/mature market")

    print()
    print(f"🏆 Multi-Dimensional Intelligence Analysis Complete!")
    print(f"📊 Ready for business strategy development and tactical execution")

print("\\n" + "=" * 80)
print("📊 Comprehensive Intelligence Dashboard Complete")'''

# Find and replace the Stage 9 dashboard cell (look for the specific title)
cell_found = False
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Look for the specific dashboard cell with less restrictive matching
        if ('MULTI-DIMENSIONAL INTELLIGENCE - COMPREHENSIVE COMPETITIVE DASHBOARD' in source_text and
            'stage9_results.message' in source_text):

            print("Found Stage 9 dashboard cell - replacing with comprehensive version...")

            # Replace with comprehensive dashboard
            cell['source'] = comprehensive_dashboard.splitlines(keepends=True)
            print("✅ Enhanced Stage 9 dashboard successfully!")
            cell_found = True
            break

if not cell_found:
    print("❌ Could not find Stage 9 dashboard cell to replace")
    print("Looking for cells containing the dashboard title...")

    for i, cell in enumerate(notebook['cells']):
        if cell['cell_type'] == 'code' and 'source' in cell:
            source_text = ''.join(cell['source'])
            if 'MULTI-DIMENSIONAL INTELLIGENCE' in source_text:
                print(f"Found cell {i} with MULTI-DIMENSIONAL INTELLIGENCE content")
                print(f"First 200 chars: {source_text[:200]}")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")