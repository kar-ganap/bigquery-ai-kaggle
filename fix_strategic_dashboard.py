#!/usr/bin/env python3
"""
Fix Strategic Analysis Dashboard cell AttributeError
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Updated Strategic Analysis Dashboard cell
fixed_dashboard_cell = '''# Strategic Analysis Dashboard

print("🎯 STRATEGIC ANALYSIS DASHBOARD")
print("=" * 50)

if 'stage8_results' not in locals() or stage8_results is None:
    print("❌ Stage 8 analysis not available - run Stage 8 first")
else:
    print(f"✅ Strategic Analysis Status: {stage8_results.status}")

    # Check what attributes are available and display appropriately
    available_attrs = [attr for attr in dir(stage8_results) if not attr.startswith('_')]
    print(f"📋 Analysis completed successfully")
    print()

    # 1. CURRENT STATE ANALYSIS
    print("🔍 1. CURRENT COMPETITIVE STATE")
    print("-" * 40)

    if 'positioning_df' in locals() and not positioning_df.empty:
        target_data = positioning_df[positioning_df['brand'] == context.brand]
        total_competitors = len(positioning_df) - 1

        if not target_data.empty:
            target_rank = int(target_data.iloc[0]['rank_position'])
            target_score = target_data.iloc[0]['avg_cta_aggressiveness']
            target_category = target_data.iloc[0]['market_position']

            print(f"🎯 {context.brand} Current Position:")
            print(f"   • Market Rank: #{target_rank} of {len(positioning_df)} brands")
            print(f"   • Aggressiveness Score: {target_score:.1f}/10")
            print(f"   • Market Category: {target_category}")
            print(f"   • Total Competitors Analyzed: {total_competitors}")

            # Market context
            market_median = positioning_df['avg_cta_aggressiveness'].median()
            market_avg = positioning_df['avg_cta_aggressiveness'].mean()

            print(f"\\n📊 Market Context:")
            print(f"   • Market Median: {market_median:.1f}/10")
            print(f"   • Market Average: {market_avg:.1f}/10")
            print(f"   • Gap vs Median: {target_score - market_median:+.1f} points")
            print(f"   • Gap vs Average: {target_score - market_avg:+.1f} points")

    print()

    # 2. COMPETITIVE THREATS
    print("🚨 2. COMPETITIVE THREAT ANALYSIS")
    print("-" * 40)

    if 'positioning_df' in locals() and not positioning_df.empty:
        target_score = positioning_df[positioning_df['brand'] == context.brand]['avg_cta_aggressiveness'].iloc[0]
        threats = positioning_df[
            (positioning_df['brand'] != context.brand) &
            (positioning_df['avg_cta_aggressiveness'] > target_score)
        ].sort_values('avg_cta_aggressiveness', ascending=False)

        if not threats.empty:
            print(f"⚠️ {len(threats)} competitors more aggressive than {context.brand}:")
            for i, (_, threat) in enumerate(threats.head(5).iterrows(), 1):
                gap = threat['avg_cta_aggressiveness'] - target_score
                threat_level = "🔴 HIGH" if gap > 2.0 else "🟡 MEDIUM" if gap > 1.0 else "🟢 LOW"
                print(f"   {i}. {threat['brand']}: {threat['avg_cta_aggressiveness']:.1f}/10 (+{gap:.1f}) {threat_level}")
        else:
            print("✅ No competitors more aggressive - market leadership position")

    print()

    # 3. STRATEGIC OPPORTUNITIES
    print("💡 3. STRATEGIC OPPORTUNITIES")
    print("-" * 40)

    if 'positioning_df' in locals() and not positioning_df.empty:
        opportunities = positioning_df[
            (positioning_df['brand'] != context.brand) &
            (positioning_df['avg_cta_aggressiveness'] < target_score)
        ].sort_values('avg_cta_aggressiveness', ascending=True)

        if not opportunities.empty:
            print(f"📈 {len(opportunities)} competitors less aggressive - potential gaps to exploit:")
            for i, (_, opp) in enumerate(opportunities.head(5).iterrows(), 1):
                gap = target_score - opp['avg_cta_aggressiveness']
                opp_level = "🟢 HIGH" if gap > 2.0 else "🟡 MEDIUM" if gap > 1.0 else "🔵 LOW"
                print(f"   {i}. {opp['brand']}: {opp['avg_cta_aggressiveness']:.1f}/10 (-{gap:.1f}) {opp_level}")
        else:
            print("⚠️ All competitors more aggressive - defensive positioning needed")

    print()

    # 4. STRATEGIC RECOMMENDATIONS
    print("🎖️ 4. STRATEGIC RECOMMENDATIONS")
    print("-" * 40)

    if 'positioning_df' in locals() and not positioning_df.empty:
        target_data = positioning_df[positioning_df['brand'] == context.brand]
        if not target_data.empty:
            target_score = target_data.iloc[0]['avg_cta_aggressiveness']
            target_rank = int(target_data.iloc[0]['rank_position'])
            market_median = positioning_df['avg_cta_aggressiveness'].median()

            print("Based on competitive analysis:")

            if target_rank <= 3:
                print("🏆 MARKET LEADER STRATEGY:")
                print("   • Maintain aggressive positioning")
                print("   • Monitor competitor escalation")
                print("   • Test premium/consultative approaches")
            elif target_score > market_median:
                print("📈 AGGRESSIVE PLAYER STRATEGY:")
                print("   • Leverage above-median positioning")
                print("   • Differentiate through message quality")
                print("   • Monitor customer fatigue signals")
            elif target_score < market_median:
                print("🔄 CATCH-UP STRATEGY:")
                print("   • Consider increasing CTA aggressiveness")
                print("   • Test urgency-driven messaging")
                print("   • Analyze competitor successful tactics")
            else:
                print("⚖️ BALANCED STRATEGY:")
                print("   • Well-positioned at market median")
                print("   • Focus on execution quality")
                print("   • Test selective aggressive campaigns")

print("\\n" + "=" * 50)
print("📊 Dashboard Complete - Strategic insights generated")
'''

# Find and replace the Strategic Analysis Dashboard cell
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Strategic Analysis Dashboard cell
        if 'Strategic Analysis Dashboard' in source_text and 'stage8_results.message' in source_text:
            print("Found and fixing Strategic Analysis Dashboard cell...")

            # Replace the entire cell content
            cell['source'] = fixed_dashboard_cell.splitlines(keepends=True)
            print("Fixed AttributeError and enhanced dashboard!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\\n🎯 Fixed Strategic Analysis Dashboard:")
print("   • Removed reference to non-existent 'message' attribute")
print("   • Enhanced competitive threat analysis")
print("   • Added strategic opportunity identification")
print("   • Included actionable recommendations")
print("   • Improved market context analysis")