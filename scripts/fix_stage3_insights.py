#!/usr/bin/env python3
"""Fix Stage 3 insights cell to handle string/int conversion and correct attributes"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Find the Stage 3 insights cell and fix it
for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') == 'code':
        source = ''.join(cell.get('source', []))
        if 'META AD ACTIVITY INSIGHTS' in source and 'total_estimated_ads' in source:
            print(f"Found Stage 3 insights cell at index {i}")

            # Fixed content using correct attribute names and safe type conversion
            new_content = '''# Meta Ad Activity Insights and Next Steps
if ranked_competitors:
    print("💡 META AD ACTIVITY INSIGHTS")
    print("=" * 35)

    # Competitive landscape analysis - safely handle string/int conversion
    estimated_ads_list = []
    for c in ranked_competitors:
        ads = getattr(c, 'estimated_ad_count', 0)
        if isinstance(ads, str):
            if ads.isdigit():
                estimated_ads_list.append(int(ads))
        elif isinstance(ads, int) and ads > 0:
            estimated_ads_list.append(ads)

    total_estimated_ads = sum(estimated_ads_list)

    # Count active competitors using correct attribute names
    active_count = len([c for c in ranked_competitors
                       if getattr(c, 'meta_classification', '').startswith(('Major', 'Moderate', 'Minor'))])

    print(f"🎯 Competitive Landscape Overview:")
    print(f"   • {active_count} competitors actively advertising on Meta")
    print(f"   • ~{total_estimated_ads:,} total competitor ads estimated")

    competition_level = ('highly competitive' if active_count >= 4
                        else 'moderately competitive' if active_count >= 2
                        else 'low competition')
    print(f"   • Market appears {competition_level} on Meta")

    # Top competitor analysis
    if ranked_competitors:
        top_competitor = ranked_competitors[0]
        top_ads_raw = getattr(top_competitor, 'estimated_ad_count', 0)

        # Safe conversion for top competitor ads
        if isinstance(top_ads_raw, str):
            top_ads = int(top_ads_raw) if top_ads_raw.isdigit() else 0
        else:
            top_ads = top_ads_raw if isinstance(top_ads_raw, int) else 0

        print(f"\\n🏆 Leading Meta Advertiser:")
        print(f"   • {top_competitor.company_name}")
        print(f"   • Estimated {top_ads:,} ads")
        print(f"   • Classification: {getattr(top_competitor, 'meta_classification', 'Unknown')}")
        print(f"   • Meta Tier: {getattr(top_competitor, 'meta_tier', 'Unknown')}")
        print(f"   • Market Overlap: {top_competitor.market_overlap_pct}%")

    # Readiness for next stage
    print(f"\\n🚀 Ready for Stage 4 (Meta Ads Ingestion):")
    print(f"   ✅ {len(ranked_competitors)} Meta-active competitors identified")
    print(f"   ✅ Classifications and ad volumes estimated")
    print(f"   ✅ Competitors ranked by advertising intensity")

    if total_estimated_ads > 0:
        expected_range = f"~{total_estimated_ads//4}-{total_estimated_ads//2}"
    else:
        expected_range = "~50-200"
    print(f"   📊 Expected ad collection: {expected_range} ads")

    # Store competitor brands for context (needed for later stages)
    context.competitor_brands = [comp.company_name for comp in ranked_competitors]
    print(f"   💾 Stored {len(context.competitor_brands)} competitor brands in context")

else:
    print("⚠️ No Meta-active competitors to analyze")
    print("   Consider:")
    print("   • Expanding search criteria")
    print("   • Checking different time periods")
    print("   • Investigating non-Meta advertising channels")'''

            # Split into lines and add newlines
            lines = new_content.split('\n')
            cell['source'] = [line + '\n' for line in lines[:-1]] + [lines[-1]]
            break

# Save the fixed notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Fixed Stage 3 insights cell: {notebook_path}")
print("   - Fixed string/int conversion for estimated_ad_count")
print("   - Fixed activity_level → meta_classification")
print("   - Added safe type checking and conversion")
print("   - Improved competitor classification detection")
print("\\n📋 Stage 3 insights should now work without errors")