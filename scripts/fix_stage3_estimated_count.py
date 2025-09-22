#!/usr/bin/env python3
"""Fix Stage 3 analysis to properly handle estimated_count formats like '20+', '50+', etc."""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

def create_count_extraction_helper():
    """Create a helper function to extract numeric values from estimated_count"""
    return '''def extract_numeric_count(estimated_count):
    """Extract numeric value from estimated_count (handles '20+', '50+', etc.)"""
    if isinstance(estimated_count, int):
        return estimated_count
    elif isinstance(estimated_count, str):
        # Handle formats like "20+", "50+", "100+"
        if estimated_count.endswith('+'):
            try:
                return int(estimated_count[:-1])  # Remove '+' and convert
            except ValueError:
                return 0
        # Handle pure digits
        elif estimated_count.isdigit():
            return int(estimated_count)
        else:
            return 0
    else:
        return 0'''

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Find and fix both Stage 3 analysis cells
cells_fixed = 0

for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') == 'code':
        source = ''.join(cell.get('source', []))

        # Fix the analysis cell
        if 'META AD ACTIVITY RANKING RESULTS' in source and 'estimated_ads_list' in source:
            print(f"Fixing Stage 3 analysis cell at index {i}")

            new_content = f'''{create_count_extraction_helper()}

# Analyze and display ranking results
if ranked_competitors:
    print("📋 META AD ACTIVITY RANKING RESULTS")
    print("=" * 40)

    # Create a summary DataFrame for display
    ranking_data = []
    for i, competitor in enumerate(ranked_competitors):
        # Extract activity metrics using correct attribute names from RankingStage
        meta_classification = getattr(competitor, 'meta_classification', 'Unknown')
        estimated_ads = getattr(competitor, 'estimated_ad_count', 'N/A')
        meta_tier = getattr(competitor, 'meta_tier', 0)

        # Extract numeric count properly
        estimated_ads_int = extract_numeric_count(estimated_ads)

        ranking_data.append({{
            'Rank': i + 1,
            'Company': competitor.company_name,
            'Classification': meta_classification,
            'Est. Ads': estimated_ads,
            'Numeric Count': estimated_ads_int,
            'Meta Tier': meta_tier,
            'Quality Score': f"{{competitor.quality_score:.3f}}",
            'Confidence': f"{{competitor.confidence:.3f}}",
            'Market Overlap': f"{{competitor.market_overlap_pct}}%"
        }})

    ranking_df = pd.DataFrame(ranking_data)

    print(f"📊 Meta-Active Competitors (Ranked by Quality Score):")
    display(ranking_df)

    # Show ranking statistics
    print(f"\\n📈 Meta Ad Activity Statistics:")
    print(f"   Input Competitors: {{len(curated_competitors)}}")
    print(f"   Meta-Active: {{len(ranked_competitors)}}")
    print(f"   Activity Filter Rate: {{len(ranked_competitors)/len(curated_competitors)*100:.1f}}%")

    # Meta classification breakdown
    classifications = [getattr(c, 'meta_classification', 'Unknown') for c in ranked_competitors]
    classification_counts = {{}}
    for classification in classifications:
        classification_counts[classification] = classification_counts.get(classification, 0) + 1

    print(f"\\n🎯 Meta Classification Breakdown:")
    for classification, count in classification_counts.items():
        print(f"   • {{classification}}: {{count}} competitors")

    # Ad volume analysis using the improved extraction
    estimated_ads_list = [extract_numeric_count(getattr(c, 'estimated_ad_count', 0))
                         for c in ranked_competitors]
    estimated_ads_list = [count for count in estimated_ads_list if count > 0]

    if estimated_ads_list:
        print(f"\\n📊 Estimated Ad Volume:")
        print(f"   Total Estimated Ads: {{sum(estimated_ads_list):,}}")
        print(f"   Average per Competitor: {{sum(estimated_ads_list)/len(estimated_ads_list):.0f}}")
        print(f"   Range: {{min(estimated_ads_list)}} - {{max(estimated_ads_list)}} ads")
    else:
        print(f"\\n📊 No valid ad volume data available")

    # Meta tier analysis
    meta_tiers = [getattr(c, 'meta_tier', 0) for c in ranked_competitors]
    if meta_tiers and max(meta_tiers) > 0:
        print(f"\\n⭐ Meta Tier Distribution:")
        tier_counts = {{}}
        tier_names = {{3: 'Major Player (20+)', 2: 'Moderate Player (11-19)', 1: 'Minor Player (1-10)', 0: 'No Presence'}}
        for tier in meta_tiers:
            tier_name = tier_names.get(tier, f'Tier {{tier}}')
            tier_counts[tier_name] = tier_counts.get(tier_name, 0) + 1

        for tier_name, count in tier_counts.items():
            print(f"   • {{tier_name}}: {{count}} competitors")

else:
    print("⚠️ No Meta-active competitors found")
    print("   This could mean:")
    print("   • No competitors are currently advertising on Meta")
    print("   • Meta Ad Library API issues")
    print("   • All competitors below activity threshold")'''

            lines = new_content.split('\n')
            cell['source'] = [line + '\n' for line in lines[:-1]] + [lines[-1]]
            cells_fixed += 1

        # Fix the insights cell
        elif 'META AD ACTIVITY INSIGHTS' in source and 'total_estimated_ads' in source:
            print(f"Fixing Stage 3 insights cell at index {i}")

            new_content = f'''{create_count_extraction_helper()}

# Meta Ad Activity Insights and Next Steps
if ranked_competitors:
    print("💡 META AD ACTIVITY INSIGHTS")
    print("=" * 35)

    # Competitive landscape analysis using improved count extraction
    estimated_ads_list = [extract_numeric_count(getattr(c, 'estimated_ad_count', 0))
                         for c in ranked_competitors]
    estimated_ads_list = [count for count in estimated_ads_list if count > 0]
    total_estimated_ads = sum(estimated_ads_list)

    # Count active competitors using correct attribute names
    active_count = len([c for c in ranked_competitors
                       if getattr(c, 'meta_classification', '').startswith(('Major', 'Moderate', 'Minor'))])

    print(f"🎯 Competitive Landscape Overview:")
    print(f"   • {{active_count}} competitors actively advertising on Meta")
    print(f"   • ~{{total_estimated_ads:,}} total competitor ads estimated")

    competition_level = ('highly competitive' if active_count >= 4
                        else 'moderately competitive' if active_count >= 2
                        else 'low competition')
    print(f"   • Market appears {{competition_level}} on Meta")

    # Top competitor analysis
    if ranked_competitors:
        top_competitor = ranked_competitors[0]
        top_ads_raw = getattr(top_competitor, 'estimated_ad_count', 0)
        top_ads = extract_numeric_count(top_ads_raw)

        print(f"\\n🏆 Leading Meta Advertiser:")
        print(f"   • {{top_competitor.company_name}}")
        print(f"   • Estimated {{top_ads:,}} ads ({{top_ads_raw}})")
        print(f"   • Classification: {{getattr(top_competitor, 'meta_classification', 'Unknown')}}")
        print(f"   • Meta Tier: {{getattr(top_competitor, 'meta_tier', 'Unknown')}}")
        print(f"   • Market Overlap: {{top_competitor.market_overlap_pct}}%")

    # Readiness for next stage
    print(f"\\n🚀 Ready for Stage 4 (Meta Ads Ingestion):")
    print(f"   ✅ {{len(ranked_competitors)}} Meta-active competitors identified")
    print(f"   ✅ Classifications and ad volumes estimated")
    print(f"   ✅ Competitors ranked by advertising intensity")

    if total_estimated_ads > 0:
        expected_range = f"~{{total_estimated_ads//4}}-{{total_estimated_ads//2}}"
    else:
        expected_range = "~50-200"
    print(f"   📊 Expected ad collection: {{expected_range}} ads")

    # Store competitor brands for context (needed for later stages)
    context.competitor_brands = [comp.company_name for comp in ranked_competitors]
    print(f"   💾 Stored {{len(context.competitor_brands)}} competitor brands in context")

else:
    print("⚠️ No Meta-active competitors to analyze")
    print("   Consider:")
    print("   • Expanding search criteria")
    print("   • Checking different time periods")
    print("   • Investigating non-Meta advertising channels")'''

            lines = new_content.split('\n')
            cell['source'] = [line + '\n' for line in lines[:-1]] + [lines[-1]]
            cells_fixed += 1

# Save the fixed notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Fixed {cells_fixed} Stage 3 cells: {notebook_path}")
print("   - Added extract_numeric_count() helper function")
print("   - Now handles '20+', '50+' format from MetaAdsFetcher")
print("   - Shows both raw and numeric values for clarity")
print("   - Proper summation of estimated ad counts")
print("\\n📋 Stage 3 should now show correct ad count totals")