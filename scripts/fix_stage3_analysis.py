#!/usr/bin/env python3
"""Fix Stage 3 analysis to use correct attribute names from RankingStage"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Find the Stage 3 analysis cell and fix it
for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') == 'code':
        source = ''.join(cell.get('source', []))
        if 'META AD ACTIVITY RANKING RESULTS' in source and 'activity_level' in source:
            print(f"Found Stage 3 analysis cell at index {i}")

            # Fixed content using correct attribute names
            new_content = '''# Analyze and display ranking results
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

        # Convert estimated_ads to integer if it's a string number
        if isinstance(estimated_ads, str):
            try:
                estimated_ads_int = int(estimated_ads) if estimated_ads.isdigit() else 0
            except:
                estimated_ads_int = 0
        else:
            estimated_ads_int = estimated_ads if isinstance(estimated_ads, int) else 0

        ranking_data.append({
            'Rank': i + 1,
            'Company': competitor.company_name,
            'Classification': meta_classification,
            'Est. Ads': estimated_ads,
            'Meta Tier': meta_tier,
            'Quality Score': f"{competitor.quality_score:.3f}",
            'Confidence': f"{competitor.confidence:.3f}",
            'Market Overlap': f"{competitor.market_overlap_pct}%"
        })

    ranking_df = pd.DataFrame(ranking_data)

    print(f"📊 Meta-Active Competitors (Ranked by Quality Score):")
    display(ranking_df)

    # Show ranking statistics
    print(f"\\n📈 Meta Ad Activity Statistics:")
    print(f"   Input Competitors: {len(curated_competitors)}")
    print(f"   Meta-Active: {len(ranked_competitors)}")
    print(f"   Activity Filter Rate: {len(ranked_competitors)/len(curated_competitors)*100:.1f}%")

    # Meta classification breakdown
    classifications = [getattr(c, 'meta_classification', 'Unknown') for c in ranked_competitors]
    classification_counts = {}
    for classification in classifications:
        classification_counts[classification] = classification_counts.get(classification, 0) + 1

    print(f"\\n🎯 Meta Classification Breakdown:")
    for classification, count in classification_counts.items():
        print(f"   • {classification}: {count} competitors")

    # Ad volume analysis (handle string/int conversion safely)
    estimated_ads_list = []
    for c in ranked_competitors:
        ads = getattr(c, 'estimated_ad_count', 0)
        if isinstance(ads, str):
            if ads.isdigit():
                estimated_ads_list.append(int(ads))
        elif isinstance(ads, int) and ads > 0:
            estimated_ads_list.append(ads)

    if estimated_ads_list:
        print(f"\\n📊 Estimated Ad Volume:")
        print(f"   Total Estimated Ads: {sum(estimated_ads_list):,}")
        print(f"   Average per Competitor: {sum(estimated_ads_list)/len(estimated_ads_list):.0f}")
        print(f"   Range: {min(estimated_ads_list)} - {max(estimated_ads_list)} ads")
    else:
        print(f"\\n📊 No valid ad volume data available")

    # Meta tier analysis
    meta_tiers = [getattr(c, 'meta_tier', 0) for c in ranked_competitors]
    if meta_tiers and max(meta_tiers) > 0:
        print(f"\\n⭐ Meta Tier Distribution:")
        tier_counts = {}
        tier_names = {3: 'Major Player (20+)', 2: 'Moderate Player (11-19)', 1: 'Minor Player (1-10)', 0: 'No Presence'}
        for tier in meta_tiers:
            tier_name = tier_names.get(tier, f'Tier {tier}')
            tier_counts[tier_name] = tier_counts.get(tier_name, 0) + 1

        for tier_name, count in tier_counts.items():
            print(f"   • {tier_name}: {count} competitors")

else:
    print("⚠️ No Meta-active competitors found")
    print("   This could mean:")
    print("   • No competitors are currently advertising on Meta")
    print("   • Meta Ad Library API issues")
    print("   • All competitors below activity threshold")'''

            # Split into lines and add newlines
            lines = new_content.split('\n')
            cell['source'] = [line + '\n' for line in lines[:-1]] + [lines[-1]]
            break

# Save the fixed notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Fixed Stage 3 analysis cell: {notebook_path}")
print("   - Fixed attribute names to match RankingStage output")
print("   - meta_classification instead of activity_level")
print("   - estimated_ad_count with proper string/int handling")
print("   - meta_tier instead of activity_score")
print("   - Added safe type conversion for estimated ads")
print("\\n📋 Stage 3 analysis should now work correctly")