#!/usr/bin/env python3
"""Add Stage 3 (Meta Ad Activity Ranking) cells to the notebook"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Stage 3 markdown header
stage3_header = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "---\n",
        "\n",
        "## 📊 Stage 3: Meta Ad Activity Ranking\n",
        "\n",
        "**Purpose**: Probe and rank competitors by their actual Meta advertising activity\n",
        "\n",
        "**Input**: ~7 validated competitors from Stage 2\n",
        "**Output**: ~4 Meta-active competitors with activity estimates\n",
        "**BigQuery Impact**: No new tables (uses Meta Ad Library API directly)\n",
        "\n",
        "**Process**:\n",
        "- Real-time Meta Ad Library probing\n",
        "- Activity classification (Major/Minor/None)\n",
        "- Ad volume estimation\n",
        "- Ranking algorithm scoring\n",
        "- Filtering for active advertisers only"
    ]
}

# Stage 3 execution cell
stage3_execution = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Execute Stage 3: Meta Ad Activity Ranking\n",
        "print(\"📊 STAGE 3: META AD ACTIVITY RANKING\")\n",
        "print(\"=\" * 50)\n",
        "print(\"Probing Meta Ad Library to rank competitors by advertising activity...\")\n",
        "print()\n",
        "\n",
        "# Import required stage\n",
        "from src.pipeline.stages.ranking import RankingStage\n",
        "\n",
        "# Time the ranking process\n",
        "stage3_start = time.time()\n",
        "\n",
        "try:\n",
        "    # Check if we have curation results\n",
        "    if not curated_competitors:\n",
        "        raise ValueError(\"No curated competitors found. Run Stage 2 first.\")\n",
        "    \n",
        "    print(f\"📥 Input: {len(curated_competitors)} validated competitors\")\n",
        "    print(\"🔍 Probing Meta Ad Library for each competitor...\")\n",
        "    print()\n",
        "    \n",
        "    # Initialize and run ranking stage\n",
        "    ranking_stage = RankingStage(context, dry_run=False)\n",
        "    ranked_competitors = ranking_stage.run(curated_competitors, progress)\n",
        "    \n",
        "    stage3_duration = time.time() - stage3_start\n",
        "    \n",
        "    print(f\"\\n✅ Stage 3 Complete!\")\n",
        "    print(f\"⏱️  Duration: {stage3_duration:.1f} seconds\")\n",
        "    print(f\"📊 Meta-Active Competitors: {len(ranked_competitors)}\")\n",
        "    print(f\"🎯 Activity Filter: {len(ranked_competitors)}/{len(curated_competitors)} ({len(ranked_competitors)/len(curated_competitors)*100:.1f}% active)\")\n",
        "    \n",
        "except Exception as e:\n",
        "    stage3_duration = time.time() - stage3_start\n",
        "    print(f\"\\n❌ Stage 3 Failed after {stage3_duration:.1f}s\")\n",
        "    print(f\"Error: {e}\")\n",
        "    ranked_competitors = []"
    ]
}

# Stage 3 analysis cell
stage3_analysis = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Analyze and display ranking results\n",
        "if ranked_competitors:\n",
        "    print(\"📋 META AD ACTIVITY RANKING RESULTS\")\n",
        "    print(\"=\" * 40)\n",
        "    \n",
        "    # Create a summary DataFrame for display\n",
        "    ranking_data = []\n",
        "    for i, competitor in enumerate(ranked_competitors):\n",
        "        # Extract activity metrics\n",
        "        activity_level = getattr(competitor, 'activity_level', 'Unknown')\n",
        "        estimated_ads = getattr(competitor, 'estimated_ad_count', 0)\n",
        "        activity_score = getattr(competitor, 'activity_score', 0)\n",
        "        last_seen = getattr(competitor, 'last_ad_seen', 'Unknown')\n",
        "        \n",
        "        ranking_data.append({\n",
        "            'Rank': i + 1,\n",
        "            'Company': competitor.company_name,\n",
        "            'Activity Level': activity_level,\n",
        "            'Est. Ads': estimated_ads,\n",
        "            'Activity Score': f\"{activity_score:.3f}\",\n",
        "            'Confidence': f\"{competitor.confidence:.3f}\",\n",
        "            'Market Overlap': f\"{competitor.market_overlap_pct}%\",\n",
        "            'Last Seen': last_seen\n",
        "        })\n",
        "    \n",
        "    ranking_df = pd.DataFrame(ranking_data)\n",
        "    \n",
        "    print(f\"📊 Meta-Active Competitors (Ranked by Activity):\")\n",
        "    display(ranking_df)\n",
        "    \n",
        "    # Show ranking statistics\n",
        "    print(f\"\\n📈 Meta Ad Activity Statistics:\")\n",
        "    print(f\"   Input Competitors: {len(curated_competitors)}\")\n",
        "    print(f\"   Meta-Active: {len(ranked_competitors)}\")\n",
        "    print(f\"   Activity Filter Rate: {len(ranked_competitors)/len(curated_competitors)*100:.1f}%\")\n",
        "    \n",
        "    # Activity level breakdown\n",
        "    activity_levels = [getattr(c, 'activity_level', 'Unknown') for c in ranked_competitors]\n",
        "    activity_counts = {}\n",
        "    for level in activity_levels:\n",
        "        activity_counts[level] = activity_counts.get(level, 0) + 1\n",
        "    \n",
        "    print(f\"\\n🎯 Activity Level Breakdown:\")\n",
        "    for level, count in activity_counts.items():\n",
        "        print(f\"   • {level}: {count} competitors\")\n",
        "    \n",
        "    # Ad volume analysis\n",
        "    estimated_ads = [getattr(c, 'estimated_ad_count', 0) for c in ranked_competitors if getattr(c, 'estimated_ad_count', 0) > 0]\n",
        "    if estimated_ads:\n",
        "        print(f\"\\n📊 Estimated Ad Volume:\")\n",
        "        print(f\"   Total Estimated Ads: {sum(estimated_ads):,}\")\n",
        "        print(f\"   Average per Competitor: {sum(estimated_ads)/len(estimated_ads):.0f}\")\n",
        "        print(f\"   Range: {min(estimated_ads)} - {max(estimated_ads)} ads\")\n",
        "    \n",
        "    # Activity scores\n",
        "    activity_scores = [getattr(c, 'activity_score', 0) for c in ranked_competitors]\n",
        "    if activity_scores and max(activity_scores) > 0:\n",
        "        print(f\"\\n⭐ Activity Scoring:\")\n",
        "        print(f\"   Score Range: {min(activity_scores):.3f} - {max(activity_scores):.3f}\")\n",
        "        print(f\"   Average Score: {sum(activity_scores)/len(activity_scores):.3f}\")\n",
        "    \n",
        "else:\n",
        "    print(\"⚠️ No Meta-active competitors found\")\n",
        "    print(\"   This could mean:\")\n",
        "    print(\"   • No competitors are currently advertising on Meta\")\n",
        "    print(\"   • Meta Ad Library API issues\")\n",
        "    print(\"   • All competitors below activity threshold\")"
    ]
}

# Stage 3 insights cell
stage3_insights = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Meta Ad Activity Insights and Next Steps\n",
        "if ranked_competitors:\n",
        "    print(\"💡 META AD ACTIVITY INSIGHTS\")\n",
        "    print(\"=\" * 35)\n",
        "    \n",
        "    # Competitive landscape analysis\n",
        "    total_estimated_ads = sum(getattr(c, 'estimated_ad_count', 0) for c in ranked_competitors)\n",
        "    active_count = len([c for c in ranked_competitors if getattr(c, 'activity_level', '') in ['Major', 'Minor']])\n",
        "    \n",
        "    print(f\"🎯 Competitive Landscape Overview:\")\n",
        "    print(f\"   • {active_count} competitors actively advertising on Meta\")\n",
        "    print(f\"   • ~{total_estimated_ads:,} total competitor ads estimated\")\n",
        "    print(f\"   • Market appears {'highly competitive' if active_count >= 4 else 'moderately competitive' if active_count >= 2 else 'low competition'} on Meta\")\n",
        "    \n",
        "    # Top competitor analysis\n",
        "    if ranked_competitors:\n",
        "        top_competitor = ranked_competitors[0]\n",
        "        top_ads = getattr(top_competitor, 'estimated_ad_count', 0)\n",
        "        print(f\"\\n🏆 Leading Meta Advertiser:\")\n",
        "        print(f\"   • {top_competitor.company_name}\")\n",
        "        print(f\"   • Estimated {top_ads:,} ads\")\n",
        "        print(f\"   • Activity Level: {getattr(top_competitor, 'activity_level', 'Unknown')}\")\n",
        "        print(f\"   • Market Overlap: {top_competitor.market_overlap_pct}%\")\n",
        "    \n",
        "    # Readiness for next stage\n",
        "    print(f\"\\n🚀 Ready for Stage 4 (Meta Ads Ingestion):\")\n",
        "    print(f\"   ✅ {len(ranked_competitors)} Meta-active competitors identified\")\n",
        "    print(f\"   ✅ Activity levels and ad volumes estimated\")\n",
        "    print(f\"   ✅ Competitors ranked by advertising intensity\")\n",
        "    print(f\"   📊 Expected ad collection: ~{total_estimated_ads//4}-{total_estimated_ads//2} ads\")\n",
        "    \n",
        "    # Store competitor brands for context (needed for later stages)\n",
        "    context.competitor_brands = [comp.company_name for comp in ranked_competitors]\n",
        "    print(f\"   💾 Stored {len(context.competitor_brands)} competitor brands in context\")\n",
        "    \n",
        "else:\n",
        "    print(\"⚠️ No Meta-active competitors to analyze\")\n",
        "    print(\"   Consider:\")\n",
        "    print(\"   • Expanding search criteria\")\n",
        "    print(\"   • Checking different time periods\")\n",
        "    print(\"   • Investigating non-Meta advertising channels\")"
    ]
}

# Stage 3 summary markdown
stage3_summary = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "### Stage 3 Summary\n",
        "\n",
        "**✅ Meta Ad Activity Ranking Complete**\n",
        "\n",
        "**Key Achievements:**\n",
        "- Probed Meta Ad Library for real-time activity data\n",
        "- Classified competitors by advertising intensity\n",
        "- Estimated ad volumes and activity scores\n",
        "- Filtered for Meta-active advertisers only\n",
        "- Ranked competitors by advertising activity\n",
        "\n",
        "**Outputs:**\n",
        "- Meta-active competitor rankings\n",
        "- Activity level classifications (Major/Minor/None)\n",
        "- Ad volume estimates and activity scores\n",
        "- Competitive landscape insights\n",
        "\n",
        "**Next Stage:** Meta Ads Ingestion (Stage 4) - Collect actual ads from active competitors"
    ]
}

# Find the last Stage 2 cell and insert after it
# Look for Stage 2 summary to know where to insert
insert_position = None
for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') == 'markdown':
        source = ''.join(cell.get('source', []))
        if 'Stage 2 Summary' in source:
            insert_position = i + 1
            break

if insert_position is None:
    # Fallback: insert at the end
    insert_position = len(nb['cells'])

new_cells = [stage3_header, stage3_execution, stage3_analysis, stage3_insights, stage3_summary]

for i, cell in enumerate(new_cells):
    nb['cells'].insert(insert_position + i, cell)

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Added Stage 3 (Meta Ad Activity Ranking) to notebook: {notebook_path}")
print("   - Added Stage 3 header and explanation")
print("   - Added Stage 3 execution cell with Meta Ad Library probing")
print("   - Added activity analysis and ranking results")
print("   - Added competitive insights and readiness assessment")
print("   - Added Stage 3 summary")
print(f"\\n📋 New cells added after position {insert_position}")
print("   Close and reopen notebook to see the new Stage 3 section")