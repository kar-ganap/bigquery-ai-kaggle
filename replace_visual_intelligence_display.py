#!/usr/bin/env python3
"""
Replace the generic Stage 6 display with meaningful visual intelligence insights
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# Find the Stage 6 analysis cell
stage6_analysis_index = None
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'Analyze and display Stage 6 Visual Intelligence results' in source:
            stage6_analysis_index = i
            break

if stage6_analysis_index is None:
    print("❌ Could not find Stage 6 analysis cell")
    exit(1)

print(f"Found Stage 6 analysis cell at index {stage6_analysis_index}")

# Create the meaningful visual intelligence analysis cell
meaningful_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Visual Intelligence - Competitive Positioning Analysis\n",
        "import pandas as pd\n",
        "from IPython.display import display\n",
        "\n",
        "print(\"🎨 VISUAL INTELLIGENCE - COMPETITIVE POSITIONING ANALYSIS\")\n",
        "print(\"=\" * 70)\n",
        "\n",
        "if stage6_results is None:\n",
        "    print(\"❌ No visual intelligence results found\")\n",
        "    print(\"   Make sure you ran Stage 6 Visual Intelligence first\")\n",
        "    print(\"   Check the output above for any errors\")\n",
        "else:\n",
        "    try:\n",
        "        from src.utils.bigquery_client import run_query\n",
        "        \n",
        "        # First show basic execution summary\n",
        "        print(\"📊 EXECUTION SUMMARY:\")\n",
        "        print(f\"   🎯 Total ads analyzed: {stage6_results.sampled_ads}\")\n",
        "        print(f\"   👁️ Visual insights generated: {stage6_results.visual_insights}\")\n",
        "        print(f\"   🏆 Competitive insights: {stage6_results.competitive_insights}\")\n",
        "        print(f\"   💰 Estimated cost: ${stage6_results.cost_estimate:.2f}\")\n",
        "        print()\n",
        "        \n",
        "        # Find the visual intelligence table (most recent)\n",
        "        tables_query = \"\"\"\n",
        "        SELECT table_name\n",
        "        FROM `bigquery-ai-kaggle-469620.ads_demo.INFORMATION_SCHEMA.TABLES`\n",
        "        WHERE table_name LIKE 'visual_intelligence_%'\n",
        "        ORDER BY creation_time DESC\n",
        "        LIMIT 1\n",
        "        \"\"\"\n",
        "        \n",
        "        tables_result = run_query(tables_query)\n",
        "        \n",
        "        if not tables_result.empty:\n",
        "            visual_table = tables_result.iloc[0]['table_name']\n",
        "            print(f\"📋 Analyzing table: {visual_table}\")\n",
        "            print()\n",
        "            \n",
        "            # Get competitive positioning matrix\n",
        "            positioning_query = f\"\"\"\n",
        "            SELECT \n",
        "                brand,\n",
        "                COUNT(*) as ads_analyzed,\n",
        "                ROUND(AVG(visual_text_alignment_score), 2) as avg_alignment,\n",
        "                ROUND(AVG(brand_consistency_score), 2) as avg_consistency,\n",
        "                ROUND(AVG(creative_fatigue_risk), 2) as avg_fatigue_risk,\n",
        "                ROUND(AVG(luxury_positioning_score), 2) as avg_luxury_positioning,\n",
        "                ROUND(AVG(boldness_score), 2) as avg_boldness,\n",
        "                ROUND(AVG(visual_differentiation_level), 2) as avg_differentiation\n",
        "            FROM `bigquery-ai-kaggle-469620.ads_demo.{visual_table}`\n",
        "            WHERE visual_text_alignment_score IS NOT NULL\n",
        "            GROUP BY brand\n",
        "            ORDER BY ads_analyzed DESC\n",
        "            \"\"\"\n",
        "            \n",
        "            positioning_result = run_query(positioning_query)\n",
        "            \n",
        "            if not positioning_result.empty:\n",
        "                print(\"🏆 COMPETITIVE POSITIONING MATRIX\")\n",
        "                print(\"Visual strategy analysis across all competitors:\")\n",
        "                print()\n",
        "                \n",
        "                # Create positioning DataFrame\n",
        "                pos_df = positioning_result[['brand', 'ads_analyzed', 'avg_alignment', 'avg_consistency', \n",
        "                                           'avg_fatigue_risk', 'avg_luxury_positioning', 'avg_boldness', \n",
        "                                           'avg_differentiation']].copy()\n",
        "                \n",
        "                pos_df.columns = ['Brand', 'Ads', 'Alignment', 'Consistency', 'Fatigue Risk', \n",
        "                                'Luxury Score', 'Boldness', 'Uniqueness']\n",
        "                \n",
        "                display(pos_df)\n",
        "                \n",
        "                print(\"\\n📊 METRIC EXPLANATIONS:\")\n",
        "                print(\"• Alignment (0-1): How well visuals match text messaging\")\n",
        "                print(\"• Consistency (0-1): Visual brand coherence across campaigns\")\n",
        "                print(\"• Fatigue Risk (0-1): How stale/overused the creative feels\")\n",
        "                print(\"• Luxury Score (0-1): 0=accessible/mass market, 1=luxury/premium\")\n",
        "                print(\"• Boldness (0-1): 0=subtle/conservative, 1=bold/attention-grabbing\")\n",
        "                print(\"• Uniqueness (0-1): How differentiated vs category-standard\")\n",
        "                \n",
        "                # Competitive insights\n",
        "                print(\"\\n🎯 KEY COMPETITIVE INSIGHTS:\")\n",
        "                \n",
        "                # Find top performers in each category\n",
        "                max_luxury = positioning_result.loc[positioning_result['avg_luxury_positioning'].idxmax()]\n",
        "                max_bold = positioning_result.loc[positioning_result['avg_boldness'].idxmax()]\n",
        "                max_unique = positioning_result.loc[positioning_result['avg_differentiation'].idxmax()]\n",
        "                max_consistent = positioning_result.loc[positioning_result['avg_consistency'].idxmax()]\n",
        "                \n",
        "                print(f\"💎 Most Premium Positioning: {max_luxury['brand']} ({max_luxury['avg_luxury_positioning']})\")\n",
        "                print(f\"🔥 Most Bold Visual Approach: {max_bold['brand']} ({max_bold['avg_boldness']})\")\n",
        "                print(f\"⭐ Most Visually Unique: {max_unique['brand']} ({max_unique['avg_differentiation']})\")\n",
        "                print(f\"🏆 Most Brand Consistent: {max_consistent['brand']} ({max_consistent['avg_consistency']})\")\n",
        "                \n",
        "                # Get demographic and style insights\n",
        "                demo_query = f\"\"\"\n",
        "                SELECT \n",
        "                    brand,\n",
        "                    target_demographic,\n",
        "                    visual_style,\n",
        "                    COUNT(*) as count\n",
        "                FROM `bigquery-ai-kaggle-469620.ads_demo.{visual_table}`\n",
        "                WHERE target_demographic IS NOT NULL\n",
        "                GROUP BY brand, target_demographic, visual_style\n",
        "                ORDER BY brand, count DESC\n",
        "                \"\"\"\n",
        "                \n",
        "                demo_result = run_query(demo_query)\n",
        "                \n",
        "                if not demo_result.empty:\n",
        "                    print(\"\\n👥 TARGET DEMOGRAPHICS & VISUAL STYLES:\")\n",
        "                    current_brand = None\n",
        "                    for _, row in demo_result.iterrows():\n",
        "                        if row['brand'] != current_brand:\n",
        "                            current_brand = row['brand']\n",
        "                            print(f\"\\n{current_brand}:\")\n",
        "                        demo = row['target_demographic'] or 'Unknown'\n",
        "                        style = row['visual_style'] or 'Unknown'\n",
        "                        print(f\"   👥 {demo} | 🎨 {style} ({row['count']} ads)\")\n",
        "                \n",
        "                print(\"\\n✅ MULTIMODAL AI ANALYSIS COMPLETE!\")\n",
        "                print(\"🎯 This reveals competitive visual positioning that text analysis alone cannot capture.\")\n",
        "                print(\"💡 Use these insights to identify visual differentiation opportunities and threats.\")\n",
        "                \n",
        "            else:\n",
        "                print(\"⚠️ Visual intelligence table exists but contains no processed insights\")\n",
        "                \n",
        "        else:\n",
        "            print(\"⚠️ No visual intelligence table found\")\n",
        "            print(\"   The visual analysis may have failed or not completed yet\")\n",
        "            \n",
        "    except Exception as e:\n",
        "        print(f\"⚠️ Could not analyze visual intelligence results: {e}\")\n",
        "        print(\"   Falling back to basic summary...\")\n",
        "        print(f\"   🎯 Total ads analyzed: {stage6_results.sampled_ads}\")\n",
        "        print(f\"   💰 Estimated cost: ${stage6_results.cost_estimate:.2f}\")"
    ]
}

# Replace the existing cell
cells[stage6_analysis_index] = meaningful_cell

# Update the notebook
notebook['cells'] = cells

# Write back
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully replaced Stage 6 analysis with meaningful insights!")
print(f"   Now shows competitive positioning matrix, visual strategy analysis, and AI insights")
print(f"   Reveals the true value: multimodal competitive intelligence")