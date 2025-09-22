#!/usr/bin/env python3
"""
Replace the Stage 5 preview with comprehensive strategic intelligence analysis
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# Find and remove existing preview cell if it exists
preview_indices = []
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'STRATEGIC INTELLIGENCE PREVIEW' in source:
            preview_indices.append(i)

# Remove existing preview cells (in reverse order to maintain indices)
for i in reversed(preview_indices):
    del cells[i]
    print(f"Removed existing preview cell at index {i}")

# Find the "### Stage 5 Summary" cell
stage5_summary_index = None
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'markdown' and cell.get('source'):
        source = cell['source'] if isinstance(cell['source'], str) else ''.join(cell['source'])
        if '### Stage 5 Summary' in source:
            stage5_summary_index = i
            break

if stage5_summary_index is None:
    print("❌ Could not find Stage 5 Summary cell")
    exit(1)

print(f"Found Stage 5 Summary at index {stage5_summary_index}")

# Create the comprehensive strategic intelligence analysis cell
comprehensive_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Comprehensive Strategic Intelligence Analysis\n",
        "print(\"🧠 COMPREHENSIVE STRATEGIC INTELLIGENCE ANALYSIS\")\n",
        "print(\"=\" * 60)\n",
        "\n",
        "try:\n",
        "    from src.utils.bigquery_client import run_query\n",
        "    \n",
        "    # 1. Overall Summary with median values\n",
        "    overall_query = \"\"\"\n",
        "    SELECT\n",
        "        COUNT(*) as total_ads,\n",
        "        AVG(promotional_intensity) as avg_promotional,\n",
        "        AVG(urgency_score) as avg_urgency,\n",
        "        AVG(brand_voice_score) as avg_brand_voice,\n",
        "        APPROX_QUANTILES(promotional_intensity, 2)[OFFSET(1)] as median_promotional,\n",
        "        APPROX_QUANTILES(urgency_score, 2)[OFFSET(1)] as median_urgency,\n",
        "        APPROX_QUANTILES(brand_voice_score, 2)[OFFSET(1)] as median_brand_voice\n",
        "    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "    WHERE funnel IS NOT NULL\n",
        "    \"\"\"\n",
        "    \n",
        "    # 2. Overall Funnel Distribution (FIXED percentages)\n",
        "    funnel_overall_query = \"\"\"\n",
        "    SELECT\n",
        "        CASE\n",
        "            WHEN UPPER(funnel) LIKE 'UPPER%' THEN 'Upper'\n",
        "            WHEN UPPER(funnel) LIKE 'MID%' THEN 'Mid'\n",
        "            WHEN UPPER(funnel) LIKE 'LOWER%' THEN 'Lower'\n",
        "            ELSE funnel\n",
        "        END as normalized_funnel,\n",
        "        COUNT(*) as funnel_count\n",
        "    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "    WHERE funnel IS NOT NULL\n",
        "    GROUP BY normalized_funnel\n",
        "    ORDER BY funnel_count DESC\n",
        "    \"\"\"\n",
        "    \n",
        "    # 3. Top messaging angles overall\n",
        "    angles_overall_query = \"\"\"\n",
        "    SELECT \n",
        "        angle,\n",
        "        COUNT(*) as angle_count,\n",
        "        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` WHERE funnel IS NOT NULL), 1) as percentage\n",
        "    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`,\n",
        "    UNNEST(angles) as angle\n",
        "    WHERE funnel IS NOT NULL\n",
        "    GROUP BY angle\n",
        "    ORDER BY angle_count DESC\n",
        "    LIMIT 6\n",
        "    \"\"\"\n",
        "    \n",
        "    # 4. By Brand Summary\n",
        "    brand_summary_query = \"\"\"\n",
        "    SELECT\n",
        "        brand,\n",
        "        COUNT(*) as brand_total,\n",
        "        AVG(promotional_intensity) as avg_promotional,\n",
        "        AVG(urgency_score) as avg_urgency, \n",
        "        AVG(brand_voice_score) as avg_brand_voice,\n",
        "        APPROX_QUANTILES(promotional_intensity, 2)[OFFSET(1)] as median_promotional,\n",
        "        APPROX_QUANTILES(urgency_score, 2)[OFFSET(1)] as median_urgency,\n",
        "        APPROX_QUANTILES(brand_voice_score, 2)[OFFSET(1)] as median_brand_voice\n",
        "    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "    WHERE funnel IS NOT NULL\n",
        "    GROUP BY brand\n",
        "    ORDER BY brand_total DESC\n",
        "    \"\"\"\n",
        "    \n",
        "    # 5. Funnel Distribution by Brand\n",
        "    funnel_by_brand_query = \"\"\"\n",
        "    SELECT\n",
        "        brand,\n",
        "        CASE\n",
        "            WHEN UPPER(funnel) LIKE 'UPPER%' THEN 'Upper'\n",
        "            WHEN UPPER(funnel) LIKE 'MID%' THEN 'Mid'\n",
        "            WHEN UPPER(funnel) LIKE 'LOWER%' THEN 'Lower'\n",
        "            ELSE funnel\n",
        "        END as normalized_funnel,\n",
        "        COUNT(*) as funnel_count\n",
        "    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "    WHERE funnel IS NOT NULL\n",
        "    GROUP BY brand, normalized_funnel\n",
        "    ORDER BY brand, funnel_count DESC\n",
        "    \"\"\"\n",
        "    \n",
        "    print(\"📊 Executing comprehensive analysis...\")\n",
        "    overall_result = run_query(overall_query)\n",
        "    funnel_overall_result = run_query(funnel_overall_query)\n",
        "    angles_result = run_query(angles_overall_query)\n",
        "    brand_summary_result = run_query(brand_summary_query)\n",
        "    funnel_by_brand_result = run_query(funnel_by_brand_query)\n",
        "    \n",
        "    if not overall_result.empty:\n",
        "        # Display Overall Results\n",
        "        print()\n",
        "        print(\"📈 OVERALL STRATEGIC INTELLIGENCE SUMMARY:\")\n",
        "        row = overall_result.iloc[0]\n",
        "        total_ads = int(row['total_ads'])\n",
        "        print(f\"   📊 Total strategically labeled ads: {total_ads}\")\n",
        "        print(f\"   📈 Promotional Intensity: avg={row['avg_promotional']:.2f}, median={row['median_promotional']:.2f}\")\n",
        "        print(f\"   ⚡ Urgency Score: avg={row['avg_urgency']:.2f}, median={row['median_urgency']:.2f}\")\n",
        "        print(f\"   🎨 Brand Voice Score: avg={row['avg_brand_voice']:.2f}, median={row['median_brand_voice']:.2f}\")\n",
        "        \n",
        "        # Overall Funnel Distribution with CORRECT percentages\n",
        "        print()\n",
        "        print(\"🎯 OVERALL FUNNEL STAGE DISTRIBUTION:\")\n",
        "        if not funnel_overall_result.empty:\n",
        "            total_percentage = 0\n",
        "            for _, row in funnel_overall_result.iterrows():\n",
        "                percentage = (row['funnel_count'] / total_ads) * 100\n",
        "                total_percentage += percentage\n",
        "                print(f\"   • {row['normalized_funnel']}: {row['funnel_count']} ads ({percentage:.1f}%)\")\n",
        "            print(f\"   ✓ Total: {total_percentage:.1f}%\")\n",
        "        \n",
        "        # Top Messaging Angles\n",
        "        print()\n",
        "        print(\"🎯 TOP MESSAGING ANGLES (Overall):\")\n",
        "        if not angles_result.empty:\n",
        "            for _, row in angles_result.iterrows():\n",
        "                print(f\"   • {row['angle']}: {row['angle_count']} ads ({row['percentage']}%)\")\n",
        "        \n",
        "        # By Brand Analysis\n",
        "        print()\n",
        "        print(\"🏢 BY BRAND STRATEGIC ANALYSIS:\")\n",
        "        print(\"=\" * 40)\n",
        "        if not brand_summary_result.empty:\n",
        "            for _, brand_row in brand_summary_result.iterrows():\n",
        "                brand = brand_row['brand']\n",
        "                brand_total = int(brand_row['brand_total'])\n",
        "                brand_pct = (brand_total / total_ads) * 100\n",
        "                \n",
        "                print(f\"\\n🏷️  {brand.upper()} ({brand_total} ads, {brand_pct:.1f}% of total)\")\n",
        "                print(f\"   📈 Promotional: avg={brand_row['avg_promotional']:.2f}, median={brand_row['median_promotional']:.2f}\")\n",
        "                print(f\"   ⚡ Urgency: avg={brand_row['avg_urgency']:.2f}, median={brand_row['median_urgency']:.2f}\")\n",
        "                print(f\"   🎨 Brand Voice: avg={brand_row['avg_brand_voice']:.2f}, median={brand_row['median_brand_voice']:.2f}\")\n",
        "                \n",
        "                # Funnel distribution for this brand\n",
        "                brand_funnel = funnel_by_brand_result[funnel_by_brand_result['brand'] == brand]\n",
        "                if not brand_funnel.empty:\n",
        "                    print(f\"   🎯 Funnel Distribution:\")\n",
        "                    for _, funnel_row in brand_funnel.iterrows():\n",
        "                        funnel_pct = (funnel_row['funnel_count'] / brand_total) * 100\n",
        "                        print(f\"      • {funnel_row['normalized_funnel']}: {funnel_row['funnel_count']} ads ({funnel_pct:.1f}%)\")\n",
        "        \n",
        "        print(\"\\n✅ Comprehensive strategic intelligence analysis complete!\")\n",
        "        print(\"🎯 These insights reveal brand positioning, messaging strategies, and competitive landscapes.\")\n",
        "    else:\n",
        "        print(\"⚠️  No strategic intelligence data found - ensure Stage 5 completed successfully\")\n",
        "        \n",
        "except Exception as e:\n",
        "    print(f\"⚠️  Could not generate comprehensive analysis: {e}\")\n",
        "    print(\"   This is normal if Stage 5 hasn't run yet or if there's no data available\")"
    ]
}

# Insert the comprehensive analysis cell before the Stage 5 Summary
cells.insert(stage5_summary_index, comprehensive_cell)

# Update the notebook
notebook['cells'] = cells

# Write back
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully added comprehensive strategic intelligence analysis!")
print(f"   Inserted at index {stage5_summary_index}")
print(f"   Total cells now: {len(cells)}")
print(f"   Features: Overall + by-brand funnel distribution, median/avg metrics, top messaging angles")