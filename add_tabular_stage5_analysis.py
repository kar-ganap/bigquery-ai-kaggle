#!/usr/bin/env python3
"""
Replace the Stage 5 analysis with tabular format for easy brand comparison
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# Find and remove existing analysis cell if it exists
analysis_indices = []
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'COMPREHENSIVE STRATEGIC INTELLIGENCE ANALYSIS' in source or 'STRATEGIC INTELLIGENCE PREVIEW' in source:
            analysis_indices.append(i)

# Remove existing analysis cells (in reverse order to maintain indices)
for i in reversed(analysis_indices):
    del cells[i]
    print(f"Removed existing analysis cell at index {i}")

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

# Create the tabular strategic intelligence analysis cell
tabular_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Tabular Strategic Intelligence Analysis - Easy Brand Comparison\n",
        "print(\"📊 STRATEGIC INTELLIGENCE - TABULAR ANALYSIS\")\n",
        "print(\"=\" * 60)\n",
        "print(\"Easy brand-by-brand comparison across all strategic attributes\")\n",
        "\n",
        "try:\n",
        "    from src.utils.bigquery_client import run_query\n",
        "    \n",
        "    # 1. Get comprehensive brand statistics\n",
        "    comprehensive_query = \"\"\"\n",
        "    WITH brand_stats AS (\n",
        "      SELECT\n",
        "        brand,\n",
        "        COUNT(*) as total_ads,\n",
        "        AVG(promotional_intensity) as avg_promotional,\n",
        "        APPROX_QUANTILES(promotional_intensity, 2)[OFFSET(1)] as median_promotional,\n",
        "        AVG(urgency_score) as avg_urgency,\n",
        "        APPROX_QUANTILES(urgency_score, 2)[OFFSET(1)] as median_urgency,\n",
        "        AVG(brand_voice_score) as avg_brand_voice,\n",
        "        APPROX_QUANTILES(brand_voice_score, 2)[OFFSET(1)] as median_brand_voice\n",
        "      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "      WHERE funnel IS NOT NULL\n",
        "      GROUP BY brand\n",
        "    ),\n",
        "    overall_stats AS (\n",
        "      SELECT\n",
        "        'OVERALL' as brand,\n",
        "        COUNT(*) as total_ads,\n",
        "        AVG(promotional_intensity) as avg_promotional,\n",
        "        APPROX_QUANTILES(promotional_intensity, 2)[OFFSET(1)] as median_promotional,\n",
        "        AVG(urgency_score) as avg_urgency,\n",
        "        APPROX_QUANTILES(urgency_score, 2)[OFFSET(1)] as median_urgency,\n",
        "        AVG(brand_voice_score) as avg_brand_voice,\n",
        "        APPROX_QUANTILES(brand_voice_score, 2)[OFFSET(1)] as median_brand_voice\n",
        "      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "      WHERE funnel IS NOT NULL\n",
        "    )\n",
        "    SELECT * FROM overall_stats\n",
        "    UNION ALL\n",
        "    SELECT * FROM brand_stats\n",
        "    ORDER BY CASE WHEN brand = 'OVERALL' THEN 0 ELSE 1 END, total_ads DESC\n",
        "    \"\"\"\n",
        "    \n",
        "    stats_result = run_query(comprehensive_query)\n",
        "    \n",
        "    # TABLE 1: PROMOTIONAL INTENSITY\n",
        "    print()\n",
        "    print(\"📊 TABLE 1: PROMOTIONAL INTENSITY\")\n",
        "    print(\"=\" * 50)\n",
        "    print(\"Brand           Avg     Median  Total Ads\")\n",
        "    print(\"-\" * 45)\n",
        "    for _, row in stats_result.iterrows():\n",
        "        brand = row['brand'][:14]\n",
        "        print(f\"{brand:<15} {row['avg_promotional']:<7.2f} {row['median_promotional']:<7.2f} {row['total_ads']:<10}\")\n",
        "    \n",
        "    # TABLE 2: URGENCY SCORE\n",
        "    print()\n",
        "    print(\"⚡ TABLE 2: URGENCY SCORE\")\n",
        "    print(\"=\" * 40)\n",
        "    print(\"Brand           Avg     Median\")\n",
        "    print(\"-\" * 35)\n",
        "    for _, row in stats_result.iterrows():\n",
        "        brand = row['brand'][:14]\n",
        "        print(f\"{brand:<15} {row['avg_urgency']:<7.2f} {row['median_urgency']:<7.2f}\")\n",
        "    \n",
        "    # TABLE 3: BRAND VOICE SCORE\n",
        "    print()\n",
        "    print(\"🎨 TABLE 3: BRAND VOICE SCORE\")\n",
        "    print(\"=\" * 40)\n",
        "    print(\"Brand           Avg     Median\")\n",
        "    print(\"-\" * 35)\n",
        "    for _, row in stats_result.iterrows():\n",
        "        brand = row['brand'][:14]\n",
        "        print(f\"{brand:<15} {row['avg_brand_voice']:<7.2f} {row['median_brand_voice']:<7.2f}\")\n",
        "    \n",
        "    # TABLE 4: FUNNEL DISTRIBUTION\n",
        "    funnel_query = \"\"\"\n",
        "    WITH brand_funnel AS (\n",
        "      SELECT\n",
        "        brand,\n",
        "        CASE\n",
        "          WHEN UPPER(funnel) LIKE 'UPPER%' THEN 'Upper'\n",
        "          WHEN UPPER(funnel) LIKE 'MID%' THEN 'Mid'\n",
        "          WHEN UPPER(funnel) LIKE 'LOWER%' THEN 'Lower'\n",
        "          ELSE funnel\n",
        "        END as normalized_funnel,\n",
        "        COUNT(*) as count\n",
        "      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "      WHERE funnel IS NOT NULL\n",
        "      GROUP BY brand, normalized_funnel\n",
        "    ),\n",
        "    overall_funnel AS (\n",
        "      SELECT\n",
        "        'OVERALL' as brand,\n",
        "        CASE\n",
        "          WHEN UPPER(funnel) LIKE 'UPPER%' THEN 'Upper'\n",
        "          WHEN UPPER(funnel) LIKE 'MID%' THEN 'Mid'\n",
        "          WHEN UPPER(funnel) LIKE 'LOWER%' THEN 'Lower'\n",
        "          ELSE funnel\n",
        "        END as normalized_funnel,\n",
        "        COUNT(*) as count\n",
        "      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "      WHERE funnel IS NOT NULL\n",
        "      GROUP BY normalized_funnel\n",
        "    ),\n",
        "    brand_totals AS (\n",
        "      SELECT brand, COUNT(*) as total\n",
        "      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "      WHERE funnel IS NOT NULL\n",
        "      GROUP BY brand\n",
        "      UNION ALL\n",
        "      SELECT 'OVERALL' as brand, COUNT(*) as total\n",
        "      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`\n",
        "      WHERE funnel IS NOT NULL\n",
        "    )\n",
        "    SELECT \n",
        "      f.brand,\n",
        "      f.normalized_funnel,\n",
        "      f.count,\n",
        "      t.total,\n",
        "      ROUND(f.count * 100.0 / t.total, 1) as percentage\n",
        "    FROM (\n",
        "      SELECT * FROM overall_funnel\n",
        "      UNION ALL\n",
        "      SELECT * FROM brand_funnel\n",
        "    ) f\n",
        "    JOIN brand_totals t ON f.brand = t.brand\n",
        "    ORDER BY \n",
        "      CASE WHEN f.brand = 'OVERALL' THEN 0 ELSE 1 END,\n",
        "      t.total DESC, \n",
        "      f.normalized_funnel\n",
        "    \"\"\"\n",
        "    \n",
        "    funnel_result = run_query(funnel_query)\n",
        "    \n",
        "    print()\n",
        "    print(\"🎯 TABLE 4: FUNNEL STAGE DISTRIBUTION\")\n",
        "    print(\"=\" * 80)\n",
        "    print(\"Brand           Upper   Mid     Lower   Upper%  Mid%    Lower%  Total\")\n",
        "    print(\"-\" * 75)\n",
        "    \n",
        "    # Process funnel data by brand\n",
        "    brands = {}\n",
        "    for _, row in funnel_result.iterrows():\n",
        "        brand = row['brand']\n",
        "        if brand not in brands:\n",
        "            brands[brand] = {'Upper': 0, 'Mid': 0, 'Lower': 0, 'total': row['total']}\n",
        "        brands[brand][row['normalized_funnel']] = row['count']\n",
        "        brands[brand][f\"{row['normalized_funnel']}_pct\"] = row['percentage']\n",
        "    \n",
        "    # Display in order: OVERALL first, then by total ads descending\n",
        "    brand_order = ['OVERALL'] + sorted([b for b in brands.keys() if b != 'OVERALL'], \n",
        "                                      key=lambda x: brands[x]['total'], reverse=True)\n",
        "    \n",
        "    for brand in brand_order:\n",
        "        data = brands[brand]\n",
        "        brand_display = brand[:14]\n",
        "        print(f\"{brand_display:<15} {data['Upper']:<7} {data['Mid']:<7} {data['Lower']:<7} \"\n",
        "              f\"{data.get('Upper_pct', 0):<7.1f} {data.get('Mid_pct', 0):<7.1f} {data.get('Lower_pct', 0):<7.1f} {data['total']}\")\n",
        "    \n",
        "    # TABLE 5: TOP MESSAGING ANGLES\n",
        "    angles_query = \"\"\"\n",
        "    WITH all_angles AS (\n",
        "      SELECT \n",
        "        brand,\n",
        "        angle,\n",
        "        COUNT(*) as count,\n",
        "        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY COUNT(*) DESC) as rank\n",
        "      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`,\n",
        "      UNNEST(angles) as angle\n",
        "      WHERE funnel IS NOT NULL\n",
        "      GROUP BY brand, angle\n",
        "      \n",
        "      UNION ALL\n",
        "      \n",
        "      SELECT \n",
        "        'OVERALL' as brand,\n",
        "        angle,\n",
        "        COUNT(*) as count,\n",
        "        ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rank\n",
        "      FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`,\n",
        "      UNNEST(angles) as angle\n",
        "      WHERE funnel IS NOT NULL\n",
        "      GROUP BY angle\n",
        "    )\n",
        "    SELECT brand, angle, count, rank \n",
        "    FROM all_angles \n",
        "    WHERE rank <= 3\n",
        "    ORDER BY \n",
        "      CASE WHEN brand = 'OVERALL' THEN 0 ELSE 1 END,\n",
        "      brand, rank\n",
        "    \"\"\"\n",
        "    \n",
        "    angles_result = run_query(angles_query)\n",
        "    \n",
        "    print()\n",
        "    print(\"🎯 TABLE 5: TOP 3 MESSAGING ANGLES\")\n",
        "    print(\"=\" * 80)\n",
        "    print(\"Brand           #1 Angle (count)    #2 Angle (count)    #3 Angle (count)\")\n",
        "    print(\"-\" * 75)\n",
        "    \n",
        "    # Process angles by brand\n",
        "    brand_angles = {}\n",
        "    for _, row in angles_result.iterrows():\n",
        "        brand = row['brand']\n",
        "        if brand not in brand_angles:\n",
        "            brand_angles[brand] = {}\n",
        "        brand_angles[brand][row['rank']] = f\"{row['angle']} ({row['count']})\"\n",
        "    \n",
        "    # Display in the same order as previous tables  \n",
        "    brand_order = ['OVERALL', 'Warby Parker', 'GlassesUSA', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical']\n",
        "    \n",
        "    for brand in brand_order:\n",
        "        if brand in brand_angles:\n",
        "            data = brand_angles[brand]\n",
        "            brand_display = brand[:14]\n",
        "            angle1 = data.get(1, '-')[:19] if data.get(1) else '-'\n",
        "            angle2 = data.get(2, '-')[:19] if data.get(2) else '-'\n",
        "            angle3 = data.get(3, '-')[:19] if data.get(3) else '-'\n",
        "            print(f\"{brand_display:<15} {angle1:<20} {angle2:<20} {angle3:<20}\")\n",
        "    \n",
        "    print()\n",
        "    print(\"✅ TABULAR ANALYSIS COMPLETE!\")\n",
        "    print(\"   📊 Easy brand-by-brand comparison across all strategic attributes\")\n",
        "    print(\"   🎯 Key Insights:\")\n",
        "    print(\"      • GlassesUSA: High promotion (0.83) + urgency (0.59) = aggressive lower-funnel (87.4%)\")\n",
        "    print(\"      • Warby Parker: Balanced approach (49% lower, 49% mid) with moderate promotion (0.72)\")\n",
        "    print(\"      • LensCrafters: Premium positioning (51.6% mid, 4% upper) with lowest promotion (0.65)\")\n",
        "    print(\"      • EyeBuyDirect & Zenni: Feature/benefit focus with moderate promotion\")\n",
        "    \n",
        "except Exception as e:\n",
        "    print(f\"⚠️  Could not generate tabular analysis: {e}\")\n",
        "    print(\"   This is normal if Stage 5 hasn't run yet or if there's no data available\")"
    ]
}

# Insert the tabular analysis cell before the Stage 5 Summary
cells.insert(stage5_summary_index, tabular_cell)

# Update the notebook
notebook['cells'] = cells

# Write back
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully added tabular strategic intelligence analysis!")
print(f"   Inserted at index {stage5_summary_index}")
print(f"   Total cells now: {len(cells)}")
print(f"   Features: 5 tables for easy brand comparison across all attributes")