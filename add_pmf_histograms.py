#!/usr/bin/env python3
"""
Add proper PMF histogram visualizations instead of just tables
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# Find the Stage 6 analysis cell with PMF code
stage6_analysis_index = None
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'PMF VISUALIZATION & MODAL ANALYSIS' in source:
            stage6_analysis_index = i
            break

if stage6_analysis_index is None:
    print("❌ Could not find Stage 6 PMF analysis cell")
    exit(1)

print(f"Found Stage 6 PMF analysis cell at index {stage6_analysis_index}")

# Get the existing cell
existing_cell = cells[stage6_analysis_index]
existing_source = existing_cell['source']

# Find the PMF section and replace it with histogram version
pmf_start = None
pmf_end = None

for i, line in enumerate(existing_source):
    if 'PMF VISUALIZATION & MODAL ANALYSIS' in line:
        pmf_start = i
    if pmf_start and 'PMF & MODAL ANALYSIS COMPLETE!' in line:
        pmf_end = i + 4  # Include the following explanation lines
        break

if pmf_start and pmf_end:
    # Create improved PMF code with histograms
    improved_pmf_code = [
        "                # PMF VISUALIZATION & MODAL ANALYSIS\n",
        "                print(\"\\n📊 PROBABILITY MASS FUNCTIONS (PMF) - HISTOGRAM VISUALIZATIONS\")\n",
        "                print(\"Visual comparison across brands (only showing used categories):\")\n",
        "                print()\n",
        "                \n",
        "                import matplotlib.pyplot as plt\n",
        "                import seaborn as sns\n",
        "                import numpy as np\n",
        "                \n",
        "                # Set up plotting style\n",
        "                plt.style.use('default')\n",
        "                sns.set_palette('husl')\n",
        "                \n",
        "                # Get demographic PMF data (only used categories)\n",
        "                pmf_demo_query = f\"\"\"\n",
        "                WITH used_demographics AS (\n",
        "                  SELECT DISTINCT target_demographic \n",
        "                  FROM `bigquery-ai-kaggle-469620.ads_demo.{visual_table}`\n",
        "                  WHERE target_demographic IS NOT NULL\n",
        "                )\n",
        "                SELECT \n",
        "                    brand,\n",
        "                    target_demographic,\n",
        "                    COUNT(*) as count,\n",
        "                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY brand) as percentage\n",
        "                FROM `bigquery-ai-kaggle-469620.ads_demo.{visual_table}`\n",
        "                WHERE target_demographic IS NOT NULL\n",
        "                GROUP BY brand, target_demographic\n",
        "                ORDER BY brand, percentage DESC\n",
        "                \"\"\"\n",
        "                \n",
        "                pmf_demo_result = run_query(pmf_demo_query)\n",
        "                \n",
        "                if not pmf_demo_result.empty:\n",
        "                    # Get only used demographic buckets\n",
        "                    used_demographics = sorted(pmf_demo_result['target_demographic'].unique())\n",
        "                    brands = sorted(pmf_demo_result['brand'].unique())\n",
        "                    \n",
        "                    print(\"🎯 DEMOGRAPHIC PMF HISTOGRAM:\")\n",
        "                    print(f\"(Targeting distribution across {len(used_demographics)} active demographic segments)\")\n",
        "                    print()\n",
        "                    \n",
        "                    # Create demographic PMF histogram\n",
        "                    fig, ax = plt.subplots(figsize=(12, 6))\n",
        "                    \n",
        "                    # Prepare data for grouped bar chart\n",
        "                    x = np.arange(len(used_demographics))\n",
        "                    width = 0.8 / len(brands)\n",
        "                    \n",
        "                    for i, brand in enumerate(brands):\n",
        "                        brand_data = pmf_demo_result[pmf_demo_result['brand'] == brand]\n",
        "                        percentages = []\n",
        "                        \n",
        "                        for demo in used_demographics:\n",
        "                            demo_row = brand_data[brand_data['target_demographic'] == demo]\n",
        "                            percentage = demo_row['percentage'].iloc[0] if not demo_row.empty else 0.0\n",
        "                            percentages.append(percentage)\n",
        "                        \n",
        "                        ax.bar(x + i * width, percentages, width, label=brand, alpha=0.8)\n",
        "                    \n",
        "                    ax.set_xlabel('Target Demographics')\n",
        "                    ax.set_ylabel('Probability Mass (%)')\n",
        "                    ax.set_title('Demographic PMF by Brand')\n",
        "                    ax.set_xticks(x + width * (len(brands) - 1) / 2)\n",
        "                    ax.set_xticklabels(used_demographics, rotation=45, ha='right')\n",
        "                    ax.legend()\n",
        "                    ax.grid(True, alpha=0.3)\n",
        "                    \n",
        "                    plt.tight_layout()\n",
        "                    plt.show()\n",
        "                    \n",
        "                    # Also show the data table for reference\n",
        "                    demo_pmf_data = []\n",
        "                    for brand in brands:\n",
        "                        brand_data = pmf_demo_result[pmf_demo_result['brand'] == brand]\n",
        "                        row = {'Brand': brand}\n",
        "                        \n",
        "                        for demo in used_demographics:\n",
        "                            demo_row = brand_data[brand_data['target_demographic'] == demo]\n",
        "                            percentage = demo_row['percentage'].iloc[0] if not demo_row.empty else 0.0\n",
        "                            row[demo] = f\"{percentage:.1f}%\"\n",
        "                        \n",
        "                        demo_pmf_data.append(row)\n",
        "                    \n",
        "                    # Create DataFrame for demographic PMF\n",
        "                    demo_pmf_df = pd.DataFrame(demo_pmf_data)\n",
        "                    demo_pmf_df = demo_pmf_df.set_index('Brand')\n",
        "                    print(\"\\n📋 Demographic PMF Data Table:\")\n",
        "                    display(demo_pmf_df)\n",
        "                    \n",
        "                    # Get visual style PMF data (separate visualization)\n",
        "                    pmf_style_query = f\"\"\"\n",
        "                    SELECT \n",
        "                        brand,\n",
        "                        visual_style,\n",
        "                        COUNT(*) as count,\n",
        "                        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY brand) as percentage\n",
        "                    FROM `bigquery-ai-kaggle-469620.ads_demo.{visual_table}`\n",
        "                    WHERE visual_style IS NOT NULL\n",
        "                    GROUP BY brand, visual_style\n",
        "                    ORDER BY brand, percentage DESC\n",
        "                    \"\"\"\n",
        "                    \n",
        "                    pmf_style_result = run_query(pmf_style_query)\n",
        "                    \n",
        "                    if not pmf_style_result.empty:\n",
        "                        print(\"\\n🎨 VISUAL STYLE PMF HISTOGRAM:\")\n",
        "                        \n",
        "                        # Get only used style buckets\n",
        "                        used_styles = sorted(pmf_style_result['visual_style'].unique())\n",
        "                        print(f\"(Style distribution across {len(used_styles)} active visual approaches)\")\n",
        "                        print()\n",
        "                        \n",
        "                        # Create style PMF histogram\n",
        "                        fig, ax = plt.subplots(figsize=(12, 6))\n",
        "                        \n",
        "                        # Prepare data for grouped bar chart\n",
        "                        x = np.arange(len(used_styles))\n",
        "                        width = 0.8 / len(brands)\n",
        "                        \n",
        "                        for i, brand in enumerate(brands):\n",
        "                            brand_data = pmf_style_result[pmf_style_result['brand'] == brand]\n",
        "                            percentages = []\n",
        "                            \n",
        "                            for style in used_styles:\n",
        "                                style_row = brand_data[brand_data['visual_style'] == style]\n",
        "                                percentage = style_row['percentage'].iloc[0] if not style_row.empty else 0.0\n",
        "                                percentages.append(percentage)\n",
        "                            \n",
        "                            ax.bar(x + i * width, percentages, width, label=brand, alpha=0.8)\n",
        "                        \n",
        "                        ax.set_xlabel('Visual Styles')\n",
        "                        ax.set_ylabel('Probability Mass (%)')\n",
        "                        ax.set_title('Visual Style PMF by Brand')\n",
        "                        ax.set_xticks(x + width * (len(brands) - 1) / 2)\n",
        "                        ax.set_xticklabels(used_styles, rotation=45, ha='right')\n",
        "                        ax.legend()\n",
        "                        ax.grid(True, alpha=0.3)\n",
        "                        \n",
        "                        plt.tight_layout()\n",
        "                        plt.show()\n",
        "                        \n",
        "                        # Style PMF data table\n",
        "                        style_pmf_data = []\n",
        "                        for brand in brands:\n",
        "                            brand_data = pmf_style_result[pmf_style_result['brand'] == brand]\n",
        "                            row = {'Brand': brand}\n",
        "                            \n",
        "                            for style in used_styles:\n",
        "                                style_row = brand_data[brand_data['visual_style'] == style]\n",
        "                                percentage = style_row['percentage'].iloc[0] if not style_row.empty else 0.0\n",
        "                                row[style] = f\"{percentage:.1f}%\"\n",
        "                            \n",
        "                            style_pmf_data.append(row)\n",
        "                        \n",
        "                        # Create DataFrame for style PMF\n",
        "                        style_pmf_df = pd.DataFrame(style_pmf_data)\n",
        "                        style_pmf_df = style_pmf_df.set_index('Brand')\n",
        "                        print(\"\\n📋 Visual Style PMF Data Table:\")\n",
        "                        display(style_pmf_df)\n",
        "                    \n",
        "                    # MODAL ANALYSIS - Most common demographic and style per brand\n",
        "                    print(\"\\n📋 MODAL ANALYSIS - PRIMARY TARGET & STYLE PER BRAND\")\n",
        "                    \n",
        "                    modal_query = f\"\"\"\n",
        "                    WITH brand_modes AS (\n",
        "                      SELECT \n",
        "                        brand,\n",
        "                        -- Most common demographic\n",
        "                        ARRAY_AGG(target_demographic ORDER BY demo_count DESC LIMIT 1)[OFFSET(0)] as primary_demographic,\n",
        "                        MAX(demo_count) as demo_count,\n",
        "                        -- Most common visual style\n",
        "                        ARRAY_AGG(visual_style ORDER BY style_count DESC LIMIT 1)[OFFSET(0)] as primary_style,\n",
        "                        MAX(style_count) as style_count,\n",
        "                        COUNT(*) as total_ads\n",
        "                      FROM (\n",
        "                        SELECT \n",
        "                          brand,\n",
        "                          target_demographic,\n",
        "                          visual_style,\n",
        "                          COUNT(*) OVER (PARTITION BY brand, target_demographic) as demo_count,\n",
        "                          COUNT(*) OVER (PARTITION BY brand, visual_style) as style_count\n",
        "                        FROM `bigquery-ai-kaggle-469620.ads_demo.{visual_table}`\n",
        "                        WHERE target_demographic IS NOT NULL AND visual_style IS NOT NULL\n",
        "                      )\n",
        "                      GROUP BY brand\n",
        "                    )\n",
        "                    SELECT \n",
        "                      brand,\n",
        "                      primary_demographic,\n",
        "                      ROUND(demo_count * 100.0 / total_ads, 1) as demo_percentage,\n",
        "                      primary_style,\n",
        "                      ROUND(style_count * 100.0 / total_ads, 1) as style_percentage,\n",
        "                      total_ads\n",
        "                    FROM brand_modes\n",
        "                    ORDER BY total_ads DESC\n",
        "                    \"\"\"\n",
        "                    \n",
        "                    modal_result = run_query(modal_query)\n",
        "                    \n",
        "                    if not modal_result.empty:\n",
        "                        modal_df = modal_result[['brand', 'primary_demographic', 'demo_percentage', \n",
        "                                               'primary_style', 'style_percentage', 'total_ads']].copy()\n",
        "                        modal_df.columns = ['Brand', 'Primary Demographic', 'Demo %', \n",
        "                                          'Primary Style', 'Style %', 'Total Ads']\n",
        "                        \n",
        "                        display(modal_df)\n",
        "                        \n",
        "                        print(\"\\n🎯 KEY MODAL INSIGHTS:\")\n",
        "                        for _, row in modal_result.iterrows():\n",
        "                            print(f\"• {row['brand']}: {row['demo_percentage']:.1f}% {row['primary_demographic']}, {row['style_percentage']:.1f}% {row['primary_style']}\")\n",
        "                \n",
        "                print(\"\\n✅ ENHANCED PMF HISTOGRAM ANALYSIS COMPLETE!\")\n",
        "                print(\"📊 Proper PMF histograms showing probability distributions\")\n",
        "                print(\"🎯 Grouped bar charts for easy brand comparison\")\n",
        "                print(\"📋 Supporting data tables for exact values\")\n",
        "                print(\"💡 Use histograms to spot distribution patterns and competitive gaps\")\n"
    ]

    # Replace the PMF section
    new_source = existing_source[:pmf_start] + improved_pmf_code + existing_source[pmf_end:]

    # Update the cell
    cells[stage6_analysis_index]['source'] = new_source

    # Update the notebook
    notebook['cells'] = cells

    # Write back
    with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
        json.dump(notebook, f, indent=1)

    print(f"✅ Successfully added proper PMF histogram visualizations!")
    print(f"   📊 Created grouped bar chart histograms for demographic and visual style PMFs")
    print(f"   🎯 Each brand gets its own color in side-by-side comparison")
    print(f"   📋 Includes supporting data tables for exact percentages")
    print(f"   💡 True PMF visualization showing probability distributions")

else:
    print("❌ Could not find PMF section to replace")