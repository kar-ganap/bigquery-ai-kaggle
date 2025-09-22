#!/usr/bin/env python3
"""
Update the visual intelligence display to include PMF visualization and modal analysis
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# Find the Stage 6 analysis cell (the one we just updated)
stage6_analysis_index = None
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'VISUAL INTELLIGENCE - COMPETITIVE POSITIONING ANALYSIS' in source:
            stage6_analysis_index = i
            break

if stage6_analysis_index is None:
    print("❌ Could not find Stage 6 visual intelligence analysis cell")
    exit(1)

print(f"Found Stage 6 analysis cell at index {stage6_analysis_index}")

# Get the existing cell and add PMF visualization
existing_cell = cells[stage6_analysis_index]
existing_source = existing_cell['source']

# Find where to insert the new PMF code (after demographic analysis)
insert_index = None
for i, line in enumerate(existing_source):
    if 'Use these insights to identify visual differentiation opportunities' in line:
        insert_index = i + 1
        break

if insert_index:
    # Insert PMF visualization code
    pmf_code = [
        "                \n",
        "                # PMF VISUALIZATION & MODAL ANALYSIS\n",
        "                print(\"\\n📊 PROBABILITY MASS FUNCTIONS (PMF) - DEMOGRAPHIC DISTRIBUTION\")\n",
        "                print(\"Visual comparison of target audience across brands:\")\n",
        "                print()\n",
        "                \n",
        "                # Get demographic PMF data\n",
        "                pmf_demo_query = f\"\"\"\n",
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
        "                    # Create PMF visualization (text-based for notebook)\n",
        "                    import matplotlib.pyplot as plt\n",
        "                    import numpy as np\n",
        "                    \n",
        "                    # All possible demographic buckets\n",
        "                    all_demographics = ['YOUNG_ADULTS', 'PROFESSIONALS', 'FAMILIES', 'AFFLUENT', 'SENIORS']\n",
        "                    brands = pmf_demo_result['brand'].unique()\n",
        "                    \n",
        "                    print(\"🎯 DEMOGRAPHIC PMF BY BRAND:\")\n",
        "                    print(\"(Probability Mass Function - shows targeting distribution)\")\n",
        "                    print()\n",
        "                    \n",
        "                    # Create PMF matrix for display\n",
        "                    pmf_data = []\n",
        "                    for brand in brands:\n",
        "                        brand_data = pmf_demo_result[pmf_demo_result['brand'] == brand]\n",
        "                        row = {'Brand': brand}\n",
        "                        \n",
        "                        for demo in all_demographics:\n",
        "                            demo_row = brand_data[brand_data['target_demographic'] == demo]\n",
        "                            percentage = demo_row['percentage'].iloc[0] if not demo_row.empty else 0.0\n",
        "                            row[demo] = f\"{percentage:.1f}%\"\n",
        "                        \n",
        "                        pmf_data.append(row)\n",
        "                    \n",
        "                    # Create DataFrame for PMF\n",
        "                    pmf_df = pd.DataFrame(pmf_data)\n",
        "                    pmf_df = pmf_df.set_index('Brand')\n",
        "                    display(pmf_df)\n",
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
        "                print(\"\\n✅ PMF & MODAL ANALYSIS COMPLETE!\")\n",
        "                print(\"📊 PMF shows targeting distribution across demographic buckets\")\n",
        "                print(\"📋 Modal analysis identifies each brand's primary audience and visual approach\")\n",
        "                print(\"🎯 Use this to identify demographic positioning gaps and opportunities\")\n"
    ]

    # Insert the PMF code
    existing_source[insert_index:insert_index] = pmf_code

    # Update the cell
    cells[stage6_analysis_index]['source'] = existing_source

    # Update the notebook
    notebook['cells'] = cells

    # Write back
    with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
        json.dump(notebook, f, indent=1)

    print(f"✅ Successfully added PMF visualization and modal analysis!")
    print(f"   Features: Demographic PMF matrix, visual style modal analysis, competitive insights")
    print(f"   Benefits: Clear brand comparison, targeting distribution, positioning gaps")

else:
    print("❌ Could not find insertion point for PMF code")