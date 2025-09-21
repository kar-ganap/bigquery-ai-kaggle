#!/usr/bin/env python3
"""Add Stage 4 (Meta Ads Ingestion) cells to the notebook"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Stage 4 markdown header
stage4_header = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "---\n",
        "\n",
        "## 📱 Stage 4: Meta Ads Ingestion\n",
        "\n",
        "**Purpose**: Parallel fetching of actual Meta ads from active competitors with intelligent deduplication\n",
        "\n",
        "**Input**: ~4 Meta-active competitors from Stage 3\n",
        "**Output**: ~200-400 ads from 4-5 brands (including target brand)\n",
        "**BigQuery Impact**: Creates `ads_raw_*` table and updates `ads_with_dates` with deduplication\n",
        "\n",
        "**Process**:\n",
        "- Multi-threaded ad collection (3 parallel workers)\n",
        "- Fetch ads for competitors + target brand\n",
        "- Normalize ad data to pipeline format\n",
        "- Load to BigQuery with intelligent deduplication\n",
        "- Update `ads_with_dates` with new Option C deduplication logic\n",
        "\n",
        "**New Feature**: API variability handling with ROW_NUMBER() deduplication"
    ]
}

# Stage 4 execution cell
stage4_execution = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Execute Stage 4: Meta Ads Ingestion\n",
        "print(\"📱 STAGE 4: META ADS INGESTION\")\n",
        "print(\"=\" * 50)\n",
        "print(\"Parallel fetching of Meta ads from active competitors...\")\n",
        "print()\n",
        "\n",
        "# Import required stage\n",
        "from src.pipeline.stages.ingestion import IngestionStage\n",
        "\n",
        "# Time the ingestion process\n",
        "stage4_start = time.time()\n",
        "\n",
        "try:\n",
        "    # Check if we have ranked competitors\n",
        "    if not ranked_competitors:\n",
        "        raise ValueError(\"No ranked competitors found. Run Stage 3 first.\")\n",
        "    \n",
        "    print(f\"📥 Input: {len(ranked_competitors)} Meta-active competitors\")\n",
        "    print(\"🚀 Starting parallel ad collection with 3 workers...\")\n",
        "    print()\n",
        "    \n",
        "    # Initialize and run ingestion stage\n",
        "    ingestion_stage = IngestionStage(context, dry_run=False, verbose=True)\n",
        "    ingestion_results = ingestion_stage.run(ranked_competitors, progress)\n",
        "    \n",
        "    stage4_duration = time.time() - stage4_start\n",
        "    \n",
        "    print(f\"\\n✅ Stage 4 Complete!\")\n",
        "    print(f\"⏱️  Duration: {stage4_duration:.1f} seconds\")\n",
        "    print(f\"📊 Total Ads Collected: {ingestion_results.total_ads}\")\n",
        "    print(f\"🏢 Brands with Ads: {len(ingestion_results.brands)}\")\n",
        "    if ingestion_results.ads_table_id:\n",
        "        print(f\"💾 BigQuery Table: {ingestion_results.ads_table_id}\")\n",
        "        print(f\"🧠 Intelligent Deduplication: Applied to ads_with_dates\")\n",
        "    \n",
        "except Exception as e:\n",
        "    stage4_duration = time.time() - stage4_start\n",
        "    print(f\"\\n❌ Stage 4 Failed after {stage4_duration:.1f}s\")\n",
        "    print(f\"Error: {e}\")\n",
        "    ingestion_results = None"
    ]
}

# Stage 4 analysis cell
stage4_analysis = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Analyze and display ingestion results\n",
        "if ingestion_results and ingestion_results.total_ads > 0:\n",
        "    print(\"📋 META ADS INGESTION RESULTS\")\n",
        "    print(\"=\" * 35)\n",
        "    \n",
        "    # Create brand-wise breakdown\n",
        "    brand_data = []\n",
        "    \n",
        "    # Count ads per brand from the actual results\n",
        "    brand_counts = {}\n",
        "    for ad in ingestion_results.ads:\n",
        "        brand = ad.get('brand', 'Unknown')\n",
        "        brand_counts[brand] = brand_counts.get(brand, 0) + 1\n",
        "    \n",
        "    total_competitor_ads = 0\n",
        "    for i, brand in enumerate(brand_counts.keys(), 1):\n",
        "        count = brand_counts[brand]\n",
        "        is_target = brand.lower() == context.brand.lower()\n",
        "        brand_type = \"Target Brand\" if is_target else \"Competitor\"\n",
        "        \n",
        "        if not is_target:\n",
        "            total_competitor_ads += count\n",
        "        \n",
        "        brand_data.append({\n",
        "            'Rank': i,\n",
        "            'Brand': brand,\n",
        "            'Type': brand_type,\n",
        "            'Ads Collected': count,\n",
        "            'Percentage': f\"{count/ingestion_results.total_ads*100:.1f}%\"\n",
        "        })\n",
        "    \n",
        "    # Sort by ad count\n",
        "    brand_data.sort(key=lambda x: x['Ads Collected'], reverse=True)\n",
        "    \n",
        "    brand_df = pd.DataFrame(brand_data)\n",
        "    \n",
        "    print(f\"📊 Ad Collection by Brand:\")\n",
        "    display(brand_df)\n",
        "    \n",
        "    # Show ingestion statistics\n",
        "    print(f\"\\n📈 Ingestion Summary:\")\n",
        "    print(f\"   Total Ads: {ingestion_results.total_ads:,}\")\n",
        "    print(f\"   Competitor Ads: {total_competitor_ads:,}\")\n",
        "    print(f\"   Target Brand Ads: {ingestion_results.total_ads - total_competitor_ads:,}\")\n",
        "    print(f\"   Brands Represented: {len(ingestion_results.brands)}\")\n",
        "    print(f\"   Collection Rate: {ingestion_results.total_ads/len(ranked_competitors):.0f} ads per competitor\")\n",
        "    \n",
        "    # Sample ad preview\n",
        "    if ingestion_results.ads:\n",
        "        print(f\"\\n📋 Sample Ad Preview (First 3 Ads):\")\n",
        "        for i, ad in enumerate(ingestion_results.ads[:3], 1):\n",
        "            brand = ad.get('brand', 'Unknown')\n",
        "            title = ad.get('title', 'No title')[:60]\n",
        "            text = ad.get('creative_text', 'No text')[:100]\n",
        "            print(f\"   {i}. {brand}: '{title}' - {text}...\")\n",
        "    \n",
        "    # Data quality check\n",
        "    print(f\"\\n🔍 Data Quality Check:\")\n",
        "    ads_with_text = sum(1 for ad in ingestion_results.ads if ad.get('creative_text', '').strip())\n",
        "    ads_with_images = sum(1 for ad in ingestion_results.ads if ad.get('image_urls') or ad.get('image_url'))\n",
        "    ads_with_video = sum(1 for ad in ingestion_results.ads if ad.get('video_urls') or ad.get('video_url'))\n",
        "    \n",
        "    print(f\"   Ads with Text: {ads_with_text} ({ads_with_text/ingestion_results.total_ads*100:.1f}%)\")\n",
        "    print(f\"   Ads with Images: {ads_with_images} ({ads_with_images/ingestion_results.total_ads*100:.1f}%)\")\n",
        "    print(f\"   Ads with Video: {ads_with_video} ({ads_with_video/ingestion_results.total_ads*100:.1f}%)\")\n",
        "    \n",
        "else:\n",
        "    print(\"⚠️ No ads were collected\")\n",
        "    print(\"   This could mean:\")\n",
        "    print(\"   • Meta Ad Library API issues\")\n",
        "    print(\"   • Competitors have stopped advertising\")\n",
        "    print(\"   • Rate limiting or access restrictions\")"
    ]
}

# Stage 4 BigQuery verification cell
stage4_bigquery = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Verify BigQuery impact and deduplication\n",
        "if ingestion_results and ingestion_results.ads_table_id:\n",
        "    print(\"📊 BIGQUERY IMPACT VERIFICATION\")\n",
        "    print(\"=\" * 40)\n",
        "    \n",
        "    try:\n",
        "        # Check the main ads table\n",
        "        ads_query = f\"\"\"\n",
        "        SELECT \n",
        "            COUNT(*) as total_ads,\n",
        "            COUNT(DISTINCT brand) as unique_brands,\n",
        "            COUNT(DISTINCT ad_archive_id) as unique_ad_ids,\n",
        "            COUNT(CASE WHEN creative_text IS NOT NULL AND creative_text != '' THEN 1 END) as ads_with_text,\n",
        "            COUNT(CASE WHEN image_url IS NOT NULL THEN 1 END) as ads_with_images\n",
        "        FROM `{ingestion_results.ads_table_id}`\n",
        "        \"\"\"\n",
        "        \n",
        "        ads_stats = run_query(ads_query)\n",
        "        \n",
        "        if not ads_stats.empty:\n",
        "            row = ads_stats.iloc[0]\n",
        "            print(f\"✅ Raw Ads Table: {ingestion_results.ads_table_id.split('.')[-1]}\")\n",
        "            print(f\"   Total Ads: {row['total_ads']:,}\")\n",
        "            print(f\"   Unique Brands: {row['unique_brands']}\")\n",
        "            print(f\"   Unique Ad IDs: {row['unique_ad_ids']:,}\")\n",
        "            print(f\"   Ads with Text: {row['ads_with_text']:,}\")\n",
        "            print(f\"   Ads with Images: {row['ads_with_images']:,}\")\n",
        "        \n",
        "        # Check ads_with_dates table for deduplication\n",
        "        dedup_query = f\"\"\"\n",
        "        SELECT \n",
        "            COUNT(*) as total_ads,\n",
        "            COUNT(DISTINCT ad_archive_id) as unique_ad_ids,\n",
        "            COUNT(DISTINCT brand) as unique_brands,\n",
        "            MIN(_ingestion_timestamp) as earliest_ingestion,\n",
        "            MAX(_ingestion_timestamp) as latest_ingestion\n",
        "        FROM `{BQ_FULL_DATASET}.ads_with_dates`\n",
        "        \"\"\"\n",
        "        \n",
        "        dedup_stats = run_query(dedup_query)\n",
        "        \n",
        "        if not dedup_stats.empty:\n",
        "            row = dedup_stats.iloc[0]\n",
        "            print(f\"\\n🧠 Deduplicated Table: ads_with_dates\")\n",
        "            print(f\"   Total Ads: {row['total_ads']:,}\")\n",
        "            print(f\"   Unique Ad IDs: {row['unique_ad_ids']:,}\")\n",
        "            print(f\"   Unique Brands: {row['unique_brands']}\")\n",
        "            print(f\"   Ingestion Span: {row['earliest_ingestion']} to {row['latest_ingestion']}\")\n",
        "            \n",
        "            # Calculate deduplication effectiveness\n",
        "            raw_total = ads_stats.iloc[0]['total_ads']\n",
        "            dedup_total = row['total_ads']\n",
        "            if dedup_total < raw_total:\n",
        "                print(f\"   📉 Deduplication: {raw_total - dedup_total} duplicates removed\")\n",
        "            else:\n",
        "                print(f\"   ✅ No duplicates detected in this run\")\n",
        "        \n",
        "        # Sample ads from BigQuery\n",
        "        sample_query = f\"\"\"\n",
        "        SELECT brand, title, LEFT(creative_text, 80) as preview_text\n",
        "        FROM `{ingestion_results.ads_table_id}`\n",
        "        WHERE creative_text IS NOT NULL\n",
        "        ORDER BY RAND()\n",
        "        LIMIT 5\n",
        "        \"\"\"\n",
        "        \n",
        "        sample_data = run_query(sample_query)\n",
        "        \n",
        "        if not sample_data.empty:\n",
        "            print(f\"\\n📋 Random Ad Sample from BigQuery:\")\n",
        "            display(sample_data)\n",
        "        \n",
        "        print(f\"\\n💡 Stage 4 BigQuery Impact:\")\n",
        "        print(f\"   ✅ Created {ingestion_results.ads_table_id.split('.')[-1]} with raw ads\")\n",
        "        print(f\"   🧠 Updated ads_with_dates with intelligent deduplication\")\n",
        "        print(f\"   📊 Ready for Stage 5 (Strategic Labeling)\")\n",
        "        \n",
        "    except Exception as e:\n",
        "        print(f\"❌ Error verifying BigQuery tables: {e}\")\n",
        "        \n",
        "else:\n",
        "    print(\"⚠️ No BigQuery table created - ingestion may have failed\")"
    ]
}

# Stage 4 readiness cell
stage4_readiness = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Stage 5 Readiness Assessment\n",
        "if ingestion_results and ingestion_results.total_ads > 0:\n",
        "    print(\"🚀 STAGE 5 READINESS ASSESSMENT\")\n",
        "    print(\"=\" * 40)\n",
        "    \n",
        "    # Assess data quality for strategic labeling\n",
        "    text_ads = sum(1 for ad in ingestion_results.ads if ad.get('creative_text', '').strip())\n",
        "    image_ads = sum(1 for ad in ingestion_results.ads if ad.get('image_urls') or ad.get('image_url'))\n",
        "    \n",
        "    print(f\"📊 Data Quality Assessment:\")\n",
        "    text_quality = \"Excellent\" if text_ads > ingestion_results.total_ads * 0.8 else \"Good\" if text_ads > ingestion_results.total_ads * 0.5 else \"Fair\"\n",
        "    image_quality = \"Excellent\" if image_ads > ingestion_results.total_ads * 0.8 else \"Good\" if image_ads > ingestion_results.total_ads * 0.5 else \"Fair\"\n",
        "    \n",
        "    print(f\"   Text Content: {text_quality} ({text_ads}/{ingestion_results.total_ads} ads)\")\n",
        "    print(f\"   Image Content: {image_quality} ({image_ads}/{ingestion_results.total_ads} ads)\")\n",
        "    \n",
        "    # Competitive analysis readiness\n",
        "    competitor_brands = [b for b in ingestion_results.brands if b.lower() != context.brand.lower()]\n",
        "    print(f\"\\n🎯 Competitive Analysis Readiness:\")\n",
        "    print(f\"   Competitor Brands: {len(competitor_brands)} ({', '.join(competitor_brands)})\")\n",
        "    print(f\"   Target Brand: {context.brand}\")\n",
        "    print(f\"   Cross-Brand Analysis: {'Ready' if len(competitor_brands) >= 2 else 'Limited'}\")\n",
        "    \n",
        "    # Strategic labeling preview\n",
        "    if text_ads >= 10:\n",
        "        print(f\"\\n🏷️  Strategic Labeling Preview:\")\n",
        "        print(f\"   ✅ Sufficient text content for AI analysis\")\n",
        "        print(f\"   ✅ Ready for product focus classification\")\n",
        "        print(f\"   ✅ Ready for messaging theme analysis\")\n",
        "        print(f\"   ✅ Ready for CTA strategy assessment\")\n",
        "    else:\n",
        "        print(f\"\\n⚠️  Limited Strategic Labeling Capability:\")\n",
        "        print(f\"   📉 Only {text_ads} ads with text content\")\n",
        "        print(f\"   💡 Consider expanding ad collection\")\n",
        "    \n",
        "    # Store results for next stage\n",
        "    print(f\"\\n💾 Data Preparation Complete:\")\n",
        "    print(f\"   📊 {ingestion_results.total_ads} ads ready for strategic analysis\")\n",
        "    print(f\"   🏢 {len(ingestion_results.brands)} brands for competitive comparison\")\n",
        "    print(f\"   🎯 Cross-competitive intelligence analysis enabled\")\n",
        "    \n",
        "else:\n",
        "    print(\"❌ Stage 5 Not Ready - No ads collected\")\n",
        "    print(\"   Cannot proceed to Strategic Labeling without ad data\")"
    ]
}

# Stage 4 summary markdown
stage4_summary = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "### Stage 4 Summary\n",
        "\n",
        "**✅ Meta Ads Ingestion Complete**\n",
        "\n",
        "**Key Achievements:**\n",
        "- Parallel ad collection from Meta-active competitors\n",
        "- Multi-threaded processing with 3 workers\n",
        "- Comprehensive ad data normalization\n",
        "- BigQuery table creation with intelligent deduplication\n",
        "- API variability handling with Option C deduplication\n",
        "\n",
        "**Outputs:**\n",
        "- Raw ads table (`ads_raw_*`) with complete ad dataset\n",
        "- Updated `ads_with_dates` with deduplicated historical data\n",
        "- Multi-brand competitive dataset ready for analysis\n",
        "- Quality-assessed content for strategic labeling\n",
        "\n",
        "**Next Stage:** Strategic Labeling (Stage 5) - AI-powered ad classification and theme analysis"
    ]
}

# Find the last Stage 3 cell and insert after it
# Look for Stage 3 summary to know where to insert
insert_position = None
for i, cell in enumerate(nb['cells']):
    if cell.get('cell_type') == 'markdown':
        source = ''.join(cell.get('source', []))
        if 'Stage 3 Summary' in source:
            insert_position = i + 1
            break

if insert_position is None:
    # Fallback: insert at the end
    insert_position = len(nb['cells'])

new_cells = [stage4_header, stage4_execution, stage4_analysis, stage4_bigquery, stage4_readiness, stage4_summary]

for i, cell in enumerate(new_cells):
    nb['cells'].insert(insert_position + i, cell)

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Added Stage 4 (Meta Ads Ingestion) to notebook: {notebook_path}")
print("   - Added Stage 4 header with intelligent deduplication overview")
print("   - Added parallel ingestion execution with progress tracking")
print("   - Added comprehensive results analysis and brand breakdown")
print("   - Added BigQuery verification with deduplication assessment")
print("   - Added Stage 5 readiness evaluation")
print("   - Added Stage 4 summary")
print(f"\\n📋 New cells added after position {insert_position}")
print("   Close and reopen notebook to see the new Stage 4 section")