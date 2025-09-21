#!/usr/bin/env python3
"""Add Stage 2 (AI Curation) cells to the notebook"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Stage 2 markdown header
stage2_header = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "---\n",
        "\n",
        "## 🎯 Stage 2: AI Competitor Curation\n",
        "\n",
        "**Purpose**: AI-powered validation and filtering of competitor candidates using 3-round consensus validation\n",
        "\n",
        "**Input**: ~400-500 raw competitor candidates from Stage 1\n",
        "**Output**: ~7 validated, high-confidence competitors\n",
        "**BigQuery Impact**: Creates `competitors_batch_*` tables for AI processing and `competitors_raw_*` for final results\n",
        "\n",
        "**AI Process**:\n",
        "- 3-round consensus AI validation using Gemini\n",
        "- Market overlap analysis\n",
        "- Confidence scoring\n",
        "- Quality filtering"
    ]
}

# Stage 2 execution cell
stage2_execution = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Execute Stage 2: AI Competitor Curation\n",
        "print(\"🎯 STAGE 2: AI COMPETITOR CURATION\")\n",
        "print(\"=\" * 50)\n",
        "print(\"Using 3-round AI consensus to validate and filter competitors...\")\n",
        "print()\n",
        "\n",
        "# Import required stage\n",
        "from src.pipeline.stages.curation import CurationStage\n",
        "\n",
        "# Time the curation process\n",
        "stage2_start = time.time()\n",
        "\n",
        "try:\n",
        "    # Check if we have discovery results\n",
        "    if not competitors_discovered:\n",
        "        raise ValueError(\"No discovery results found. Run Stage 1 first.\")\n",
        "    \n",
        "    print(f\"📥 Input: {len(competitors_discovered)} raw competitor candidates\")\n",
        "    print(\"🤖 Starting AI validation process...\")\n",
        "    \n",
        "    # Initialize and run curation stage\n",
        "    curation_stage = CurationStage(context, dry_run=False)\n",
        "    curated_competitors = curation_stage.run(competitors_discovered, progress)\n",
        "    \n",
        "    stage2_duration = time.time() - stage2_start\n",
        "    \n",
        "    print(f\"\\n✅ Stage 2 Complete!\")\n",
        "    print(f\"⏱️  Duration: {stage2_duration:.1f} seconds\")\n",
        "    print(f\"📊 Curated Competitors: {len(curated_competitors)}\")\n",
        "    print(f\"🎯 Filtering Ratio: {len(curated_competitors)}/{len(competitors_discovered)} ({len(curated_competitors)/len(competitors_discovered)*100:.1f}%)\")\n",
        "    \n",
        "except Exception as e:\n",
        "    stage2_duration = time.time() - stage2_start\n",
        "    print(f\"\\n❌ Stage 2 Failed after {stage2_duration:.1f}s\")\n",
        "    print(f\"Error: {e}\")\n",
        "    curated_competitors = []"
    ]
}

# Stage 2 analysis cell
stage2_analysis = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Analyze and display curation results\n",
        "if curated_competitors:\n",
        "    print(\"📋 AI CURATION RESULTS ANALYSIS\")\n",
        "    print(\"=\" * 40)\n",
        "    \n",
        "    # Create a summary DataFrame for display\n",
        "    curation_data = []\n",
        "    for i, competitor in enumerate(curated_competitors):\n",
        "        curation_data.append({\n",
        "            'Rank': i + 1,\n",
        "            'Company': competitor.company_name,\n",
        "            'Confidence': f\"{competitor.confidence:.3f}\",\n",
        "            'Quality Score': f\"{competitor.quality_score:.3f}\",\n",
        "            'Market Overlap': f\"{competitor.market_overlap_pct}%\",\n",
        "            'AI Consensus': getattr(competitor, 'ai_consensus', 'N/A'),\n",
        "            'Reasoning': (competitor.reasoning[:60] + \"...\") if hasattr(competitor, 'reasoning') and len(competitor.reasoning) > 60 else getattr(competitor, 'reasoning', 'N/A')\n",
        "        })\n",
        "    \n",
        "    curation_df = pd.DataFrame(curation_data)\n",
        "    \n",
        "    print(f\"📊 Validated Competitors (AI Curated):\")\n",
        "    display(curation_df)\n",
        "    \n",
        "    # Show curation statistics\n",
        "    print(f\"\\n📈 AI Curation Statistics:\")\n",
        "    print(f\"   Input Candidates: {len(competitors_discovered)}\")\n",
        "    print(f\"   Output Competitors: {len(curated_competitors)}\")\n",
        "    print(f\"   Success Rate: {len(curated_competitors)/len(competitors_discovered)*100:.1f}%\")\n",
        "    \n",
        "    # Confidence and quality analysis\n",
        "    confidences = [c.confidence for c in curated_competitors]\n",
        "    quality_scores = [c.quality_score for c in curated_competitors]\n",
        "    market_overlaps = [c.market_overlap_pct for c in curated_competitors]\n",
        "    \n",
        "    print(f\"   Confidence Range: {min(confidences):.3f} - {max(confidences):.3f}\")\n",
        "    print(f\"   Average Confidence: {sum(confidences)/len(confidences):.3f}\")\n",
        "    print(f\"   Quality Score Range: {min(quality_scores):.3f} - {max(quality_scores):.3f}\")\n",
        "    print(f\"   Average Quality: {sum(quality_scores)/len(quality_scores):.3f}\")\n",
        "    print(f\"   Market Overlap Range: {min(market_overlaps)}% - {max(market_overlaps)}%\")\n",
        "    print(f\"   Average Market Overlap: {sum(market_overlaps)/len(market_overlaps):.1f}%\")\n",
        "    \n",
        "else:\n",
        "    print(\"⚠️ No competitors were curated - check error above\")"
    ]
}

# Stage 2 BigQuery analysis cell
stage2_bigquery = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Examine BigQuery impact of Stage 2\n",
        "print(\"📊 BIGQUERY IMPACT ANALYSIS - STAGE 2\")\n",
        "print(\"=\" * 45)\n",
        "\n",
        "try:\n",
        "    # Check if competitors_raw table was created by curation stage\n",
        "    raw_table_name = f\"competitors_raw_{demo_run_id}\"\n",
        "    \n",
        "    # Query the newly created table\n",
        "    bigquery_query = f\"\"\"\n",
        "    SELECT \n",
        "        COUNT(*) as total_rows,\n",
        "        COUNT(DISTINCT company_name) as unique_companies,\n",
        "        COUNT(DISTINCT source_url) as unique_sources,\n",
        "        ROUND(AVG(raw_score), 3) as avg_raw_score,\n",
        "        MIN(raw_score) as min_score,\n",
        "        MAX(raw_score) as max_score\n",
        "    FROM `{BQ_FULL_DATASET}.{raw_table_name}`\n",
        "    \"\"\"\n",
        "    \n",
        "    bq_results = run_query(bigquery_query)\n",
        "    \n",
        "    if not bq_results.empty:\n",
        "        row = bq_results.iloc[0]\n",
        "        print(f\"✅ BigQuery Table Created: {raw_table_name}\")\n",
        "        print(f\"📊 Table Statistics:\")\n",
        "        print(f\"   Total Rows: {row['total_rows']:,}\")\n",
        "        print(f\"   Unique Companies: {row['unique_companies']:,}\")\n",
        "        print(f\"   Unique Sources: {row['unique_sources']:,}\")\n",
        "        print(f\"   Score Range: {row['min_score']:.3f} - {row['max_score']:.3f}\")\n",
        "        print(f\"   Average Score: {row['avg_raw_score']:.3f}\")\n",
        "        \n",
        "        # Show sample of the BigQuery data\n",
        "        sample_query = f\"\"\"\n",
        "        SELECT company_name, raw_score, query_used, source_url\n",
        "        FROM `{BQ_FULL_DATASET}.{raw_table_name}`\n",
        "        ORDER BY raw_score DESC\n",
        "        LIMIT 5\n",
        "        \"\"\"\n",
        "        \n",
        "        sample_data = run_query(sample_query)\n",
        "        print(f\"\\n📋 Sample BigQuery Data (Top 5 by Score):\")\n",
        "        display(sample_data)\n",
        "        \n",
        "        print(f\"\\n💡 Stage 2 BigQuery Impact:\")\n",
        "        print(f\"   ✅ Created competitors_raw_{demo_run_id} table\")\n",
        "        print(f\"   📊 Stored {row['total_rows']} raw discovery candidates\")\n",
        "        print(f\"   🎯 Ready for Stage 3 (Meta Ad Activity Ranking)\")\n",
        "        \n",
        "    else:\n",
        "        print(\"⚠️ No data found in BigQuery table\")\n",
        "        \n",
        "except Exception as e:\n",
        "    print(f\"❌ Error accessing BigQuery: {e}\")\n",
        "    print(\"   This might be expected if curation stage failed\")\n",
        "    print(f\"   Expected table: {BQ_FULL_DATASET}.competitors_raw_{demo_run_id}\")"
    ]
}

# Stage 2 summary markdown
stage2_summary = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "### Stage 2 Summary\n",
        "\n",
        "**✅ AI Competitor Curation Complete**\n",
        "\n",
        "**Key Achievements:**\n",
        "- Applied 3-round AI consensus validation to filter candidates\n",
        "- Generated confidence scores and quality metrics\n",
        "- Calculated market overlap percentages\n",
        "- Created BigQuery table with raw discovery data\n",
        "\n",
        "**Outputs:**\n",
        "- Validated competitor list with AI confidence scores\n",
        "- `competitors_raw_*` BigQuery table for downstream processing\n",
        "- Quality metrics and market analysis\n",
        "\n",
        "**Next Stage:** Meta Ad Activity Ranking (Stage 3)"
    ]
}

# Insert the new cells after Cell 13 (Stage 1 summary)
insert_position = 14

new_cells = [stage2_header, stage2_execution, stage2_analysis, stage2_bigquery, stage2_summary]

for i, cell in enumerate(new_cells):
    nb['cells'].insert(insert_position + i, cell)

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Added Stage 2 (AI Curation) to notebook: {notebook_path}")
print("   - Added Stage 2 header and explanation")
print("   - Added Stage 2 execution cell with AI curation")
print("   - Added results analysis and BigQuery verification")
print("   - Added Stage 2 summary")
print(f"\\n📋 New cells added after position {insert_position}")
print("   Close and reopen notebook to see the new Stage 2 section")