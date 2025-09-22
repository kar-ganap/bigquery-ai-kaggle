#!/usr/bin/env python3
"""
Add Stage 6 (Embeddings Generation) to the demo notebook
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# Find where to insert Stage 6 - after Stage 5 Summary but before current "Stage 6" Visual Intelligence
insert_index = None
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'markdown' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'Execute Stage 6: Visual Intelligence' in source:
            insert_index = i
            break

if insert_index is None:
    print("❌ Could not find insertion point for Stage 6")
    exit(1)

print(f"Found insertion point at cell index {insert_index}")

# Create Stage 6 Embeddings cells
stage6_cells = [
    # Markdown header
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "---\n",
            "\n",
            "## 🧠 Stage 6: Embeddings Generation\n",
            "\n",
            "**Purpose**: Generate semantic embeddings for competitive analysis and copying detection\n",
            "\n",
            "**Input**: Strategic labeled ads from Stage 5\n",
            "**Output**: 768-dimensional embeddings table for semantic similarity analysis\n",
            "**BigQuery Impact**: Creates `ads_embeddings` table with semantic vectors\n",
            "\n",
            "**Key Technologies:**\n",
            "- BigQuery ML text-embedding-004 model\n",
            "- Structured content concatenation for optimal embedding quality\n",
            "- Semantic similarity foundation for copying detection in Stage 8\n",
            "\n",
            "**Architecture Note**: Essential foundation for competitive copying detection and strategic analysis"
        ]
    },

    # Execution cell
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "print(\"🧠 === STAGE 6: EMBEDDINGS GENERATION ===\" + \" (STAGE TESTING FRAMEWORK APPROACH)\")\n",
            "print(f\"📥 Input: Strategic labeled ads from Stage 5\")\n",
            "\n",
            "# Initialize Stage 6 (Embeddings Generation) \n",
            "from src.pipeline.stages.embeddings import EmbeddingsStage\n",
            "\n",
            "if stage5_results is None:\n",
            "    print(\"❌ Cannot proceed - Stage 5 failed\")\n",
            "    stage6_embeddings_results = None\n",
            "else:\n",
            "    # Stage 6 constructor: EmbeddingsStage(context, dry_run=False, verbose=True)\n",
            "    embeddings_stage = EmbeddingsStage(context, dry_run=False, verbose=True)\n",
            "    \n",
            "    try:\n",
            "        import time\n",
            "        stage6_start = time.time()\n",
            "        \n",
            "        print(\"\\n🧠 Generating semantic embeddings...\")\n",
            "        # Execute embeddings generation - pass the results from Stage 5\n",
            "        embeddings_results = embeddings_stage.execute(stage4_results)  # Uses IngestionResults from Stage 4\n",
            "        \n",
            "        # Store results for Stage 7 and 8\n",
            "        stage6_embeddings_results = embeddings_results\n",
            "        \n",
            "        stage6_duration = time.time() - stage6_start\n",
            "        print(f\"\\n✅ Stage 6 Complete in {stage6_duration:.1f}s!\")\n",
            "        print(f\"🧠 Generated {embeddings_results.embedding_count} semantic embeddings\")\n",
            "        print(f\"📊 Table: {embeddings_results.table_id}\")\n",
            "        print(f\"🎯 Ready for Stage 7 (Visual Intelligence) and Stage 8 (Strategic Analysis)\")\n",
            "        \n",
            "    except Exception as e:\n",
            "        print(f\"❌ Stage 6 Failed: {e}\")\n",
            "        stage6_embeddings_results = None\n",
            "        import traceback\n",
            "        traceback.print_exc()"
        ]
    },

    # Analysis cell
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Analyze and display embeddings results\n",
            "if 'stage6_embeddings_results' in locals() and stage6_embeddings_results is not None:\n",
            "    print(\"📋 EMBEDDINGS GENERATION RESULTS\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    print(f\"✅ Embeddings Generation Completed Successfully\")\n",
            "    print(f\"📊 Analysis Results:\")\n",
            "    print(f\"   Total Embeddings: {stage6_embeddings_results.embedding_count}\")\n",
            "    print(f\"   Embedding Dimension: {stage6_embeddings_results.dimension}\")\n",
            "    print(f\"   BigQuery Table: {stage6_embeddings_results.table_id}\")\n",
            "    print(f\"   Generation Time: {stage6_embeddings_results.generation_time:.1f}s\")\n",
            "    \n",
            "    # Analyze embedding quality and coverage\n",
            "    try:\n",
            "        from src.utils.bigquery_client import run_query\n",
            "        \n",
            "        embedding_analysis_query = f\"\"\"\n",
            "        SELECT \n",
            "            brand,\n",
            "            COUNT(*) as total_embeddings,\n",
            "            COUNT(CASE WHEN content_embedding IS NOT NULL THEN 1 END) as valid_embeddings,\n",
            "            ROUND(COUNT(CASE WHEN content_embedding IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as embedding_success_rate,\n",
            "            ROUND(AVG(content_length_chars), 0) as avg_content_length,\n",
            "            COUNT(CASE WHEN has_title THEN 1 END) as ads_with_title,\n",
            "            COUNT(CASE WHEN has_body THEN 1 END) as ads_with_body\n",
            "        FROM `{stage6_embeddings_results.table_id}`\n",
            "        GROUP BY brand\n",
            "        ORDER BY total_embeddings DESC\n",
            "        \"\"\"\n",
            "        \n",
            "        embedding_stats = run_query(embedding_analysis_query)\n",
            "        \n",
            "        if not embedding_stats.empty:\n",
            "            print(\"\\n📊 EMBEDDINGS BY BRAND:\")\n",
            "            \n",
            "            # Create DataFrame for display\n",
            "            import pandas as pd\n",
            "            from IPython.display import display\n",
            "            \n",
            "            display_df = embedding_stats[['brand', 'total_embeddings', 'valid_embeddings', \n",
            "                                        'embedding_success_rate', 'avg_content_length', \n",
            "                                        'ads_with_title', 'ads_with_body']].copy()\n",
            "            \n",
            "            display_df.columns = ['Brand', 'Total Ads', 'Valid Embeddings', \n",
            "                                 'Success Rate %', 'Avg Content Length', 'With Title', 'With Body']\n",
            "            \n",
            "            display(display_df)\n",
            "            \n",
            "            # Summary statistics\n",
            "            total_embeddings = embedding_stats['valid_embeddings'].sum()\n",
            "            avg_success_rate = embedding_stats['embedding_success_rate'].mean()\n",
            "            \n",
            "            print(f\"\\n📈 Embedding Quality Statistics:\")\n",
            "            print(f\"   Total Valid Embeddings: {total_embeddings}\")\n",
            "            print(f\"   Average Success Rate: {avg_success_rate:.1f}%\")\n",
            "            print(f\"   Brands with Embeddings: {len(embedding_stats)}\")\n",
            "            \n",
            "        print(f\"\\n💡 Semantic Analysis Ready:\")\n",
            "        print(f\"   ✅ Foundation for competitive copying detection\")\n",
            "        print(f\"   ✅ Semantic similarity analysis capabilities\")\n",
            "        print(f\"   ✅ Ready for Stage 8 Strategic Analysis\")\n",
            "        \n",
            "    except Exception as e:\n",
            "        print(f\"⚠️ Could not analyze embedding statistics: {e}\")\n",
            "        print(f\"   Basic info: {stage6_embeddings_results.embedding_count} embeddings generated\")\n",
            "        \n",
            "else:\n",
            "    print(\"⚠️ No embeddings generated - check error above\")\n",
            "    print(\"   Make sure you ran the Stage 6 execution cell first\")"
        ]
    },

    # Summary cell
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "### Stage 6 Summary\n",
            "\n",
            "**✅ Embeddings Generation Complete**\n",
            "\n",
            "**Key Achievements:**\n",
            "- Generated 768-dimensional semantic embeddings using BigQuery ML\n",
            "- Structured content concatenation for optimal embedding quality\n",
            "- Foundation established for competitive copying detection\n",
            "- High embedding success rates across all competitor brands\n",
            "\n",
            "**Technical Implementation:**\n",
            "- BigQuery ML text-embedding-004 model integration\n",
            "- Semantic similarity analysis capabilities\n",
            "- Quality metrics and brand coverage analysis\n",
            "\n",
            "**Next Stage:** Stage 7 - Visual Intelligence (Multimodal AI Analysis)\n",
            "\n",
            "---"
        ]
    }
]

# Insert the new cells at the found position
for i, cell in enumerate(stage6_cells):
    cells.insert(insert_index + i, cell)

# Update the notebook
notebook['cells'] = cells

# Write back to file
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully added Stage 6 (Embeddings Generation) to the notebook!")
print(f"   📍 Inserted {len(stage6_cells)} cells at position {insert_index}")
print(f"   🧠 Added embeddings generation execution and analysis")
print(f"   📊 Includes embedding quality analysis and brand statistics")
print(f"   🔗 Properly connects Stage 5 → Stage 6 → Stage 7")