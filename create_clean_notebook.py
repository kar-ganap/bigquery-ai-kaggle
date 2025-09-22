#!/usr/bin/env python3
"""
Create a clean notebook by copying essential cells in correct order
"""
import json

# Read the corrupted notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔧 CREATING CLEAN NOTEBOOK")
print("=" * 50)
print(f"📊 Source cells: {len(cells)}")

# Find essential cells by unique content signatures
essential_cells = {}

for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

        # Introduction
        if 'L4 Temporal Intelligence Framework' in source and 'Competitive Intelligence Journey' in source and not essential_cells.get('intro'):
            essential_cells['intro'] = cell
            print(f"✅ Found intro: cell {i}")

        # Setup
        elif 'Import required libraries' in source and 'import sys' in source and not essential_cells.get('setup_imports'):
            essential_cells['setup_imports'] = cell
            print(f"✅ Found setup_imports: cell {i}")

        elif 'Load environment variables from .env file' in source and not essential_cells.get('setup_env'):
            essential_cells['setup_env'] = cell
            print(f"✅ Found setup_env: cell {i}")

        # Stage 0
        elif 'Stage 0: Clean Slate Preparation' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage0_header'):
            essential_cells['stage0_header'] = cell
            print(f"✅ Found stage0_header: cell {i}")

        elif 'def get_dataset_table_count()' in source and not essential_cells.get('stage0_function'):
            essential_cells['stage0_function'] = cell
            print(f"✅ Found stage0_function: cell {i}")

        elif 'Execute clean slate preparation' in source and 'PRESERVING EXISTING ads_with_dates' in source and not essential_cells.get('stage0_execute'):
            essential_cells['stage0_execute'] = cell
            print(f"✅ Found stage0_execute: cell {i}")

        elif 'Check state after cleanup' in source and 'AFTER CLEANUP' in source and not essential_cells.get('stage0_check'):
            essential_cells['stage0_check'] = cell
            print(f"✅ Found stage0_check: cell {i}")

        elif 'Stage 0 Summary' in source and 'Clean slate preparation completed' in source and not essential_cells.get('stage0_summary'):
            essential_cells['stage0_summary'] = cell
            print(f"✅ Found stage0_summary: cell {i}")

        # Stage 1
        elif 'Stage 1: Discovery Engine' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage1_header'):
            essential_cells['stage1_header'] = cell
            print(f"✅ Found stage1_header: cell {i}")

        elif 'Initialize demo pipeline context' in source and 'demo_run_id' in source and not essential_cells.get('stage1_init'):
            essential_cells['stage1_init'] = cell
            print(f"✅ Found stage1_init: cell {i}")

        elif 'Execute Stage 1: Discovery Engine' in source and 'STAGE TESTING FRAMEWORK' in source and not essential_cells.get('stage1_execute'):
            essential_cells['stage1_execute'] = cell
            print(f"✅ Found stage1_execute: cell {i}")

        elif 'Analyze and display discovery results' in source and 'stage1_results' in source and not essential_cells.get('stage1_analyze'):
            essential_cells['stage1_analyze'] = cell
            print(f"✅ Found stage1_analyze: cell {i}")

        elif 'Examine Stage 1 Discovery Results' in source and 'In-Memory Analysis' in source and not essential_cells.get('stage1_examine'):
            essential_cells['stage1_examine'] = cell
            print(f"✅ Found stage1_examine: cell {i}")

        elif 'Stage 1 Summary' in source and 'Discovery Engine completed successfully' in source and not essential_cells.get('stage1_summary'):
            essential_cells['stage1_summary'] = cell
            print(f"✅ Found stage1_summary: cell {i}")

        # Stage 2
        elif 'Stage 2: AI Competitor Curation' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage2_header'):
            essential_cells['stage2_header'] = cell
            print(f"✅ Found stage2_header: cell {i}")

        elif 'Execute Stage 2: AI Competitor Curation' in source and 'STAGE TESTING FRAMEWORK' in source and not essential_cells.get('stage2_execute'):
            essential_cells['stage2_execute'] = cell
            print(f"✅ Found stage2_execute: cell {i}")

        elif 'Analyze and display curation results' in source and 'stage2_results' in source and not essential_cells.get('stage2_analyze'):
            essential_cells['stage2_analyze'] = cell
            print(f"✅ Found stage2_analyze: cell {i}")

        elif 'Examine BigQuery impact of Stage 2' in source and 'BIGQUERY IMPACT ANALYSIS - STAGE 2' in source and not essential_cells.get('stage2_bigquery'):
            essential_cells['stage2_bigquery'] = cell
            print(f"✅ Found stage2_bigquery: cell {i}")

        elif 'Stage 2 Summary' in source and 'AI Competitor Curation Complete' in source and not essential_cells.get('stage2_summary'):
            essential_cells['stage2_summary'] = cell
            print(f"✅ Found stage2_summary: cell {i}")

        # Stage 3
        elif 'Stage 3: Meta Ad Activity Ranking' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage3_header'):
            essential_cells['stage3_header'] = cell
            print(f"✅ Found stage3_header: cell {i}")

        elif 'Execute Stage 3: Meta Ad Activity Ranking' in source and 'STAGE TESTING FRAMEWORK' in source and not essential_cells.get('stage3_execute'):
            essential_cells['stage3_execute'] = cell
            print(f"✅ Found stage3_execute: cell {i}")

        elif 'def extract_numeric_count(estimated_count)' in source and not essential_cells.get('stage3_function'):
            essential_cells['stage3_function'] = cell
            print(f"✅ Found stage3_function: cell {i}")

        elif 'Meta Ad Activity Insights and Next Steps' in source and 'stage3_results' in source and not essential_cells.get('stage3_insights'):
            essential_cells['stage3_insights'] = cell
            print(f"✅ Found stage3_insights: cell {i}")

        elif 'Stage 3 Summary' in source and 'Meta Ad Activity Ranking Complete' in source and not essential_cells.get('stage3_summary'):
            essential_cells['stage3_summary'] = cell
            print(f"✅ Found stage3_summary: cell {i}")

        # Stage 4
        elif 'Stage 4: Meta Ads Ingestion' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage4_header'):
            essential_cells['stage4_header'] = cell
            print(f"✅ Found stage4_header: cell {i}")

        elif 'Execute Stage 4: Ad Ingestion' in source and 'STAGE TESTING FRAMEWORK' in source and not essential_cells.get('stage4_execute'):
            essential_cells['stage4_execute'] = cell
            print(f"✅ Found stage4_execute: cell {i}")

        elif 'Analyze and display ingestion results' in source and 'stage4_results' in source and not essential_cells.get('stage4_analyze'):
            essential_cells['stage4_analyze'] = cell
            print(f"✅ Found stage4_analyze: cell {i}")

        elif 'Verify BigQuery impact - Raw data only' in source and 'ingestion_results' in source and not essential_cells.get('stage4_verify'):
            essential_cells['stage4_verify'] = cell
            print(f"✅ Found stage4_verify: cell {i}")

        elif 'Stage 5 Readiness Assessment' in source and 'stage4_results' in source and not essential_cells.get('stage4_readiness'):
            essential_cells['stage4_readiness'] = cell
            print(f"✅ Found stage4_readiness: cell {i}")

        elif 'Stage 4 Summary' in source and 'Meta Ads Ingestion Complete' in source and not essential_cells.get('stage4_summary'):
            essential_cells['stage4_summary'] = cell
            print(f"✅ Found stage4_summary: cell {i}")

        # Stage 5
        elif 'Execute Stage 5: Strategic Labeling' in source and 'STAGE 5: STRATEGIC LABELING' in source and not essential_cells.get('stage5_execute'):
            essential_cells['stage5_execute'] = cell
            print(f"✅ Found stage5_execute: cell {i}")

        elif 'Strategic Intelligence Analysis - Clean DataFrame Format' in source and not essential_cells.get('stage5_analysis'):
            essential_cells['stage5_analysis'] = cell
            print(f"✅ Found stage5_analysis: cell {i}")

        elif 'Stage 5 Summary' in source and 'Strategic Labeling Complete' in source and not essential_cells.get('stage5_summary'):
            essential_cells['stage5_summary'] = cell
            print(f"✅ Found stage5_summary: cell {i}")

        # Stage 6
        elif 'Stage 6: Embeddings Generation' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage6_header'):
            essential_cells['stage6_header'] = cell
            print(f"✅ Found stage6_header: cell {i}")

        elif 'STAGE 6: EMBEDDINGS GENERATION' in source and 'STAGE TESTING FRAMEWORK' in source and not essential_cells.get('stage6_execute'):
            essential_cells['stage6_execute'] = cell
            print(f"✅ Found stage6_execute: cell {i}")

        elif 'Analyze and display embeddings results' in source and 'stage6_embeddings_results' in source and not essential_cells.get('stage6_analyze'):
            essential_cells['stage6_analyze'] = cell
            print(f"✅ Found stage6_analyze: cell {i}")

        elif 'Stage 6 Summary' in source and 'Embeddings Generation Complete' in source and not essential_cells.get('stage6_summary'):
            essential_cells['stage6_summary'] = cell
            print(f"✅ Found stage6_summary: cell {i}")

        # Stage 7
        elif 'Execute Stage 7: Visual Intelligence' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage7_header'):
            essential_cells['stage7_header'] = cell
            print(f"✅ Found stage7_header: cell {i}")

        elif 'STAGE 7: VISUAL INTELLIGENCE' in source and 'STAGE TESTING FRAMEWORK' in source and not essential_cells.get('stage7_execute'):
            essential_cells['stage7_execute'] = cell
            print(f"✅ Found stage7_execute: cell {i}")

        elif 'Visual Intelligence - Competitive Positioning Analysis' in source and not essential_cells.get('stage7_analysis'):
            essential_cells['stage7_analysis'] = cell
            print(f"✅ Found stage7_analysis: cell {i}")

        # Stage 8
        elif 'Stage 8: Strategic Analysis' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage8_header'):
            essential_cells['stage8_header'] = cell
            print(f"✅ Found stage8_header: cell {i}")

        elif 'STAGE 8: STRATEGIC ANALYSIS' in source and 'STAGE TESTING FRAMEWORK' in source and not essential_cells.get('stage8_execute'):
            essential_cells['stage8_execute'] = cell
            print(f"✅ Found stage8_execute: cell {i}")

        elif 'Strategic Analysis Dashboard' in source and 'import pandas as pd' in source and not essential_cells.get('stage8_dashboard'):
            essential_cells['stage8_dashboard'] = cell
            print(f"✅ Found stage8_dashboard: cell {i}")

        elif 'Stage 8 Summary' in source and 'Strategic Analysis Complete' in source and not essential_cells.get('stage8_summary'):
            essential_cells['stage8_summary'] = cell
            print(f"✅ Found stage8_summary: cell {i}")

        # Stage 9
        elif 'Stage 9: Multi-Dimensional Intelligence' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('stage9_header'):
            essential_cells['stage9_header'] = cell
            print(f"✅ Found stage9_header: cell {i}")

        elif 'STAGE 9: MULTI-DIMENSIONAL INTELLIGENCE' in source and 'STAGE TESTING FRAMEWORK' in source and not essential_cells.get('stage9_execute'):
            essential_cells['stage9_execute'] = cell
            print(f"✅ Found stage9_execute: cell {i}")

        elif 'Multi-Dimensional Intelligence Dashboard' in source and not essential_cells.get('stage9_dashboard'):
            essential_cells['stage9_dashboard'] = cell
            print(f"✅ Found stage9_dashboard: cell {i}")

        elif 'Stage 9 Summary' in source and 'Multi-Dimensional Intelligence Complete' in source and not essential_cells.get('stage9_summary'):
            essential_cells['stage9_summary'] = cell
            print(f"✅ Found stage9_summary: cell {i}")

        # Final sections
        elif 'Complete Pipeline Execution' in source and 'STAGES 6-10' in source and not essential_cells.get('pipeline_execution'):
            essential_cells['pipeline_execution'] = cell
            print(f"✅ Found pipeline_execution: cell {i}")

        elif 'COMPETITIVE INTELLIGENCE PIPELINE - COMPLETE SHOWCASE' in source and cell.get('cell_type') == 'markdown' and not essential_cells.get('showcase_header'):
            essential_cells['showcase_header'] = cell
            print(f"✅ Found showcase_header: cell {i}")

        elif 'COMPETITIVE INTELLIGENCE PIPELINE - CAPABILITIES SHOWCASE' in source and cell.get('cell_type') == 'code' and not essential_cells.get('showcase_code'):
            essential_cells['showcase_code'] = cell
            print(f"✅ Found showcase_code: cell {i}")

        elif 'Demo Complete: L4 Temporal Intelligence Framework' in source and not essential_cells.get('demo_complete'):
            essential_cells['demo_complete'] = cell
            print(f"✅ Found demo_complete: cell {i}")

        elif 'Technical Achievements' in source and 'AI Integration Excellence' in source and not essential_cells.get('technical_achievements'):
            essential_cells['technical_achievements'] = cell
            print(f"✅ Found technical_achievements: cell {i}")

        elif 'Future Enhancements & Extensions' in source and not essential_cells.get('future_enhancements'):
            essential_cells['future_enhancements'] = cell
            print(f"✅ Found future_enhancements: cell {i}")

# Define the correct order for the clean notebook
clean_order = [
    'intro',
    'setup_imports',
    'setup_env',
    'stage0_header',
    'stage0_function',
    'stage0_execute',
    'stage0_check',
    'stage0_summary',
    'stage1_header',
    'stage1_init',
    'stage1_execute',
    'stage1_analyze',
    'stage1_examine',
    'stage1_summary',
    'stage2_header',
    'stage2_execute',
    'stage2_analyze',
    'stage2_bigquery',
    'stage2_summary',
    'stage3_header',
    'stage3_execute',
    'stage3_function',
    'stage3_insights',
    'stage3_summary',
    'stage4_header',
    'stage4_execute',
    'stage4_analyze',
    'stage4_verify',
    'stage4_readiness',
    'stage4_summary',
    'stage5_execute',
    'stage5_analysis',
    'stage5_summary',
    'stage6_header',
    'stage6_execute',
    'stage6_analyze',
    'stage6_summary',
    'stage7_header',
    'stage7_execute',
    'stage7_analysis',
    'stage8_header',
    'stage8_execute',
    'stage8_dashboard',
    'stage8_summary',
    'stage9_header',
    'stage9_execute',
    'stage9_dashboard',
    'stage9_summary',
    'pipeline_execution',
    'showcase_header',
    'showcase_code',
    'demo_complete',
    'technical_achievements',
    'future_enhancements'
]

# Build the clean notebook
clean_cells = []
missing_cells = []

for key in clean_order:
    if key in essential_cells:
        clean_cells.append(essential_cells[key])
    else:
        missing_cells.append(key)

print(f"\n📊 CLEAN NOTEBOOK SUMMARY:")
print(f"   Essential cells found: {len(essential_cells)}")
print(f"   Clean cells added: {len(clean_cells)}")
print(f"   Missing cells: {len(missing_cells)}")

if missing_cells:
    print(f"\n❌ Missing cells:")
    for missing in missing_cells:
        print(f"   • {missing}")

# Create the clean notebook
clean_notebook = {
    "cells": clean_cells,
    "metadata": notebook.get("metadata", {}),
    "nbformat": notebook.get("nbformat", 4),
    "nbformat_minor": notebook.get("nbformat_minor", 2)
}

# Write the clean notebook
with open('notebooks/demo_competitive_intelligence_clean.ipynb', 'w') as f:
    json.dump(clean_notebook, f, indent=1)

print(f"\n✅ Created clean notebook: demo_competitive_intelligence_clean.ipynb")
print(f"🎯 Ready to replace the corrupted version!")