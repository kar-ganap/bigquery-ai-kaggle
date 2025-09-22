#!/usr/bin/env python3
"""
Precise cell reordering based on exact content matching
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔧 PRECISE CELL REORDERING")
print("=" * 50)

# Create a mapping based on unique content signatures
cell_mapping = {}

for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

        # Create unique signatures for each cell
        if 'L4 Temporal Intelligence Framework' in source and 'Competitive Intelligence Journey' in source:
            cell_mapping['intro'] = i
        elif 'Import required libraries' in source and 'import sys' in source:
            cell_mapping['setup_imports'] = i
        elif 'Load environment variables from .env file' in source:
            cell_mapping['setup_env'] = i
        elif 'Stage 0: Clean Slate Preparation' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage0_header'] = i
        elif 'get_dataset_table_count()' in source and 'def get_dataset_table_count' in source:
            cell_mapping['stage0_function'] = i
        elif 'Execute clean slate preparation' in source and 'PRESERVING EXISTING ads_with_dates' in source:
            cell_mapping['stage0_execute'] = i
        elif 'Check state after cleanup' in source and 'AFTER CLEANUP' in source:
            cell_mapping['stage0_check'] = i
        elif 'Stage 0 Summary' in source and 'Clean slate preparation completed' in source:
            cell_mapping['stage0_summary'] = i
        elif 'Stage 1: Discovery Engine' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage1_header'] = i
        elif 'Initialize demo pipeline context' in source and 'demo_run_id' in source:
            cell_mapping['stage1_init'] = i
        elif 'Execute Stage 1: Discovery Engine' in source and 'STAGE TESTING FRAMEWORK' in source:
            cell_mapping['stage1_execute'] = i
        elif 'Analyze and display discovery results' in source and 'stage1_results' in source:
            cell_mapping['stage1_analyze'] = i
        elif 'Examine Stage 1 Discovery Results' in source and 'In-Memory Analysis' in source:
            cell_mapping['stage1_examine'] = i
        elif 'Stage 1 Summary' in source and 'Discovery Engine completed successfully' in source:
            cell_mapping['stage1_summary'] = i
        elif 'Stage 2: AI Competitor Curation' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage2_header'] = i
        elif 'Execute Stage 2: AI Competitor Curation' in source and 'STAGE TESTING FRAMEWORK' in source:
            cell_mapping['stage2_execute'] = i
        elif 'Analyze and display curation results' in source and 'stage2_results' in source:
            cell_mapping['stage2_analyze'] = i
        elif 'Examine BigQuery impact of Stage 2' in source and 'BIGQUERY IMPACT ANALYSIS - STAGE 2' in source:
            cell_mapping['stage2_bigquery'] = i
        elif 'Stage 2 Summary' in source and 'AI Competitor Curation Complete' in source:
            cell_mapping['stage2_summary'] = i
        elif 'Stage 3: Meta Ad Activity Ranking' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage3_header'] = i
        elif 'Execute Stage 3: Meta Ad Activity Ranking' in source and 'STAGE TESTING FRAMEWORK' in source:
            cell_mapping['stage3_execute'] = i
        elif 'def extract_numeric_count(estimated_count)' in source:
            cell_mapping['stage3_function'] = i
        elif 'Meta Ad Activity Insights and Next Steps' in source and 'stage3_results' in source:
            cell_mapping['stage3_insights'] = i
        elif 'Stage 3 Summary' in source and 'Meta Ad Activity Ranking Complete' in source:
            cell_mapping['stage3_summary'] = i
        elif 'Stage 4: Meta Ads Ingestion' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage4_header'] = i
        elif 'Execute Stage 4: Ad Ingestion' in source and 'STAGE TESTING FRAMEWORK' in source:
            cell_mapping['stage4_execute'] = i
        elif 'Analyze and display ingestion results' in source and 'stage4_results' in source:
            cell_mapping['stage4_analyze'] = i
        elif 'Verify BigQuery impact - Raw data only' in source and 'ingestion_results' in source:
            cell_mapping['stage4_verify'] = i
        elif 'Stage 5 Readiness Assessment' in source and 'stage4_results' in source:
            cell_mapping['stage4_readiness'] = i
        elif 'Stage 4 Summary' in source and 'Meta Ads Ingestion Complete' in source:
            cell_mapping['stage4_summary'] = i
        elif 'Execute Stage 5: Strategic Labeling' in source and 'STAGE 5: STRATEGIC LABELING' in source:
            cell_mapping['stage5_execute'] = i
        elif 'Strategic Intelligence Analysis - Clean DataFrame Format' in source:
            cell_mapping['stage5_analysis'] = i
        elif 'Stage 5 Summary' in source and 'Strategic Labeling Complete' in source:
            cell_mapping['stage5_summary'] = i
        elif 'Stage 6: Embeddings Generation' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage6_header'] = i
        elif 'Execute Stage 6: Embeddings Generation' in source and 'STAGE TESTING FRAMEWORK' in source:
            cell_mapping['stage6_execute'] = i
        elif 'Analyze and display embeddings results' in source and 'stage6_embeddings_results' in source:
            cell_mapping['stage6_analyze'] = i
        elif 'Stage 6 Summary' in source and 'Embeddings Generation Complete' in source:
            cell_mapping['stage6_summary'] = i
        elif 'Execute Stage 7: Visual Intelligence' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage7_header'] = i
        elif 'Execute Stage 7: Visual Intelligence' in source and 'STAGE TESTING FRAMEWORK' in source:
            cell_mapping['stage7_execute'] = i
        elif 'Visual Intelligence - Competitive Positioning Analysis' in source:
            cell_mapping['stage7_analysis'] = i
        elif 'Stage 8: Strategic Analysis' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage8_header'] = i
        elif 'Execute Stage 8: Strategic Analysis' in source and 'STAGE TESTING FRAMEWORK' in source:
            cell_mapping['stage8_execute'] = i
        elif 'Strategic Analysis Dashboard' in source and 'import pandas as pd' in source:
            cell_mapping['stage8_dashboard'] = i
        elif 'Stage 8 Summary' in source and 'Strategic Analysis Complete' in source:
            cell_mapping['stage8_summary'] = i
        elif 'Stage 9: Multi-Dimensional Intelligence' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['stage9_header'] = i
        elif 'Execute Stage 9: Multi-Dimensional Intelligence' in source and 'STAGE TESTING FRAMEWORK' in source:
            cell_mapping['stage9_execute'] = i
        elif 'Multi-Dimensional Intelligence Dashboard' in source:
            cell_mapping['stage9_dashboard'] = i
        elif 'Stage 9 Summary' in source and 'Multi-Dimensional Intelligence Complete' in source:
            cell_mapping['stage9_summary'] = i
        elif 'Complete Pipeline Execution' in source and 'STAGES 6-10' in source:
            cell_mapping['pipeline_execution'] = i
        elif 'COMPETITIVE INTELLIGENCE PIPELINE - COMPLETE SHOWCASE' in source and cell.get('cell_type') == 'markdown':
            cell_mapping['showcase_header'] = i
        elif 'COMPETITIVE INTELLIGENCE PIPELINE - CAPABILITIES SHOWCASE' in source and cell.get('cell_type') == 'code':
            cell_mapping['showcase_code'] = i
        elif 'Demo Complete: L4 Temporal Intelligence Framework' in source:
            cell_mapping['demo_complete'] = i
        elif 'Technical Achievements' in source and 'AI Integration Excellence' in source:
            cell_mapping['technical_achievements'] = i
        elif 'Future Enhancements & Extensions' in source:
            cell_mapping['future_enhancements'] = i

# Define the correct order
correct_order_keys = [
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

# Build the reordered cells list
reordered_cells = []
used_indices = set()

for key in correct_order_keys:
    if key in cell_mapping:
        idx = cell_mapping[key]
        reordered_cells.append(cells[idx])
        used_indices.add(idx)
        print(f"✅ {key}: cell {idx}")
    else:
        print(f"❌ Missing: {key}")

# Add any remaining cells
for i, cell in enumerate(cells):
    if i not in used_indices:
        reordered_cells.append(cell)
        print(f"⚠️  Uncategorized cell {i} added at end")

# Update notebook
notebook['cells'] = reordered_cells

# Write back
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"\n✅ Successfully reordered {len(reordered_cells)} cells!")
print(f"📊 Mapped cells: {len([k for k in correct_order_keys if k in cell_mapping])}")
print(f"🔍 Remaining cells: {len(reordered_cells) - len([k for k in correct_order_keys if k in cell_mapping])}")

print(f"\n🎯 Notebook is now properly sequenced by stage order!")