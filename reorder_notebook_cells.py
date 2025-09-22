#!/usr/bin/env python3
"""
Reorder notebook cells to proper stage sequence
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔧 REORDERING NOTEBOOK CELLS")
print("=" * 50)

# Analyze and categorize cells
cell_analysis = []

for i, cell in enumerate(cells):
    cell_info = {
        'index': i,
        'cell': cell,
        'type': cell.get('cell_type', 'unknown'),
        'category': 'unknown',
        'stage': 'unknown',
        'content_preview': ''
    }

    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        cell_info['content_preview'] = source[:100].replace('\n', ' ')

        # Categorize by content
        if cell.get('cell_type') == 'markdown':
            if 'L4 Temporal Intelligence Framework' in source and 'Competitive Intelligence Journey' in source:
                cell_info['category'] = 'intro'
                cell_info['stage'] = 'Intro'
            elif 'Stage 0:' in source or 'Clean Slate' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 0'
            elif 'Stage 1:' in source or 'Discovery Engine' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 1'
            elif 'Stage 2:' in source or 'AI Competitor Curation' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 2'
            elif 'Stage 3:' in source or 'Meta Ad Activity Ranking' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 3'
            elif 'Stage 4:' in source or 'Meta Ads Ingestion' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 4'
            elif 'Stage 5:' in source or 'Strategic Labeling' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 5'
            elif 'Stage 6:' in source or 'Embeddings Generation' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 6'
            elif 'Stage 7:' in source or 'Visual Intelligence' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 7'
            elif 'Stage 8:' in source or 'Strategic Analysis' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 8'
            elif 'Stage 9:' in source or 'Multi-Dimensional Intelligence' in source:
                cell_info['category'] = 'stage_header'
                cell_info['stage'] = 'Stage 9'
            elif 'Summary' in source:
                # Find which stage this summary belongs to
                if 'Stage 0' in source:
                    cell_info['stage'] = 'Stage 0'
                elif 'Stage 1' in source:
                    cell_info['stage'] = 'Stage 1'
                elif 'Stage 2' in source:
                    cell_info['stage'] = 'Stage 2'
                elif 'Stage 3' in source:
                    cell_info['stage'] = 'Stage 3'
                elif 'Stage 4' in source:
                    cell_info['stage'] = 'Stage 4'
                elif 'Stage 5' in source:
                    cell_info['stage'] = 'Stage 5'
                elif 'Stage 6' in source:
                    cell_info['stage'] = 'Stage 6'
                elif 'Stage 7' in source:
                    cell_info['stage'] = 'Stage 7'
                elif 'Stage 8' in source:
                    cell_info['stage'] = 'Stage 8'
                elif 'Stage 9' in source:
                    cell_info['stage'] = 'Stage 9'
                cell_info['category'] = 'stage_summary'
            elif 'COMPETITIVE INTELLIGENCE PIPELINE' in source and 'COMPLETE SHOWCASE' in source:
                cell_info['category'] = 'final_showcase'
                cell_info['stage'] = 'Final'
            elif 'Technical Achievements' in source or 'Future Enhancements' in source:
                cell_info['category'] = 'final_summary'
                cell_info['stage'] = 'Final'
            else:
                cell_info['category'] = 'intro_or_other'

        elif cell.get('cell_type') == 'code':
            if 'Import required libraries' in source:
                cell_info['category'] = 'setup'
                cell_info['stage'] = 'Setup'
            elif 'Load environment variables' in source:
                cell_info['category'] = 'setup'
                cell_info['stage'] = 'Setup'
            elif 'STAGE 0:' in source or 'clean slate' in source.lower():
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 0'
            elif 'STAGE 1:' in source or 'stage1_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 1'
            elif 'STAGE 2:' in source or 'stage2_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 2'
            elif 'STAGE 3:' in source or 'stage3_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 3'
            elif 'STAGE 4:' in source or 'stage4_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 4'
            elif 'STAGE 5:' in source or 'stage5_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 5'
            elif 'STAGE 6:' in source or 'stage6_embeddings_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 6'
            elif 'STAGE 7:' in source or 'stage7_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 7'
            elif 'STAGE 8:' in source or 'stage8_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 8'
            elif 'STAGE 9:' in source or 'stage9_results' in source:
                cell_info['category'] = 'stage_execution'
                cell_info['stage'] = 'Stage 9'
            elif 'COMPETITIVE INTELLIGENCE PIPELINE - CAPABILITIES SHOWCASE' in source:
                cell_info['category'] = 'final_showcase'
                cell_info['stage'] = 'Final'
            else:
                # Try to determine stage from analysis context
                if 'Initialize demo pipeline' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 1'
                elif 'stage1_results' in source and 'DISCOVERY' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 1'
                elif 'stage2_results' in source and 'CURATION' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 2'
                elif 'stage3_results' in source and 'RANKING' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 3'
                elif 'stage4_results' in source and 'INGESTION' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 4'
                elif 'stage5_results' in source and ('Strategic' in source or 'STRATEGIC' in source):
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 5'
                elif 'stage6_embeddings_results' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 6'
                elif 'stage7_results' in source and ('VISUAL' in source or 'Visual' in source):
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 7'
                elif 'stage8_results' in source and 'STRATEGIC' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 8'
                elif 'stage9_results' in source and 'MULTI-DIMENSIONAL' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 9'
                elif 'get_dataset_table_count' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 0'
                elif 'BigQuery Dataset State' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 0'
                elif 'COMPLETE PIPELINE EXECUTION' in source:
                    cell_info['category'] = 'final_showcase'
                    cell_info['stage'] = 'Final'
                else:
                    cell_info['category'] = 'stage_analysis'

    cell_analysis.append(cell_info)

# Define the correct order
stage_order = [
    ('Intro', ['intro']),
    ('Setup', ['setup']),
    ('Stage 0', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 1', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 2', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 3', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 4', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 5', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 6', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 7', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 8', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Stage 9', ['stage_header', 'stage_execution', 'stage_analysis', 'stage_summary']),
    ('Final', ['final_showcase', 'final_summary'])
]

# Reorder cells
reordered_cells = []

for stage, categories in stage_order:
    stage_cells = [cell for cell in cell_analysis if cell['stage'] == stage]

    if stage_cells:
        print(f"📋 Processing {stage}: {len(stage_cells)} cells")

        # Sort cells within stage by category priority
        category_priority = {cat: i for i, cat in enumerate(categories)}

        def get_sort_key(cell):
            cat_priority = category_priority.get(cell['category'], 999)
            return (cat_priority, cell['index'])

        stage_cells.sort(key=get_sort_key)

        for cell in stage_cells:
            reordered_cells.append(cell['cell'])

# Handle any remaining uncategorized cells
remaining_cells = [cell for cell in cell_analysis if cell['stage'] == 'unknown']
if remaining_cells:
    print(f"⚠️  Adding {len(remaining_cells)} uncategorized cells at the end")
    for cell in remaining_cells:
        reordered_cells.append(cell['cell'])

# Update the notebook
notebook['cells'] = reordered_cells

# Write back to file
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"\n✅ Successfully reordered {len(reordered_cells)} cells!")
print(f"📊 Final cell count: {len(reordered_cells)}")

# Verify the new order
print(f"\n🔍 VERIFICATION - New cell order:")
for i, cell in enumerate(reordered_cells[:10]):  # Show first 10
    source = cell.get('source', [''])
    if isinstance(source, list):
        source_text = ''.join(source)
    else:
        source_text = source
    preview = source_text[:80].replace('\n', ' ') if source_text else 'Empty cell'
    print(f"   {i:2d}. [{cell.get('cell_type', 'unknown'):8}] {preview}")

if len(reordered_cells) > 10:
    print(f"   ... and {len(reordered_cells) - 10} more cells")

print(f"\n🎯 Notebook cells are now properly ordered by stage sequence!")