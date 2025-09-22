#!/usr/bin/env python3
"""
Analyze and display the current cell order to understand the jumbling
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("📋 CURRENT CELL ORDER ANALYSIS")
print("=" * 50)

# Analyze each cell and categorize it
cell_analysis = []

for i, cell in enumerate(cells):
    cell_info = {
        'index': i,
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
            if 'Stage 0:' in source or 'Clean Slate' in source:
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
                if 'Stage 1' in source:
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
            if 'STAGE 0:' in source:
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
                if 'stage1_results' in source and 'DISCOVERY' in source:
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
                elif 'stage5_results' in source or 'Strategic Labeling' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 5'
                elif 'stage6_embeddings_results' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 6'
                elif 'stage7_results' in source and ('VISUAL' in source or 'PMF' in source):
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 7'
                elif 'stage8_results' in source and 'STRATEGIC' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 8'
                elif 'stage9_results' in source and 'MULTI-DIMENSIONAL' in source:
                    cell_info['category'] = 'stage_analysis'
                    cell_info['stage'] = 'Stage 9'
                else:
                    cell_info['category'] = 'stage_analysis'

    cell_analysis.append(cell_info)

# Display current order
print("📊 CURRENT CELL ORDER:")
for i, cell_info in enumerate(cell_analysis):
    print(f"{i:2d}. [{cell_info['type']:8}] {cell_info['stage']:8} | {cell_info['category']:15} | {cell_info['content_preview']}")

# Group by stage to see the jumbling
print("\n🔍 CELLS BY STAGE:")
stages_order = ['Intro', 'Stage 0', 'Stage 1', 'Stage 2', 'Stage 3', 'Stage 4', 'Stage 5', 'Stage 6', 'Stage 7', 'Stage 8', 'Stage 9', 'Final']

for stage in stages_order:
    stage_cells = [cell for cell in cell_analysis if cell['stage'] == stage]
    if stage_cells:
        print(f"\n{stage}:")
        for cell in stage_cells:
            print(f"   Cell {cell['index']:2d}: {cell['category']}")

# Identify the jumbling pattern
print("\n⚠️  JUMBLING IDENTIFIED:")
current_stage = None
jumbles = []

for cell in cell_analysis:
    if cell['stage'] in stages_order:
        stage_index = stages_order.index(cell['stage'])
        if current_stage is not None and stage_index < current_stage:
            jumbles.append(f"Cell {cell['index']}: {cell['stage']} appears after later stages")
        current_stage = max(current_stage or 0, stage_index)

for jumble in jumbles:
    print(f"   • {jumble}")

print(f"\n📋 TOTAL CELLS: {len(cells)}")
print(f"🔀 JUMBLES DETECTED: {len(jumbles)}")

if len(jumbles) > 0:
    print("\n✅ REORDERING NEEDED - Cells are indeed jumbled!")
else:
    print("\n✅ NO REORDERING NEEDED - Cells are in correct order!")