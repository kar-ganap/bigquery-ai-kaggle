#!/usr/bin/env python3
"""
Final fix for notebook cell ordering - manual categorization approach
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔧 FINAL CELL ORDER FIX")
print("=" * 50)

# Manual categorization based on the analysis output
correct_order = []

# 1. Introduction
intro_cells = []
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'markdown' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'L4 Temporal Intelligence Framework' in source and 'Competitive Intelligence Journey' in source:
            intro_cells.append(cell)
            print(f"📍 Found intro cell: {i}")

# 2. Setup cells
setup_cells = []
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'Import required libraries' in source or 'Load environment variables' in source:
            setup_cells.append(cell)
            print(f"📍 Found setup cell: {i}")

# 3. Stage 0 cells
stage0_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 0:' in source or 'Clean Slate' in source or
            'get_dataset_table_count' in source or
            'clean slate preparation' in source or
            'BigQuery Dataset State' in source or
            'Stage 0 Summary' in source):
            stage0_cells.append(cell)
            print(f"📍 Found Stage 0 cell: {i}")

# 4. Stage 1 cells
stage1_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 1:' in source or 'Discovery Engine' in source or
            'STAGE 1: DISCOVERY' in source or
            'stage1_results' in source or
            'Initialize demo pipeline' in source or
            'Stage 1 Summary' in source):
            stage1_cells.append(cell)
            print(f"📍 Found Stage 1 cell: {i}")

# 5. Stage 2 cells
stage2_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 2:' in source or 'AI Competitor Curation' in source or
            'STAGE 2: AI' in source or
            'stage2_results' in source or
            'Stage 2 Summary' in source or
            'BIGQUERY IMPACT ANALYSIS - STAGE 2' in source):
            stage2_cells.append(cell)
            print(f"📍 Found Stage 2 cell: {i}")

# 6. Stage 3 cells
stage3_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 3:' in source or 'Meta Ad Activity Ranking' in source or
            'STAGE 3: META' in source or
            'stage3_results' in source or
            'Stage 3 Summary' in source or
            'extract_numeric_count' in source):
            stage3_cells.append(cell)
            print(f"📍 Found Stage 3 cell: {i}")

# 7. Stage 4 cells
stage4_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 4:' in source or 'Meta Ads Ingestion' in source or
            'STAGE 4: AD INGESTION' in source or
            'stage4_results' in source or
            'Stage 4 Summary' in source or
            'Stage 5 Readiness Assessment' in source or
            'ingestion_results' in source):
            stage4_cells.append(cell)
            print(f"📍 Found Stage 4 cell: {i}")

# 8. Stage 5 cells
stage5_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('STAGE 5: STRATEGIC LABELING' in source or
            'stage5_results' in source or
            'Stage 5 Summary' in source or
            'Strategic Intelligence Analysis' in source):
            stage5_cells.append(cell)
            print(f"📍 Found Stage 5 cell: {i}")

# 9. Stage 6 cells
stage6_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 6:' in source or 'Embeddings Generation' in source or
            'STAGE 6: EMBEDDINGS' in source or
            'stage6_embeddings_results' in source or
            'Stage 6 Summary' in source):
            stage6_cells.append(cell)
            print(f"📍 Found Stage 6 cell: {i}")

# 10. Stage 7 cells
stage7_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 7:' in source or 'Visual Intelligence' in source or
            'STAGE 7: VISUAL' in source or
            'stage7_results' in source or
            'Visual Intelligence - Competitive Positioning' in source):
            stage7_cells.append(cell)
            print(f"📍 Found Stage 7 cell: {i}")

# 11. Stage 8 cells
stage8_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 8:' in source or 'Strategic Analysis' in source or
            'STAGE 8: STRATEGIC ANALYSIS' in source or
            'stage8_results' in source or
            'Stage 8 Summary' in source or
            'Strategic Analysis Dashboard' in source):
            stage8_cells.append(cell)
            print(f"📍 Found Stage 8 cell: {i}")

# 12. Stage 9 cells
stage9_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('Stage 9:' in source or 'Multi-Dimensional Intelligence' in source or
            'STAGE 9: MULTI-DIMENSIONAL' in source or
            'stage9_results' in source or
            'Stage 9 Summary' in source or
            'Multi-Dimensional Intelligence Dashboard' in source):
            stage9_cells.append(cell)
            print(f"📍 Found Stage 9 cell: {i}")

# 13. Final showcase cells
final_cells = []
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if ('COMPETITIVE INTELLIGENCE PIPELINE - COMPLETE SHOWCASE' in source or
            'CAPABILITIES SHOWCASE' in source or
            'COMPLETE PIPELINE EXECUTION' in source or
            'Demo Complete: L4 Temporal Intelligence' in source or
            'Technical Achievements' in source or
            'Future Enhancements' in source):
            final_cells.append(cell)
            print(f"📍 Found final cell: {i}")

# Build the correct order
correct_order = (intro_cells + setup_cells + stage0_cells + stage1_cells +
                stage2_cells + stage3_cells + stage4_cells + stage5_cells +
                stage6_cells + stage7_cells + stage8_cells + stage9_cells + final_cells)

# Find any remaining cells
used_cells = set()
for cell in correct_order:
    for i, orig_cell in enumerate(cells):
        if cell is orig_cell:
            used_cells.add(i)

remaining_cells = [cells[i] for i in range(len(cells)) if i not in used_cells]

if remaining_cells:
    print(f"⚠️  Found {len(remaining_cells)} uncategorized cells - adding at end")
    correct_order.extend(remaining_cells)

# Update notebook
notebook['cells'] = correct_order

# Write back
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"\n✅ Reordered notebook with {len(correct_order)} cells")
print(f"📊 Cell distribution:")
print(f"   Intro: {len(intro_cells)}")
print(f"   Setup: {len(setup_cells)}")
print(f"   Stage 0: {len(stage0_cells)}")
print(f"   Stage 1: {len(stage1_cells)}")
print(f"   Stage 2: {len(stage2_cells)}")
print(f"   Stage 3: {len(stage3_cells)}")
print(f"   Stage 4: {len(stage4_cells)}")
print(f"   Stage 5: {len(stage5_cells)}")
print(f"   Stage 6: {len(stage6_cells)}")
print(f"   Stage 7: {len(stage7_cells)}")
print(f"   Stage 8: {len(stage8_cells)}")
print(f"   Stage 9: {len(stage9_cells)}")
print(f"   Final: {len(final_cells)}")
print(f"   Remaining: {len(remaining_cells)}")

print(f"\n🎯 Notebook is now properly sequenced!")