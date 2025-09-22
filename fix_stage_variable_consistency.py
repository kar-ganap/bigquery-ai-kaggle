#!/usr/bin/env python3
"""
Fix all stage variable naming inconsistencies after renumbering
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔧 FIXING STAGE VARIABLE CONSISTENCY")
print("=" * 50)

# Track all fixes made
fixes_made = []

# Fix all cells
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = cell['source']
        if isinstance(source, list):
            source_text = ''.join(source)
        else:
            source_text = source

        original_source = source_text

        # Issues to fix:
        # 1. Visual Intelligence stage still has leftover "Stage 6" references in print statements
        if 'STAGE 7: VISUAL INTELLIGENCE' in source_text and 'Stage 6 Complete!' in source_text:
            source_text = source_text.replace('Stage 6 Complete!', 'Stage 7 Complete!')
            source_text = source_text.replace('stage6_duration', 'stage7_duration')
            source_text = source_text.replace('Ready for Stage 7 (Advanced Analytics)', 'Ready for Stage 8 (Strategic Analysis)')
            fixes_made.append(f"Cell {i}: Fixed Visual Intelligence print statements")

        # 2. Visual Intelligence execution cell comment still says "Initialize Stage 6"
        if 'Execute Stage 7: Visual Intelligence' in source_text and 'Initialize Stage 6 (Visual Intelligence)' in source_text:
            source_text = source_text.replace('Initialize Stage 6 (Visual Intelligence)', 'Initialize Stage 7 (Visual Intelligence)')
            source_text = source_text.replace('Stage 6 constructor:', 'Stage 7 constructor:')
            fixes_made.append(f"Cell {i}: Fixed Visual Intelligence initialization comment")

        # 3. Any remaining "stage6_results" that should be "stage7_results"
        if 'stage6_results' in source_text and 'stage6_embeddings_results' not in source_text:
            # Don't replace stage6_embeddings_results, but do replace standalone stage6_results
            # Use word boundary to avoid replacing stage6_embeddings_results
            import re
            source_text = re.sub(r'\bstage6_results\b', 'stage7_results', source_text)
            fixes_made.append(f"Cell {i}: Fixed stage6_results → stage7_results")

        # 4. Showcase cell that still has old variable names
        if 'stage6_results' in source_text and "'stage6_results' in locals()" in source_text:
            source_text = source_text.replace("'stage6_results'", "'stage7_results'")
            source_text = source_text.replace("stage6_results", "stage7_results")
            fixes_made.append(f"Cell {i}: Fixed showcase variable references")

        # Update the cell if changes were made
        if source_text != original_source:
            if isinstance(source, list):
                cell['source'] = [source_text]
            else:
                cell['source'] = source_text

# Also fix any markdown cells with incorrect stage references
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'markdown' and cell.get('source'):
        source = cell['source']
        if isinstance(source, list):
            source_text = ''.join(source)
        else:
            source_text = source

        original_source = source_text

        # Fix any markdown references to incorrect stage numbers
        if 'Stage 6: Visual Intelligence' in source_text and 'Stage 7:' not in source_text:
            source_text = source_text.replace('Stage 6: Visual Intelligence', 'Stage 7: Visual Intelligence')
            fixes_made.append(f"Cell {i}: Fixed markdown stage reference")

        # Update the cell if changes were made
        if source_text != original_source:
            if isinstance(source, list):
                cell['source'] = [source_text]
            else:
                cell['source'] = source_text

# Update the notebook
notebook['cells'] = cells

# Write back to file
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully fixed stage variable consistency!")
print(f"📋 Fixes made:")
for fix in fixes_made:
    print(f"   • {fix}")

if not fixes_made:
    print("✅ No issues found - all stage variables are already consistent!")

print(f"\n🎯 CORRECTED STAGE VARIABLE MAPPING:")
print(f"   stage1_results → Discovery Engine")
print(f"   stage2_results → AI Competitor Curation")
print(f"   stage3_results → Meta Ad Activity Ranking")
print(f"   stage4_results → Meta Ads Ingestion")
print(f"   stage5_results → Strategic Labeling")
print(f"   stage6_embeddings_results → Embeddings Generation")
print(f"   stage7_results → Visual Intelligence")
print(f"   stage8_results → Strategic Analysis")
print(f"   stage9_results → Multi-Dimensional Intelligence")

print(f"\n✅ All stage dependencies should now be properly aligned!")