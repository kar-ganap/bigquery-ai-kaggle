#!/usr/bin/env python3
"""
Find and fix the real intro cell position
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔧 FINDING REAL INTRO CELL")
print("=" * 50)

real_intro_index = None

# Search for the real intro - should be a markdown cell with the journey description
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'markdown' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

        # Look for specific intro markers
        if ('🚀 L4 Temporal Intelligence Framework Demo' in source or
            'Competitive Intelligence Journey: Stage-by-Stage Demo' in source):
            print(f"📍 Found real intro at cell {i}")
            print(f"   Content preview: {source[:100]}...")
            real_intro_index = i
            break

if real_intro_index is not None:
    # Move the real intro to position 0
    if real_intro_index != 0:
        intro_cell = cells.pop(real_intro_index)
        cells.insert(0, intro_cell)
        print(f"✅ Moved real intro from position {real_intro_index} to position 0")

        # Write back
        notebook['cells'] = cells
        with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
            json.dump(notebook, f, indent=1)

        print("✅ Notebook updated with correct intro position")
    else:
        print("✅ Real intro already at position 0")
else:
    print("❌ Could not find the real intro cell")

    # Let's see what we have in the first few cells
    print("\n🔍 Current first 5 cells:")
    for i in range(min(5, len(cells))):
        cell = cells[i]
        if cell.get('source'):
            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
            preview = source[:100].replace('\n', ' ')
            print(f"   {i}. [{cell.get('cell_type', 'unknown'):8}] {preview}")

print(f"\n📊 Total cells: {len(cells)}")