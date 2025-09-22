#!/usr/bin/env python3
"""
Fix the order in the clean notebook - find the actual intro cell
"""
import json

# Read the clean notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔧 FIXING CLEAN NOTEBOOK ORDER")
print("=" * 50)

# Find the correct intro cell
intro_found = False
for i, cell in enumerate(cells):
    if cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

        # Look for the real intro
        if 'L4 Temporal Intelligence Framework' in source and 'Competitive Intelligence Journey' in source and 'Stage-by-Stage Demo' in source:
            print(f"📍 Found actual intro at cell {i}")

            # Move this cell to position 0
            if i != 0:
                intro_cell = cells.pop(i)
                cells.insert(0, intro_cell)
                print(f"✅ Moved intro from position {i} to position 0")
                intro_found = True
            else:
                print("✅ Intro already at position 0")
                intro_found = True
            break

if not intro_found:
    print("❌ Could not find the correct intro cell")

# Write back the corrected notebook
notebook['cells'] = cells
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Fixed notebook order - intro now at position 0")
print(f"📊 Total cells: {len(cells)}")

# Show first few cells to verify
print(f"\n🔍 First 5 cells:")
for i in range(min(5, len(cells))):
    cell = cells[i]
    source = ''.join(cell['source']) if isinstance(cell['source'], list) and cell.get('source') else 'Empty cell'
    preview = source[:80].replace('\n', ' ') if source else 'Empty'
    print(f"   {i}. [{cell.get('cell_type', 'unknown'):8}] {preview}")