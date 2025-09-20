#!/usr/bin/env python3
"""Fix notebook to add missing time import and check for other issues"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

print("Fixing notebook issues...")

# Fix Cell 10 - Add time import
if len(nb['cells']) > 10:
    cell = nb['cells'][10]
    source = cell.get('source', [])

    # Check if time import is missing
    source_text = ''.join(source)
    if 'import time' not in source_text and 'time.time()' in source_text:
        print("  - Adding missing 'import time' to Cell 10")
        # Add import at the beginning
        new_source = ['import time\n', '\n'] + source
        cell['source'] = new_source

# Also check Cell 1 for missing imports that might be needed
if len(nb['cells']) > 1:
    cell = nb['cells'][1]
    source_text = ''.join(cell.get('source', []))

    # Check if time is already imported in Cell 1
    if 'import time' not in source_text:
        print("  - Adding 'import time' to Cell 1 imports")
        source = cell.get('source', [])
        # Find where to insert time import (after other imports)
        insert_idx = 0
        for i, line in enumerate(source):
            if line.strip().startswith('import ') or line.strip().startswith('from '):
                insert_idx = i + 1

        # Insert time import
        source.insert(insert_idx, 'import time\n')
        cell['source'] = source

# Save the fixed notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Fixed notebook: {notebook_path}")
print("   - Added missing time import")
print("   - Notebook should now work when reloaded")
print("\n📋 To apply changes:")
print("   1. Save your current notebook")
print("   2. Close and reopen the notebook file")
print("   3. Re-run the cells in order")