#!/usr/bin/env python3
"""
Fix the missing stage5_results variable that Stage 6 needs
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# Find the Stage 5 cell that creates labeling_results
stage5_cell_index = None
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'labeling_results = labeling_stage.run(ingestion_results, progress)' in source:
            stage5_cell_index = i
            break

if stage5_cell_index is None:
    print("❌ Could not find Stage 5 execution cell")
    exit(1)

print(f"Found Stage 5 execution cell at index {stage5_cell_index}")

# Get the cell content
stage5_cell = cells[stage5_cell_index]
source_lines = stage5_cell['source']

# Find the line after labeling_results assignment and add the stage5_results assignment
new_source_lines = []
for line in source_lines:
    new_source_lines.append(line)
    # Add the assignment right after labeling_results is created
    if 'labeling_results = labeling_stage.run(ingestion_results, progress)' in line:
        new_source_lines.append('    \n')
        new_source_lines.append('    # Store results for Stage 6\n')
        new_source_lines.append('    stage5_results = labeling_results\n')

# Also add it in the exception handler
final_source_lines = []
for line in new_source_lines:
    final_source_lines.append(line)
    if 'labeling_results = None' in line:
        final_source_lines.append('    stage5_results = None\n')

# Update the cell
stage5_cell['source'] = final_source_lines

# Update the notebook
notebook['cells'] = cells

# Write back
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully fixed stage5_results variable!")
print(f"   Added 'stage5_results = labeling_results' assignment")
print(f"   Stage 6 will now have access to the results from Stage 5")