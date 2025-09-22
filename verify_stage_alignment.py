#!/usr/bin/env python3
"""
Verify that all stage variable names and dependencies are properly aligned
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔍 STAGE ALIGNMENT VERIFICATION")
print("=" * 50)

# Define the correct stage mapping
correct_stages = {
    'stage1_results': 'Discovery Engine',
    'stage2_results': 'AI Competitor Curation',
    'stage3_results': 'Meta Ad Activity Ranking',
    'stage4_results': 'Meta Ads Ingestion',
    'stage5_results': 'Strategic Labeling',
    'stage6_embeddings_results': 'Embeddings Generation',
    'stage7_results': 'Visual Intelligence',
    'stage8_results': 'Strategic Analysis',
    'stage9_results': 'Multi-Dimensional Intelligence'
}

# Check for stage variable definitions
stage_definitions = {}
stage_dependencies = {}

for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

        # Find stage result assignments
        for stage_var in correct_stages.keys():
            if f'{stage_var} = ' in source and f'if {stage_var}' not in source:
                stage_definitions[stage_var] = i

        # Find stage dependencies (what each stage needs as input)
        for stage_var in correct_stages.keys():
            if f'if {stage_var} is None:' in source:
                # Find which stage this check is in
                for check_stage in correct_stages.keys():
                    if f'{check_stage} = ' in source:
                        if check_stage not in stage_dependencies:
                            stage_dependencies[check_stage] = []
                        stage_dependencies[check_stage].append(stage_var)

print("📊 STAGE VARIABLE DEFINITIONS:")
for stage_var in correct_stages.keys():
    if stage_var in stage_definitions:
        print(f"   ✅ {stage_var} → {correct_stages[stage_var]} (Cell {stage_definitions[stage_var]})")
    else:
        print(f"   ❌ {stage_var} → {correct_stages[stage_var]} (NOT FOUND)")

print("\n🔗 STAGE DEPENDENCIES:")
for stage_var, deps in stage_dependencies.items():
    print(f"   {stage_var} depends on: {', '.join(deps)}")

# Verify the dependency chain is logical
print("\n🎯 DEPENDENCY CHAIN VERIFICATION:")
expected_deps = {
    'stage2_results': ['stage1_results'],
    'stage3_results': ['stage2_results'],
    'stage4_results': ['stage3_results'],
    'stage5_results': ['stage4_results'],
    'stage6_embeddings_results': ['stage5_results'],
    'stage7_results': ['stage5_results'],  # Visual Intelligence uses Stage 5 results
    'stage8_results': ['stage6_embeddings_results', 'stage5_results'],  # Strategic Analysis uses embeddings and labels
    'stage9_results': ['stage8_results']  # Multi-dimensional uses strategic analysis
}

dependency_issues = []
for stage, expected in expected_deps.items():
    actual = stage_dependencies.get(stage, [])
    if set(actual) != set(expected):
        dependency_issues.append(f"{stage}: expected {expected}, found {actual}")

if dependency_issues:
    print("❌ DEPENDENCY ISSUES FOUND:")
    for issue in dependency_issues:
        print(f"   • {issue}")
else:
    print("✅ All dependencies are correctly aligned!")

# Check for any remaining incorrect stage references
print("\n🔍 CHECKING FOR INCORRECT REFERENCES:")
incorrect_refs = []

for i, cell in enumerate(cells):
    if cell.get('cell_type') in ['code', 'markdown'] and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']

        # Check for old "stage6_results" that should be "stage7_results"
        if 'stage6_results' in source and 'stage6_embeddings_results' not in source:
            incorrect_refs.append(f"Cell {i}: Found 'stage6_results' (should be 'stage7_results')")

        # Check for incorrect stage numbers in comments/prints
        if 'Stage 6: Visual Intelligence' in source:
            incorrect_refs.append(f"Cell {i}: Found 'Stage 6: Visual Intelligence' (should be 'Stage 7')")

        if 'Stage 6 Complete' in source and 'Embeddings' not in source:
            incorrect_refs.append(f"Cell {i}: Found 'Stage 6 Complete' in non-embeddings context")

if incorrect_refs:
    print("❌ INCORRECT REFERENCES FOUND:")
    for ref in incorrect_refs:
        print(f"   • {ref}")
else:
    print("✅ No incorrect stage references found!")

print("\n🎯 FINAL VERIFICATION SUMMARY:")
print(f"   Stage Definitions: {len(stage_definitions)}/9 found")
print(f"   Dependency Issues: {len(dependency_issues)} found")
print(f"   Incorrect References: {len(incorrect_refs)} found")

if len(stage_definitions) == 9 and len(dependency_issues) == 0 and len(incorrect_refs) == 0:
    print("\n🏆 ✅ ALL STAGE ALIGNMENTS ARE CORRECT!")
    print("   🎯 Pipeline flow: Discovery → Curation → Ranking → Ingestion → Labeling → Embeddings → Visual → Strategic → Intelligence")
    print("   🔗 All dependencies properly configured")
    print("   📊 All variable names consistently aligned")
else:
    print(f"\n⚠️  ISSUES FOUND - Manual review recommended")

print("\n📋 CORRECT STAGE MAPPING:")
for stage_var, stage_name in correct_stages.items():
    print(f"   {stage_var} → {stage_name}")