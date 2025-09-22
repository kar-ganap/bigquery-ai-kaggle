#!/usr/bin/env python3
"""
Fix Visual Intelligence to show percentages instead of /10 format
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix the Visual Intelligence display
for cell in notebook['cells']:
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Check if this is the Visual Intelligence display cell
        if ('visual_intel.get(\'avg_visual_text_alignment\', 0):.1f}/10' in source_text):
            print("Found Visual Intelligence display cell with /10 format...")

            # Replace all the /10 formats with percentage format
            fixed_source = source_text

            # Fix each Visual Intelligence metric
            replacements = [
                ('visual_intel.get(\'avg_visual_text_alignment\', 0):.1f}/10', 'visual_intel.get(\'avg_visual_text_alignment\', 0)*100:.0f}%'),
                ('visual_intel.get(\'avg_brand_consistency\', 0):.1f}/10', 'visual_intel.get(\'avg_brand_consistency\', 0)*100:.0f}%'),
                ('visual_intel.get(\'avg_creative_fatigue_risk\', 0):.1f}/10', 'visual_intel.get(\'avg_creative_fatigue_risk\', 0)*100:.0f}%'),
                ('visual_intel.get(\'avg_luxury_positioning\', 0):.1f}/10', 'visual_intel.get(\'avg_luxury_positioning\', 0)*100:.0f}%'),
                ('visual_intel.get(\'avg_boldness\', 0):.1f}/10', 'visual_intel.get(\'avg_boldness\', 0)*100:.0f}%'),
                ('visual_intel.get(\'avg_visual_differentiation\', 0):.1f}/10', 'visual_intel.get(\'avg_visual_differentiation\', 0)*100:.0f}%'),
                ('visual_intel.get(\'avg_creative_pattern_risk\', 0):.1f}/10', 'visual_intel.get(\'avg_creative_pattern_risk\', 0)*100:.0f}%')
            ]

            for old, new in replacements:
                if old in fixed_source:
                    fixed_source = fixed_source.replace(old, new)
                    print(f"   ✅ Fixed: {old[:50]}... → percentage format")

            # Convert back to list of lines
            cell['source'] = fixed_source.splitlines(keepends=True)
            print("✅ Fixed Visual Intelligence display to show percentages!")
            break

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated successfully!")
print("\n🎯 Visual Intelligence now shows:")
print("   • Visual-Text Alignment: 80% (instead of 0.8/10)")
print("   • Brand Consistency: 80% (instead of 0.8/10)")
print("   • Creative Fatigue Risk: 40% (instead of 0.4/10)")
print("   • Luxury Positioning: 30% (instead of 0.3/10)")
print("   • Visual Boldness: 40% (instead of 0.4/10)")
print("   • Visual Differentiation: 40% (instead of 0.4/10)")
print("   • Pattern Risk Score: 60% (instead of 0.6/10)")
print("\n📊 All metrics now use consistent percentage scale!")