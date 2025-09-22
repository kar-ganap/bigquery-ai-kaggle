#!/usr/bin/env python3
"""
Add the missing competitive insights section to Stage 8 Deep Dive
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the Stage 8 code cell that's missing competitive insights
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])

        # Look for the cell that has DataFrame display but NO competitive insights
        if ("=== STAGE 8 DEEP DIVE:" in source_text and
            "display(positioning_df)" in source_text and
            "COMPETITIVE INSIGHTS" not in source_text):

            print(f"Found Stage 8 cell #{i} missing competitive insights...")

            # Get current source as lines
            current_lines = cell['source']

            # Find where to insert the competitive insights (after the display call)
            insert_index = -1
            for j, line in enumerate(current_lines):
                if "display(positioning_df)" in line:
                    # Insert after the next print() call
                    for k in range(j+1, len(current_lines)):
                        if "print()" in current_lines[k]:
                            insert_index = k + 1
                            break
                    break

            if insert_index != -1:
                # Competitive insights code to insert
                insights_code = [
                    "\n",
                    "            # Additional Competitive Intelligence Analysis\n",
                    "            print(\"\\n🧠 COMPETITIVE INSIGHTS\")\n",
                    "            print(\"=\" * 30)\n",
                    "\n",
                    "            target_data = positioning_df[positioning_df['brand'] == context.brand]\n",
                    "            competitor_data = positioning_df[positioning_df['brand'] != context.brand]\n",
                    "\n",
                    "            if not target_data.empty and not competitor_data.empty:\n",
                    "                target_score = target_data.iloc[0]['avg_cta_aggressiveness']\n",
                    "                market_median = competitor_data['avg_cta_aggressiveness'].median()\n",
                    "\n",
                    "                print(f\"🎯 {context.brand}: {target_score:.1f}/10\")\n",
                    "                print(f\"📊 Market Median: {market_median:.1f}/10\")\n",
                    "                print(f\"📈 Gap: {target_score - market_median:+.1f} points\")\n",
                    "\n",
                    "                # Show competitive threats (higher scores)\n",
                    "                threats = competitor_data[competitor_data['avg_cta_aggressiveness'] > target_score]\n",
                    "                if not threats.empty:\n",
                    "                    print(\"\\n🚨 More Aggressive Competitors:\")\n",
                    "                    for _, comp in threats.head(3).iterrows():\n",
                    "                        gap = comp['avg_cta_aggressiveness'] - target_score\n",
                    "                        print(f\"   • {comp['brand']}: +{gap:.1f} points\")\n",
                    "\n",
                    "                # Show opportunities (lower scores)\n",
                    "                opportunities = competitor_data[competitor_data['avg_cta_aggressiveness'] < target_score]\n",
                    "                if not opportunities.empty:\n",
                    "                    print(\"\\n💡 Less Aggressive Competitors:\")\n",
                    "                    for _, comp in opportunities.head(3).iterrows():\n",
                    "                        gap = target_score - comp['avg_cta_aggressiveness']\n",
                    "                        print(f\"   • {comp['brand']}: -{gap:.1f} points\")\n",
                    "\n",
                    "                # Strategic recommendations\n",
                    "                print(\"\\n📋 STRATEGIC RECOMMENDATIONS\")\n",
                    "                print(\"=\" * 30)\n",
                    "\n",
                    "                if target_score > market_median + 1:\n",
                    "                    print(\"✅ Strong aggressive positioning\")\n",
                    "                    print(\"🎯 Focus: Maintain leadership, test premium messaging\")\n",
                    "                elif target_score < market_median - 1:\n",
                    "                    print(\"⚡ Opportunity: Increase CTA aggressiveness\")\n",
                    "                    print(\"🎯 Focus: More urgent/direct call-to-actions\")\n",
                    "                else:\n",
                    "                    print(\"📊 Moderate positioning\")\n",
                    "                    print(\"🎯 Focus: Differentiate through unique value props\")\n",
                    "\n"
                ]

                # Insert the competitive insights code
                new_lines = current_lines[:insert_index] + insights_code + current_lines[insert_index:]
                cell['source'] = new_lines

                print("✅ Added competitive insights section to Stage 8!")
                break
            else:
                print("❌ Could not find insertion point after display call")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("✅ Notebook updated!")
print("\n🎯 Added complete competitive insights analysis:")
print("   • Target brand vs market median comparison")
print("   • Competitive threats identification")
print("   • Opportunities analysis")
print("   • Strategic recommendations")