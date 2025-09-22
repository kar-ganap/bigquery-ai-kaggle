#!/usr/bin/env python3
"""
Add structured DataFrame for TEMPORAL FATIGUE INSIGHTS
"""

import json

# Load the notebook
notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find the target cell and add DataFrame creation for insights
target_cell = None
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        source_text = ''.join(cell['source'])
        if 'TEMPORAL CREATIVE FATIGUE ANALYSIS USING EXISTING LOGIC' in source_text:
            target_cell = i
            break

if target_cell is not None:
    # Get the current cell content
    current_lines = notebook['cells'][target_cell]['source']

    # Find where to insert the DataFrame code (after insights section)
    insertion_point = None
    for i, line in enumerate(current_lines):
        if "🔍 TEMPORAL FATIGUE INSIGHTS (From Existing Sophisticated View):" in line:
            insertion_point = i
            break

    if insertion_point is not None:
        # Create the new lines to insert
        dataframe_code = '''
        # Create structured DataFrame for temporal fatigue insights
        print("\\n📊 TEMPORAL FATIGUE INSIGHTS DASHBOARD:")

        insights_data = []

        for brand in brands:
            brand_latest = latest_data[latest_data['brand'] == brand]
            brand_all = fatigue_df[fatigue_df['brand'] == brand].sort_values('week_start')

            if not brand_latest.empty and len(brand_all) >= 2:
                current_fatigue = brand_latest['avg_fatigue_score'].iloc[0]
                current_originality = brand_latest['avg_originality'].iloc[0]
                competitor_influence = brand_latest['avg_competitor_influence_week'].iloc[0]
                risk_level = brand_latest['risk_level'].iloc[0]
                promotional_intensity = brand_latest['avg_promotional_intensity'].iloc[0]

                # 8-week trend
                if len(brand_all) >= 8:
                    fatigue_8w_ago = brand_all['avg_fatigue_score'].iloc[0]
                    trend_8w = current_fatigue - fatigue_8w_ago
                else:
                    trend_8w = 0

                # Generate observations based on existing logic
                observations = []

                # Risk level assessment
                if current_fatigue >= 0.8:
                    observations.append("🚨 CRITICAL: Existing logic flagged critical fatigue - urgent refresh")
                    observations.append("📋 Action: Replace derivative content immediately with original creative")
                elif current_fatigue >= 0.6:
                    observations.append("⚠️ HIGH RISK: Existing logic detected high fatigue - plan refresh")
                    observations.append("📋 Action: Develop new creative concepts, reduce competitor influence")
                elif current_fatigue >= 0.4:
                    observations.append("💡 MODERATE: Existing logic monitoring fatigue - consider variations")
                    observations.append("📋 Action: Test new creative angles, increase originality")
                else:
                    observations.append("✅ LOW RISK: Existing logic shows healthy creative performance")
                    observations.append("📋 Action: Continue monitoring, maintain creative diversity")

                # Specific insights based on existing logic
                if current_originality < 0.4:
                    observations.append("🔍 Warning: Low originality detected by existing logic")
                if competitor_influence > 0.6:
                    observations.append("⚠️ High competitor influence flagged by existing logic")
                if promotional_intensity > 0.7:
                    observations.append("📢 High promotional intensity may increase fatigue risk")

                # Trend analysis
                if abs(trend_8w) >= 0.05:
                    trend_direction = "worsening" if trend_8w > 0 else "improving"
                    observations.append(f"📈 Trend: {trend_direction} over 8 weeks ({trend_8w:+.3f})")

                insights_data.append({
                    'Brand': brand,
                    'Current Fatigue': f"{current_fatigue:.3f} ({risk_level})",
                    'Originality Score': f"{current_originality:.3f}",
                    'Competitor Influence': f"{competitor_influence:.3f}",
                    'Promotional Intensity': f"{promotional_intensity:.3f}",
                    '8-Week Trend': f"{trend_8w:+.3f}",
                    'Observations': " • ".join(observations)
                })

        # Create and display the insights DataFrame
        if insights_data:
            insights_df = pd.DataFrame(insights_data)

            print("\\n" + "="*120)
            print("📊 TEMPORAL FATIGUE INSIGHTS DASHBOARD")
            print("="*120)

            # Display each brand as a separate section for better readability
            for idx, row in insights_df.iterrows():
                print(f"\\n🎨 {row['Brand'].upper()}:")
                print("-" * 80)
                print(f"Current Fatigue:       {row['Current Fatigue']}")
                print(f"Originality Score:     {row['Originality Score']}")
                print(f"Competitor Influence:  {row['Competitor Influence']}")
                print(f"Promotional Intensity: {row['Promotional Intensity']}")
                print(f"8-Week Trend:          {row['8-Week Trend']}")
                print(f"\\nObservations:")
                for obs in row['Observations'].split(' • '):
                    if obs.strip():
                        print(f"   • {obs.strip()}")

            print("\\n" + "="*120)

            # Also create a compact tabular view
            print("\\n📋 COMPACT SUMMARY TABLE:")
            compact_df = insights_df[['Brand', 'Current Fatigue', 'Originality Score',
                                    'Competitor Influence', 'Promotional Intensity', '8-Week Trend']].copy()
            display(compact_df)

            # Risk prioritization
            print("\\n🚨 RISK PRIORITIZATION (Sorted by Fatigue Level):")
            risk_df = compact_df.copy()
            # Extract numeric fatigue for sorting
            risk_df['Fatigue_Numeric'] = insights_df.apply(lambda x: float(x['Current Fatigue'].split()[0]), axis=1)
            risk_df = risk_df.sort_values('Fatigue_Numeric', ascending=False)
            risk_df = risk_df.drop('Fatigue_Numeric', axis=1).reset_index(drop=True)
            risk_df.index = range(1, len(risk_df) + 1)  # Start index from 1
            display(risk_df)

        else:
            print("⚠️ No temporal fatigue insights data available")

'''

        # Convert to lines and insert
        new_lines = dataframe_code.strip().split('\n')
        new_lines = [line + '\n' for line in new_lines]

        # Insert after the insights header but before the individual brand analysis
        current_lines[insertion_point+1:insertion_point+1] = new_lines

        # Update the cell
        notebook['cells'][target_cell]['source'] = current_lines

        print("✅ Added DataFrame creation for TEMPORAL FATIGUE INSIGHTS!")
    else:
        print("❌ Could not find insertion point for DataFrame code")
else:
    print("❌ Could not find the creative fatigue cell")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print("\n✅ Notebook updated with structured insights DataFrame!")
print("\n🎯 What was added:")
print("   1. 📊 Structured DataFrame with all key metrics per brand")
print("   2. 🎨 Formatted brand sections with clear metric display")
print("   3. 📋 Compact summary table for quick comparison")
print("   4. 🚨 Risk prioritization table sorted by fatigue level")
print("   5. 💡 Comprehensive observations with actionable insights")
print("   6. 📈 Trend analysis with directional indicators")
print("\n📋 DataFrame Structure:")
print("   • Brand: Company name")
print("   • Current Fatigue: Score + risk level")
print("   • Originality Score: Creative uniqueness metric")
print("   • Competitor Influence: Market derivative level")
print("   • Promotional Intensity: Aggressive marketing level")
print("   • 8-Week Trend: Change direction and magnitude")
print("   • Observations: Actionable insights and recommendations")
print("\n✨ The insights are now presented in a clear, structured format!")