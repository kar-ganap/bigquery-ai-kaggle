#!/usr/bin/env python3
"""
Update Visual Intelligence to Stage 7 and add Stage 8 Strategic Analysis
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# First, find and update Visual Intelligence from "Stage 6" to "Stage 7"
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'markdown' and cell.get('source'):
        source = cell['source']
        if isinstance(source, list):
            source_text = ''.join(source)
        else:
            source_text = source

        if 'Execute Stage 6: Visual Intelligence' in source_text:
            # Update the header
            new_source = source_text.replace('Execute Stage 6: Visual Intelligence', 'Execute Stage 7: Visual Intelligence')
            cell['source'] = [new_source] if isinstance(source, list) else new_source
            print(f"✅ Updated Visual Intelligence header to Stage 7 at cell {i}")
            break

# Update the execution cell for Visual Intelligence
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = cell['source']
        if isinstance(source, list):
            source_text = ''.join(source)
        else:
            source_text = source

        if 'STAGE 6: VISUAL INTELLIGENCE' in source_text:
            # Update the print statement and variable names
            new_source = source_text.replace('STAGE 6: VISUAL INTELLIGENCE', 'STAGE 7: VISUAL INTELLIGENCE')
            new_source = new_source.replace('stage6_results = None', 'stage7_results = None')
            new_source = new_source.replace('stage6_results = visual_results', 'stage7_results = visual_results')
            new_source = new_source.replace('print(f"❌ Stage 6 Failed: {e}")', 'print(f"❌ Stage 7 Failed: {e}")')
            new_source = new_source.replace('stage6_results = None', 'stage7_results = None')
            cell['source'] = [new_source] if isinstance(source, list) else new_source
            print(f"✅ Updated Visual Intelligence execution to Stage 7 at cell {i}")
            break

# Update the Visual Intelligence analysis cell
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'code' and cell.get('source'):
        source = cell['source']
        if isinstance(source, list):
            source_text = ''.join(source)
        else:
            source_text = source

        if 'if stage6_results is None:' in source_text:
            # Update variable references
            new_source = source_text.replace('stage6_results', 'stage7_results')
            new_source = new_source.replace('Stage 6 Visual Intelligence', 'Stage 7 Visual Intelligence')
            cell['source'] = [new_source] if isinstance(source, list) else new_source
            print(f"✅ Updated Visual Intelligence analysis to use stage7_results at cell {i}")
            break

# Now find where to insert Stage 8 - after Visual Intelligence
insert_index = None
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'markdown' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        # Look for a cell that might be after Visual Intelligence
        if 'Next Stage' in source and 'Visual Intelligence' in source:
            insert_index = i + 1
            break

# If we didn't find the specific pattern, look for any markdown that comes after Visual Intelligence
if insert_index is None:
    visual_found = False
    for i, cell in enumerate(cells):
        if cell.get('cell_type') == 'markdown' and cell.get('source'):
            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
            if 'Execute Stage 7: Visual Intelligence' in source or 'Visual Intelligence' in source:
                visual_found = True
            elif visual_found and ('---' in source or 'Stage' in source or 'Summary' in source):
                insert_index = i + 1
                break

# If still not found, append at the end
if insert_index is None:
    insert_index = len(cells)

print(f"Inserting Stage 8 at cell index {insert_index}")

# Create Stage 8 Strategic Analysis cells
stage8_cells = [
    # Markdown header
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "---\n",
            "\n",
            "## 🧠 Stage 8: Strategic Analysis\n",
            "\n",
            "**Purpose**: The analytical brain that transforms competitive data into strategic insights\n",
            "\n",
            "**Input**: Embeddings from Stage 6, Strategic labels from Stage 5, Visual intelligence from Stage 7\n",
            "**Output**: Comprehensive strategic analysis with competitive intelligence\n",
            "\n",
            "**Key Modules:**\n",
            "- 📊 **Current State Analysis**: Promotional intensity, urgency scores, market positioning\n",
            "- 🎯 **Competitive Copying Detection**: Who's copying whom using semantic embeddings\n",
            "- 📈 **Temporal Intelligence**: Momentum analysis, velocity changes, trend evolution\n",
            "- 📱 **CTA Intelligence**: Call-to-action aggressiveness scoring across competitors\n",
            "- 🔮 **Strategic Forecasting**: 7/14/30-day predictions with business impact assessment\n",
            "\n",
            "**Architecture Note**: This is where raw competitive data becomes actionable strategic intelligence"
        ]
    },

    # Execution cell
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "print(\"🧠 === STAGE 8: STRATEGIC ANALYSIS ===\" + \" (STAGE TESTING FRAMEWORK APPROACH)\")\n",
            "print(f\"📥 Input: Embeddings from Stage 6, Strategic labels from Stage 5\")\n",
            "\n",
            "# Initialize Stage 8 (Strategic Analysis) \n",
            "from src.pipeline.stages.analysis import AnalysisStage\n",
            "\n",
            "if stage6_embeddings_results is None:\n",
            "    print(\"❌ Cannot proceed - Stage 6 (Embeddings) failed\")\n",
            "    stage8_results = None\n",
            "elif stage5_results is None:\n",
            "    print(\"❌ Cannot proceed - Stage 5 (Strategic Labeling) failed\")\n",
            "    stage8_results = None\n",
            "else:\n",
            "    # Stage 8 constructor: AnalysisStage(context, dry_run=False, verbose=True)\n",
            "    analysis_stage = AnalysisStage(context, dry_run=False, verbose=True)\n",
            "    \n",
            "    try:\n",
            "        import time\n",
            "        stage8_start = time.time()\n",
            "        \n",
            "        print(\"\\n🧠 Executing strategic analysis...\")\n",
            "        print(\"   📊 Current state analysis...\")\n",
            "        print(\"   🎯 Competitive copying detection...\")\n",
            "        print(\"   📈 Temporal intelligence analysis...\")\n",
            "        print(\"   📱 CTA aggressiveness scoring...\")\n",
            "        print(\"   🔮 Strategic forecasting...\")\n",
            "        \n",
            "        # Execute strategic analysis - uses embeddings for copying detection\n",
            "        analysis_results = analysis_stage.execute(stage6_embeddings_results)\n",
            "        \n",
            "        # Store results for Stage 9\n",
            "        stage8_results = analysis_results\n",
            "        \n",
            "        stage8_duration = time.time() - stage8_start\n",
            "        print(f\"\\n✅ Stage 8 Complete in {stage8_duration:.1f}s!\")\n",
            "        print(f\"🧠 Strategic analysis complete with {analysis_results.status} status\")\n",
            "        print(f\"📊 Current state metrics generated\")\n",
            "        print(f\"🎯 Competitive analysis complete\")\n",
            "        print(f\"🔮 Forecasting and business impact assessment ready\")\n",
            "        print(f\"⚡ Ready for Stage 9 (Multi-Dimensional Intelligence)\")\n",
            "        \n",
            "    except Exception as e:\n",
            "        print(f\"❌ Stage 8 Failed: {e}\")\n",
            "        stage8_results = None\n",
            "        import traceback\n",
            "        traceback.print_exc()"
        ]
    },

    # Strategic Analysis Dashboard
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Strategic Analysis Dashboard\n",
            "import pandas as pd\n",
            "from IPython.display import display\n",
            "\n",
            "print(\"🧠 STRATEGIC ANALYSIS - COMPREHENSIVE INTELLIGENCE DASHBOARD\")\n",
            "print(\"=\" * 70)\n",
            "\n",
            "if stage8_results is None:\n",
            "    print(\"❌ No strategic analysis results found\")\n",
            "    print(\"   Make sure you ran Stage 8 Strategic Analysis first\")\n",
            "    print(\"   Check the output above for any errors\")\n",
            "else:\n",
            "    print(f\"✅ Strategic Analysis Status: {stage8_results.status}\")\n",
            "    print(f\"📋 Analysis Message: {stage8_results.message}\")\n",
            "    print()\n",
            "    \n",
            "    # 1. CURRENT STATE ANALYSIS\n",
            "    print(\"📊 CURRENT STATE ANALYSIS\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage8_results, 'current_state') and stage8_results.current_state:\n",
            "        current_state = stage8_results.current_state\n",
            "        \n",
            "        # Create strategic metrics DataFrame\n",
            "        metrics_data = [\n",
            "            {'Metric': 'Promotional Intensity', 'Score': f\"{current_state.get('promotional_intensity', 0):.3f}\", 'Interpretation': 'How aggressive the promotional messaging is'},\n",
            "            {'Metric': 'Urgency Score', 'Score': f\"{current_state.get('urgency_score', 0):.3f}\", 'Interpretation': 'Level of urgency in messaging'},\n",
            "            {'Metric': 'Brand Voice Score', 'Score': f\"{current_state.get('brand_voice_score', 0):.3f}\", 'Interpretation': 'Consistency of brand voice'},\n",
            "            {'Metric': 'Market Position', 'Score': current_state.get('market_position', 'unknown'), 'Interpretation': 'Strategic market positioning'},\n",
            "            {'Metric': 'Promotional Volatility', 'Score': f\"{current_state.get('promotional_volatility', 0):.3f}\", 'Interpretation': 'Consistency of promotional approach'},\n",
            "            {'Metric': 'CTA Aggressiveness', 'Score': f\"{current_state.get('avg_cta_aggressiveness', 0):.2f}/10\", 'Interpretation': 'Average call-to-action intensity'}\n",
            "        ]\n",
            "        \n",
            "        metrics_df = pd.DataFrame(metrics_data)\n",
            "        display(metrics_df)\n",
            "        \n",
            "        print(f\"\\n🎯 Strategic Position: {current_state.get('market_position', 'unknown').upper()}\")\n",
            "        print(f\"💡 Key Insight: Promotional intensity of {current_state.get('promotional_intensity', 0):.1%} indicates {'aggressive' if current_state.get('promotional_intensity', 0) > 0.6 else 'moderate' if current_state.get('promotional_intensity', 0) > 0.4 else 'conservative'} market approach\")\n",
            "    \n",
            "    # 2. COMPETITIVE COPYING INTELLIGENCE\n",
            "    print(\"\\n🎯 COMPETITIVE COPYING INTELLIGENCE\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage8_results, 'influence') and stage8_results.influence:\n",
            "        influence = stage8_results.influence\n",
            "        \n",
            "        if influence.get('copying_detected', False):\n",
            "            print(f\"🚨 COPYING DETECTED!\")\n",
            "            print(f\"   Top Copier: {influence.get('top_copier', 'Unknown')}\")\n",
            "            print(f\"   Similarity Score: {influence.get('similarity_score', 0):.1%}\")\n",
            "            print(f\"   Average Lag Time: {influence.get('lag_days', 0)} days\")\n",
            "            print(f\"   ⚠️  Monitor {influence.get('top_copier', 'Unknown')} for strategic copying patterns\")\n",
            "        else:\n",
            "            print(f\"✅ No significant copying patterns detected\")\n",
            "            print(f\"   Your strategies appear to be unique in the competitive landscape\")\n",
            "            print(f\"   Continue monitoring for emerging competitive responses\")\n",
            "    \n",
            "    # 3. TEMPORAL INTELLIGENCE\n",
            "    print(\"\\n📈 TEMPORAL INTELLIGENCE & EVOLUTION\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage8_results, 'evolution') and stage8_results.evolution:\n",
            "        evolution = stage8_results.evolution\n",
            "        \n",
            "        # Create evolution metrics DataFrame\n",
            "        evolution_data = [\n",
            "            {'Metric': 'Momentum Status', 'Value': evolution.get('momentum_status', 'STABLE'), 'Timeframe': 'Current'},\n",
            "            {'Metric': 'Velocity Change (7d)', 'Value': f\"{evolution.get('velocity_change_7d', 0):.1%}\", 'Timeframe': 'Short-term'},\n",
            "            {'Metric': 'Velocity Change (30d)', 'Value': f\"{evolution.get('velocity_change_30d', 0):.1%}\", 'Timeframe': 'Medium-term'},\n",
            "            {'Metric': 'Trend Direction', 'Value': evolution.get('trend_direction', 'stable'), 'Timeframe': 'Overall'}\n",
            "        ]\n",
            "        \n",
            "        evolution_df = pd.DataFrame(evolution_data)\n",
            "        display(evolution_df)\n",
            "        \n",
            "        momentum = evolution.get('momentum_status', 'STABLE')\n",
            "        if momentum == 'ACCELERATING':\n",
            "            print(f\"🚀 Market momentum is ACCELERATING - capitalize on current strategies\")\n",
            "        elif momentum == 'DECELERATING':\n",
            "            print(f\"⚠️  Market momentum is DECELERATING - consider strategy adjustment\")\n",
            "        else:\n",
            "            print(f\"📊 Market momentum is STABLE - maintain current positioning\")\n",
            "    \n",
            "    # 4. STRATEGIC FORECASTING\n",
            "    print(\"\\n🔮 STRATEGIC FORECASTING & BUSINESS IMPACT\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage8_results, 'forecasts') and stage8_results.forecasts:\n",
            "        forecasts = stage8_results.forecasts\n",
            "        \n",
            "        print(f\"📋 Executive Summary: {forecasts.get('executive_summary', 'No forecast available')}\")\n",
            "        print(f\"🎯 Business Impact Score: {forecasts.get('business_impact_score', 0)}/5\")\n",
            "        print(f\"📊 Confidence Level: {forecasts.get('confidence', 'UNKNOWN')}\")\n",
            "        \n",
            "        # Timeline forecasts\n",
            "        timeline_data = []\n",
            "        if 'next_7_days' in forecasts:\n",
            "            timeline_data.append({'Timeframe': '7 Days', 'Forecast': forecasts['next_7_days'], 'Focus': 'Tactical'})\n",
            "        if 'next_14_days' in forecasts:\n",
            "            timeline_data.append({'Timeframe': '14 Days', 'Forecast': forecasts['next_14_days'], 'Focus': 'Strategic'})\n",
            "        if 'next_30_days' in forecasts:\n",
            "            timeline_data.append({'Timeframe': '30 Days', 'Forecast': forecasts['next_30_days'], 'Focus': 'Market Position'})\n",
            "        \n",
            "        if timeline_data:\n",
            "            timeline_df = pd.DataFrame(timeline_data)\n",
            "            print(\"\\n📅 Forecast Timeline:\")\n",
            "            display(timeline_df)\n",
            "        \n",
            "        # Business impact assessment\n",
            "        impact_score = forecasts.get('business_impact_score', 0)\n",
            "        if impact_score >= 4:\n",
            "            print(f\"\\n🚨 HIGH IMPACT: Significant market changes predicted - immediate strategic response recommended\")\n",
            "        elif impact_score >= 3:\n",
            "            print(f\"\\n⚠️  MEDIUM IMPACT: Notable competitive shifts expected - monitor and prepare responses\")\n",
            "        else:\n",
            "            print(f\"\\n✅ LOW IMPACT: Stable competitive environment predicted - maintain current strategies\")\n",
            "    \n",
            "    print(\"\\n✅ STRATEGIC ANALYSIS COMPLETE!\")\n",
            "    print(\"🎯 Strategic intelligence ready for business decision-making\")\n",
            "    print(\"💡 Use these insights to guide competitive positioning and tactical adjustments\")\n",
            "    print(\"⚡ Ready for Stage 9 (Multi-Dimensional Intelligence)\")"
        ]
    },

    # Summary cell
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "### Stage 8 Summary\n",
            "\n",
            "**✅ Strategic Analysis Complete**\n",
            "\n",
            "**Key Achievements:**\n",
            "- 📊 **Current State Analysis**: Comprehensive strategic position assessment\n",
            "- 🎯 **Competitive Copying Detection**: Semantic similarity analysis using embeddings\n",
            "- 📈 **Temporal Intelligence**: Market momentum and evolution tracking\n",
            "- 📱 **CTA Intelligence**: Call-to-action strategy analysis across competitors\n",
            "- 🔮 **Strategic Forecasting**: Business impact predictions with confidence levels\n",
            "\n",
            "**Strategic Intelligence Generated:**\n",
            "- Market positioning assessment (offensive/defensive/balanced)\n",
            "- Competitive influence patterns and copying detection\n",
            "- Temporal momentum analysis with velocity tracking\n",
            "- Predictive forecasting with business impact scoring\n",
            "\n",
            "**Business Value:**\n",
            "- Actionable strategic insights for decision-making\n",
            "- Competitive threat assessment and opportunity identification\n",
            "- Timeline-based forecasting for tactical planning\n",
            "\n",
            "**Next Stage:** Stage 9 - Multi-Dimensional Intelligence (Comprehensive Intelligence Dashboard)\n",
            "\n",
            "---"
        ]
    }
]

# Insert the new cells at the found position
for i, cell in enumerate(stage8_cells):
    cells.insert(insert_index + i, cell)

# Update the notebook
notebook['cells'] = cells

# Write back to file
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully updated Visual Intelligence to Stage 7 and added Stage 8!")
print(f"   🔄 Updated Visual Intelligence numbering from Stage 6 → Stage 7")
print(f"   📍 Inserted {len(stage8_cells)} Stage 8 cells at position {insert_index}")
print(f"   🧠 Added comprehensive strategic analysis dashboard")
print(f"   📊 Includes current state, copying detection, temporal intelligence, and forecasting")
print(f"   🔗 Properly connects Stage 6 (Embeddings) → Stage 7 (Visual) → Stage 8 (Strategic)")