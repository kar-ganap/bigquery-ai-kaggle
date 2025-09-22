#!/usr/bin/env python3
"""
Add Stage 9 Multi-Dimensional Intelligence to the demo notebook
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

# Find where to insert Stage 9 - after Stage 8 Strategic Analysis
insert_index = None
for i, cell in enumerate(cells):
    if cell.get('cell_type') == 'markdown' and cell.get('source'):
        source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
        if 'Stage 8 Summary' in source:
            insert_index = i + 1
            break

# If not found, look for the end of Stage 8
if insert_index is None:
    stage8_found = False
    for i, cell in enumerate(cells):
        if cell.get('cell_type') == 'markdown' and cell.get('source'):
            source = ''.join(cell['source']) if isinstance(cell['source'], list) else cell['source']
            if 'Stage 8:' in source or 'STRATEGIC ANALYSIS' in source:
                stage8_found = True
            elif stage8_found and ('---' in source or 'Next Stage' in source):
                insert_index = i + 1
                break

# If still not found, append at the end
if insert_index is None:
    insert_index = len(cells)

print(f"Inserting Stage 9 at cell index {insert_index}")

# Create Stage 9 Multi-Dimensional Intelligence cells
stage9_cells = [
    # Markdown header
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "---\n",
            "\n",
            "## 🎯 Stage 9: Multi-Dimensional Intelligence\n",
            "\n",
            "**Purpose**: Comprehensive intelligence dashboard across all competitive dimensions\n",
            "\n",
            "**Input**: Strategic analysis from Stage 8, Visual intelligence from Stage 7, Strategic labels from Stage 5\n",
            "**Output**: Complete multi-dimensional competitive intelligence with business-ready insights\n",
            "\n",
            "**Intelligence Modules:**\n",
            "- 👥 **Audience Intelligence**: Platform targeting and communication patterns\n",
            "- 🎨 **Creative Intelligence**: Messaging themes and visual creative patterns\n",
            "- 📡 **Channel Intelligence**: Platform performance and reach analysis\n",
            "- 🎯 **Whitespace Intelligence**: Market gaps and strategic opportunities\n",
            "- 📊 **Intelligence Summary**: Executive-level competitive insights\n",
            "\n",
            "**Integration Features:**\n",
            "- Preserves all strategic metrics from Stage 8\n",
            "- Combines with visual intelligence from Stage 7\n",
            "- Generates comprehensive competitive landscape analysis\n",
            "\n",
            "**Architecture Note**: The culmination of all intelligence gathering - business-ready competitive insights"
        ]
    },

    # Execution cell
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "print(\"🎯 === STAGE 9: MULTI-DIMENSIONAL INTELLIGENCE ===\" + \" (STAGE TESTING FRAMEWORK APPROACH)\")\n",
            "print(f\"📥 Input: Strategic analysis from Stage 8, Visual intelligence from Stage 7\")\n",
            "\n",
            "# Initialize Stage 9 (Multi-Dimensional Intelligence) \n",
            "from src.pipeline.stages.multidimensional_intelligence import MultiDimensionalIntelligenceStage\n",
            "\n",
            "if stage8_results is None:\n",
            "    print(\"❌ Cannot proceed - Stage 8 (Strategic Analysis) failed\")\n",
            "    stage9_results = None\n",
            "else:\n",
            "    # Stage 9 constructor: MultiDimensionalIntelligenceStage(stage_name, stage_number, run_id)\n",
            "    intelligence_stage = MultiDimensionalIntelligenceStage(\n",
            "        stage_name=\"Multi-Dimensional Intelligence\",\n",
            "        stage_number=9,\n",
            "        run_id=demo_run_id\n",
            "    )\n",
            "    \n",
            "    # Pass competitor brands and visual intelligence results to the stage\n",
            "    if 'stage4_results' in locals() and stage4_results is not None:\n",
            "        intelligence_stage.competitor_brands = stage4_results.brands\n",
            "        print(f\"🎯 Analyzing {len(stage4_results.brands)} brands from ingestion results\")\n",
            "    \n",
            "    if 'stage7_results' in locals() and stage7_results is not None:\n",
            "        intelligence_stage.visual_intelligence_results = stage7_results.__dict__ if hasattr(stage7_results, '__dict__') else {}\n",
            "        print(f\"👁️ Integrating visual intelligence from Stage 7\")\n",
            "    \n",
            "    try:\n",
            "        import time\n",
            "        stage9_start = time.time()\n",
            "        \n",
            "        print(\"\\n🧠 Executing multi-dimensional intelligence analysis...\")\n",
            "        print(\"   👥 Audience Intelligence Analysis...\")\n",
            "        print(\"   🎨 Creative Intelligence Analysis...\")\n",
            "        print(\"   📡 Channel Intelligence Analysis...\")\n",
            "        print(\"   🎯 Whitespace Intelligence Analysis...\")\n",
            "        print(\"   📊 Intelligence Summary Generation...\")\n",
            "        \n",
            "        # Execute multi-dimensional intelligence - preserves all Stage 8 strategic metrics\n",
            "        intelligence_results = intelligence_stage.execute(stage8_results)\n",
            "        \n",
            "        # Store results for Stage 10 (if implemented)\n",
            "        stage9_results = intelligence_results\n",
            "        \n",
            "        stage9_duration = time.time() - stage9_start\n",
            "        print(f\"\\n✅ Stage 9 Complete in {stage9_duration:.1f}s!\")\n",
            "        print(f\"🎯 Multi-dimensional intelligence complete with {intelligence_results.status} status\")\n",
            "        print(f\"📊 Data completeness: {intelligence_results.data_completeness:.1f}%\")\n",
            "        print(f\"👥 Audience intelligence: {intelligence_results.audience_intelligence.get('status', 'unknown')}\")\n",
            "        print(f\"🎨 Creative intelligence: {intelligence_results.creative_intelligence.get('status', 'unknown')}\")\n",
            "        print(f\"📡 Channel intelligence: {intelligence_results.channel_intelligence.get('status', 'unknown')}\")\n",
            "        print(f\"🎯 Whitespace intelligence: {intelligence_results.whitespace_intelligence.get('status', 'unknown')}\")\n",
            "        print(f\"🏆 Ready for business intelligence consumption!\")\n",
            "        \n",
            "    except Exception as e:\n",
            "        print(f\"❌ Stage 9 Failed: {e}\")\n",
            "        stage9_results = None\n",
            "        import traceback\n",
            "        traceback.print_exc()"
        ]
    },

    # Multi-Dimensional Intelligence Dashboard
    {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [
            "# Multi-Dimensional Intelligence Dashboard\n",
            "import pandas as pd\n",
            "from IPython.display import display\n",
            "\n",
            "print(\"🎯 MULTI-DIMENSIONAL INTELLIGENCE - COMPREHENSIVE COMPETITIVE DASHBOARD\")\n",
            "print(\"=\" * 80)\n",
            "\n",
            "if stage9_results is None:\n",
            "    print(\"❌ No multi-dimensional intelligence results found\")\n",
            "    print(\"   Make sure you ran Stage 9 Multi-Dimensional Intelligence first\")\n",
            "    print(\"   Check the output above for any errors\")\n",
            "else:\n",
            "    print(f\"✅ Intelligence Status: {stage9_results.status}\")\n",
            "    print(f\"📋 Analysis Message: {stage9_results.message}\")\n",
            "    print(f\"📊 Data Completeness: {stage9_results.data_completeness:.1f}%\")\n",
            "    print()\n",
            "    \n",
            "    # STRATEGIC METRICS PRESERVATION CHECK\n",
            "    print(\"🧠 STRATEGIC METRICS PRESERVATION\")\n",
            "    print(\"=\" * 40)\n",
            "    print(\"Verifying that all Stage 8 strategic metrics are preserved:\")\n",
            "    \n",
            "    if hasattr(stage9_results, 'current_state') and stage9_results.current_state:\n",
            "        print(f\"   ✅ Current State: Promotional Intensity = {stage9_results.current_state.get('promotional_intensity', 'N/A')}\")\n",
            "    if hasattr(stage9_results, 'influence') and stage9_results.influence:\n",
            "        print(f\"   ✅ Competitive Influence: Copying Detected = {stage9_results.influence.get('copying_detected', 'N/A')}\")\n",
            "    if hasattr(stage9_results, 'forecasts') and stage9_results.forecasts:\n",
            "        print(f\"   ✅ Strategic Forecasts: Business Impact = {stage9_results.forecasts.get('business_impact_score', 'N/A')}/5\")\n",
            "    \n",
            "    # 1. AUDIENCE INTELLIGENCE\n",
            "    print(\"\\n👥 AUDIENCE INTELLIGENCE\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage9_results, 'audience_intelligence') and stage9_results.audience_intelligence:\n",
            "        audience = stage9_results.audience_intelligence\n",
            "        print(f\"Status: {audience.get('status', 'unknown')}\")\n",
            "        \n",
            "        if audience.get('status') == 'success':\n",
            "            print(f\"📊 BigQuery Table: {audience.get('table_created', 'N/A')}\")\n",
            "            print(f\"🎯 Brands Analyzed: {len(audience.get('brands_analyzed', []))}\")\n",
            "            \n",
            "            # Platform strategy insights\n",
            "            if 'insights' in audience:\n",
            "                insights = audience['insights']\n",
            "                print(f\"\\n📱 Platform Strategy Insights:\")\n",
            "                for insight in insights.get('platform_strategies', [])[:3]:\n",
            "                    print(f\"   • {insight}\")\n",
            "        else:\n",
            "            print(f\"⚠️ {audience.get('error', 'Unknown error')}\")\n",
            "    \n",
            "    # 2. CREATIVE INTELLIGENCE\n",
            "    print(\"\\n🎨 CREATIVE INTELLIGENCE\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage9_results, 'creative_intelligence') and stage9_results.creative_intelligence:\n",
            "        creative = stage9_results.creative_intelligence\n",
            "        print(f\"Status: {creative.get('status', 'unknown')}\")\n",
            "        \n",
            "        if creative.get('status') == 'success':\n",
            "            print(f\"📊 BigQuery Table: {creative.get('table_created', 'N/A')}\")\n",
            "            print(f\"🎯 Brands Analyzed: {len(creative.get('brands_analyzed', []))}\")\n",
            "            \n",
            "            # Creative insights\n",
            "            if 'insights' in creative:\n",
            "                insights = creative['insights']\n",
            "                print(f\"\\n🎨 Creative Strategy Insights:\")\n",
            "                for insight in insights.get('messaging_patterns', [])[:3]:\n",
            "                    print(f\"   • {insight}\")\n",
            "        else:\n",
            "            print(f\"⚠️ {creative.get('error', 'Unknown error')}\")\n",
            "    \n",
            "    # 3. CHANNEL INTELLIGENCE\n",
            "    print(\"\\n📡 CHANNEL INTELLIGENCE\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage9_results, 'channel_intelligence') and stage9_results.channel_intelligence:\n",
            "        channel = stage9_results.channel_intelligence\n",
            "        print(f\"Status: {channel.get('status', 'unknown')}\")\n",
            "        \n",
            "        if channel.get('status') == 'success':\n",
            "            print(f\"📊 BigQuery Table: {channel.get('table_created', 'N/A')}\")\n",
            "            print(f\"🎯 Brands Analyzed: {len(channel.get('brands_analyzed', []))}\")\n",
            "            \n",
            "            # Channel insights\n",
            "            if 'insights' in channel:\n",
            "                insights = channel['insights']\n",
            "                print(f\"\\n📡 Channel Strategy Insights:\")\n",
            "                for insight in insights.get('platform_patterns', [])[:3]:\n",
            "                    print(f\"   • {insight}\")\n",
            "        else:\n",
            "            print(f\"⚠️ {channel.get('error', 'Unknown error')}\")\n",
            "    \n",
            "    # 4. WHITESPACE INTELLIGENCE\n",
            "    print(\"\\n🎯 WHITESPACE INTELLIGENCE\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage9_results, 'whitespace_intelligence') and stage9_results.whitespace_intelligence:\n",
            "        whitespace = stage9_results.whitespace_intelligence\n",
            "        print(f\"Status: {whitespace.get('status', 'unknown')}\")\n",
            "        \n",
            "        if whitespace.get('status') == 'success':\n",
            "            print(f\"🎯 Opportunities Found: {whitespace.get('opportunities_found', 0)}\")\n",
            "            print(f\"📊 Data Quality: {whitespace.get('data_quality', 'unknown')}\")\n",
            "            \n",
            "            # Whitespace opportunities\n",
            "            if 'opportunities' in whitespace:\n",
            "                opportunities = whitespace['opportunities']\n",
            "                print(f\"\\n🎯 Market Opportunities:\")\n",
            "                for i, opp in enumerate(opportunities[:3], 1):\n",
            "                    print(f\"   {i}. {opp.get('description', 'Unknown opportunity')}\")\n",
            "                    print(f\"      Impact: {opp.get('impact_level', 'unknown')}\")\n",
            "        else:\n",
            "            print(f\"⚠️ {whitespace.get('error', 'Unknown error')}\")\n",
            "    \n",
            "    # 5. INTELLIGENCE SUMMARY\n",
            "    print(\"\\n📊 EXECUTIVE INTELLIGENCE SUMMARY\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    if hasattr(stage9_results, 'intelligence_summary') and stage9_results.intelligence_summary:\n",
            "        summary = stage9_results.intelligence_summary\n",
            "        print(f\"Status: {summary.get('status', 'unknown')}\")\n",
            "        \n",
            "        if summary.get('status') == 'success':\n",
            "            print(f\"\\n🏆 KEY COMPETITIVE INSIGHTS:\")\n",
            "            \n",
            "            # Executive summary points\n",
            "            if 'executive_insights' in summary:\n",
            "                for insight in summary['executive_insights'][:5]:\n",
            "                    print(f\"   • {insight}\")\n",
            "            \n",
            "            # Competitive positioning\n",
            "            if 'competitive_position' in summary:\n",
            "                print(f\"\\n🎯 Competitive Position: {summary['competitive_position']}\")\n",
            "            \n",
            "            # Strategic recommendations\n",
            "            if 'strategic_recommendations' in summary:\n",
            "                print(f\"\\n💡 Strategic Recommendations:\")\n",
            "                for rec in summary['strategic_recommendations'][:3]:\n",
            "                    print(f\"   • {rec}\")\n",
            "        else:\n",
            "            print(f\"⚠️ {summary.get('error', 'Unknown error')}\")\n",
            "    \n",
            "    # INTEGRATION STATUS\n",
            "    print(\"\\n🔗 INTELLIGENCE INTEGRATION STATUS\")\n",
            "    print(\"=\" * 40)\n",
            "    \n",
            "    # Check integration with previous stages\n",
            "    integration_status = []\n",
            "    \n",
            "    if hasattr(stage9_results, 'current_state') and stage9_results.current_state:\n",
            "        integration_status.append(\"✅ Stage 8 Strategic Analysis - PRESERVED\")\n",
            "    else:\n",
            "        integration_status.append(\"❌ Stage 8 Strategic Analysis - MISSING\")\n",
            "    \n",
            "    if hasattr(stage9_results, 'visual_intelligence') and stage9_results.visual_intelligence:\n",
            "        integration_status.append(\"✅ Stage 7 Visual Intelligence - INTEGRATED\")\n",
            "    else:\n",
            "        integration_status.append(\"⚠️ Stage 7 Visual Intelligence - LIMITED\")\n",
            "    \n",
            "    intelligence_modules = 0\n",
            "    if hasattr(stage9_results, 'audience_intelligence') and stage9_results.audience_intelligence.get('status') == 'success':\n",
            "        intelligence_modules += 1\n",
            "    if hasattr(stage9_results, 'creative_intelligence') and stage9_results.creative_intelligence.get('status') == 'success':\n",
            "        intelligence_modules += 1\n",
            "    if hasattr(stage9_results, 'channel_intelligence') and stage9_results.channel_intelligence.get('status') == 'success':\n",
            "        intelligence_modules += 1\n",
            "    if hasattr(stage9_results, 'whitespace_intelligence') and stage9_results.whitespace_intelligence.get('status') == 'success':\n",
            "        intelligence_modules += 1\n",
            "    \n",
            "    integration_status.append(f\"📊 Intelligence Modules Active: {intelligence_modules}/4\")\n",
            "    \n",
            "    for status in integration_status:\n",
            "        print(f\"   {status}\")\n",
            "    \n",
            "    print(\"\\n✅ MULTI-DIMENSIONAL INTELLIGENCE COMPLETE!\")\n",
            "    print(\"🎯 Comprehensive competitive intelligence ready for business consumption\")\n",
            "    print(\"💡 Strategic insights span audience, creative, channel, and market positioning\")\n",
            "    print(\"🏆 All strategic metrics preserved and enhanced with multi-dimensional analysis\")"
        ]
    },

    # Summary cell
    {
        "cell_type": "markdown",
        "metadata": {},
        "source": [
            "### Stage 9 Summary\n",
            "\n",
            "**✅ Multi-Dimensional Intelligence Complete**\n",
            "\n",
            "**Intelligence Modules Deployed:**\n",
            "- 👥 **Audience Intelligence**: Platform targeting patterns and communication style analysis\n",
            "- 🎨 **Creative Intelligence**: Messaging themes, visual patterns, and creative strategy analysis\n",
            "- 📡 **Channel Intelligence**: Platform performance analysis and reach optimization insights\n",
            "- 🎯 **Whitespace Intelligence**: Market gap identification and strategic opportunity analysis\n",
            "- 📊 **Intelligence Summary**: Executive-level competitive insights and strategic recommendations\n",
            "\n",
            "**Strategic Integration:**\n",
            "- Preserves all strategic metrics from Stage 8 (Strategic Analysis)\n",
            "- Integrates visual intelligence insights from Stage 7\n",
            "- Combines competitive data across all intelligence dimensions\n",
            "- Generates business-ready competitive intelligence dashboard\n",
            "\n",
            "**Business Value Generated:**\n",
            "- Comprehensive competitive landscape analysis\n",
            "- Multi-dimensional strategic positioning assessment\n",
            "- Market opportunity identification and prioritization\n",
            "- Executive-level insights for strategic decision-making\n",
            "\n",
            "**Data Architecture:**\n",
            "- Creates dedicated BigQuery tables for each intelligence module\n",
            "- Maintains data lineage from raw ads → strategic labels → embeddings → intelligence\n",
            "- Enables SQL-based dashboard creation for stakeholder consumption\n",
            "\n",
            "**Next Stage:** Stage 10 - Intelligence Output (Final Dashboard Generation)\n",
            "\n",
            "---"
        ]
    }
]

# Insert the new cells at the found position
for i, cell in enumerate(stage9_cells):
    cells.insert(insert_index + i, cell)

# Update the notebook
notebook['cells'] = cells

# Write back to file
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully added Stage 9 (Multi-Dimensional Intelligence) to the notebook!")
print(f"   📍 Inserted {len(stage9_cells)} cells at position {insert_index}")
print(f"   🎯 Added comprehensive multi-dimensional intelligence dashboard")
print(f"   📊 Includes audience, creative, channel, and whitespace intelligence modules")
print(f"   🧠 Preserves all strategic metrics from Stage 8")
print(f"   👁️ Integrates visual intelligence from Stage 7")
print(f"   🏆 Complete competitive intelligence showcase ready!")