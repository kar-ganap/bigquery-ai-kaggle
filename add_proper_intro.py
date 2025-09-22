#!/usr/bin/env python3
"""
Add the proper intro cell to the beginning of the notebook
"""
import json

# Read the notebook
with open('notebooks/demo_competitive_intelligence.ipynb', 'r') as f:
    notebook = json.load(f)

cells = notebook['cells']

print("🔧 ADDING PROPER INTRO CELL")
print("=" * 50)

# Create the proper intro cell
intro_cell = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "# L4 Temporal Intelligence Framework\n",
        "\n",
        "## Competitive Intelligence Journey: Stage-by-Stage Demo\n",
        "\n",
        "**Interactive showcase of next-generation competitive intelligence powered by BigQuery AI**\n",
        "\n",
        "### 🎯 Demo Overview\n",
        "\n",
        "This notebook demonstrates the complete **L4 Temporal Intelligence Framework** - a comprehensive system that transforms static competitive snapshots into dynamic, actionable business intelligence.\n",
        "\n",
        "### 🚀 What You'll Experience\n",
        "\n",
        "**Stage-by-Stage Journey:**\n",
        "- **Stage 0**: Clean Slate Preparation\n",
        "- **Stage 1**: Discovery Engine - Intelligent competitor identification\n",
        "- **Stage 2**: AI Competitor Curation - Smart validation and filtering\n",
        "- **Stage 3**: Meta Ad Activity Ranking - Real-time market positioning\n",
        "- **Stage 4**: Meta Ads Ingestion - Parallel competitive data collection\n",
        "- **Stage 5**: Strategic Labeling - AI-powered competitive categorization\n",
        "- **Stage 6**: Embeddings Generation - Semantic intelligence layer\n",
        "- **Stage 7**: Visual Intelligence - Multimodal creative analysis\n",
        "- **Stage 8**: Strategic Analysis - Comprehensive competitive dashboard\n",
        "- **Stage 9**: Multi-Dimensional Intelligence - Advanced analytics synthesis\n",
        "\n",
        "### 🧠 AI-Powered Technologies\n",
        "\n",
        "- **Gemini 2.0 Flash Thinking** - Advanced reasoning and analysis\n",
        "- **text-embedding-004** - State-of-the-art semantic embeddings\n",
        "- **BigQuery Vector Search** - High-performance similarity matching\n",
        "- **Multimodal AI** - Visual and textual content analysis\n",
        "\n",
        "### 📊 Business Impact\n",
        "\n",
        "Transform your competitive strategy with:\n",
        "- **Real-time competitive monitoring**\n",
        "- **AI-powered market insights**\n",
        "- **Predictive competitive intelligence**\n",
        "- **Automated strategic recommendations**\n",
        "\n",
        "---\n",
        "\n",
        "**🎪 Ready to explore the future of competitive intelligence? Let's begin!**"
    ]
}

# Remove the incorrect first cell (demo complete) and insert proper intro
if cells and "Demo Complete" in ''.join(cells[0].get('source', [])):
    cells.pop(0)  # Remove the misplaced demo complete cell
    print("🗑️  Removed misplaced 'Demo Complete' cell from position 0")

# Insert the proper intro at the beginning
cells.insert(0, intro_cell)
print("✅ Added proper intro cell at position 0")

# Update the notebook
notebook['cells'] = cells

# Write back
with open('notebooks/demo_competitive_intelligence.ipynb', 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Notebook updated with proper intro")
print(f"📊 Total cells: {len(cells)}")

# Show first few cells to verify
print(f"\n🔍 First 3 cells:")
for i in range(min(3, len(cells))):
    cell = cells[i]
    source = ''.join(cell['source']) if isinstance(cell['source'], list) and cell.get('source') else 'Empty cell'
    preview = source[:80].replace('\n', ' ') if source else 'Empty'
    print(f"   {i}. [{cell.get('cell_type', 'unknown'):8}] {preview}")