#!/usr/bin/env python3
"""Fix notebook to properly use environment variables"""

import json
import os

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Add a new cell after cell 1 to load environment variables
env_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": [
        "# Load environment variables from .env file\n",
        "import os\n",
        "from pathlib import Path\n",
        "\n",
        "# Since we're in notebooks/, go up one directory to find .env\n",
        "project_root = Path.cwd().parent\n",
        "env_file = project_root / '.env'\n",
        "\n",
        "# Load environment variables manually (since we're in Jupyter, not using uv run)\n",
        "if env_file.exists():\n",
        "    with open(env_file) as f:\n",
        "        for line in f:\n",
        "            line = line.strip()\n",
        "            if line and not line.startswith('#'):\n",
        "                if '=' in line:\n",
        "                    key, value = line.split('=', 1)\n",
        "                    os.environ[key] = value\n",
        "    print('✅ Environment variables loaded from .env')\n",
        "else:\n",
        "    print('⚠️  .env file not found, using defaults')\n",
        "\n",
        "# Get BigQuery configuration from environment\n",
        "BQ_PROJECT = os.environ.get('BQ_PROJECT', 'bigquery-ai-kaggle-469620')\n",
        "BQ_DATASET = os.environ.get('BQ_DATASET', 'ads_demo')\n",
        "BQ_FULL_DATASET = f'{BQ_PROJECT}.{BQ_DATASET}'\n",
        "\n",
        "print(f'📊 BigQuery Project: {BQ_PROJECT}')\n",
        "print(f'📊 BigQuery Dataset: {BQ_DATASET}')\n",
        "print(f'📊 Full Dataset Path: {BQ_FULL_DATASET}')"
    ]
}

# Insert the environment loading cell after cell 1
nb['cells'].insert(2, env_cell)

# Fix cell 9 (was 8, now 9 after insertion)
if len(nb['cells']) > 9:
    cell = nb['cells'][9]
    source = cell.get('source', [])
    new_source = []
    for line in source:
        # Replace the problematic line
        if 'context.dataset_id' in line:
            line = line.replace('{context.dataset_id}', '{BQ_FULL_DATASET}')\
                      .replace('print(f"📊 BigQuery Dataset: {BQ_FULL_DATASET}")',
                              'print(f"📊 BigQuery Dataset: {BQ_FULL_DATASET}")')
        new_source.append(line)
    cell['source'] = new_source

# Fix cell 12 (was 11, now 12 after insertion)
if len(nb['cells']) > 12:
    cell = nb['cells'][12]
    source = cell.get('source', [])
    new_source = []
    for line in source:
        # Replace references to context.project_id and context.dataset_id
        if 'context.project_id' in line or 'context.dataset_id' in line:
            line = line.replace('{context.project_id}.{context.dataset_id}', '{BQ_FULL_DATASET}')\
                      .replace('context.project_id', 'BQ_PROJECT')\
                      .replace('context.dataset_id', 'BQ_DATASET')
        new_source.append(line)
    cell['source'] = new_source

# Also add the import for run_query at the top of cell 12 if not present
if len(nb['cells']) > 12:
    cell = nb['cells'][12]
    source = cell.get('source', [])

    # Check if run_query import is present
    has_import = any('from src.utils.bigquery_client import' in line for line in source)

    if not has_import:
        # Add the import at the beginning
        new_source = [
            "from src.utils.bigquery_client import run_query\n",
            "\n"
        ] + source
        cell['source'] = new_source

# Save the fixed notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Fixed notebook: {notebook_path}")
print("   - Added environment variable loading cell")
print("   - Fixed references to context.dataset_id and context.project_id")
print("   - Now using BQ_PROJECT and BQ_DATASET from .env")