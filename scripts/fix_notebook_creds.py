#!/usr/bin/env python3
"""Fix notebook to properly handle credentials path"""

import json

notebook_path = "/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar/notebooks/demo_competitive_intelligence.ipynb"

# Load the notebook
with open(notebook_path, 'r') as f:
    nb = json.load(f)

# Find and update the environment loading cell (should be cell 2 after our previous insertion)
if len(nb['cells']) > 2 and nb['cells'][2]['cell_type'] == 'code':
    cell = nb['cells'][2]
    source = cell.get('source', [])

    # Update the cell to fix the credentials path
    new_source = [
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
        "                    # Fix relative paths to be relative to project root\n",
        "                    if key == 'GOOGLE_APPLICATION_CREDENTIALS' and value.startswith('./'):\n",
        "                        value = str(project_root / value[2:])\n",
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
        "print(f'📊 Full Dataset Path: {BQ_FULL_DATASET}')\n",
        "print(f'🔑 Credentials Path: {os.environ.get(\"GOOGLE_APPLICATION_CREDENTIALS\", \"Not set\")}')\n",
        "\n",
        "# Verify credentials file exists\n",
        "creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')\n",
        "if creds_path and os.path.exists(creds_path):\n",
        "    print(f'✅ Credentials file found at {creds_path}')\n",
        "else:\n",
        "    print(f'⚠️  Credentials file not found at {creds_path}')"
    ]

    cell['source'] = new_source

# Save the fixed notebook
with open(notebook_path, 'w') as f:
    json.dump(nb, f, indent=2)

print(f"✅ Fixed notebook credentials handling: {notebook_path}")
print("   - Fixed GOOGLE_APPLICATION_CREDENTIALS path to be absolute")
print("   - Added verification that credentials file exists")