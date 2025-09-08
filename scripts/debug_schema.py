#!/usr/bin/env python3
"""Debug schema of mock data"""

import os
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620") 
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

# Check what columns exist
query = f"""
SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
LIMIT 1
"""

try:
    result = client.query(query).to_dataframe()
    print("Available columns:")
    for col in result.columns:
        print(f"  {col}")
        
    print(f"\nSample row:")
    print(result.iloc[0].to_dict())
        
except Exception as e:
    print(f"Error: {e}")