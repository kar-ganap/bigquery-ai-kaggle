#!/usr/bin/env python3
"""
HARD Test #1: Create Mock Competitive Copying Scenarios
Creates synthetic copying scenarios with known ground truth for testing detection accuracy
"""

import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
import random

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def create_mock_copying_test_data():
    """Create synthetic copying scenarios with known ground truth"""
    
    # Define copying scenario templates
    copying_scenarios = [
        {
            "original_brand": "Nike_Mock",
            "copying_brand": "Adidas_Mock", 
            "original_text": "Just Do It. Unleash your potential with premium athletic performance.",
            "copied_text": "Just Go For It. Unleash your power with premium athletic performance.",
            "similarity_score": 0.89,
            "days_between": 5,
            "scenario_type": "DIRECT_COPYING"
        },
        {
            "original_brand": "UnderArmour_Mock",
            "copying_brand": "Puma_Mock",
            "original_text": "I WILL. Protect this house with relentless performance gear.",
            "copied_text": "WE WILL. Defend your space with relentless performance gear.", 
            "similarity_score": 0.87,
            "days_between": 3,
            "scenario_type": "DIRECT_COPYING"
        },
        {
            "original_brand": "Adidas_Mock",
            "copying_brand": "Nike_Mock",
            "original_text": "Impossible is Nothing. Push boundaries with cutting-edge innovation.",
            "copied_text": "Nothing is Impossible. Break limits with cutting-edge innovation.",
            "similarity_score": 0.91,
            "days_between": 7,
            "scenario_type": "DIRECT_COPYING"
        },
        {
            "original_brand": "Puma_Mock", 
            "copying_brand": "UnderArmour_Mock",
            "original_text": "Forever Faster. Speed meets style in our latest collection.",
            "copied_text": "Always Stronger. Strength meets design in our new collection.",
            "similarity_score": 0.78,
            "days_between": 10,
            "scenario_type": "THEMATIC_COPYING"
        },
        {
            "original_brand": "Nike_Mock",
            "copying_brand": "Puma_Mock", 
            "original_text": "Get 50% off all running shoes. Limited time offer ends Sunday.",
            "copied_text": "Save 50% on all athletic footwear. Limited time deal ends Monday.",
            "similarity_score": 0.86,
            "days_between": 2,
            "scenario_type": "PROMOTIONAL_COPYING"
        },
        # Non-copying scenarios (false positive tests)
        {
            "original_brand": "Nike_Mock",
            "copying_brand": "Adidas_Mock",
            "original_text": "Run faster, jump higher, be stronger with our new training line.",
            "copied_text": "Comfort meets performance in our sustainable lifestyle collection.",
            "similarity_score": 0.32,
            "days_between": 15,
            "scenario_type": "NO_COPYING"
        },
        {
            "original_brand": "UnderArmour_Mock",
            "copying_brand": "Puma_Mock",
            "original_text": "Sweat-wicking technology keeps you cool during intense workouts.",
            "copied_text": "Discover the heritage of street culture in our retro sneaker line.",
            "similarity_score": 0.18,
            "days_between": 30,
            "scenario_type": "NO_COPYING"
        }
    ]
    
    # Create mock data records
    mock_records = []
    base_timestamp = datetime.now() - timedelta(days=90)
    
    for i, scenario in enumerate(copying_scenarios):
        # Original ad record
        original_record = {
            'ad_id': f'mock_original_{i}',
            'brand': scenario['original_brand'],
            'creative_text': scenario['original_text'],
            'title': f'Mock Campaign {i}A',
            'media_type': 'IMAGE',
            'start_timestamp': base_timestamp,
            'end_timestamp': (base_timestamp + timedelta(days=14)),
            'active_days': 14,
            'publisher_platforms': 'FACEBOOK,INSTAGRAM',
            'test_scenario_type': scenario['scenario_type'],
            'expected_similarity': scenario['similarity_score'],
            'is_mock_data': True,
            'mock_scenario_id': i
        }
        
        # Copied ad record (appearing later)
        copied_timestamp = base_timestamp + timedelta(days=scenario['days_between'])
        copied_record = {
            'ad_id': f'mock_copied_{i}',
            'brand': scenario['copying_brand'], 
            'creative_text': scenario['copied_text'],
            'title': f'Mock Campaign {i}B',
            'media_type': 'IMAGE',
            'start_timestamp': copied_timestamp,
            'end_timestamp': (copied_timestamp + timedelta(days=14)),
            'active_days': 14,
            'publisher_platforms': 'FACEBOOK,INSTAGRAM',
            'test_scenario_type': scenario['scenario_type'],
            'expected_similarity': scenario['similarity_score'],
            'is_mock_data': True,
            'mock_scenario_id': i
        }
        
        mock_records.extend([original_record, copied_record])
        base_timestamp += timedelta(days=20)  # Space out scenarios
    
    return mock_records

def insert_mock_data_to_bigquery():
    """Insert mock copying test data into BigQuery"""
    
    mock_data = create_mock_copying_test_data()
    
    # Convert to DataFrame
    df = pd.DataFrame(mock_data)
    
    print(f"🧪 HARD TEST #1: Creating Mock Competitive Copying Scenarios")
    print("=" * 60)
    print(f"📊 Mock Data Summary:")
    print(f"   Total mock ads: {len(df)}")
    print(f"   Copying scenarios: {len(df[df['test_scenario_type'].str.contains('COPYING')])}")
    print(f"   Non-copying scenarios: {len(df[df['test_scenario_type'] == 'NO_COPYING'])}")
    
    # Define table schema to match ads_with_dates
    schema = [
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("brand", "STRING"), 
        bigquery.SchemaField("creative_text", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("media_type", "STRING"),
        bigquery.SchemaField("start_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("end_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("active_days", "INTEGER"),
        bigquery.SchemaField("publisher_platforms", "STRING"),
        bigquery.SchemaField("test_scenario_type", "STRING"),
        bigquery.SchemaField("expected_similarity", "FLOAT"),
        bigquery.SchemaField("is_mock_data", "BOOLEAN"),
        bigquery.SchemaField("mock_scenario_id", "INTEGER")
    ]
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.mock_copying_test_data"
    
    try:
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        
        # Insert data
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema
        )
        
        job = client.load_table_from_dataframe(df, table, job_config=job_config)
        job.result()  # Wait for completion
        
        print(f"✅ Mock data inserted successfully:")
        print(f"   Table: {table_id}")
        print(f"   Rows inserted: {len(df)}")
        
        # Display scenarios for review
        print(f"\n📋 Mock Copying Scenarios Created:")
        
        unique_scenarios = df['mock_scenario_id'].unique()
        for scenario_id in unique_scenarios:
            scenario_data = df[df['mock_scenario_id'] == scenario_id]
            first_record = scenario_data.iloc[0]
            
            print(f"\n🏷️  Scenario {scenario_id}: {first_record['test_scenario_type']}")
            print(f"   Expected similarity: {first_record['expected_similarity']:.2f}")
            
            original = scenario_data[scenario_data['ad_id'].str.contains('original')].iloc[0]
            copied = scenario_data[scenario_data['ad_id'].str.contains('copied')].iloc[0]
            
            print(f"   Original ({original['brand']}): \"{original['creative_text'][:60]}...\"")
            print(f"   Copied ({copied['brand']}): \"{copied['creative_text'][:60]}...\"")
        
        return True
        
    except Exception as e:
        print(f"❌ Error inserting mock data: {e}")
        return False

def validate_mock_data():
    """Validate that mock data was inserted correctly"""
    
    query = f"""
    SELECT 
      'MOCK_DATA_VALIDATION' AS test_name,
      COUNT(*) AS total_mock_records,
      COUNT(DISTINCT mock_scenario_id) AS unique_scenarios,
      COUNT(DISTINCT brand) AS unique_mock_brands,
      
      -- Scenario distribution  
      COUNTIF(test_scenario_type = 'DIRECT_COPYING') AS direct_copying_scenarios,
      COUNTIF(test_scenario_type = 'THEMATIC_COPYING') AS thematic_copying_scenarios,
      COUNTIF(test_scenario_type = 'PROMOTIONAL_COPYING') AS promotional_copying_scenarios,
      COUNTIF(test_scenario_type = 'NO_COPYING') AS no_copying_scenarios,
      
      -- Expected similarity distribution
      AVG(expected_similarity) AS avg_expected_similarity,
      MIN(expected_similarity) AS min_expected_similarity,
      MAX(expected_similarity) AS max_expected_similarity
      
    FROM `{PROJECT_ID}.{DATASET_ID}.mock_copying_test_data`
    WHERE is_mock_data = true
    """
    
    try:
        result = client.query(query).result()
        
        for row in result:
            print(f"\n🔍 Mock Data Validation:")
            print(f"   Total records: {row.total_mock_records}")
            print(f"   Unique scenarios: {row.unique_scenarios}")  
            print(f"   Mock brands: {row.unique_mock_brands}")
            print(f"   Direct copying: {row.direct_copying_scenarios}")
            print(f"   Thematic copying: {row.thematic_copying_scenarios}")
            print(f"   Promotional copying: {row.promotional_copying_scenarios}")
            print(f"   No copying: {row.no_copying_scenarios}")
            print(f"   Similarity range: {row.min_expected_similarity:.2f} - {row.max_expected_similarity:.2f}")
            
    except Exception as e:
        print(f"❌ Error validating mock data: {e}")

if __name__ == "__main__":
    print("🧪 HARD TEST #1 SETUP: MOCK COMPETITIVE COPYING SCENARIOS")
    print("=" * 60)
    
    # Create and insert mock copying test data
    success = insert_mock_data_to_bigquery()
    
    if success:
        # Validate the inserted data
        validate_mock_data()
        
        print("\n✅ HARD TEST #1 SETUP COMPLETE: Mock copying scenarios ready for testing")
        print("🎯 Next: Run competitive copying detection against this ground truth data")
    else:
        print("\n❌ HARD TEST #1 SETUP FAILED: Could not create mock data")