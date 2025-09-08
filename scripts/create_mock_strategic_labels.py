#!/usr/bin/env python3
"""
Create Mock Strategic Labels Data for Comprehensive Forecasting Testing
Generates realistic competitive scenarios with known ground truth for validation
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime, timedelta
import json

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def create_mock_strategic_scenarios():
    """Create mock strategic scenarios with known ground truth patterns"""
    
    # Get existing ads as base data
    existing_ads_query = f"""
    SELECT 
      ad_id,
      brand,
      creative_text,
      title,
      media_type,
      start_timestamp,
      active_days,
      publisher_platforms
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    WHERE start_timestamp IS NOT NULL
      AND brand IS NOT NULL
    ORDER BY brand, start_timestamp
    LIMIT 300  -- Working set for mock scenarios
    """
    
    print("📊 Creating Mock Strategic Labels for Forecasting Testing")
    print("=" * 60)
    print("Scenarios: Nike promotional surge, Under Armour premium pivot,")
    print("           Adidas urgency campaign, cross-brand influence cascade")
    print("-" * 60)
    
    try:
        existing_df = client.query(existing_ads_query).result().to_dataframe()
        
        if len(existing_df) == 0:
            print("❌ No existing ads data found")
            return False
            
        print(f"📈 Base data: {len(existing_df)} ads across {existing_df['brand'].nunique()} brands")
        
        # Create strategic scenarios
        mock_strategic_data = []
        
        for _, row in existing_df.iterrows():
            ad_start = pd.to_datetime(row['start_timestamp']).tz_localize(None) if pd.to_datetime(row['start_timestamp']).tz is not None else pd.to_datetime(row['start_timestamp'])
            brand = row['brand']
            
            # Calculate week from start date for scenario timing
            week_offset = (ad_start - pd.to_datetime('2024-10-01')).days // 7
            
            # SCENARIO 1: Nike Black Friday Promotional Surge (4-week buildup)
            if brand == 'Nike' or (brand not in ['Nike'] and 'nike' in str(row['creative_text']).lower()):
                brand = 'Nike'  # Normalize
                if week_offset <= 2:  # Early October
                    promotional_intensity = 0.25 + np.random.normal(0, 0.05)
                    urgency_score = 0.15 + np.random.normal(0, 0.05)
                    brand_voice_score = 0.75 + np.random.normal(0, 0.05)
                    primary_angle = 'FEATURE_FOCUSED'
                    angles = ['FEATURE_FOCUSED', 'ASPIRATIONAL']
                elif week_offset <= 4:  # Mid October
                    promotional_intensity = 0.35 + np.random.normal(0, 0.05)
                    urgency_score = 0.25 + np.random.normal(0, 0.05) 
                    brand_voice_score = 0.70 + np.random.normal(0, 0.05)
                    primary_angle = 'PROMOTIONAL' if np.random.random() > 0.3 else 'FEATURE_FOCUSED'
                    angles = ['PROMOTIONAL', 'FEATURE_FOCUSED'] if primary_angle == 'PROMOTIONAL' else ['FEATURE_FOCUSED', 'ASPIRATIONAL']
                elif week_offset <= 6:  # Late October
                    promotional_intensity = 0.55 + np.random.normal(0, 0.05)
                    urgency_score = 0.45 + np.random.normal(0, 0.05)
                    brand_voice_score = 0.60 + np.random.normal(0, 0.05)
                    primary_angle = 'PROMOTIONAL'
                    angles = ['PROMOTIONAL', 'URGENCY', 'SCARCITY']
                else:  # November - Black Friday
                    promotional_intensity = 0.80 + np.random.normal(0, 0.08)
                    urgency_score = 0.85 + np.random.normal(0, 0.05)
                    brand_voice_score = 0.45 + np.random.normal(0, 0.05)
                    primary_angle = 'URGENCY'
                    angles = ['URGENCY', 'PROMOTIONAL', 'SCARCITY']
            
            # SCENARIO 2: Under Armour Premium → Mass Market Pivot (6-week transition)
            elif brand == 'Under Armour':
                if week_offset <= 1:  # Early premium positioning
                    promotional_intensity = 0.15 + np.random.normal(0, 0.03)
                    urgency_score = 0.10 + np.random.normal(0, 0.03)
                    brand_voice_score = 0.85 + np.random.normal(0, 0.05)
                    primary_angle = 'ASPIRATIONAL'
                    angles = ['ASPIRATIONAL', 'FEATURE_FOCUSED', 'TRUST']
                elif week_offset <= 3:  # Transition begins
                    promotional_intensity = 0.25 + np.random.normal(0, 0.05)
                    urgency_score = 0.15 + np.random.normal(0, 0.05)
                    brand_voice_score = 0.70 + np.random.normal(0, 0.05)
                    primary_angle = 'FEATURE_FOCUSED'
                    angles = ['FEATURE_FOCUSED', 'BENEFIT_FOCUSED', 'ASPIRATIONAL']
                elif week_offset <= 5:  # Mid-transition
                    promotional_intensity = 0.45 + np.random.normal(0, 0.05)
                    urgency_score = 0.30 + np.random.normal(0, 0.05)
                    brand_voice_score = 0.55 + np.random.normal(0, 0.05)
                    primary_angle = 'BENEFIT_FOCUSED'
                    angles = ['BENEFIT_FOCUSED', 'PROMOTIONAL', 'RATIONAL']
                else:  # Full mass market pivot
                    promotional_intensity = 0.65 + np.random.normal(0, 0.08)
                    urgency_score = 0.45 + np.random.normal(0, 0.08)
                    brand_voice_score = 0.35 + np.random.normal(0, 0.08)
                    primary_angle = 'PROMOTIONAL'
                    angles = ['PROMOTIONAL', 'RATIONAL', 'BENEFIT_FOCUSED']
            
            # SCENARIO 3: Adidas Counter-Strategy (Premium Defense + Seasonal Urgency)
            elif brand == 'adidas Originals' or brand == 'PUMA':  # Use PUMA as Adidas
                brand = 'Adidas'  # Normalize
                if week_offset <= 2:  # Initial premium positioning
                    promotional_intensity = 0.20 + np.random.normal(0, 0.03)
                    urgency_score = 0.12 + np.random.normal(0, 0.03)
                    brand_voice_score = 0.80 + np.random.normal(0, 0.05)
                    primary_angle = 'ASPIRATIONAL'
                    angles = ['ASPIRATIONAL', 'EMOTIONAL', 'TRUST']
                elif week_offset <= 4:  # Response to Nike buildup
                    promotional_intensity = 0.18 + np.random.normal(0, 0.03)  # Maintain premium
                    urgency_score = 0.20 + np.random.normal(0, 0.05)
                    brand_voice_score = 0.85 + np.random.normal(0, 0.03)  # Strengthen premium
                    primary_angle = 'EMOTIONAL'
                    angles = ['EMOTIONAL', 'ASPIRATIONAL', 'SOCIAL_PROOF']
                elif week_offset <= 6:  # Seasonal urgency while maintaining premium
                    promotional_intensity = 0.35 + np.random.normal(0, 0.05)
                    urgency_score = 0.65 + np.random.normal(0, 0.08)
                    brand_voice_score = 0.75 + np.random.normal(0, 0.05)
                    primary_angle = 'URGENCY'
                    angles = ['URGENCY', 'EMOTIONAL', 'SCARCITY']
                else:  # Holiday premium urgency strategy
                    promotional_intensity = 0.45 + np.random.normal(0, 0.08)
                    urgency_score = 0.75 + np.random.normal(0, 0.08)
                    brand_voice_score = 0.70 + np.random.normal(0, 0.08)
                    primary_angle = 'URGENCY' if np.random.random() > 0.4 else 'EMOTIONAL'
                    angles = ['URGENCY', 'EMOTIONAL', 'ASPIRATIONAL']
            
            else:  # Default scenario for any other brands
                promotional_intensity = 0.30 + np.random.normal(0, 0.15)
                urgency_score = 0.25 + np.random.normal(0, 0.15)
                brand_voice_score = 0.60 + np.random.normal(0, 0.20)
                primary_angle = np.random.choice(['PROMOTIONAL', 'EMOTIONAL', 'RATIONAL', 'FEATURE_FOCUSED'])
                angles = [primary_angle, np.random.choice(['TRUST', 'BENEFIT_FOCUSED', 'ASPIRATIONAL'])]
            
            # Ensure values are within valid ranges
            promotional_intensity = max(0.0, min(1.0, promotional_intensity))
            urgency_score = max(0.0, min(1.0, urgency_score))
            brand_voice_score = max(0.0, min(1.0, brand_voice_score))
            
            # Determine funnel and persona based on strategic positioning
            if brand_voice_score >= 0.70:  # Premium positioning
                funnel = np.random.choice(['Upper', 'Mid'], p=[0.7, 0.3])
                persona = np.random.choice(['New Customer', 'General Market'], p=[0.6, 0.4])
            elif promotional_intensity >= 0.60:  # Promotional focus
                funnel = np.random.choice(['Lower', 'Mid'], p=[0.8, 0.2])
                persona = np.random.choice(['General Market', 'Existing Customer'], p=[0.7, 0.3])
            else:  # Balanced approach
                funnel = np.random.choice(['Upper', 'Mid', 'Lower'], p=[0.3, 0.4, 0.3])
                persona = np.random.choice(['New Customer', 'Existing Customer', 'General Market'], p=[0.4, 0.3, 0.3])
            
            # Generate relevant topics based on scenario
            if promotional_intensity >= 0.60:
                topics = ['discount', 'sale', 'offer', 'savings']
            elif urgency_score >= 0.60:
                topics = ['limited time', 'hurry', 'deadline', 'exclusive']
            elif brand_voice_score >= 0.70:
                topics = ['premium', 'quality', 'innovation', 'performance']
            else:
                topics = ['product', 'benefits', 'features', 'value']
            
            # Add some relevant topics
            topics = topics[:2] + [np.random.choice(['comfort', 'style', 'technology', 'durability'])]
            
            mock_strategic_data.append({
                'ad_id': row['ad_id'],
                'brand': brand,
                'creative_text': row['creative_text'],
                'title': row['title'],
                'media_type': row['media_type'],
                'start_timestamp': row['start_timestamp'],
                'active_days': row['active_days'],
                'publisher_platforms': row['publisher_platforms'],
                
                # Strategic labels with ground truth scenarios
                'funnel': funnel,
                'angles': angles,
                'persona': persona,
                'topics': topics,
                'urgency_score': round(urgency_score, 3),
                'promotional_intensity': round(promotional_intensity, 3),
                'brand_voice_score': round(brand_voice_score, 3),
                'primary_angle': primary_angle,
                
                # Week classification for validation
                'week_start': ad_start.strftime('%Y-%m-%d'),
                'week_offset': week_offset,
                'scenario_phase': f"{brand}_week_{week_offset}",
                
                # Ground truth labels for testing
                'ground_truth_scenario': f"{brand}_{get_scenario_label(brand, week_offset)}"
            })
        
        # Convert to DataFrame for BigQuery upload
        mock_df = pd.DataFrame(mock_strategic_data)
        
        print(f"\n📋 Mock Strategic Scenarios Created:")
        print(f"   Total ads with strategic labels: {len(mock_df)}")
        print(f"   Brands: {mock_df['brand'].unique()}")
        print(f"   Week range: {mock_df['week_offset'].min()} to {mock_df['week_offset'].max()}")
        
        # Show scenario distribution
        for brand in mock_df['brand'].unique():
            brand_data = mock_df[mock_df['brand'] == brand]
            print(f"   {brand}: {len(brand_data)} ads, promotional_intensity: {brand_data['promotional_intensity'].min():.2f}-{brand_data['promotional_intensity'].max():.2f}")
        
        return upload_mock_strategic_data(mock_df)
        
    except Exception as e:
        print(f"❌ Error creating mock scenarios: {e}")
        return False

def get_scenario_label(brand, week_offset):
    """Get ground truth scenario label for validation"""
    if brand == 'Nike':
        if week_offset <= 2:
            return 'baseline_premium'
        elif week_offset <= 4:
            return 'promotional_buildup'
        elif week_offset <= 6:
            return 'pre_black_friday_surge'
        else:
            return 'black_friday_peak'
    elif brand == 'Under Armour':
        if week_offset <= 1:
            return 'premium_positioning'
        elif week_offset <= 3:
            return 'transition_begins'
        elif week_offset <= 5:
            return 'mid_transition'
        else:
            return 'mass_market_pivot'
    elif brand == 'Adidas':
        if week_offset <= 2:
            return 'premium_defense_baseline'
        elif week_offset <= 4:
            return 'competitive_response'
        elif week_offset <= 6:
            return 'seasonal_urgency'
        else:
            return 'premium_urgency_strategy'
    else:
        return 'baseline'

def upload_mock_strategic_data(mock_df):
    """Upload mock strategic data to BigQuery"""
    
    # Prepare data for BigQuery (convert lists to JSON strings for arrays)
    upload_df = mock_df.copy()
    upload_df['angles'] = upload_df['angles'].apply(json.dumps)
    upload_df['topics'] = upload_df['topics'].apply(json.dumps)
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock"
    
    # Define schema to match our comprehensive forecasting expectations
    schema = [
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("creative_text", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("media_type", "STRING"),
        bigquery.SchemaField("start_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("active_days", "INTEGER"),
        bigquery.SchemaField("publisher_platforms", "STRING"),
        bigquery.SchemaField("funnel", "STRING"),
        bigquery.SchemaField("angles", "STRING"),  # JSON array as string
        bigquery.SchemaField("persona", "STRING"),
        bigquery.SchemaField("topics", "STRING"),  # JSON array as string  
        bigquery.SchemaField("urgency_score", "FLOAT"),
        bigquery.SchemaField("promotional_intensity", "FLOAT"),
        bigquery.SchemaField("brand_voice_score", "FLOAT"),
        bigquery.SchemaField("primary_angle", "STRING"),
        bigquery.SchemaField("week_start", "STRING"),
        bigquery.SchemaField("week_offset", "INTEGER"),
        bigquery.SchemaField("scenario_phase", "STRING"),
        bigquery.SchemaField("ground_truth_scenario", "STRING"),
    ]
    
    print(f"\n🔄 Uploading mock strategic data to BigQuery...")
    
    try:
        # Create table with mock data
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
        )
        
        job = client.load_table_from_dataframe(upload_df, table_id, job_config=job_config)
        job.result()  # Wait for job to complete
        
        print(f"✅ Successfully uploaded {len(upload_df)} mock strategic labels")
        print(f"📊 Table: {table_id}")
        
        # Validate upload
        validation_query = f"""
        SELECT 
          brand,
          COUNT(*) AS ads,
          AVG(promotional_intensity) AS avg_promo_intensity,
          AVG(urgency_score) AS avg_urgency_score,
          AVG(brand_voice_score) AS avg_brand_voice,
          COUNT(DISTINCT ground_truth_scenario) AS scenario_phases
        FROM `{table_id}`
        GROUP BY brand
        ORDER BY brand
        """
        
        validation_df = client.query(validation_query).result().to_dataframe()
        
        print(f"\n📋 Mock Data Validation:")
        for _, row in validation_df.iterrows():
            print(f"   {row['brand']}: {row['ads']} ads, scenarios: {row['scenario_phases']}")
            print(f"     Promotional: {row['avg_promo_intensity']:.3f}, Urgency: {row['avg_urgency_score']:.3f}, Brand Voice: {row['avg_brand_voice']:.3f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error uploading mock data: {e}")
        return False

def create_mock_data_view():
    """Create a view that properly parses the mock data arrays"""
    
    view_query = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.v_ads_strategic_labels_mock` AS
    SELECT 
      ad_id,
      brand,
      creative_text,
      title,
      media_type,
      start_timestamp,
      active_days,
      publisher_platforms,
      funnel,
      
      -- Parse JSON arrays back to BigQuery arrays
      ARRAY(SELECT JSON_UNQUOTE(angle) FROM UNNEST(JSON_EXTRACT_ARRAY(angles)) AS angle) AS angles,
      persona,
      ARRAY(SELECT JSON_UNQUOTE(topic) FROM UNNEST(JSON_EXTRACT_ARRAY(topics)) AS topic) AS topics,
      
      urgency_score,
      promotional_intensity,
      brand_voice_score,
      primary_angle,
      
      -- Add week calculation for forecasting
      DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week_start,
      week_offset,
      scenario_phase,
      ground_truth_scenario,
      
      -- Analysis fields
      CURRENT_TIMESTAMP() AS analysis_timestamp
      
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
    """
    
    print(f"\n🔧 Creating mock data view with proper array parsing...")
    
    try:
        client.query(view_query).result()
        print(f"✅ Created view: v_ads_strategic_labels_mock")
        return True
    except Exception as e:
        print(f"❌ Error creating view: {e}")
        return False

if __name__ == "__main__":
    print("🎭 MOCK STRATEGIC LABELS CREATION")
    print("=" * 50)
    print("Creating realistic competitive scenarios:")
    print("1. Nike: Promotional intensity surge (0.25 → 0.80)")
    print("2. Under Armour: Premium → Mass pivot (brand_voice 0.85 → 0.35)")
    print("3. Adidas: Premium defense + seasonal urgency")
    print("4. Cross-brand influence cascade patterns")
    print("=" * 50)
    
    # Create mock strategic scenarios
    mock_success = create_mock_strategic_scenarios()
    
    if mock_success:
        # Create view for easier querying
        view_success = create_mock_data_view()
        
        if view_success:
            print("\n" + "=" * 50)
            print("✅ MOCK STRATEGIC LABELS CREATED SUCCESSFULLY")
            print("🎯 Ready for comprehensive forecasting testing")
            print("📊 Data includes: All strategic metrics with ground truth scenarios")
            print("🚀 Next step: Run comprehensive forecasting validation")
            print("=" * 50)
        else:
            print("\n⚠️  Mock data created but view creation failed")
    else:
        print("\n❌ Failed to create mock strategic labels")