#!/usr/bin/env python3
"""
Debug script to check what's in the strategic data table
"""
import os
from google.cloud import bigquery

PROJECT_ID = "bigquery-ai-kaggle-469620"
DATASET_ID = "ads_demo"

client = bigquery.Client(project=PROJECT_ID)

def debug_strategic_data():
    print("🔍 DEBUGGING FINAL STRATEGIC METRICS")
    print("=" * 60)
    
    # Check what we actually have in the strategic table
    print("\n1️⃣ CHECKING STRATEGIC TABLE CONTENT:")
    strategic_query = f"""
    SELECT 
        brand,
        COUNT(*) as total_records,
        AVG(promotional_intensity) as avg_promo,
        AVG(urgency_score) as avg_urgency,
        AVG(brand_voice_score) as avg_brand_voice,
        STDDEV(promotional_intensity) as promo_volatility
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY brand
    """
    
    try:
        result = client.query(strategic_query).result()
        for row in result:
            print(f"   Brand: {row.brand}")
            print(f"   Total records: {row.total_records}")
            print(f"   Avg promotional intensity: {row.avg_promo}")
            print(f"   Avg urgency score: {row.avg_urgency}")
            print(f"   Avg brand voice score: {row.avg_brand_voice}")
            print(f"   Promotional volatility: {row.promo_volatility}")
    except Exception as e:
        print(f"   ❌ Query failed: {e}")
    
    # Check the exact query used by Analysis stage
    print("\n2️⃣ SIMULATING ANALYSIS STAGE QUERY:")
    analysis_query = f"""
    SELECT 
        brand,
        AVG(promotional_intensity) as avg_promotional_intensity,
        AVG(urgency_score) as avg_urgency_score, 
        AVG(brand_voice_score) as avg_brand_voice_score,
        STDDEV(promotional_intensity) as promotional_volatility,
        CASE 
            WHEN AVG(promotional_intensity) > 0.6 THEN 'offensive'
            WHEN AVG(promotional_intensity) > 0.4 THEN 'balanced'
            ELSE 'defensive'
        END as market_position
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY brand
    """
    
    try:
        result = client.query(analysis_query).result()
        if result.total_rows == 0:
            print("   ❌ No results returned!")
        else:
            for row in result:
                print(f"   ✅ Analysis query results:")
                print(f"      promotional_intensity: {float(row.get('avg_promotional_intensity', 0))}")
                print(f"      urgency_score: {float(row.get('avg_urgency_score', 0))}")
                print(f"      brand_voice_score: {float(row.get('avg_brand_voice_score', 0))}")
                print(f"      market_position: {row.get('market_position', 'unknown')}")
                print(f"      promotional_volatility: {float(row.get('promotional_volatility', 0))}")
    except Exception as e:
        print(f"   ❌ Analysis query failed: {e}")
        
    # Check raw sample data
    print("\n3️⃣ SAMPLE RAW DATA:")
    sample_query = f"""
    SELECT 
        brand,
        ad_archive_id,
        promotional_intensity,
        urgency_score,
        brand_voice_score
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    WHERE brand = 'Warby Parker'
    LIMIT 5
    """
    
    try:
        result = client.query(sample_query).result()
        for row in result:
            print(f"   {row.brand} | PI: {row.promotional_intensity} | US: {row.urgency_score} | BVS: {row.brand_voice_score}")
    except Exception as e:
        print(f"   ❌ Sample query failed: {e}")

if __name__ == "__main__":
    debug_strategic_data()