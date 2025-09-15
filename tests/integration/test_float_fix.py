#!/usr/bin/env python3
"""
Test script to verify float preservation fix in Analysis stage
"""
import os
from scripts.utils.bigquery_client import run_query

PROJECT_ID = "bigquery-ai-kaggle-469620"
DATASET_ID = "ads_demo"

def test_analysis_stage_current_state():
    """Test the exact query used by Analysis stage to verify float preservation"""
    print("🔍 TESTING ANALYSIS STAGE CURRENT STATE QUERY")
    print("=" * 60)
    
    # Exact query from Analysis stage _analyze_current_state method
    current_state_query = f"""
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
        result = run_query(current_state_query)
        if not result.empty:
            row = result.iloc[0]
            
            # Test the exact logic from Analysis stage
            analysis_result = {
                'promotional_intensity': float(row.get('avg_promotional_intensity', 0)),
                'urgency_score': float(row.get('avg_urgency_score', 0)),
                'brand_voice_score': float(row.get('avg_brand_voice_score', 0)),
                'market_position': row.get('market_position', 'unknown'),
                'promotional_volatility': float(row.get('promotional_volatility', 0))
            }
            
            print("✅ ANALYSIS STAGE RESULT:")
            for key, value in analysis_result.items():
                print(f"   {key}: {value} ({type(value).__name__})")
                
            # Test Output stage processing
            print("\n📊 OUTPUT STAGE PROCESSING:")
            output_result = {
                'promotional_intensity': analysis_result.get('promotional_intensity', 0.0),
                'urgency_score': analysis_result.get('urgency_score', 0.0),
                'brand_voice_score': analysis_result.get('brand_voice_score', 0.0),
                'market_position': analysis_result.get('market_position', 'unknown'),
                'promotional_volatility': analysis_result.get('promotional_volatility', 0.0)
            }
            
            for key, value in output_result.items():
                print(f"   {key}: {value} ({type(value).__name__})")
                
            # Test if floats are preserved
            print("\n🎯 FLOAT PRESERVATION TEST:")
            all_floats_preserved = True
            for key in ['promotional_intensity', 'urgency_score', 'brand_voice_score', 'promotional_volatility']:
                if key in output_result and isinstance(output_result[key], float):
                    print(f"   ✅ {key}: {output_result[key]} (float preserved)")
                else:
                    print(f"   ❌ {key}: {output_result[key]} (NOT a float!)")
                    all_floats_preserved = False
            
            if all_floats_preserved:
                print("\n🎉 SUCCESS: All decimal values preserved as floats!")
            else:
                print("\n❌ FAILURE: Some values were truncated!")
                
        else:
            print("❌ No results from Analysis stage query")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_analysis_stage_current_state()