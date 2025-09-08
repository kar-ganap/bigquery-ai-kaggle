#!/usr/bin/env python3
"""
MODERATE Test #2: CTA Aggressiveness Scoring Validation
Tests whether CTA scoring correlates with business intuition
"""

import os
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def validate_cta_aggressiveness():
    """Validate CTA aggressiveness scoring against business logic expectations"""
    
    query = f"""
    WITH cta_validation_sample AS (
      SELECT 
        brand,
        creative_text,
        title,
        
        -- Our predicted aggressiveness from manual review sample
        predicted_aggressiveness_score,
        
        -- Regenerate CTA features for validation
        CASE 
          WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
            r'(LIMITED TIME|HURRY|URGENT|NOW|TODAY ONLY|EXPIRES|DEADLINE|LAST CHANCE|FINAL|ENDING|WHILE SUPPLIES LAST|DON\\'T WAIT|ACT FAST|QUICK|IMMEDIATE)') 
          THEN 1.0 ELSE 0.0 
        END AS has_urgency_signals,
        
        CASE 
          WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
            r'(\d+%\s*(OFF|DISCOUNT)|SAVE \$\d+|FREE|SALE|DEAL|OFFER|SPECIAL|PROMOTION|DISCOUNT|COUPON)') 
          THEN 1.0 ELSE 0.0 
        END AS has_promotional_signals,
        
        CASE 
          WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
            r'(BUY NOW|SHOP NOW|GET NOW|CLAIM|GRAB|SECURE|RESERVE|ORDER|PURCHASE|CHECKOUT|ADD TO CART|DOWNLOAD NOW)') 
          THEN 1.0 ELSE 0.0 
        END AS has_action_pressure,
        
        CASE 
          WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
            r'(ONLY \d+ LEFT|FEW LEFT|ALMOST GONE|SELLING FAST|HIGH DEMAND|POPULAR|TRENDING|EXCLUSIVE|RARE|SCARCE)') 
          THEN 1.0 ELSE 0.0 
        END AS has_scarcity_signals,
        
        -- Calculate expected aggressiveness score
        (0.4 * CASE 
          WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
            r'(LIMITED TIME|HURRY|URGENT|NOW|TODAY ONLY|EXPIRES|DEADLINE|LAST CHANCE|FINAL|ENDING|WHILE SUPPLIES LAST|DON\\'T WAIT|ACT FAST|QUICK|IMMEDIATE)') 
          THEN 1.0 ELSE 0.0 
        END +
        0.25 * CASE 
          WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
            r'(\d+%\s*(OFF|DISCOUNT)|SAVE \$\d+|FREE|SALE|DEAL|OFFER|SPECIAL|PROMOTION|DISCOUNT|COUPON)') 
          THEN 1.0 ELSE 0.0 
        END +
        0.25 * CASE 
          WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
            r'(BUY NOW|SHOP NOW|GET NOW|CLAIM|GRAB|SECURE|RESERVE|ORDER|PURCHASE|CHECKOUT|ADD TO CART|DOWNLOAD NOW)') 
          THEN 1.0 ELSE 0.0 
        END +
        0.1 * CASE 
          WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
            r'(ONLY \d+ LEFT|FEW LEFT|ALMOST GONE|SELLING FAST|HIGH DEMAND|POPULAR|TRENDING|EXCLUSIVE|RARE|SCARCE)') 
          THEN 1.0 ELSE 0.0 
        END) AS calculated_aggressiveness_score,
        
        -- Business intuition categories for validation
        CASE 
          WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP NOW%' 
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%BUY NOW%'
          THEN 'Expected High Aggressiveness'
          WHEN UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%DISCOVER%' 
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%EXPLORE%'
               OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%REWRITE THE RULES%'
          THEN 'Expected Low Aggressiveness'
          ELSE 'Expected Medium Aggressiveness'
        END AS business_expectation
        
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
      WHERE creative_text IS NOT NULL
      ORDER BY RAND()
      LIMIT 50
    )
    SELECT 
      'CTA_AGGRESSIVENESS_VALIDATION' AS test_name,
      
      COUNT(*) AS total_ads_tested,
      
      -- Signal detection validation
      AVG(has_urgency_signals) AS pct_with_urgency_signals,
      AVG(has_promotional_signals) AS pct_with_promotional_signals,
      AVG(has_action_pressure) AS pct_with_action_pressure,
      AVG(has_scarcity_signals) AS pct_with_scarcity_signals,
      
      -- Score distribution validation
      AVG(calculated_aggressiveness_score) AS avg_calculated_score,
      MIN(calculated_aggressiveness_score) AS min_calculated_score,
      MAX(calculated_aggressiveness_score) AS max_calculated_score,
      
      -- Business expectation correlation
      AVG(CASE WHEN business_expectation = 'Expected High Aggressiveness' 
               THEN calculated_aggressiveness_score END) AS avg_score_high_expectation,
      AVG(CASE WHEN business_expectation = 'Expected Low Aggressiveness' 
               THEN calculated_aggressiveness_score END) AS avg_score_low_expectation,
      AVG(CASE WHEN business_expectation = 'Expected Medium Aggressiveness' 
               THEN calculated_aggressiveness_score END) AS avg_score_medium_expectation,
      
      -- Validation results
      CASE 
        WHEN AVG(CASE WHEN business_expectation = 'Expected High Aggressiveness' 
                      THEN calculated_aggressiveness_score END) > 
             AVG(CASE WHEN business_expectation = 'Expected Low Aggressiveness' 
                      THEN calculated_aggressiveness_score END)
        THEN 'PASS - High > Low as expected'
        ELSE 'FAIL - Scoring logic needs review'
      END AS correlation_test_result,
      
      -- Sample examples for manual validation
      STRING_AGG(
        CASE WHEN business_expectation = 'Expected High Aggressiveness' 
        THEN CONCAT(brand, ': "', SUBSTR(creative_text, 1, 50), '..." (Score: ', 
                   ROUND(calculated_aggressiveness_score, 2), ')') 
        END, ' | ' LIMIT 3
      ) AS high_aggressiveness_examples,
      
      STRING_AGG(
        CASE WHEN business_expectation = 'Expected Low Aggressiveness' 
        THEN CONCAT(brand, ': "', SUBSTR(creative_text, 1, 50), '..." (Score: ', 
                   ROUND(calculated_aggressiveness_score, 2), ')') 
        END, ' | ' LIMIT 3
      ) AS low_aggressiveness_examples
      
    FROM cta_validation_sample
    """
    
    print("🔍 MODERATE TEST #2: CTA Aggressiveness Scoring Validation")
    print("=" * 60)
    
    try:
        df = client.query(query).result().to_dataframe()
        
        if len(df) > 0:
            row = df.iloc[0]
            
            print(f"📊 Validation Results:")
            print(f"   Total ads tested: {row['total_ads_tested']}")
            print(f"   Avg calculated score: {row['avg_calculated_score']:.3f}")
            print(f"   Score range: {row['min_calculated_score']:.2f} - {row['max_calculated_score']:.2f}")
            
            print(f"\n🎯 Business Logic Correlation:")
            print(f"   High expectation avg score: {row['avg_score_high_expectation']:.3f}")
            print(f"   Medium expectation avg score: {row['avg_score_medium_expectation']:.3f}")  
            print(f"   Low expectation avg score: {row['avg_score_low_expectation']:.3f}")
            
            print(f"\n✅ Test Result: {row['correlation_test_result']}")
            
            print(f"\n📝 Sample Examples:")
            if row['high_aggressiveness_examples']:
                print(f"   High aggressiveness: {row['high_aggressiveness_examples']}")
            if row['low_aggressiveness_examples']:
                print(f"   Low aggressiveness: {row['low_aggressiveness_examples']}")
                
            return row['correlation_test_result'].startswith('PASS')
        else:
            print("❌ No validation data returned")
            return False
            
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        return False

def analyze_signal_distribution():
    """Analyze distribution of CTA signals to validate scoring components"""
    
    query = f"""
    SELECT 
      'CTA_SIGNAL_DISTRIBUTION' AS analysis_type,
      
      COUNTIF(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SHOP%NOW%' 
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%BUY%NOW%'
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%GET%NOW%') AS direct_action_ctas,
      
      COUNTIF(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%LIMITED%TIME%' 
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%HURRY%'
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%NOW%') AS urgency_signals,
      
      COUNTIF(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%SALE%' 
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%OFF%'
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%FREE%') AS promotional_signals,
      
      COUNTIF(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%DISCOVER%' 
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%EXPLORE%'
              OR UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) LIKE '%LEARN%') AS brand_awareness_ctas,
      
      COUNT(*) AS total_ads
      
    FROM `{PROJECT_ID}.{DATASET_ID}.ads_with_dates`
    WHERE creative_text IS NOT NULL
    """
    
    print("\n🔍 CTA Signal Distribution Analysis:")
    print("=" * 40)
    
    try:
        result = client.query(query).result()
        
        for row in result:
            print(f"📊 Signal Distribution:")
            print(f"   Total ads analyzed: {row.total_ads}")
            print(f"   Direct action CTAs: {row.direct_action_ctas} ({row.direct_action_ctas/row.total_ads*100:.1f}%)")
            print(f"   Urgency signals: {row.urgency_signals} ({row.urgency_signals/row.total_ads*100:.1f}%)")
            print(f"   Promotional signals: {row.promotional_signals} ({row.promotional_signals/row.total_ads*100:.1f}%)")
            print(f"   Brand awareness CTAs: {row.brand_awareness_ctas} ({row.brand_awareness_ctas/row.total_ads*100:.1f}%)")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    print("🧪 MODERATE TEST #2: CTA AGGRESSIVENESS SCORING VALIDATION")
    print("=" * 60)
    
    # Run validation
    validation_passed = validate_cta_aggressiveness()
    
    # Analyze signal distribution
    analyze_signal_distribution()
    
    if validation_passed:
        print("\n✅ MODERATE TEST #2 PASSED: CTA aggressiveness scoring correlates with business intuition")
        print("🎯 Goal: Aggressiveness scores align with expected business logic")
    else:
        print("\n❌ MODERATE TEST #2 NEEDS REVIEW: Check scoring algorithm")