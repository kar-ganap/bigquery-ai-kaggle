#!/usr/bin/env python3
"""
Debug script to test start_timestamp vs start_date_string in temporal intelligence
"""
import os
from google.cloud import bigquery

def test_timestamp_vs_string_approaches():
    """Test both approaches to see which gives correct velocity_change_7d"""
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-creds.json'
    client = bigquery.Client(project="bigquery-ai-kaggle-469620")
    
    print("🔬 TESTING TIMESTAMP vs STRING APPROACHES")
    print("="*60)
    
    # Test 1: Current approach using start_timestamp
    print("\n1️⃣ CURRENT APPROACH (start_timestamp):")
    print("-" * 40)
    
    timestamp_query = """
    SELECT 
        r.brand,
        -- Using start_timestamp (current approach)
        COUNT(CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END) as ads_last_7d,
        COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END) as ads_prior_7d,
        SAFE_DIVIDE(
            COUNT(CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) - 
            COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                            AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END), 
            NULLIF(COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                          THEN 1 END), 0)
        ) as velocity_change_7d_timestamp
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r
    WHERE r.brand = 'Warby Parker'
    GROUP BY r.brand
    """
    
    results = client.query(timestamp_query)
    for row in results:
        print(f"  📊 Using start_timestamp:")
        print(f"     Ads last 7d: {row.ads_last_7d}")
        print(f"     Ads prior 7d: {row.ads_prior_7d}")
        print(f"     velocity_change_7d: {row.velocity_change_7d_timestamp}")
    
    # Test 2: Alternative approach using start_date_string with parsing
    print("\n2️⃣ ALTERNATIVE APPROACH (start_date_string with parsing):")
    print("-" * 40)
    
    string_query = """
    SELECT 
        r.brand,
        -- Using start_date_string with inline parsing
        COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', r.start_date_string)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END) as ads_last_7d,
        COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', r.start_date_string)) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END) as ads_prior_7d,
        SAFE_DIVIDE(
            COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', r.start_date_string)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) - 
            COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', r.start_date_string)) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                            AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END), 
            NULLIF(COUNT(CASE WHEN DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', r.start_date_string)) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                          THEN 1 END), 0)
        ) as velocity_change_7d_string
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r
    WHERE r.brand = 'Warby Parker'
    GROUP BY r.brand
    """
    
    results = client.query(string_query)
    for row in results:
        print(f"  📊 Using start_date_string:")
        print(f"     Ads last 7d: {row.ads_last_7d}")
        print(f"     Ads prior 7d: {row.ads_prior_7d}")
        print(f"     velocity_change_7d: {row.velocity_change_7d_string}")
    
    # Test 3: Verify data consistency between both approaches
    print("\n3️⃣ DATA CONSISTENCY CHECK:")
    print("-" * 40)
    
    consistency_query = """
    SELECT 
        brand,
        COUNT(*) as total_ads,
        COUNT(start_timestamp) as has_start_timestamp,
        COUNT(start_date_string) as has_start_date_string,
        COUNT(CASE WHEN start_timestamp IS NOT NULL AND start_date_string IS NOT NULL
                   AND DATE(start_timestamp) = DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))
                   THEN 1 END) as matching_dates,
        COUNT(CASE WHEN start_timestamp IS NOT NULL AND start_date_string IS NOT NULL
                   AND DATE(start_timestamp) != DATE(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))
                   THEN 1 END) as mismatched_dates
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY brand
    """
    
    results = client.query(consistency_query)
    for row in results:
        print(f"  🔍 Data consistency:")
        print(f"     Total ads: {row.total_ads}")
        print(f"     Has start_timestamp: {row.has_start_timestamp}")
        print(f"     Has start_date_string: {row.has_start_date_string}")
        print(f"     Matching dates: {row.matching_dates}")
        print(f"     Mismatched dates: {row.mismatched_dates}")
        
        if row.mismatched_dates > 0:
            print(f"  ⚠️  WARNING: {row.mismatched_dates} mismatched dates!")
        else:
            print(f"  ✅ All dates are consistent between timestamp and string")

if __name__ == "__main__":
    test_timestamp_vs_string_approaches()