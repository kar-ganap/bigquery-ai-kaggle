#!/usr/bin/env python3
"""
Comprehensive investigation into end_timestamp usage patterns across the system.
Understanding the full impact and ensuring proper temporal intelligence utilizes end dates.
"""
import os
from google.cloud import bigquery

def investigate_end_timestamp_architecture():
    """Comprehensive investigation of end_timestamp usage patterns"""
    
    client = bigquery.Client(project="bigquery-ai-kaggle-469620")
    
    print("🔍 COMPREHENSIVE END_TIMESTAMP ARCHITECTURE INVESTIGATION")
    print("="*70)
    
    # 1. Check end_timestamp schema and data quality
    print("\n1️⃣ CHECKING END_TIMESTAMP DATA QUALITY:")
    print("-" * 40)
    
    try:
        end_timestamp_quality_query = """
        SELECT 
            brand,
            COUNT(*) as total_ads,
            COUNT(start_timestamp) as has_start_timestamp,
            COUNT(end_timestamp) as has_end_timestamp,
            COUNT(start_date_string) as has_start_date_string,
            COUNT(end_date_string) as has_end_date_string,
            
            -- Active vs Ended ads
            COUNT(CASE WHEN end_timestamp IS NULL THEN 1 END) as active_ads,
            COUNT(CASE WHEN end_timestamp IS NOT NULL THEN 1 END) as ended_ads,
            
            -- Date ranges
            MIN(DATE(start_timestamp)) as earliest_start,
            MAX(DATE(start_timestamp)) as latest_start,
            MIN(DATE(end_timestamp)) as earliest_end,
            MAX(DATE(end_timestamp)) as latest_end,
            
            -- Duration analysis
            AVG(CASE WHEN end_timestamp IS NOT NULL 
                    THEN DATE_DIFF(DATE(end_timestamp), DATE(start_timestamp), DAY) + 1 
                    ELSE NULL END) as avg_duration_ended,
            MAX(CASE WHEN end_timestamp IS NOT NULL 
                    THEN DATE_DIFF(DATE(end_timestamp), DATE(start_timestamp), DAY) + 1 
                    ELSE NULL END) as max_duration_ended,
            
            -- Current duration for active ads
            AVG(CASE WHEN end_timestamp IS NULL 
                    THEN DATE_DIFF(CURRENT_DATE(), DATE(start_timestamp), DAY) + 1 
                    ELSE NULL END) as avg_duration_active
            
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
        WHERE brand = 'Warby Parker'
        GROUP BY brand
        """
        
        results = client.query(end_timestamp_quality_query)
        for row in results:
            print(f"  📊 Brand: {row.brand}")
            print(f"     Total ads: {row.total_ads}")
            print(f"     Has start_timestamp: {row.has_start_timestamp}")
            print(f"     Has end_timestamp: {row.has_end_timestamp}")
            print(f"     Has start_date_string: {row.has_start_date_string}")
            print(f"     Has end_date_string: {row.has_end_date_string}")
            print(f"     Active ads (no end): {row.active_ads}")
            print(f"     Ended ads (has end): {row.ended_ads}")
            print(f"     Start range: {row.earliest_start} to {row.latest_start}")
            print(f"     End range: {row.earliest_end} to {row.latest_end}")
            print(f"     Avg duration (ended): {row.avg_duration_ended:.1f} days" if row.avg_duration_ended else "     Avg duration (ended): N/A")
            print(f"     Max duration (ended): {row.max_duration_ended} days" if row.max_duration_ended else "     Max duration (ended): N/A")
            print(f"     Avg duration (active): {row.avg_duration_active:.1f} days" if row.avg_duration_active else "     Avg duration (active): N/A")
            
    except Exception as e:
        print(f"  ❌ End timestamp quality check failed: {e}")
    
    # 2. Test end_timestamp impact on temporal intelligence
    print("\n2️⃣ TESTING END_TIMESTAMP IMPACT ON TEMPORAL INTELLIGENCE:")
    print("-" * 40)
    
    try:
        # Compare velocity calculations using start_timestamp only vs considering end_timestamp
        temporal_comparison_query = """
        SELECT 
            r.brand,
            
            -- Current approach: Only using start_timestamp
            COUNT(CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_started_last_7d,
            COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_started_prior_7d,
                      
            -- Alternative approach: Consider ads that were ACTIVE during periods
            COUNT(CASE WHEN DATE(r.start_timestamp) <= CURRENT_DATE() - 1
                        AND (r.end_timestamp IS NULL OR DATE(r.end_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY))
                      THEN 1 END) as ads_active_last_7d,
            COUNT(CASE WHEN DATE(r.start_timestamp) <= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                        AND (r.end_timestamp IS NULL OR DATE(r.end_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY))
                      THEN 1 END) as ads_active_prior_7d,
                      
            -- Ads that ENDED in recent periods (churn analysis)
            COUNT(CASE WHEN r.end_timestamp IS NOT NULL 
                        AND DATE(r.end_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_ended_last_7d,
            COUNT(CASE WHEN r.end_timestamp IS NOT NULL 
                        AND DATE(r.end_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                                  AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_ended_prior_7d,
                      
            -- Currently active ads
            COUNT(CASE WHEN r.end_timestamp IS NULL THEN 1 END) as currently_active_ads
            
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates` r
        WHERE r.brand = 'Warby Parker'
        GROUP BY r.brand
        """
        
        temporal_results = client.query(temporal_comparison_query)
        for row in temporal_results:
            print(f"  🎯 TEMPORAL INTELLIGENCE COMPARISON:")
            print(f"     Brand: {row.brand}")
            print(f"")
            print(f"  📈 START-BASED METRICS (current approach):")
            print(f"     Ads started last 7d: {row.ads_started_last_7d}")
            print(f"     Ads started prior 7d: {row.ads_started_prior_7d}")
            print(f"")
            print(f"  🎪 ACTIVITY-BASED METRICS (considering end_timestamp):")
            print(f"     Ads active last 7d: {row.ads_active_last_7d}")
            print(f"     Ads active prior 7d: {row.ads_active_prior_7d}")
            print(f"")
            print(f"  📉 END-BASED METRICS (churn analysis):")
            print(f"     Ads ended last 7d: {row.ads_ended_last_7d}")
            print(f"     Ads ended prior 7d: {row.ads_ended_prior_7d}")
            print(f"")
            print(f"  📊 CURRENT STATE:")
            print(f"     Currently active ads: {row.currently_active_ads}")
            
            # Calculate different velocity interpretations
            if row.ads_started_prior_7d > 0:
                start_velocity = (row.ads_started_last_7d - row.ads_started_prior_7d) / row.ads_started_prior_7d
                print(f"     Start velocity change: {start_velocity:.3f}")
            else:
                print(f"     Start velocity change: NULL (no ads in prior period)")
                
            if row.ads_active_prior_7d > 0:
                active_velocity = (row.ads_active_last_7d - row.ads_active_prior_7d) / row.ads_active_prior_7d
                print(f"     Activity velocity change: {active_velocity:.3f}")
            else:
                print(f"     Activity velocity change: NULL (no active ads in prior period)")
                
    except Exception as e:
        print(f"  ❌ Temporal comparison failed: {e}")
    
    # 3. Investigate end_timestamp parsing failures
    print("\n3️⃣ INVESTIGATING END_TIMESTAMP PARSING:")
    print("-" * 40)
    
    try:
        parsing_investigation_query = """
        SELECT 
            brand,
            COUNT(*) as total_ads,
            
            -- String vs timestamp comparison
            COUNT(end_date_string) as has_end_date_string,
            COUNT(end_timestamp) as has_end_timestamp,
            COUNT(CASE WHEN end_date_string IS NOT NULL AND end_timestamp IS NULL 
                      THEN 1 END) as end_parse_failures,
                      
            -- Sample problematic end_date_string values
            ARRAY_AGG(
                CASE WHEN end_date_string IS NOT NULL AND end_timestamp IS NULL 
                     THEN end_date_string 
                     ELSE NULL 
                END 
                IGNORE NULLS 
                LIMIT 5
            ) as failed_end_date_samples,
            
            -- Check for unusual end_date patterns
            COUNT(CASE WHEN end_date_string LIKE '%null%' OR end_date_string = '' 
                      THEN 1 END) as suspicious_end_dates,
                      
            -- Duration consistency check
            COUNT(CASE WHEN end_timestamp IS NOT NULL 
                        AND DATE(end_timestamp) < DATE(start_timestamp) 
                      THEN 1 END) as invalid_durations
            
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
        WHERE brand = 'Warby Parker'
        GROUP BY brand
        """
        
        parsing_results = client.query(parsing_investigation_query)
        for row in parsing_results:
            print(f"  🔍 PARSING INVESTIGATION:")
            print(f"     Brand: {row.brand}")
            print(f"     Total ads: {row.total_ads}")
            print(f"     Has end_date_string: {row.has_end_date_string}")
            print(f"     Has end_timestamp: {row.has_end_timestamp}")
            print(f"     End parse failures: {row.end_parse_failures}")
            print(f"     Suspicious end dates: {row.suspicious_end_dates}")
            print(f"     Invalid durations: {row.invalid_durations}")
            
            if row.failed_end_date_samples and len(row.failed_end_date_samples) > 0:
                print(f"     Failed parsing samples: {row.failed_end_date_samples}")
                
            if row.end_parse_failures > 0:
                print(f"  ⚠️  WARNING: {row.end_parse_failures} end_timestamp parsing failures!")
            if row.invalid_durations > 0:
                print(f"  🚨 CRITICAL: {row.invalid_durations} ads with end before start!")
                
    except Exception as e:
        print(f"  ❌ Parsing investigation failed: {e}")
    
    # 4. Check temporal intelligence module usage of end_timestamp
    print("\n4️⃣ CHECKING TEMPORAL INTELLIGENCE MODULE END_TIMESTAMP USAGE:")
    print("-" * 40)
    
    # This would need to be done by reading the temporal intelligence module
    print("  📄 Need to examine temporal_intelligence_module.py for end_timestamp usage...")
    print("  🎯 Key questions:")
    print("     - Does it consider ad lifecycle (active period) vs just start date?")
    print("     - Should velocity track new starts or active campaigns?")
    print("     - Are we missing churn/end analysis opportunities?")
    
    return True

if __name__ == "__main__":
    # Set up environment
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-creds.json'
    investigate_end_timestamp_architecture()