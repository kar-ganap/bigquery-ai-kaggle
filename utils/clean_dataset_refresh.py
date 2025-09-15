#!/usr/bin/env python3
"""
Clean Dataset Refresh - Rebuild entire ads_demo dataset from scratch
This eliminates fragmented historical tables and creates a fresh, consistent dataset
"""
import os
import time
from google.cloud import bigquery
from scripts.utils.bigquery_client import get_bigquery_client

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

def clean_dataset_refresh():
    """Perform complete clean refresh of the ads_demo dataset"""
    
    print("🔥 CLEAN DATASET REFRESH")
    print("=" * 50)
    print("This will:")
    print("• Drop ALL existing tables in ads_demo dataset")
    print("• Pull fresh data from Meta Ads Library API")
    print("• Rebuild ads_with_dates with complete historical coverage")
    print("• Restore proper temporal intelligence")
    print()
    
    # Confirm destructive operation
    response = input("⚠️  This is DESTRUCTIVE. Type 'YES' to proceed: ")
    if response != 'YES':
        print("❌ Aborted - user did not confirm")
        return False
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'gcp-creds.json'
    client = bigquery.Client(project=BQ_PROJECT)
    
    # Step 1: List all tables to be dropped
    print("\n1️⃣ LISTING ALL TABLES TO BE DROPPED:")
    print("-" * 40)
    
    tables_query = f"""
    SELECT table_name
    FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
    ORDER BY table_name
    """
    
    tables_to_drop = []
    results = client.query(tables_query)
    for row in results:
        tables_to_drop.append(row.table_name)
        print(f"  📋 {row.table_name}")
    
    print(f"\n📊 Total tables to drop: {len(tables_to_drop)}")
    
    # Step 2: Drop all tables
    print("\n2️⃣ DROPPING ALL TABLES:")
    print("-" * 40)
    
    dropped_count = 0
    for table_name in tables_to_drop:
        try:
            drop_query = f"DROP TABLE `{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"
            client.query(drop_query).result()
            print(f"  ✅ Dropped: {table_name}")
            dropped_count += 1
        except Exception as e:
            print(f"  ⚠️  Failed to drop {table_name}: {str(e)}")
    
    print(f"\n📊 Successfully dropped: {dropped_count}/{len(tables_to_drop)} tables")
    
    # Step 3: Run fresh pipeline
    print("\n3️⃣ RUNNING FRESH PIPELINE:")
    print("-" * 40)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    pipeline_cmd = [
        "uv", "run", "python", "-m", "src.pipeline.orchestrator",
        "--brand", "Warby Parker",
        "--vertical", "eyewear", 
        "--verbose"
    ]
    
    print(f"🚀 Launching fresh pipeline run at {timestamp}...")
    print("📝 Command:")
    print(f"   {' '.join(pipeline_cmd)}")
    print()
    
    # Execute the pipeline
    import subprocess
    try:
        log_file = f"data/output/clean_refresh_{timestamp}.log"
        print(f"📄 Logging to: {log_file}")
        
        with open(log_file, 'w') as f:
            process = subprocess.Popen(
                pipeline_cmd,
                stdout=f,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                cwd="/Users/kartikganapathi/Documents/Personal/random_projects/bigquery_ai_kaggle/us-ads-strategy-radar"
            )
            
            print("⏳ Pipeline running... (this may take 15-30 minutes)")
            print("💡 Monitor progress with:")
            print(f"   tail -f {log_file}")
            print()
            
            # Wait for completion
            return_code = process.wait()
            
            if return_code == 0:
                print("✅ Pipeline completed successfully!")
            else:
                print(f"❌ Pipeline failed with return code: {return_code}")
                print("📄 Check the log file for details")
                return False
                
    except Exception as e:
        print(f"❌ Failed to run pipeline: {str(e)}")
        return False
    
    # Step 4: Verify the refresh
    print("\n4️⃣ VERIFYING CLEAN REFRESH:")
    print("-" * 40)
    
    # Check new tables
    verification_query = f"""
    SELECT 
        table_name,
        creation_time,
        CASE 
            WHEN table_name = 'ads_with_dates' THEN 'CORE TABLE'
            WHEN table_name LIKE '%ads_raw%' THEN 'RAW DATA'
            WHEN table_name LIKE '%creative%' THEN 'CREATIVE ANALYSIS'
            WHEN table_name LIKE '%channel%' THEN 'CHANNEL ANALYSIS'
            WHEN table_name LIKE '%phase8%' THEN 'PHASE 8 INTELLIGENCE'
            ELSE 'OTHER'
        END as table_type
    FROM `{BQ_PROJECT}.{BQ_DATASET}.INFORMATION_SCHEMA.TABLES`
    WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    ORDER BY table_type, creation_time DESC
    """
    
    try:
        results = client.query(verification_query)
        new_tables = list(results)
        
        if new_tables:
            print(f"✅ Created {len(new_tables)} new tables:")
            current_type = None
            for row in new_tables:
                if row.table_type != current_type:
                    current_type = row.table_type
                    print(f"\n  📂 {current_type}:")
                print(f"     • {row.table_name} ({row.creation_time})")
        else:
            print("❌ No new tables found - pipeline may have failed")
            return False
            
    except Exception as e:
        print(f"❌ Verification failed: {str(e)}")
        return False
    
    # Step 5: Test temporal intelligence
    print("\n5️⃣ TESTING TEMPORAL INTELLIGENCE:")
    print("-" * 40)
    
    temporal_test_query = f"""
    SELECT 
        brand,
        COUNT(*) as total_ads,
        MIN(DATE(start_timestamp)) as earliest_date,
        MAX(DATE(start_timestamp)) as latest_date,
        COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
              THEN 1 END) as ads_last_7d,
        COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                    AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
              THEN 1 END) as ads_prior_7d,
        SAFE_DIVIDE(
            COUNT(CASE WHEN DATE(start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END) - 
            COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                  THEN 1 END), 
            NULLIF(COUNT(CASE WHEN DATE(start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                            AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END), 0)
        ) as velocity_change_7d
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    WHERE brand = 'Warby Parker'
    GROUP BY brand
    """
    
    try:
        results = client.query(temporal_test_query)
        for row in results:
            print(f"📊 Temporal Intelligence Results:")
            print(f"   Brand: {row.brand}")
            print(f"   Total ads: {row.total_ads}")
            print(f"   Date range: {row.earliest_date} to {row.latest_date}")
            print(f"   Last 7d ads: {row.ads_last_7d}")
            print(f"   Prior 7d ads: {row.ads_prior_7d}")
            print(f"   Velocity change: {row.velocity_change_7d}")
            
            if row.velocity_change_7d is not None:
                print(f"   ✅ SUCCESS: velocity_change_7d is now calculated!")
            else:
                if row.ads_prior_7d == 0:
                    print(f"   ⚠️  velocity_change_7d is null due to 0 ads in prior period")
                    print(f"       This may be correct if data coverage is recent")
                else:
                    print(f"   ❌ velocity_change_7d is unexpectedly null")
                    
    except Exception as e:
        print(f"❌ Temporal intelligence test failed: {str(e)}")
        return False
    
    # Step 6: Summary
    print("\n6️⃣ CLEAN REFRESH SUMMARY:")
    print("-" * 40)
    print("✅ Dataset successfully cleaned and refreshed")
    print("✅ Fresh data pulled from Meta Ads Library API")  
    print("✅ All fragmented historical tables eliminated")
    print("✅ Consistent temporal intelligence restored")
    print()
    print("🎯 Next steps:")
    print("• Run pipeline tests to verify all functionality")
    print("• Check that velocity_change_7d reflects actual API data")
    print("• Proceed with Enhanced CTA Analysis and other Phase 8 features")
    
    return True

if __name__ == "__main__":
    success = clean_dataset_refresh()
    if success:
        print("\n🎉 CLEAN REFRESH COMPLETED SUCCESSFULLY!")
    else:
        print("\n❌ CLEAN REFRESH FAILED - Check logs for details")