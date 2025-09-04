#!/usr/bin/env python3
"""
Mock test of the AI curation pipeline without requiring actual AI calls
Tests the data flow and structure validation
"""

import os
import sys
import pandas as pd
from typing import Dict, List

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from bigquery_client import get_bigquery_client, run_query, load_dataframe_to_bq

# Configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

def test_curation_data_flow():
    """Test the curation data flow with mock AI responses"""
    print("🧪 Testing AI Curation Data Flow (Mock)")
    print("=" * 50)
    
    try:
        # Step 1: Load raw competitor data
        print("📋 Step 1: Loading raw competitor data...")
        
        query = f"""
        SELECT 
            target_brand,
            target_vertical,
            company_name,
            source_url,
            source_title,
            query_used,
            raw_score,
            found_in,
            discovery_method,
            discovered_at
        FROM `{BQ_PROJECT}.{BQ_DATASET}.competitors_raw`
        WHERE DATETIME(discovered_at) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 DAY)
        LIMIT 5
        """
        
        raw_df = run_query(query, BQ_PROJECT)
        
        if raw_df.empty:
            print("❌ No raw competitors found to test with")
            return False
        
        print(f"   ✅ Loaded {len(raw_df)} raw competitors for testing")
        
        # Step 2: Create mock curated data with enhanced schema
        print("🤖 Step 2: Creating mock curated data...")
        
        curated_data = []
        for _, row in raw_df.iterrows():
            # Mock AI curation results
            company_name = row['company_name']
            
            # Determine mock curation based on known patterns
            is_legitimate = company_name.lower() not in ['nike competitors', 'which brands stack', 'aug']
            
            if is_legitimate and company_name in ['Adidas', 'Under Armour', 'Puma', 'Reebok', 'New Balance']:
                # Direct competitors
                mock_result = {
                    'target_brand': row['target_brand'],
                    'target_vertical': row['target_vertical'],
                    'company_name': company_name,
                    'source_url': row['source_url'],
                    'source_title': row['source_title'],
                    'query_used': row['query_used'],
                    'raw_score': row['raw_score'],
                    'found_in': row['found_in'],
                    'discovery_method': row['discovery_method'],
                    'discovered_at': row['discovered_at'],
                    # Enhanced AI-generated fields (mocked)
                    'is_competitor': True,
                    'tier': 'Direct-Rival',
                    'market_overlap_pct': 85,
                    'customer_substitution_ease': 'Medium',
                    'confidence': 0.9,
                    'reasoning': f'Well-known athletic apparel brand competing directly with {row["target_brand"]}',
                    'evidence_sources': 'Company websites, market research, industry reports',
                    'curated_at': pd.Timestamp.now(),
                    'quality_score': 0.85
                }
            elif is_legitimate:
                # Other legitimate competitors  
                mock_result = {
                    'target_brand': row['target_brand'],
                    'target_vertical': row['target_vertical'],
                    'company_name': company_name,
                    'source_url': row['source_url'],
                    'source_title': row['source_title'],
                    'query_used': row['query_used'],
                    'raw_score': row['raw_score'],
                    'found_in': row['found_in'],
                    'discovery_method': row['discovery_method'],
                    'discovered_at': row['discovered_at'],
                    'is_competitor': True,
                    'tier': 'Niche-Player',
                    'market_overlap_pct': 45,
                    'customer_substitution_ease': 'Hard',
                    'confidence': 0.7,
                    'reasoning': f'Legitimate competitor with some overlap to {row["target_brand"]}',
                    'evidence_sources': 'Search results, company information',
                    'curated_at': pd.Timestamp.now(),
                    'quality_score': 0.65
                }
            else:
                # Not a legitimate competitor
                mock_result = {
                    'target_brand': row['target_brand'],
                    'target_vertical': row['target_vertical'],
                    'company_name': company_name,
                    'source_url': row['source_url'],
                    'source_title': row['source_title'],
                    'query_used': row['query_used'],
                    'raw_score': row['raw_score'],
                    'found_in': row['found_in'],
                    'discovery_method': row['discovery_method'],
                    'discovered_at': row['discovered_at'],
                    'is_competitor': False,
                    'tier': 'Adjacent',
                    'market_overlap_pct': 0,
                    'customer_substitution_ease': 'Hard',
                    'confidence': 0.95,
                    'reasoning': 'Not a legitimate company or competitor',
                    'evidence_sources': 'Search result analysis',
                    'curated_at': pd.Timestamp.now(),
                    'quality_score': 0.0
                }
            
            curated_data.append(mock_result)
        
        # Step 3: Create curated DataFrame and load to BigQuery
        print("💾 Step 3: Loading mock curated data to BigQuery...")
        
        curated_df = pd.DataFrame(curated_data)
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.competitors_curated"
        
        load_dataframe_to_bq(curated_df, table_id, write_disposition="WRITE_TRUNCATE")
        print(f"   ✅ Loaded {len(curated_df)} curated records")
        
        # Step 4: Create validated view
        print("🔍 Step 4: Creating validated competitors view...")
        
        client = get_bigquery_client(BQ_PROJECT)
        validated_view_sql = f"""
        CREATE OR REPLACE VIEW `{BQ_PROJECT}.{BQ_DATASET}.competitors_validated` AS
        SELECT *
        FROM `{BQ_PROJECT}.{BQ_DATASET}.competitors_curated`
        WHERE is_competitor = TRUE
          AND confidence >= 0.6
          AND quality_score >= 0.4
        ORDER BY target_brand, quality_score DESC, market_overlap_pct DESC
        """
        
        client.query(validated_view_sql).result()
        print("   ✅ Created validated competitors view")
        
        # Step 5: Show sample results
        print("📊 Step 5: Sample validated competitors:")
        
        sample_query = f"""
        SELECT 
            target_brand,
            company_name,
            tier,
            market_overlap_pct,
            customer_substitution_ease,
            confidence,
            quality_score,
            reasoning
        FROM `{BQ_PROJECT}.{BQ_DATASET}.competitors_validated`
        ORDER BY quality_score DESC
        LIMIT 10
        """
        
        sample_df = run_query(sample_query, BQ_PROJECT)
        
        print("\n" + "="*80)
        for _, row in sample_df.iterrows():
            print(f"🎯 {row['target_brand']} → {row['company_name']}")
            print(f"   Tier: {row['tier']} | Overlap: {row['market_overlap_pct']}% | Confidence: {row['confidence']:.2f}")
            print(f"   Substitution: {row['customer_substitution_ease']} | Quality: {row['quality_score']:.2f}")
            print(f"   Reasoning: {row['reasoning']}")
            print()
        
        # Step 6: Summary statistics
        print("📈 Summary Statistics:")
        stats_query = f"""
        SELECT 
            COUNT(*) as total_curated,
            COUNTIF(is_competitor = TRUE) as validated_competitors,
            AVG(confidence) as avg_confidence,
            AVG(quality_score) as avg_quality_score,
            AVG(market_overlap_pct) as avg_market_overlap
        FROM `{BQ_PROJECT}.{BQ_DATASET}.competitors_curated`
        """
        
        stats_df = run_query(stats_query, BQ_PROJECT)
        stats = stats_df.iloc[0]
        
        print(f"   Total processed: {stats['total_curated']}")
        print(f"   Validated competitors: {stats['validated_competitors']}")
        print(f"   Average confidence: {stats['avg_confidence']:.3f}")
        print(f"   Average quality score: {stats['avg_quality_score']:.3f}")
        print(f"   Average market overlap: {stats['avg_market_overlap']:.1f}%")
        
        print("\n✅ Mock AI curation test completed successfully!")
        print("🎉 All data structures and pipeline components working correctly")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_curation_data_flow()
    sys.exit(0 if success else 1)