#!/usr/bin/env python3
"""
Test the complete pipeline: Curated competitors → Page IDs → Ad fetching
This validates our enhanced approach for Subgoal 4
"""

import os
import sys
import json
from typing import List, Dict

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available, use system environment variables

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from bigquery_client import run_query
from ads_fetcher import MetaAdsFetcher

# Configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

def get_curated_competitors(limit: int = 5) -> List[str]:
    """Get curated competitor names from BigQuery"""
    
    query = f"""
    SELECT company_name, 
           MAX(quality_score) as max_quality_score,
           MAX(market_overlap_pct) as max_market_overlap
    FROM `{BQ_PROJECT}.{BQ_DATASET}.competitors_validated`
    WHERE confidence >= 0.7
      AND quality_score >= 0.6
      AND company_name NOT LIKE '%competitors%'
      AND company_name NOT LIKE '%insights%'
      AND company_name NOT LIKE '%management%'
    GROUP BY company_name
    ORDER BY max_quality_score DESC, max_market_overlap DESC
    LIMIT {limit}
    """
    
    print(f"🔍 Loading curated competitors from BigQuery...")
    df = run_query(query, BQ_PROJECT)
    
    competitors = df['company_name'].tolist()
    print(f"   ✅ Found {len(competitors)} curated competitors: {competitors}")
    
    return competitors

def test_end_to_end_pipeline():
    """Test the complete competitor ads pipeline"""
    
    print("🚀 TESTING END-TO-END COMPETITOR ADS PIPELINE")
    print("=" * 70)
    
    # Step 1: Get curated competitors
    competitors = get_curated_competitors(limit=4)  # Small test set
    
    if not competitors:
        print("❌ No curated competitors found")
        return
    
    # Step 2: Initialize ads fetcher
    fetcher = MetaAdsFetcher()
    
    # Step 3: Fetch ads for multiple competitors
    print(f"\n📱 Fetching ads for {len(competitors)} competitors...")
    
    results = fetcher.fetch_multiple_companies(
        companies=competitors,
        max_ads_per_company=10,  # Keep it small for testing
        max_total_requests=20,   # Global limit
        delay_between_requests=0.5
    )
    
    # Step 4: Analyze results
    print(f"\n📊 PIPELINE RESULTS")
    print("=" * 70)
    
    total_ads = 0
    successful_companies = 0
    ad_samples = []
    
    for company, (ads, result) in results.items():
        print(f"\n🏢 {company}:")
        
        if result.success:
            successful_companies += 1
            total_ads += len(ads)
            
            print(f"   ✅ Success: {len(ads)} ads, {result.pages_fetched} pages, {result.fetch_time:.1f}s")
            print(f"   📋 Page ID: {result.page_id}")
            
            # Collect sample creative content for embedding analysis
            for ad in ads[:2]:  # Top 2 ads per company
                snapshot = ad.get('snapshot', {})
                body_text = (snapshot.get('body', {}) or {}).get('text', '')
                title = snapshot.get('title') or ''
                
                # Combine title + body for embedding (this is what we'll send to ML.GENERATE_EMBEDDING)
                creative_text = f"{title} {body_text}".strip()
                
                if creative_text and len(creative_text) > 20:  # Ensure we have substantial content
                    ad_samples.append({
                        'company': company,
                        'ad_id': ad.get('ad_archive_id'),
                        'creative_text': creative_text,
                        'media_type': snapshot.get('display_format', 'UNKNOWN'),
                        'platforms': ad.get('publisher_platform', [])
                    })
        else:
            print(f"   ❌ Failed: {result.error}")
    
    # Step 5: Summary and next steps
    print(f"\n🎯 PIPELINE SUMMARY")
    print("=" * 70)
    
    print(f"Competitors processed: {len(competitors)}")
    print(f"Successful retrievals: {successful_companies}")
    print(f"Total ads collected: {total_ads}")
    print(f"API requests used: {fetcher.get_stats()['total_requests']}")
    
    success_rate = (successful_companies / len(competitors)) * 100 if competitors else 0
    print(f"Success rate: {success_rate:.1f}%")
    
    # Step 6: Show embedding-ready content
    if ad_samples:
        print(f"\n📝 SAMPLE CREATIVE CONTENT FOR EMBEDDINGS")
        print("=" * 70)
        print(f"Ready for ML.GENERATE_EMBEDDING: {len(ad_samples)} ads")
        
        for i, sample in enumerate(ad_samples[:5], 1):  # Show top 5
            print(f"\n{i}. {sample['company']} (Ad ID: {sample['ad_id']})")
            print(f"   Media: {sample['media_type']} | Platforms: {sample['platforms']}")
            print(f"   Text: {sample['creative_text'][:100]}...")
        
        # Save samples for Subgoal 4 development
        with open('data/temp/embedding_ready_ads.json', 'w') as f:
            json.dump(ad_samples, f, indent=2)
        print(f"\n💾 Saved {len(ad_samples)} embedding-ready ads to: data/temp/embedding_ready_ads.json")
    
    # Step 7: Readiness assessment for Subgoal 4
    print(f"\n✅ SUBGOAL 4 READINESS ASSESSMENT")
    print("=" * 70)
    
    if successful_companies >= 3 and total_ads >= 20:
        print("🎉 READY FOR SUBGOAL 4!")
        print("✓ Successfully resolving competitor page IDs")
        print("✓ Fetching real ad creative content") 
        print("✓ Pagination working for comprehensive collection")
        print("✓ Creative text ready for ML.GENERATE_EMBEDDING")
        print()
        print("Next Steps for Subgoal 4:")
        print("1. Enhance ingest_fb_ads.py with page ID resolution")
        print("2. Add ML.GENERATE_EMBEDDING to generate vectors from creative text")
        print("3. Create ads_embeddings table with vector storage")
        print("4. Test similarity search on embedded content")
    else:
        print("⚠️  Need more successful ad retrieval for Subgoal 4")
        print(f"   Current: {successful_companies} companies, {total_ads} ads")
        print(f"   Target: 3+ companies, 20+ ads")
    
    return {
        'competitors_processed': len(competitors),
        'successful_companies': successful_companies,
        'total_ads': total_ads,
        'ad_samples': len(ad_samples),
        'ready_for_subgoal4': successful_companies >= 3 and total_ads >= 20
    }

if __name__ == "__main__":
    test_end_to_end_pipeline()