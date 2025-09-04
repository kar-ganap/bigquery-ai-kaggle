#!/usr/bin/env python3
"""
Debug pagination logic to find the 22 missing Nike ads
"""

import os
import sys
import json

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from ads_fetcher import MetaAdsFetcher

def debug_nike_pagination():
    """Debug exactly what's happening with pagination"""
    
    print("🔍 DEBUG: Nike Pagination Analysis")
    print("=" * 60)
    
    fetcher = MetaAdsFetcher()
    
    # Get page ID first
    page_data = fetcher.page_resolver.resolve_page_id("Nike")
    if not page_data:
        print("❌ Could not resolve Nike page ID")
        return
        
    page_id = page_data['page_id']
    print(f"✅ Nike page ID: {page_id}")
    print()
    
    ads_collected = []
    page_details = []
    
    # Manual pagination with detailed logging
    for i, ad_or_result in enumerate(fetcher.fetch_company_ads_paginated(
        page_id=page_id,
        max_ads=500,  # High limit
        max_pages=25,  # High page limit 
        delay_between_requests=0.2  # Faster for debugging
    )):
        if hasattr(ad_or_result, 'success'):  # Final result
            result = ad_or_result
            break
        else:
            ads_collected.append(ad_or_result)
            
            # Track page boundaries (when we cross 26-30 ad batches)
            if len(ads_collected) % 26 == 0:
                current_page = len(ads_collected) // 26
                page_details.append({
                    'page': current_page,
                    'ads_so_far': len(ads_collected)
                })
                print(f"   📄 Completed page ~{current_page}: {len(ads_collected)} total ads")
    
    print(f"\n🎯 DETAILED RESULTS")
    print("=" * 60)
    
    if result.success:
        print(f"✅ Pagination completed successfully")
        print(f"   Total ads: {len(ads_collected)}")
        print(f"   Pages fetched: {result.pages_fetched}")
        print(f"   Fetch time: {result.fetch_time:.1f}s")
        print(f"   Average ads per page: {len(ads_collected)/result.pages_fetched:.1f}")
        
        # Check if we stopped due to limits
        print(f"\n🔍 STOPPING CONDITION ANALYSIS:")
        if len(ads_collected) >= 500:
            print(f"   ⚠️  Stopped due to max_ads limit (500)")
        elif result.pages_fetched >= 25:
            print(f"   ⚠️  Stopped due to max_pages limit (25)")
        else:
            print(f"   ✅ Stopped due to no more data (cursor was None/empty)")
            
        # Analyze last few pages for patterns
        print(f"\n📊 LAST FEW PAGES ANALYSIS:")
        if result.pages_fetched >= 3:
            # Estimate ads per page for last 3 pages
            total_ads = len(ads_collected)
            pages = result.pages_fetched
            
            # Rough calculation - this is approximate
            print(f"   Last few pages likely had ~{total_ads % 30} ads in final page")
            print(f"   Total pages: {pages}")
            
        # Check for any patterns in ad IDs (duplicates, gaps, etc.)
        ad_ids = [ad.get('ad_archive_id') for ad in ads_collected[:50]]  # Sample first 50
        unique_ids = set(ad_ids)
        print(f"\n🔍 DATA QUALITY CHECK (first 50 ads):")
        print(f"   Unique ad IDs: {len(unique_ids)}/{len(ad_ids)}")
        if len(unique_ids) < len(ad_ids):
            print(f"   ⚠️  Found duplicate ad IDs!")
            
        # Show the difference
        expected = 440
        actual = len(ads_collected)
        diff = expected - actual
        print(f"\n📈 COMPARISON:")
        print(f"   Expected (GUI): {expected}")
        print(f"   Retrieved (API): {actual}")
        print(f"   Missing: {diff} ads ({diff/expected*100:.1f}%)")
        
        if diff > 0:
            print(f"\n💡 POSSIBLE CAUSES:")
            print(f"   1. GUI includes inactive/pending ads we don't fetch")
            print(f"   2. GUI has different time range filter")
            print(f"   3. API has undocumented pagination limits")
            print(f"   4. Geographic targeting differences")
            print(f"   5. Race condition - ads changed between GUI view and API call")
            
    else:
        print(f"❌ Failed: {result.error}")

if __name__ == "__main__":
    debug_nike_pagination()