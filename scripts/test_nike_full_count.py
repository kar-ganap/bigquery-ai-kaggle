#!/usr/bin/env python3
"""
Test Nike ad fetching to see if we get the full count from API
Compare against the 430 ads shown in Meta Ad Library GUI
"""

import os
import sys
import time

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from ads_fetcher import MetaAdsFetcher

def test_nike_full_pagination():
    """Fetch Nike ads with full pagination to compare against GUI count"""
    
    print("🔍 TESTING NIKE ADS - FULL PAGINATION")
    print("=" * 60)
    print("Expected from GUI: 440 ads")
    print("Let's see what the API returns...")
    print()
    
    fetcher = MetaAdsFetcher()
    
    # Fetch with high limits to get everything
    print("📱 Fetching Nike ads with unlimited pagination...")
    
    ads_collected = []
    result = None
    
    # Use the generator to track progress
    for ad in fetcher.fetch_company_ads_paginated(
        company_name="Nike",
        max_ads=1000,  # High limit to get everything
        max_pages=50,  # High page limit
        delay_between_requests=0.3  # Faster for testing
    ):
        if hasattr(ad, 'success'):  # This is the final AdsFetchResult
            result = ad
            break
        else:
            ads_collected.append(ad)
            
            # Progress update every 50 ads
            if len(ads_collected) % 50 == 0:
                print(f"   📊 Collected {len(ads_collected)} ads so far...")
    
    # Final results
    print(f"\n🎯 FINAL RESULTS")
    print("=" * 60)
    
    if result and result.success:
        print(f"✅ Success!")
        print(f"   Total ads collected: {len(ads_collected)}")
        print(f"   Pages fetched: {result.pages_fetched}")
        print(f"   Page ID used: {result.page_id}")
        print(f"   Fetch time: {result.fetch_time:.1f}s")
        print(f"   API requests: {fetcher.get_stats()['total_requests']}")
        
        # Compare with GUI
        gui_count = 440
        api_count = len(ads_collected)
        
        print(f"\n📊 COMPARISON WITH GUI")
        print("=" * 60)
        print(f"GUI shows: {gui_count} ads")
        print(f"API returned: {api_count} ads")
        
        if api_count >= gui_count:
            print(f"✅ API returned MORE ads than GUI ({api_count - gui_count} extra)")
        elif api_count >= gui_count * 0.9:  # Within 90%
            print(f"✅ API returned similar count ({gui_count - api_count} fewer)")
        else:
            print(f"⚠️  API returned significantly fewer ({gui_count - api_count} fewer)")
            print(f"   This might indicate pagination limits or filtering differences")
        
        # Analyze ad types
        media_types = {}
        platforms = {}
        
        for ad in ads_collected[:100]:  # Sample first 100
            snapshot = ad.get('snapshot', {})
            media_type = snapshot.get('display_format', 'UNKNOWN')
            platform_list = ad.get('publisher_platform', [])
            
            media_types[media_type] = media_types.get(media_type, 0) + 1
            for platform in platform_list:
                platforms[platform] = platforms.get(platform, 0) + 1
        
        print(f"\n📋 AD ANALYSIS (first 100 ads)")
        print("=" * 60)
        print(f"Media types: {media_types}")
        print(f"Platforms: {platforms}")
        
        # Show some creative samples
        print(f"\n📝 CREATIVE SAMPLES")
        print("=" * 60)
        
        for i, ad in enumerate(ads_collected[:5], 1):
            snapshot = ad.get('snapshot', {})
            body_text = (snapshot.get('body', {}) or {}).get('text', '')
            title = snapshot.get('title') or ''
            creative_text = f"{title} {body_text}".strip()
            
            print(f"{i}. Ad ID: {ad.get('ad_archive_id')}")
            print(f"   Media: {snapshot.get('display_format', 'UNKNOWN')}")
            print(f"   Text: {creative_text[:80]}...")
            print()
            
    else:
        print(f"❌ Failed: {result.error if result else 'Unknown error'}")
    
    # Rate limiting analysis
    stats = fetcher.get_stats()
    print(f"\n⚡ PERFORMANCE STATS")
    print("=" * 60)
    print(f"Total API requests: {stats['total_requests']}")
    print(f"Total time: {stats['uptime_seconds']:.1f}s")
    print(f"Avg request time: {stats['avg_request_time']:.2f}s")
    
    if len(ads_collected) > 0:
        ads_per_request = len(ads_collected) / max(stats['total_requests'], 1)
        print(f"Ads per request: {ads_per_request:.1f}")
    
    gui_count = 440
    return {
        'api_count': len(ads_collected),
        'gui_count': gui_count,
        'pages_fetched': result.pages_fetched if result else 0,
        'success': result.success if result else False
    }

if __name__ == "__main__":
    test_nike_full_pagination()