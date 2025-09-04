#!/usr/bin/env python3
"""
Test script to see what the ScrapCreators Meta Ad Library API returns
"""

import os
import json
import requests
from pprint import pprint

SC_API_KEY = os.environ.get("SC_API_KEY")
BASE_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/company/ads"

def test_fetch_sample_ad():
    """Fetch a sample ad from a known brand like Nike"""
    
    if not SC_API_KEY:
        print("❌ SC_API_KEY not found in environment")
        return None
    
    # Test with Amazon (should have active ads)
    params = {
        "country": "US",
        "status": "ACTIVE",
        "companyName": "Amazon",  # API uses camelCase
        "limit": 2,  # Just get 2 ads for testing
        "trim": "false"
    }
    
    headers = {
        "x-api-key": SC_API_KEY
    }
    
    print("🔍 Fetching sample ads from Meta Ad Library API...")
    print(f"   Brand: Amazon")
    print(f"   URL: {BASE_URL}")
    print()
    
    try:
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check structure
            print("📊 API Response Structure:")
            print(f"   Total results: {data.get('total', 0)}")
            print(f"   Has cursor: {'cursor' in data}")
            print(f"   Results count: {len(data.get('results', []))}")
            print()
            
            # Show first ad structure
            if data.get('results'):
                first_ad = data['results'][0]
                
                print("🎯 Sample Ad Structure (first ad):")
                print(f"   Keys at root level: {list(first_ad.keys())}")
                print()
                
                print("📋 Key Fields:")
                print(f"   ad_archive_id: {first_ad.get('ad_archive_id')}")
                print(f"   page_name: {first_ad.get('page_name')}")
                print(f"   start_date: {first_ad.get('start_date_string', first_ad.get('start_date'))}")
                print(f"   end_date: {first_ad.get('end_date_string', first_ad.get('end_date'))}")
                print(f"   publisher_platform: {first_ad.get('publisher_platform')}")
                print(f"   url: {first_ad.get('url')[:80]}..." if first_ad.get('url') else None)
                print()
                
                # Check snapshot structure
                snapshot = first_ad.get('snapshot', {})
                if snapshot:
                    print("📸 Snapshot Structure:")
                    print(f"   Keys in snapshot: {list(snapshot.keys())[:10]}...")  # First 10 keys
                    print(f"   display_format: {snapshot.get('display_format')}")
                    print(f"   title: {snapshot.get('title')}")
                    print(f"   cta_type: {snapshot.get('cta_type')}")
                    print(f"   link_url: {snapshot.get('link_url')[:50]}..." if snapshot.get('link_url') else None)
                    
                    # Check body/creative text
                    body = snapshot.get('body', {})
                    if isinstance(body, dict):
                        print(f"   body.text: {body.get('text', '')[:100]}...")
                    elif isinstance(body, str):
                        print(f"   body: {body[:100]}...")
                    
                    # Check cards
                    cards = snapshot.get('cards', [])
                    if cards:
                        print(f"   Number of cards: {len(cards)}")
                        first_card = cards[0]
                        print(f"   First card keys: {list(first_card.keys())[:10]}...")
                        print(f"   First card body: {first_card.get('body', '')[:100]}...")
                        print(f"   First card title: {first_card.get('title')}")
                        print(f"   Has image: {'resized_image_url' in first_card or 'original_image_url' in first_card}")
                        print(f"   Has video: {'video_sd_url' in first_card or 'video_hd_url' in first_card}")
                    
                    # Check videos
                    videos = snapshot.get('videos', [])
                    if videos:
                        print(f"   Number of videos: {len(videos)}")
                
                # Save sample for reference
                with open('data/temp/sample_ad_response.json', 'w') as f:
                    json.dump(first_ad, f, indent=2)
                print()
                print("💾 Saved full sample ad to: data/temp/sample_ad_response.json")
                
                return data
            else:
                print("❌ No results returned")
                return None
                
        else:
            print(f"❌ API Error: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching ads: {e}")
        return None

if __name__ == "__main__":
    test_fetch_sample_ad()