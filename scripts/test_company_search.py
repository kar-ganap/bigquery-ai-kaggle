#!/usr/bin/env python3
"""
Test script to search for company page IDs and find the best match
"""

import os
import json
import requests
from typing import Dict, List, Optional

SC_API_KEY = os.environ.get("SC_API_KEY")
SEARCH_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/search/companies"

def search_company_page_id(company_name: str) -> Optional[Dict]:
    """
    Search for a company and return the best matching page ID
    """
    
    if not SC_API_KEY:
        print("❌ SC_API_KEY not found in environment")
        return None
    
    headers = {
        "x-api-key": SC_API_KEY
    }
    
    params = {
        "query": company_name
    }
    
    print(f"🔍 Searching for '{company_name}' page ID...")
    
    try:
        response = requests.get(SEARCH_URL, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check if we got results
            results = data.get('searchResults', [])
            print(f"   Found {len(results)} potential matches")
            
            if not results:
                print("   ❌ No matches found")
                return None
            
            # Show all results for debugging
            print("\n📋 All Results:")
            for i, result in enumerate(results[:5], 1):  # Show top 5
                print(f"\n   Result #{i}:")
                print(f"     page_id: {result.get('page_id')}")
                print(f"     name: {result.get('name')}")
                print(f"     page_alias: {result.get('page_alias')}")
                print(f"     likes: {result.get('likes', 0):,}")
                ig_followers = result.get('ig_followers') or 0
                print(f"     ig_followers: {ig_followers:,}")
                print(f"     verification: {result.get('verification')}")
                print(f"     categories: {result.get('categories', [])}")
            
            # Find best match
            best_match = find_best_match(results, company_name)
            
            if best_match:
                print(f"\n✅ Best Match Selected:")
                print(f"   Page ID: {best_match['page_id']}")
                print(f"   Name: {best_match['name']}")
                print(f"   Likes: {best_match.get('likes', 0):,}")
                print(f"   IG Followers: {best_match.get('ig_followers', 0):,}")
                
                return best_match
            else:
                print("   ❌ Could not determine best match")
                return None
                
        else:
            print(f"❌ API Error: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error searching: {e}")
        return None

def find_best_match(results: List[Dict], query_name: str) -> Optional[Dict]:
    """
    Find the best matching page from search results
    
    Ranking criteria:
    1. Exact name match (case insensitive)
    2. Exact page_alias match (case insensitive)
    3. Name starts with query
    4. Verified accounts get bonus
    5. Higher likes/followers as tiebreaker
    """
    
    if not results:
        return None
    
    query_lower = query_name.lower().strip()
    
    # Score each result
    scored_results = []
    for result in results:
        score = 0
        
        name = (result.get('name') or '').lower().strip()
        page_alias = (result.get('page_alias') or '').lower().strip()
        likes = result.get('likes', 0) or 0
        ig_followers = result.get('ig_followers', 0) or 0
        is_verified = result.get('verification') in ['VERIFIED', 'BLUE_VERIFIED']
        
        # Exact match scores
        if name == query_lower:
            score += 1000
        elif page_alias == query_lower:
            score += 900
        
        # Partial match scores
        elif name.startswith(query_lower):
            score += 500
        elif page_alias.startswith(query_lower):
            score += 400
        elif query_lower in name:
            score += 200
        elif query_lower in page_alias:
            score += 150
        
        # Verification bonus
        if is_verified:
            score += 100
        
        # Social proof as tiebreaker (normalized to 0-100 range)
        total_followers = likes + ig_followers
        if total_followers > 0:
            # Log scale to handle huge follower counts
            import math
            score += min(100, math.log10(total_followers + 1) * 10)
        
        scored_results.append({
            'result': result,
            'score': score,
            'debug': {
                'name_match': name == query_lower,
                'alias_match': page_alias == query_lower,
                'verified': is_verified,
                'total_followers': total_followers
            }
        })
    
    # Sort by score descending
    scored_results.sort(key=lambda x: x['score'], reverse=True)
    
    # Debug: show scoring
    print("\n🎯 Scoring Details (top 3):")
    for i, item in enumerate(scored_results[:3], 1):
        print(f"   #{i}: {item['result']['name']} (score: {item['score']:.1f})")
        print(f"       Debug: {item['debug']}")
    
    # Return the highest scoring result
    if scored_results and scored_results[0]['score'] > 0:
        return scored_results[0]['result']
    
    return None

def test_various_brands():
    """Test with various brand names to see how well it works"""
    
    test_brands = [
        "Nike",
        "Adidas", 
        "Amazon",
        "Meta",  # Might return Facebook
        "Google",
        "Apple",
        "Stripe",  # Smaller brand
        "Warby Parker",  # Two word brand
    ]
    
    print("="*60)
    print("🧪 TESTING MULTIPLE BRANDS")
    print("="*60)
    
    results = {}
    for brand in test_brands:
        print(f"\n{'='*60}")
        result = search_company_page_id(brand)
        
        if result:
            results[brand] = {
                'page_id': result['page_id'],
                'matched_name': result['name'],
                'likes': result.get('likes', 0),
                'ig_followers': result.get('ig_followers', 0)
            }
        else:
            results[brand] = None
        
        print()  # Space between brands
    
    # Summary
    print("\n" + "="*60)
    print("📊 SUMMARY")
    print("="*60)
    
    for brand, data in results.items():
        if data:
            print(f"✅ {brand:15} → Page ID: {data['page_id']:15} ({data['matched_name']})")
        else:
            print(f"❌ {brand:15} → Not found")
    
    # Save results for reference
    with open('data/temp/page_id_mapping.json', 'w') as f:
        json.dump(results, f, indent=2)
    print("\n💾 Saved mapping to: data/temp/page_id_mapping.json")

if __name__ == "__main__":
    test_various_brands()