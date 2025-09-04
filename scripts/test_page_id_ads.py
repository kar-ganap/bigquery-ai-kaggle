#!/usr/bin/env python3
"""
Test ad retrieval using page IDs vs company names
Compare the results to validate page ID approach
"""

import os
import sys
import requests
from typing import Dict, List

# Add utils to path  
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from page_id_resolver import PageIDResolver

SC_API_KEY = os.environ.get("SC_API_KEY")
ADS_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/company/ads"

def fetch_ads_by_name(company_name: str, limit: int = 5) -> Dict:
    """Fetch ads using company name"""
    headers = {"x-api-key": SC_API_KEY}
    params = {
        "country": "US",
        "status": "ACTIVE",
        "companyName": company_name,
        "limit": limit,
        "trim": "false"
    }
    
    try:
        response = requests.get(ADS_URL, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return {
                'method': 'companyName',
                'query': company_name,
                'total': data.get('total', 0),
                'results_count': len(data.get('results', [])),
                'success': True,
                'results': data.get('results', [])[:3]  # First 3 for comparison
            }
        else:
            return {
                'method': 'companyName',
                'query': company_name,
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text[:100]}"
            }
    except Exception as e:
        return {
            'method': 'companyName', 
            'query': company_name,
            'success': False,
            'error': str(e)
        }

def fetch_ads_by_page_id(page_id: str, company_name: str, limit: int = 5) -> Dict:
    """Fetch ads using page ID"""
    headers = {"x-api-key": SC_API_KEY}
    params = {
        "country": "US", 
        "status": "ACTIVE",
        "pageId": page_id,
        "limit": limit,
        "trim": "false"
    }
    
    try:
        response = requests.get(ADS_URL, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return {
                'method': 'pageId',
                'query': f"{company_name} ({page_id})",
                'total': data.get('total', 0),
                'results_count': len(data.get('results', [])),
                'success': True,
                'results': data.get('results', [])[:3]  # First 3 for comparison
            }
        else:
            return {
                'method': 'pageId',
                'query': f"{company_name} ({page_id})",
                'success': False,
                'error': f"HTTP {response.status_code}: {response.text[:100]}"
            }
    except Exception as e:
        return {
            'method': 'pageId',
            'query': f"{company_name} ({page_id})",
            'success': False,
            'error': str(e)
        }

def compare_ad_retrieval():
    """Compare ad retrieval using names vs page IDs"""
    
    print("🔍 COMPARING AD RETRIEVAL: COMPANY NAMES VS PAGE IDs")
    print("=" * 80)
    
    # Test companies - mix of big brands and smaller ones
    test_companies = ["Nike", "Adidas", "Stripe", "Warby Parker"]
    
    resolver = PageIDResolver()
    comparison_results = []
    
    for company in test_companies:
        print(f"\n🎯 Testing: {company}")
        print("-" * 40)
        
        # Step 1: Resolve page ID
        page_data = resolver.resolve_page_id(company)
        
        if not page_data:
            print(f"   ❌ Could not resolve page ID for {company}")
            continue
            
        page_id = page_data['page_id']
        print(f"   📋 Page ID: {page_id} (confidence: {page_data.get('confidence_score', 0):.2f})")
        
        # Step 2: Fetch ads using company name
        print(f"   🔄 Fetching ads by company name...")
        name_results = fetch_ads_by_name(company, limit=10)
        
        # Step 3: Fetch ads using page ID  
        print(f"   🔄 Fetching ads by page ID...")
        page_id_results = fetch_ads_by_page_id(page_id, company, limit=10)
        
        # Step 4: Compare results
        print(f"   📊 Results Comparison:")
        
        if name_results['success']:
            print(f"      Company Name: {name_results['total']} total ads, {name_results['results_count']} returned")
        else:
            print(f"      Company Name: Failed - {name_results.get('error', 'Unknown error')}")
            
        if page_id_results['success']:
            print(f"      Page ID:      {page_id_results['total']} total ads, {page_id_results['results_count']} returned")
        else:
            print(f"      Page ID:      Failed - {page_id_results.get('error', 'Unknown error')}")
        
        # Determine winner
        name_total = name_results.get('total', 0) if name_results['success'] else 0
        page_total = page_id_results.get('total', 0) if page_id_results['success'] else 0
        
        if page_total > name_total:
            winner = "PAGE ID"
            advantage = page_total - name_total
            print(f"      🏆 Winner: {winner} (+{advantage} more ads)")
        elif name_total > page_total:
            winner = "COMPANY NAME" 
            advantage = name_total - page_total
            print(f"      🏆 Winner: {winner} (+{advantage} more ads)")
        else:
            winner = "TIE"
            print(f"      🤝 Result: {winner}")
        
        comparison_results.append({
            'company': company,
            'page_id': page_id,
            'name_total': name_total,
            'page_id_total': page_total,
            'winner': winner,
            'name_success': name_results['success'],
            'page_id_success': page_id_results['success']
        })
        
        # Show sample ad titles for successful queries
        if page_id_results['success'] and page_id_results['results']:
            print(f"      📝 Sample ads (via Page ID):")
            for i, ad in enumerate(page_id_results['results'][:2], 1):
                title = ad.get('snapshot', {}).get('title') or "No title"
                body_text = (ad.get('snapshot', {}).get('body', {}) or {}).get('text', '')
                text_preview = body_text[:50] + "..." if body_text else "No text"
                print(f"         {i}. {text_preview}")
    
    # Summary
    print(f"\n📊 SUMMARY")
    print("=" * 80)
    
    page_id_wins = sum(1 for r in comparison_results if r['winner'] == 'PAGE ID')
    name_wins = sum(1 for r in comparison_results if r['winner'] == 'COMPANY NAME')
    ties = sum(1 for r in comparison_results if r['winner'] == 'TIE')
    
    print(f"Page ID wins:      {page_id_wins}")
    print(f"Company Name wins: {name_wins}")
    print(f"Ties:              {ties}")
    
    total_name_ads = sum(r['name_total'] for r in comparison_results)
    total_page_ads = sum(r['page_id_total'] for r in comparison_results)
    
    print(f"\nTotal ads found:")
    print(f"Via company names: {total_name_ads}")
    print(f"Via page IDs:      {total_page_ads}")
    print(f"Page ID advantage: +{total_page_ads - total_name_ads} ads ({((total_page_ads/max(total_name_ads,1)-1)*100):+.0f}%)")
    
    # Recommendation
    if page_id_wins >= name_wins and total_page_ads >= total_name_ads:
        print(f"\n✅ RECOMMENDATION: Use Page ID approach")
        print(f"   ✓ More reliable ad discovery")
        print(f"   ✓ Better API success rate") 
        print(f"   ✓ Higher total ad counts")
    else:
        print(f"\n⚠️  RECOMMENDATION: Mixed results - may need hybrid approach")
    
    return comparison_results

if __name__ == "__main__":
    if not SC_API_KEY:
        print("❌ SC_API_KEY not found in environment")
        exit(1)
        
    compare_ad_retrieval()