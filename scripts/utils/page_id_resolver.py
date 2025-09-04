"""
Page ID resolution utilities for Meta Ad Library API
Converts company names to the most appropriate Facebook/Instagram page IDs
"""

import os
import json
import math
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available, use system environment variables

SC_API_KEY = os.environ.get("SC_API_KEY")
SEARCH_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/search/companies"

class PageIDResolver:
    """Resolves company names to Facebook page IDs with intelligent matching"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or SC_API_KEY
        if not self.api_key:
            raise ValueError("SC_API_KEY required for page ID resolution")
        
        self.cache = {}  # Simple in-memory cache
    
    def resolve_page_id(self, company_name: str, force_refresh: bool = False) -> Optional[Dict]:
        """
        Resolve a company name to the best matching page ID
        
        Args:
            company_name: The company name to search for
            force_refresh: Skip cache and fetch fresh data
            
        Returns:
            Dict with page_id, matched_name, likes, ig_followers, confidence_score
            or None if no good match found
        """
        
        cache_key = company_name.lower().strip()
        
        # Check cache first
        if not force_refresh and cache_key in self.cache:
            cached_result = self.cache[cache_key]
            if cached_result.get('cached_at') and \
               (datetime.now() - datetime.fromisoformat(cached_result['cached_at'])).days < 7:
                return cached_result['data']
        
        # Search for company
        search_results = self._search_company(company_name)
        if not search_results:
            result = None
        else:
            result = self._find_best_match(search_results, company_name)
            
            # Add confidence score
            if result:
                result['confidence_score'] = self._calculate_confidence(result, company_name)
        
        # Cache result
        self.cache[cache_key] = {
            'data': result,
            'cached_at': datetime.now().isoformat()
        }
        
        return result
    
    def resolve_multiple(self, company_names: List[str]) -> Dict[str, Optional[Dict]]:
        """
        Resolve multiple company names to page IDs
        
        Returns:
            Dict mapping company_name -> page_id_info (or None)
        """
        results = {}
        
        for company_name in company_names:
            try:
                result = self.resolve_page_id(company_name)
                results[company_name] = result
            except Exception as e:
                print(f"⚠️  Failed to resolve {company_name}: {e}")
                results[company_name] = None
                
        return results
    
    def _search_company(self, company_name: str) -> List[Dict]:
        """Search for company using Meta Ad Library API"""
        
        headers = {"x-api-key": self.api_key}
        params = {"query": company_name}
        
        try:
            response = requests.get(SEARCH_URL, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return data.get('searchResults', [])
            else:
                print(f"⚠️  API error for {company_name}: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"⚠️  Search error for {company_name}: {e}")
            return []
    
    def _find_best_match(self, results: List[Dict], query_name: str) -> Optional[Dict]:
        """
        Find the best matching page from search results
        
        Ranking criteria:
        1. Exact name match (case insensitive) - 1000 points
        2. Exact page_alias match (case insensitive) - 900 points  
        3. Name starts with query - 500 points
        4. Page alias starts with query - 400 points
        5. Query contained in name - 200 points
        6. Query contained in alias - 150 points
        7. Verified account bonus - 100 points
        8. Social proof (log scale) - up to 100 points
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
            
            # Exact match scores (highest priority)
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
            
            # Verification bonus (indicates official account)
            if is_verified:
                score += 100
            
            # Social proof as tiebreaker (log scale to handle huge follower counts)
            total_followers = likes + ig_followers
            if total_followers > 0:
                score += min(100, math.log10(total_followers + 1) * 10)
            
            scored_results.append({
                'result': result,
                'score': score
            })
        
        # Sort by score descending
        scored_results.sort(key=lambda x: x['score'], reverse=True)
        
        # Return highest scoring result if it has a reasonable score
        if scored_results and scored_results[0]['score'] >= 150:  # Minimum threshold
            return scored_results[0]['result']
        
        return None
    
    def _calculate_confidence(self, result: Dict, query_name: str) -> float:
        """
        Calculate confidence score (0.0 - 1.0) for the match
        
        Based on:
        - Name similarity
        - Verification status  
        - Social proof
        """
        
        query_lower = query_name.lower().strip()
        name = (result.get('name') or '').lower().strip()
        page_alias = (result.get('page_alias') or '').lower().strip()
        is_verified = result.get('verification') in ['VERIFIED', 'BLUE_VERIFIED']
        likes = result.get('likes', 0) or 0
        ig_followers = result.get('ig_followers', 0) or 0
        
        confidence = 0.0
        
        # Name similarity (0.0 - 0.6)
        if name == query_lower or page_alias == query_lower:
            confidence += 0.6
        elif name.startswith(query_lower) or page_alias.startswith(query_lower):
            confidence += 0.4
        elif query_lower in name or query_lower in page_alias:
            confidence += 0.2
        
        # Verification bonus (0.0 - 0.3)
        if is_verified:
            confidence += 0.3
        
        # Social proof bonus (0.0 - 0.1)
        total_followers = likes + ig_followers
        if total_followers > 100000:  # Significant following
            confidence += 0.1
        elif total_followers > 10000:
            confidence += 0.05
        
        return min(confidence, 1.0)
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        return {
            'cached_entries': len(self.cache),
            'cache_keys': list(self.cache.keys())
        }
    
    def clear_cache(self):
        """Clear the cache"""
        self.cache = {}


def resolve_page_id_simple(company_name: str) -> Optional[str]:
    """
    Simple function to get just the page ID for a company
    
    Returns:
        Page ID string or None if not found
    """
    resolver = PageIDResolver()
    result = resolver.resolve_page_id(company_name)
    return result['page_id'] if result else None


def resolve_competitors_to_page_ids(competitor_names: List[str]) -> Dict[str, Optional[str]]:
    """
    Convert list of competitor names to page IDs
    
    Args:
        competitor_names: List of company names
        
    Returns:
        Dict mapping company_name -> page_id (or None if not found)
    """
    resolver = PageIDResolver()
    results = resolver.resolve_multiple(competitor_names)
    
    # Extract just page IDs
    page_ids = {}
    for name, result in results.items():
        page_ids[name] = result['page_id'] if result else None
    
    return page_ids


if __name__ == "__main__":
    # Test the resolver
    resolver = PageIDResolver()
    
    test_companies = ["Nike", "Adidas", "Amazon", "Stripe", "Warby Parker"]
    
    print("🧪 Testing Page ID Resolver")
    print("=" * 50)
    
    for company in test_companies:
        result = resolver.resolve_page_id(company)
        
        if result:
            print(f"✅ {company:15} → {result['page_id']:15} ({result.get('confidence_score', 0):.2f} confidence)")
            print(f"   Matched: {result['name']} | Likes: {result.get('likes', 0):,}")
        else:
            print(f"❌ {company:15} → Not found")
        
        print()