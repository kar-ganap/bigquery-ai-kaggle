"""
Enhanced Meta Ad Library API client with cursor pagination and page ID support
Integrates with page ID resolver for robust competitor ad collection
"""

import os
import time
import requests
from typing import Dict, List, Optional, Generator, Tuple
from dataclasses import dataclass
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available, use system environment variables

from page_id_resolver import PageIDResolver

SC_API_KEY = os.environ.get("SC_API_KEY")
ADS_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/company/ads"

@dataclass
class AdsFetchResult:
    """Result of ads fetching operation"""
    company_name: str
    page_id: Optional[str]
    total_ads_fetched: int
    pages_fetched: int
    success: bool
    error: Optional[str] = None
    fetch_time: float = 0.0

class MetaAdsFetcher:
    """Enhanced Meta Ad Library client with pagination and page ID resolution"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or SC_API_KEY
        if not self.api_key:
            raise ValueError("SC_API_KEY required for ads fetching")
        
        self.page_resolver = PageIDResolver(api_key)
        self.request_count = 0
        self.start_time = None
    
    def fetch_company_ads_paginated(self, 
                                   company_name: str = None, 
                                   page_id: str = None,
                                   country: str = "US",
                                   status: str = "ACTIVE", 
                                   max_ads: int = 100,
                                   max_pages: int = 10,
                                   delay_between_requests: float = 0.5) -> Generator[Dict, None, AdsFetchResult]:
        """
        Fetch ads with full pagination support
        
        Args:
            company_name: Company name (will resolve to page_id if page_id not provided)
            page_id: Specific page ID to use
            country: Country code for ads
            status: Ad status (ACTIVE, INACTIVE, ALL)
            max_ads: Maximum total ads to fetch
            max_pages: Maximum pages to fetch (safety limit)
            delay_between_requests: Delay between API calls (rate limiting)
            
        Yields:
            Individual ad dictionaries
            
        Returns:
            AdsFetchResult with summary stats
        """
        
        start_time = time.time()
        self.start_time = start_time
        
        # Resolve page ID if needed
        if not page_id and company_name:
            print(f"🔍 Resolving page ID for '{company_name}'...")
            page_data = self.page_resolver.resolve_page_id(company_name)
            
            if not page_data:
                return AdsFetchResult(
                    company_name=company_name,
                    page_id=None,
                    total_ads_fetched=0,
                    pages_fetched=0,
                    success=False,
                    error=f"Could not resolve page ID for {company_name}",
                    fetch_time=time.time() - start_time
                )
            
            page_id = page_data['page_id']
            print(f"   ✅ Resolved to page ID: {page_id} ({page_data['name']})")
        
        if not page_id:
            return AdsFetchResult(
                company_name=company_name or "Unknown",
                page_id=None,
                total_ads_fetched=0,
                pages_fetched=0,
                success=False,
                error="Either company_name or page_id must be provided",
                fetch_time=time.time() - start_time
            )
        
        # Fetch ads with pagination
        cursor = None
        total_ads = 0
        pages_fetched = 0
        
        print(f"📱 Fetching ads for page ID {page_id}...")
        
        while pages_fetched < max_pages and total_ads < max_ads:
            # Build request parameters
            params = {
                "country": country,
                "status": status,
                "pageId": page_id,
                "limit": min(50, max_ads - total_ads),  # API max is usually 50
                "trim": "false"
            }
            
            if cursor:
                params["cursor"] = cursor
            
            # Make request
            try:
                if pages_fetched > 0:  # Add delay between requests
                    time.sleep(delay_between_requests)
                
                self.request_count += 1
                response = requests.get(ADS_URL, params=params, headers={"x-api-key": self.api_key}, timeout=30)
                
                if response.status_code != 200:
                    error_msg = f"API error {response.status_code}: {response.text[:200]}"
                    print(f"   ❌ {error_msg}")
                    
                    return AdsFetchResult(
                        company_name=company_name or "Unknown",
                        page_id=page_id,
                        total_ads_fetched=total_ads,
                        pages_fetched=pages_fetched,
                        success=False,
                        error=error_msg,
                        fetch_time=time.time() - start_time
                    )
                
                data = response.json()
                results = data.get('results', [])
                cursor = data.get('cursor')
                
                pages_fetched += 1
                
                print(f"   📄 Page {pages_fetched}: {len(results)} ads")
                
                # Yield individual ads
                for ad in results:
                    if total_ads >= max_ads:
                        break
                    
                    yield ad
                    total_ads += 1
                
                # Check if we're done
                if not cursor or len(results) == 0:
                    print(f"   ✅ No more pages available")
                    break
                
            except Exception as e:
                error_msg = f"Request failed: {str(e)}"
                print(f"   ❌ {error_msg}")
                
                return AdsFetchResult(
                    company_name=company_name or "Unknown",
                    page_id=page_id,
                    total_ads_fetched=total_ads,
                    pages_fetched=pages_fetched,
                    success=False,
                    error=error_msg,
                    fetch_time=time.time() - start_time
                )
        
        # Yield final result
        yield AdsFetchResult(
            company_name=company_name or "Unknown",
            page_id=page_id,
            total_ads_fetched=total_ads,
            pages_fetched=pages_fetched,
            success=True,
            fetch_time=time.time() - start_time
        )
    
    def fetch_company_ads_list(self, 
                              company_name: str = None,
                              page_id: str = None,
                              max_ads: int = 100,
                              **kwargs) -> Tuple[List[Dict], AdsFetchResult]:
        """
        Fetch ads as a list (convenience method)
        
        Returns:
            (ads_list, fetch_result)
        """
        
        ads = []
        result = None
        
        # Use the generator to collect ads
        for ad in self.fetch_company_ads_paginated(company_name=company_name, 
                                                  page_id=page_id, 
                                                  max_ads=max_ads, 
                                                  **kwargs):
            if isinstance(ad, AdsFetchResult):
                result = ad
                break
            else:
                ads.append(ad)
        
        # Get the final result if we didn't break early
        if result is None:
            result = AdsFetchResult(
                company_name=company_name or "Unknown",
                page_id=page_id,
                total_ads_fetched=len(ads),
                pages_fetched=0,
                success=True,
                fetch_time=0
            )
        
        return ads, result
    
    def fetch_multiple_companies(self, 
                               companies: List[str],
                               max_ads_per_company: int = 50,
                               max_total_requests: int = 100,
                               **kwargs) -> Dict[str, Tuple[List[Dict], AdsFetchResult]]:
        """
        Fetch ads for multiple companies with global rate limiting
        
        Args:
            companies: List of company names
            max_ads_per_company: Maximum ads per company
            max_total_requests: Global request limit
            
        Returns:
            Dict mapping company_name -> (ads_list, fetch_result)
        """
        
        results = {}
        total_requests = 0
        
        print(f"🏭 Fetching ads for {len(companies)} companies...")
        print(f"   Max ads per company: {max_ads_per_company}")
        print(f"   Global request limit: {max_total_requests}")
        
        for i, company in enumerate(companies, 1):
            if total_requests >= max_total_requests:
                print(f"   ⏸️  Stopping at company {i-1}/{len(companies)} - hit request limit")
                break
            
            print(f"\n🏢 [{i}/{len(companies)}] Processing: {company}")
            
            requests_before = self.request_count
            ads, result = self.fetch_company_ads_list(
                company_name=company,
                max_ads=max_ads_per_company,
                **kwargs
            )
            
            requests_made = self.request_count - requests_before
            total_requests += requests_made
            
            results[company] = (ads, result)
            
            if result.success:
                print(f"   ✅ Success: {result.total_ads_fetched} ads in {result.fetch_time:.1f}s")
            else:
                print(f"   ❌ Failed: {result.error}")
            
            print(f"   📊 Requests used: {requests_made} (total: {total_requests}/{max_total_requests})")
        
        return results
    
    def get_stats(self) -> Dict:
        """Get fetcher statistics"""
        return {
            'total_requests': self.request_count,
            'uptime_seconds': time.time() - self.start_time if self.start_time else 0,
            'avg_request_time': (time.time() - self.start_time) / max(self.request_count, 1) if self.start_time else 0
        }


def test_pagination():
    """Test the enhanced ads fetcher with pagination"""
    
    print("🧪 TESTING ENHANCED ADS FETCHER WITH PAGINATION")
    print("=" * 70)
    
    fetcher = MetaAdsFetcher()
    
    # Test with Nike (should have many ads for pagination testing)
    print("\n🎯 Test 1: Nike with pagination (max 30 ads)")
    ads, result = fetcher.fetch_company_ads_list(company_name="Nike", max_ads=30)
    
    if result.success:
        print(f"✅ Success!")
        print(f"   Total ads: {len(ads)}")
        print(f"   Pages fetched: {result.pages_fetched}")
        print(f"   Fetch time: {result.fetch_time:.2f}s")
        
        # Show sample creative text for embedding purposes
        print(f"\n📝 Sample creative content (for embedding):")
        for i, ad in enumerate(ads[:3], 1):
            snapshot = ad.get('snapshot', {})
            body_text = (snapshot.get('body', {}) or {}).get('text', '')
            title = snapshot.get('title') or ''
            combined = f"{title} {body_text}".strip()
            print(f"   {i}. {combined[:100]}...")
    else:
        print(f"❌ Failed: {result.error}")
    
    # Test with a smaller brand
    print(f"\n🎯 Test 2: Stripe (smaller brand)")
    ads2, result2 = fetcher.fetch_company_ads_list(company_name="Stripe", max_ads=20)
    
    if result2.success:
        print(f"✅ Success: {len(ads2)} ads in {result2.pages_fetched} pages")
    else:
        print(f"❌ Failed: {result2.error}")
    
    # Show fetcher stats
    print(f"\n📊 Fetcher Statistics:")
    stats = fetcher.get_stats()
    print(f"   Total API requests: {stats['total_requests']}")
    print(f"   Average request time: {stats['avg_request_time']:.2f}s")


if __name__ == "__main__":
    test_pagination()