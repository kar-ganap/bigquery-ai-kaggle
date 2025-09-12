"""
Enhanced Meta Ad Library API client with cursor pagination and page ID support
Integrates with page ID resolver for robust competitor ad collection
"""

import os
import time
import requests
import re
from typing import Dict, List, Optional, Generator, Tuple
from dataclasses import dataclass
from datetime import datetime

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available, use system environment variables

from .page_id_resolver import PageIDResolver

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
            
            # Make request with retry logic
            max_retries = 3
            retry_delay = 1.0  # Start with 1 second
            last_error = None
            
            for attempt in range(max_retries):
                try:
                    if pages_fetched > 0 or attempt > 0:  # Add delay between requests and retries
                        time.sleep(delay_between_requests if attempt == 0 else retry_delay)
                    
                    self.request_count += 1
                    response = requests.get(ADS_URL, params=params, headers={"x-api-key": self.api_key}, timeout=30)
                    
                    if response.status_code == 200:
                        # Success - break out of retry loop
                        break
                    else:
                        # API error - prepare for retry
                        error_msg = f"API error {response.status_code}: {response.text[:200]}"
                        last_error = error_msg
                        
                        if attempt < max_retries - 1:  # Not the last attempt
                            print(f"   ⚠️  Attempt {attempt + 1}/{max_retries} failed: {error_msg}")
                            print(f"   🔄 Retrying in {retry_delay}s...")
                            retry_delay *= 2  # Exponential backoff
                            continue
                        else:
                            # Final attempt failed
                            print(f"   ❌ Final attempt failed: {error_msg}")
                            print(f"   🔄 Continuing with {total_ads} ads collected so far...")
                            
                            # Return partial success instead of complete failure
                            return AdsFetchResult(
                                company_name=company_name or "Unknown",
                                page_id=page_id,
                                total_ads_fetched=total_ads,
                                pages_fetched=pages_fetched,
                                success=total_ads > 0,  # Success if we got some ads
                                error=f"Partial failure after {max_retries} attempts: {error_msg}",
                                fetch_time=time.time() - start_time
                            )
                
                except requests.RequestException as e:
                    # Network/request exception
                    last_error = f"Request failed: {str(e)}"
                    
                    if attempt < max_retries - 1:
                        print(f"   ⚠️  Network error attempt {attempt + 1}/{max_retries}: {last_error}")
                        print(f"   🔄 Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    else:
                        print(f"   ❌ Network error after {max_retries} attempts: {last_error}")
                        return AdsFetchResult(
                            company_name=company_name or "Unknown",
                            page_id=page_id,
                            total_ads_fetched=total_ads,
                            pages_fetched=pages_fetched,
                            success=total_ads > 0,
                            error=f"Network failure after {max_retries} attempts: {last_error}",
                            fetch_time=time.time() - start_time
                        )
            
            # If we get here, we have a successful response
            try:
                
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
                # JSON parsing or other unexpected error
                error_msg = f"Response processing failed: {str(e)}"
                print(f"   ❌ {error_msg}")
                
                return AdsFetchResult(
                    company_name=company_name or "Unknown",
                    page_id=page_id,
                    total_ads_fetched=total_ads,
                    pages_fetched=pages_fetched,
                    success=total_ads > 0,  # Partial success if we got some ads
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
    
    def fetch_company_ads_with_metadata(self, company_name: str, page_id: str = None, 
                                      max_ads: int = 50, max_pages: int = 5, 
                                      delay_between_requests: float = 0.5, 
                                      country: str = "US", status: str = "ACTIVE") -> tuple:
        """
        Compatibility method for pipeline - matches old interface.
        Returns (ads_list, fetch_result_dict) to match expected interface from pipeline.
        """
        
        # Use the enhanced fetch method with page ID resolution
        ads, result = self.fetch_company_ads_list(
            company_name=company_name,
            max_ads=max_ads,
            max_pages=max_pages,
            delay_between_requests=delay_between_requests,
            country=country,
            status=status
        )
        
        # If page ID resolution failed, return empty results with clear error
        if not result.success and "Could not resolve page ID" in (result.error or ""):
            print(f"   ❌ Skipping {company_name}: Cannot resolve to valid page ID")
            return [], {"success": False, "error": result.error, "total_ads_fetched": 0}
        
        # Normalize ads to match old format expected by pipeline
        normalized_ads = []
        for ad in ads:
            # The raw ad data from API needs to be normalized
            snapshot = ad.get("snapshot", {}) or {}
            cards = snapshot.get("cards", []) or []
            
            # Extract creative text from various possible locations
            creative_text = ""
            body_text = (snapshot.get("body", {}) or {}).get("text", "")
            title = snapshot.get("title") or ""
            link_description = snapshot.get("link_description") or ""
            
            # Combine text elements
            text_parts = [title, body_text, link_description]
            creative_text = " ".join([part for part in text_parts if part]).strip()
            
            # Extract media info
            media_type = "unknown"
            if cards:
                if any("video" in str(card).lower() for card in cards):
                    media_type = "video"
                elif any("image" in str(card).lower() for card in cards):
                    media_type = "image"
                else:
                    media_type = "carousel" if len(cards) > 1 else "image"
            
            # Normalize to expected format
            normalized_ad = {
                'ad_id': ad.get("ad_archive_id"),
                'ad_archive_id': ad.get("ad_archive_id"),
                'page_name': ad.get("page_name") or company_name,
                'creative_text': creative_text,
                'publisher_platforms': ",".join(ad.get("publisher_platform", [])) if isinstance(ad.get("publisher_platform"), list) else str(ad.get("publisher_platform", "")),
                'media_type': media_type,
                'snapshot_url': ad.get("url"),
                'display_format': snapshot.get("display_format"),
                'first_seen': ad.get("start_date_string"),
                'last_seen': ad.get("end_date_string"),
                'start_date_string': ad.get("start_date_string"),
                'end_date_string': ad.get("end_date_string")
            }
            
            normalized_ads.append(normalized_ad)
        
        # Return in expected format: (ads_list, result_dict)
        result_dict = {
            "success": result.success,
            "total_ads_fetched": result.total_ads_fetched,
            "pages_fetched": result.pages_fetched,
            "fetch_time": result.fetch_time,
            "error": result.error,
            "page_id": result.page_id
        }
        
        return normalized_ads, result_dict
    
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
    
    def calculate_meta_priority_score(self, company_name: str) -> float:
        """Calculate likelihood of Meta ad presence for prioritization"""
        score = 0.5  # baseline
        name_lower = company_name.lower()
        
        # Industry knowledge - known eyewear advertisers (HIGH PRIORITY)
        # Based on industry research + common DTC eyewear brands
        HIGH_PRIORITY_BRANDS = {
            'warby parker', 'zenni', 'zenni optical', 'eyebuydirect', 'eye buy direct',
            'glassesusa', 'glasses usa', 'lenscrafters', 'lens crafters', 
            'pearle vision', 'coastal', 'clearly', 'diff eyewear', 'felix gray', 
            'roka', 'ray-ban', 'rayban', 'oakley', 'moscot', 'oliver peoples',
            'glasses.com', 'frames direct', 'framesdirect', '1-800 contacts',
            'contacts direct', 'ac lens', 'discount contact lenses', 'lens.com',
            'liingo eyewear', 'liingo', 'pair eyewear', 'yesglasses', 'goggles4u'
        }
        
        # Known holding companies/B2B (LOW PRIORITY)
        LOW_PRIORITY_BRANDS = {
            'essilor', 'luxottica', 'safilo', 'marcolin', 'de rigo', 'marchon', 
            'kering eyewear', 'lvmh', 'johnson & johnson', 'alcon', 'bausch',
            'cooper vision', 'hoya'
        }
        
        # Check exact matches first
        if name_lower in HIGH_PRIORITY_BRANDS:
            return 0.95
        if name_lower in LOW_PRIORITY_BRANDS:
            return 0.1
            
        # Positive signals (likely DTC/consumer brands)
        if '.com' in name_lower:
            score += 0.3  # "GlassesUSA.com" → DTC signal
        if 'direct' in name_lower:
            score += 0.25  # "EyeBuyDirect" → DTC signal
        if any(word in name_lower for word in ['glasses', 'eyewear', 'optical', 'vision']):
            score += 0.2  # Category-specific brands
        if re.match(r'^[A-Z][a-z]+[A-Z]', company_name):
            score += 0.15  # "LensCrafters" → brand pattern
        if len(company_name.split()) <= 2:
            score += 0.1  # Short names usually brands
            
        # Negative signals (unlikely consumer advertisers)
        if any(word in name_lower for word in ['group', 'holding', 'corp', 'inc', 'ltd']):
            score -= 0.3  # Corporate entities
        if 'manufacturing' in name_lower or 'wholesale' in name_lower:
            score -= 0.25  # B2B companies
        if len(company_name.split()) > 4:
            score -= 0.2  # Long names often descriptions
        if any(word in name_lower for word in ['solutions', 'systems', 'technologies']):
            score -= 0.15  # B2B terminology
            
        return min(max(score, 0), 1)
    
    def get_competitor_ad_tiers(self, competitor_names: List[str], country: str = "US", 
                               status: str = "ACTIVE", probe_limit: int = 20, 
                               target_count: int = 10) -> Dict[str, Dict]:
        """
        Smart probe to classify competitors by Meta ad activity tiers:
        - Tier 1: 1-10 ads (Minor Player)
        - Tier 2: 11-19 ads (Moderate Player) 
        - Tier 3: 20+ ads (Major Player)
        - Tier 0: 0 ads (No Meta Presence)
        
        Uses intelligent prioritization and early exit once target_count competitors found.
        Enhanced with page ID resolution for robust API calls.
        
        Returns: {company_name: {'tier': int, 'estimated_count': int, 'exact_count': bool}}
        """
        results = {}
        
        # Smart prioritization - calculate meta likelihood scores
        print(f"   🎯 Prioritizing {len(competitor_names)} competitors by Meta ad likelihood...")
        prioritized_competitors = []
        for company_name in competitor_names:
            priority_score = self.calculate_meta_priority_score(company_name)
            prioritized_competitors.append((company_name, priority_score))
        
        # Sort by priority (highest likelihood first)
        prioritized_competitors.sort(key=lambda x: x[1], reverse=True)
        
        # Show top priorities
        if len(prioritized_competitors) > 0:
            top_5 = prioritized_competitors[:5]
            print(f"   📊 Top priorities: {', '.join([f'{name} ({score:.2f})' for name, score in top_5])}")
        
        checked_count = 0
        found_active = 0
        
        for company_name, priority_score in prioritized_competitors:
            try:
                # First resolve company name to page ID for robust API calls
                print(f"🔍 Resolving page ID for '{company_name}'...")
                page_data = self.page_resolver.resolve_page_id(company_name)
                
                if not page_data:
                    print(f"   ❌ {company_name}: Cannot resolve to valid page ID - skipping")
                    results[company_name] = {
                        'tier': -1,  # Error tier
                        'estimated_count': 'Error',
                        'exact_count': False,
                        'classification': 'Page ID Resolution Failed'
                    }
                    checked_count += 1
                    continue
                
                page_id = page_data['page_id']
                print(f"   ✅ Resolved to page ID: {page_id} ({page_data['name']})")
                
                # Probe first page only using page ID for robust API calls
                params = {
                    "pageId": page_id,  # Use page ID instead of company name
                    "country": country,
                    "status": status,
                    "limit": min(50, probe_limit),  # Probe first page 
                    "trim": "false"  # We need cursor info to detect pagination
                }
                
                # Use the existing retry logic from fetch method
                max_retries = 3
                retry_delay = 1.0
                last_error = None
                
                for attempt in range(max_retries):
                    try:
                        if attempt > 0:
                            time.sleep(retry_delay)
                        
                        self.request_count += 1
                        resp = requests.get(ADS_URL, headers={"x-api-key": self.api_key}, params=params, timeout=30)
                        
                        if resp.status_code == 200:
                            break  # Success
                        else:
                            last_error = f"API error {resp.status_code}: {resp.text[:200]}"
                            if attempt < max_retries - 1:
                                retry_delay *= 2
                                continue
                            else:
                                raise Exception(last_error)
                                
                    except requests.RequestException as e:
                        last_error = f"Request failed: {str(e)}"
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            retry_delay *= 2
                            continue
                        else:
                            raise Exception(last_error)
                
                # Process successful response
                js = resp.json() or {}
                first_page_results = js.get("results", []) or []
                has_next_page = bool(js.get("cursor"))
                
                # Classify into tiers
                count = len(first_page_results)
                if count == 0:
                    tier = 0
                    estimated_count = 0
                    exact_count = True
                elif count <= 10 and not has_next_page:
                    tier = 1
                    estimated_count = count
                    exact_count = True
                elif count <= 19 and not has_next_page:
                    tier = 2
                    estimated_count = count  
                    exact_count = True
                else:
                    # Has 20+ or has pagination - Major Player
                    tier = 3
                    estimated_count = f"{count}+" if has_next_page else count
                    exact_count = not has_next_page
                
                results[company_name] = {
                    'tier': tier,
                    'estimated_count': estimated_count,
                    'exact_count': exact_count,
                    'classification': {
                        0: 'No Meta Presence',
                        1: 'Minor Player (1-10 ads)',
                        2: 'Moderate Player (11-19 ads)', 
                        3: 'Major Player (20+ ads)'
                    }[tier]
                }
                
                print(f"   📊 {company_name}: {results[company_name]['classification']} - {estimated_count} ads")
                
                # Count active competitors (tier > 0)
                if tier > 0:
                    found_active += 1
                
                checked_count += 1
                
                # Early exit if we found enough active competitors
                if found_active >= target_count:
                    print(f"   ✅ Found {target_count} Meta-active competitors after checking {checked_count} (hit rate: {found_active}/{checked_count})")
                    break
                    
                # Stop early if we've checked many with low success rate
                if checked_count >= 30 and found_active < max(3, target_count * 0.3):
                    print(f"   ⚠️  Low hit rate ({found_active}/{checked_count}), stopping early to avoid timeout")
                    break
                
            except Exception as e:
                checked_count += 1
                print(f"   ❌ {company_name}: Error probing ads - {e}")
                results[company_name] = {
                    'tier': -1,  # Error tier
                    'estimated_count': 'Error',
                    'exact_count': False,
                    'classification': 'API Error'
                }
                
                # Continue with errors but don't let them dominate
                if checked_count >= 40:
                    print(f"   ⏰ Checked {checked_count} competitors, stopping to avoid timeout")
                    break
        
        print(f"   📈 Final results: Found {found_active} Meta-active competitors from {checked_count} checked")
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