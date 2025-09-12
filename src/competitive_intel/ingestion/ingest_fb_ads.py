import os, requests, pandas as pd, re
from datetime import datetime
from dateutil import parser as dtp
from google.cloud import bigquery
from typing import List, Dict

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available, use system environment variables

SC_API_KEY   = os.environ["SC_API_KEY"]
BQ_PROJECT   = os.environ.get("BQ_PROJECT", "yourproj")
BQ_DATASET   = os.environ.get("BQ_DATASET", "ads_demo")
TABLE_ID     = f"{BQ_PROJECT}.{BQ_DATASET}.ads_raw"

BASE_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/company/ads"

def to_date(s_or_epoch):
    if s_or_epoch is None:
        return None
    if isinstance(s_or_epoch, str):
        try:
            return dtp.parse(s_or_epoch).date()
        except Exception:
            pass
    try:
        return datetime.utcfromtimestamp(int(s_or_epoch)).date()
    except Exception:
        return None

def normalize_result(r):
    snapshot = r.get("snapshot", {}) or {}
    cards = snapshot.get("cards", []) or []
    page_name = r.get("page_name") or snapshot.get("page_name")
    snapshot_url = r.get("url")
    publisher = r.get("publisher_platform") or []
    pub_str = ",".join(publisher) if isinstance(publisher, list) else str(publisher or "")
    base = {
        "ad_id": r.get("ad_archive_id"),
        "brand": page_name,
        "page_name": page_name,
        "snapshot_url": snapshot_url,
        "display_format": snapshot.get("display_format"),
        "publisher_platforms": pub_str,
        "first_seen": to_date(r.get("start_date_string") or r.get("start_date")),
        "last_seen":  to_date(r.get("end_date_string") or r.get("end_date")),
        "start_date_string": r.get("start_date_string"),
        "end_date_string": r.get("end_date_string"),
        "landing_url": snapshot.get("link_url"),
        "cta_type": snapshot.get("cta_type"),
    }

    rows = []
    if cards:
        for idx, c in enumerate(cards):
            image_url = c.get("resized_image_url") or c.get("original_image_url")
            video_url = c.get("video_sd_url") or c.get("video_hd_url")
            creative_text = c.get("body") or (snapshot.get("body") or {}).get("text")
            title = c.get("title") or snapshot.get("title")
            cta_type = c.get("cta_type") or base["cta_type"]
            landing_url = c.get("link_url") or base["landing_url"]

            if video_url:
                media_type = "VIDEO"
            elif image_url:
                media_type = "IMAGE"
            else:
                media_type = (base["display_format"] or "UNKNOWN").upper()

            rows.append({
                **base,
                "card_index": idx,
                "creative_text": creative_text,
                "title": title,
                "media_type": media_type,
                "image_url": image_url,
                "video_url": video_url,
                "landing_url": landing_url,
                "cta_type": cta_type,
            })
    else:
        vids = snapshot.get("videos") or []
        video_url = (vids[0] or {}).get("video_sd_url") if vids else None
        creative_text = (snapshot.get("body") or {}).get("text")
        title = snapshot.get("title")
        media_type = "VIDEO" if video_url else (base["display_format"] or "UNKNOWN").upper()
        rows.append({
            **base,
            "card_index": None,
            "creative_text": creative_text,
            "title": title,
            "media_type": media_type,
            "image_url": None,
            "video_url": video_url,
        })
    return rows

def fetch_company_ads(page_id=None, company_name=None, country="US", status="ACTIVE", trim="false"):
    """
    Fetch all ads for either a page_id OR a company_name, following the `cursor`
    for pagination until there are no more pages.
    """
    if not page_id and not company_name:
        raise ValueError("Provide either page_id or company_name")

    params = {
        "country": country,
        "status": status,
        "trim": trim,
    }
    if page_id:
        params["pageId"] = page_id
    else:
        params["companyName"] = company_name

    headers = {"x-api-key": SC_API_KEY}
    all_results = []
    seen = set()

    while True:
        resp = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        js = resp.json() or {}
        results = js.get("results", []) or []

        # simple dedupe by ad id
        for r in results:
            ad_id = r.get("ad_archive_id") or r.get("id")
            if ad_id and ad_id in seen:
                continue
            if ad_id:
                seen.add(ad_id)
            all_results.append(r)

        # grab next page cursor if present
        cursor = js.get("cursor") or js.get("nextCursor") or js.get("next_page_cursor") or js.get("nextPageCursor")
        if cursor:
            params = {**params, "cursor": cursor}
        else:
            break

    return all_results

def ensure_table(client: bigquery.Client):
    client.create_dataset(BQ_DATASET, exists_ok=True)
    schema = [
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("page_name", "STRING"),
        bigquery.SchemaField("creative_text", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("media_type", "STRING"),
        bigquery.SchemaField("first_seen", "DATE"),
        bigquery.SchemaField("last_seen", "DATE"),
        bigquery.SchemaField("start_date_string", "STRING"),
        bigquery.SchemaField("end_date_string", "STRING"),
        bigquery.SchemaField("snapshot_url", "STRING"),
        bigquery.SchemaField("landing_url", "STRING"),
        bigquery.SchemaField("card_index", "INT64"),
        bigquery.SchemaField("image_url", "STRING"),
        bigquery.SchemaField("video_url", "STRING"),
        bigquery.SchemaField("display_format", "STRING"),
        bigquery.SchemaField("publisher_platforms", "STRING"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="first_seen")
    table.clustering_fields = ["brand","media_type"]
    try:
        client.create_table(table)
    except Exception:
        pass

def load_dataframe(df: pd.DataFrame):
    client = bigquery.Client(project=BQ_PROJECT)
    ensure_table(client)
    job = client.load_table_from_dataframe(
        df, TABLE_ID,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"Loaded {len(df)} rows into {TABLE_ID}")


class MetaAdsFetcher:
    """Wrapper class for Meta ads fetching functionality"""
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or SC_API_KEY
    
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
        
        Returns: {company_name: {'tier': int, 'estimated_count': int, 'exact_count': bool}}
        """
        headers = {"x-api-key": self.api_key}
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
                # Probe first page only
                params = {
                    "companyName": company_name,
                    "country": country,
                    "status": status,
                    "trim": "true"  # Minimal data for faster response
                }
                
                resp = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
                resp.raise_for_status()
                js = resp.json() or {}
                
                first_page_results = js.get("results", []) or []
                has_next_page = bool(js.get("cursor") or js.get("nextCursor"))
                
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
        
    def fetch_competitor_ads(self, competitor_names: List[str], country: str = "US", 
                           status: str = "ACTIVE") -> Dict[str, int]:
        """Fetch ads for multiple competitors"""
        results = {}
        
        for company_name in competitor_names:
            try:
                ads = fetch_company_ads(
                    company_name=company_name, 
                    country=country, 
                    status=status
                )
                
                # Normalize the results
                rows = []
                for r in ads:
                    rows.extend(normalize_result(r))
                    
                df = pd.DataFrame(rows)
                if not df.empty:
                    load_dataframe(df)
                    results[company_name] = len(df)
                    print(f"✅ {company_name}: {len(df)} ads collected")
                else:
                    results[company_name] = 0
                    print(f"⚠️  {company_name}: No ads found")
                    
            except Exception as e:
                print(f"❌ {company_name}: Error fetching ads - {e}")
                results[company_name] = 0
                
        return results
    
    def fetch_company_ads_with_metadata(self, company_name: str, page_id: str = None, 
                                      max_ads: int = 50, max_pages: int = 5, 
                                      delay_between_requests: float = 0.5, 
                                      country: str = "US", status: str = "ACTIVE") -> tuple:
        """
        Fetch ads for a single company with additional metadata and control parameters.
        Returns (ads_list, fetch_result_dict) to match expected interface from pipeline.
        """
        import time
        
        try:
            # Fetch raw ads data
            ads = fetch_company_ads(
                page_id=page_id,
                company_name=company_name,
                country=country,
                status=status
            )
            
            # Apply max_ads limit if specified
            if max_ads and len(ads) > max_ads:
                ads = ads[:max_ads]
            
            # Normalize the results
            normalized_ads = []
            for r in ads:
                normalized_ads.extend(normalize_result(r))
            
            # Add delay simulation (though not needed for single request)
            time.sleep(delay_between_requests)
            
            # Create fetch result metadata
            fetch_result = {
                'company_name': company_name,
                'page_id': page_id,
                'ads_found': len(normalized_ads),
                'pages_processed': 1,  # API handles pagination internally
                'status': 'success'
            }
            
            return normalized_ads, fetch_result
            
        except Exception as e:
            print(f"❌ Error fetching ads for {company_name}: {str(e)}")
            fetch_result = {
                'company_name': company_name,
                'page_id': page_id,
                'ads_found': 0,
                'pages_processed': 0,
                'status': 'error',
                'error': str(e)
            }
            return [], fetch_result


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--page_id", help="Meta page id")
    group.add_argument("--company_name", help="Company/Page name search")
    ap.add_argument("--country", default="US")
    ap.add_argument("--status", default="ACTIVE")
    ap.add_argument("--trim", default="false", choices=["true","false"])
    args = ap.parse_args()

    results = fetch_company_ads(
        page_id=args.page_id,
        company_name=args.company_name,
        country=args.country,
        status=args.status,
        trim=args.trim,
    )

    rows = []
    for r in results:
        rows.extend(normalize_result(r))
    df = pd.DataFrame(rows)
    if df.empty:
        print("No rows to load.")
    else:
        load_dataframe(df)
        print(df.head(3))
