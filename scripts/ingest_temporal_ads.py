#!/usr/bin/env python3
"""
Enhanced FB Ads Ingestion for Time-Series Analysis
Collects ALL fields needed for Subgoal 6 testing including temporal data and platform info
Uses status=ALL to get historical data with reasonable limits
"""

import os, requests, pandas as pd, json
from datetime import datetime
from dateutil import parser as dtp
from google.cloud import bigquery

SC_API_KEY = os.environ["SC_API_KEY"]
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
TABLE_ID = f"{BQ_PROJECT}.{BQ_DATASET}.ads_temporal_complete"

BASE_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/company/ads"

def to_date(s_or_epoch):
    """Convert string or epoch to date"""
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

def to_timestamp(s_or_epoch):
    """Convert string or epoch to timestamp"""
    if s_or_epoch is None:
        return None
    if isinstance(s_or_epoch, str):
        try:
            return dtp.parse(s_or_epoch)
        except Exception:
            pass
    try:
        return datetime.utcfromtimestamp(int(s_or_epoch))
    except Exception:
        return None

def extract_comprehensive_ad_data(ad_response):
    """Extract ALL fields needed for comprehensive analysis"""
    snapshot = ad_response.get("snapshot", {}) or {}
    cards = snapshot.get("cards", []) or []
    page_name = ad_response.get("page_name") or snapshot.get("page_name")
    
    # Platform data - critical for platform analysis
    publisher_platforms = ad_response.get("publisher_platform") or []
    platforms_str = ",".join(publisher_platforms) if isinstance(publisher_platforms, list) else str(publisher_platforms or "")
    
    # Temporal data - critical for time-series analysis  
    start_date_string = ad_response.get("start_date_string")
    end_date_string = ad_response.get("end_date_string")
    start_timestamp = to_timestamp(start_date_string or ad_response.get("start_date"))
    end_timestamp = to_timestamp(end_date_string or ad_response.get("end_date"))
    
    # Calculate active duration
    active_days = None
    if start_timestamp and end_timestamp:
        active_days = (end_timestamp.date() - start_timestamp.date()).days + 1
    elif start_timestamp:
        active_days = (datetime.now().date() - start_timestamp.date()).days + 1
    
    # Base ad information
    base_ad = {
        # Identifiers
        "ad_id": ad_response.get("ad_archive_id"),
        "page_id": ad_response.get("page_id"),
        "brand": page_name,
        "page_name": page_name,
        
        # Temporal data (essential for time-series)
        "start_date_string": start_date_string,
        "end_date_string": end_date_string,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "first_seen": to_date(start_date_string or ad_response.get("start_date")),
        "last_seen": to_date(end_date_string or ad_response.get("end_date")),
        "active_days": active_days,
        "is_active": ad_response.get("is_active", False),
        
        # Platform data (essential for platform analysis)
        "publisher_platforms": platforms_str,
        "platforms_array": publisher_platforms if isinstance(publisher_platforms, list) else [platforms_str] if platforms_str else [],
        
        # Creative format and display
        "display_format": snapshot.get("display_format"),
        "media_type": None,  # Will be determined per card/creative
        
        # Page and context info
        "page_profile_uri": snapshot.get("page_profile_uri"),
        "page_categories": ",".join(snapshot.get("page_categories", [])),
        "page_like_count": snapshot.get("page_like_count"),
        
        # Links and CTAs
        "snapshot_url": ad_response.get("url"),
        "landing_url": snapshot.get("link_url"),
        "cta_type": snapshot.get("cta_type"),
        "cta_text": snapshot.get("cta_text"),
        
        # Additional metadata
        "reach_estimate": ad_response.get("reach_estimate"),
        "spend": ad_response.get("spend"),
        "total_active_time": ad_response.get("total_active_time"),
        
        # Raw response for debugging
        "raw_response": json.dumps(ad_response),
        
        # Ingestion metadata
        "ingested_at": datetime.now(),
        "ingestion_batch": datetime.now().strftime("%Y%m%d_%H%M%S")
    }
    
    creative_rows = []
    
    # Handle DCO/carousel ads with multiple cards
    if cards:
        for idx, card in enumerate(cards):
            # Determine media type for this card
            media_type = "IMAGE"
            if card.get("video_sd_url") or card.get("video_hd_url"):
                media_type = "VIDEO"
            elif base_ad["display_format"] == "DCO":
                media_type = "DCO"
            
            creative_row = {
                **base_ad,
                "card_index": idx,
                "media_type": media_type,
                
                # Card-specific content (essential for content analysis)
                "creative_text": card.get("body") or (snapshot.get("body") or {}).get("text"),
                "title": card.get("title") or snapshot.get("title"),
                "caption": card.get("caption") or snapshot.get("caption"),
                
                # Card-specific media
                "image_url": card.get("resized_image_url") or card.get("original_image_url"),
                "video_url": card.get("video_sd_url") or card.get("video_hd_url"),
                "video_preview_url": card.get("video_preview_image_url"),
                
                # Card-specific CTAs
                "card_cta_type": card.get("cta_type") or base_ad["cta_type"],
                "card_cta_text": card.get("cta_text") or base_ad["cta_text"],
                "card_landing_url": card.get("link_url") or base_ad["landing_url"],
            }
            creative_rows.append(creative_row)
    else:
        # Single creative (no cards)
        videos = snapshot.get("videos") or []
        video_url = (videos[0] or {}).get("video_sd_url") if videos else None
        
        media_type = "VIDEO" if video_url else (base_ad["display_format"] or "UNKNOWN").upper()
        
        creative_row = {
            **base_ad,
            "card_index": None,
            "media_type": media_type,
            
            # Single creative content
            "creative_text": (snapshot.get("body") or {}).get("text"),
            "title": snapshot.get("title"),
            "caption": snapshot.get("caption"),
            
            # Single creative media
            "image_url": None,  # Images typically in cards for single creatives
            "video_url": video_url,
            "video_preview_url": (videos[0] or {}).get("video_preview_image_url") if videos else None,
            
            # Single creative CTAs (same as base)
            "card_cta_type": base_ad["cta_type"],
            "card_cta_text": base_ad["cta_text"], 
            "card_landing_url": base_ad["landing_url"],
        }
        creative_rows.append(creative_row)
    
    return creative_rows

def fetch_company_ads_with_history(company_name, country="US", limit=100):
    """
    Fetch ads with ALL status to get historical time-series data
    Limited to prevent explosion for large advertisers
    """
    params = {
        "companyName": company_name,
        "country": country,
        "status": "ALL",  # Get both active and inactive for time-series
        "trim": "false",   # Get full response with all fields
        "limit": limit     # Reasonable limit to prevent blow-up
    }
    
    headers = {"x-api-key": SC_API_KEY}
    all_ads = []
    seen_ads = set()
    page_count = 0
    max_pages = 10  # Safety limit
    
    print(f"🔍 Fetching {company_name} ads with historical data (limit: {limit})...")
    
    while page_count < max_pages:
        try:
            resp = requests.get(BASE_URL, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json() or {}
            
            results = data.get("results", []) or []
            if not results:
                print(f"   No more results after {page_count} pages")
                break
            
            # Deduplicate
            new_ads = 0
            for ad in results:
                ad_id = ad.get("ad_archive_id")
                if ad_id and ad_id not in seen_ads:
                    seen_ads.add(ad_id)
                    all_ads.append(ad)
                    new_ads += 1
            
            print(f"   Page {page_count + 1}: {new_ads} new ads (total: {len(all_ads)})")
            
            # Check for next page
            cursor = data.get("cursor") or data.get("nextCursor")
            if cursor and len(all_ads) < limit:
                params["cursor"] = cursor
                page_count += 1
            else:
                break
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error fetching page {page_count + 1}: {e}")
            break
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            break
    
    print(f"✅ Collected {len(all_ads)} unique ads for {company_name}")
    return all_ads

def create_comprehensive_table(client):
    """Create table with ALL fields needed for comprehensive analysis"""
    schema = [
        # Identifiers
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("page_id", "STRING"),
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("page_name", "STRING"),
        
        # Temporal data (CRITICAL for time-series)
        bigquery.SchemaField("start_date_string", "STRING"),
        bigquery.SchemaField("end_date_string", "STRING"),
        bigquery.SchemaField("start_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("end_timestamp", "TIMESTAMP"),
        bigquery.SchemaField("first_seen", "DATE"),
        bigquery.SchemaField("last_seen", "DATE"),
        bigquery.SchemaField("active_days", "INT64"),
        bigquery.SchemaField("is_active", "BOOL"),
        
        # Platform data (CRITICAL for platform analysis)
        bigquery.SchemaField("publisher_platforms", "STRING"),
        bigquery.SchemaField("platforms_array", "STRING", mode="REPEATED"),
        
        # Creative content (CRITICAL for content analysis)
        bigquery.SchemaField("creative_text", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("caption", "STRING"),
        
        # Creative format and media
        bigquery.SchemaField("display_format", "STRING"),
        bigquery.SchemaField("media_type", "STRING"),
        bigquery.SchemaField("card_index", "INT64"),
        bigquery.SchemaField("image_url", "STRING"),
        bigquery.SchemaField("video_url", "STRING"),
        bigquery.SchemaField("video_preview_url", "STRING"),
        
        # CTAs and links
        bigquery.SchemaField("cta_type", "STRING"),
        bigquery.SchemaField("cta_text", "STRING"),
        bigquery.SchemaField("card_cta_type", "STRING"),
        bigquery.SchemaField("card_cta_text", "STRING"),
        bigquery.SchemaField("landing_url", "STRING"),
        bigquery.SchemaField("card_landing_url", "STRING"),
        bigquery.SchemaField("snapshot_url", "STRING"),
        
        # Page context
        bigquery.SchemaField("page_profile_uri", "STRING"),
        bigquery.SchemaField("page_categories", "STRING"),
        bigquery.SchemaField("page_like_count", "INT64"),
        
        # Performance data (when available)
        bigquery.SchemaField("reach_estimate", "INT64"),
        bigquery.SchemaField("spend", "STRING"),
        bigquery.SchemaField("total_active_time", "INT64"),
        
        # Metadata
        bigquery.SchemaField("raw_response", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("ingestion_batch", "STRING"),
    ]
    
    table = bigquery.Table(TABLE_ID, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="start_timestamp")
    table.clustering_fields = ["brand", "media_type", "is_active"]
    
    try:
        client.create_table(table, exists_ok=True)
        print(f"✅ Created/verified table: {TABLE_ID}")
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        raise

def main():
    """Enhanced ingestion for comprehensive time-series testing"""
    import argparse
    
    ap = argparse.ArgumentParser(description="Ingest FB ads with comprehensive temporal and platform data")
    ap.add_argument("--brands", nargs="+", default=["Nike", "Adidas", "Under Armour"], 
                    help="Brands to collect data for")
    ap.add_argument("--limit", type=int, default=50,
                    help="Max ads per brand (prevents blow-up for large advertisers)")
    ap.add_argument("--country", default="US")
    
    args = ap.parse_args()
    
    print("🚀 ENHANCED FB ADS INGESTION FOR TIME-SERIES ANALYSIS")
    print("="*60)
    print(f"📊 Target: {len(args.brands)} brands, {args.limit} ads each")
    print(f"🗺️  Country: {args.country}")
    print(f"📅 Status: ALL (includes historical data)")
    print(f"🎯 Goal: Comprehensive data for Subgoal 6 testing")
    print("="*60)
    
    client = bigquery.Client(project=BQ_PROJECT)
    create_comprehensive_table(client)
    
    all_creative_rows = []
    
    for brand in args.brands:
        print(f"\n📱 Processing {brand}...")
        
        try:
            # Fetch ads with history
            brand_ads = fetch_company_ads_with_history(
                company_name=brand,
                country=args.country,
                limit=args.limit
            )
            
            if not brand_ads:
                print(f"   ⚠️  No ads found for {brand}")
                continue
            
            # Extract comprehensive data
            brand_creative_rows = []
            for ad in brand_ads:
                try:
                    creative_rows = extract_comprehensive_ad_data(ad)
                    brand_creative_rows.extend(creative_rows)
                except Exception as e:
                    print(f"   ⚠️  Error processing ad {ad.get('ad_archive_id')}: {e}")
                    continue
            
            print(f"   ✅ Extracted {len(brand_creative_rows)} creative variations")
            
            # Add temporal data validation
            with_dates = sum(1 for row in brand_creative_rows if row["start_date_string"])
            platform_data = sum(1 for row in brand_creative_rows if row["publisher_platforms"])
            
            print(f"   📅 Temporal data: {with_dates}/{len(brand_creative_rows)} ads")
            print(f"   📱 Platform data: {platform_data}/{len(brand_creative_rows)} ads")
            
            all_creative_rows.extend(brand_creative_rows)
            
        except Exception as e:
            print(f"   ❌ Error processing {brand}: {e}")
            continue
    
    # Load to BigQuery
    if all_creative_rows:
        print(f"\n💾 Loading {len(all_creative_rows)} creative rows to BigQuery...")
        
        df = pd.DataFrame(all_creative_rows)
        
        # Data quality report
        print(f"\n📊 DATA QUALITY REPORT:")
        print(f"   Total creative rows: {len(df)}")
        print(f"   Unique ads: {df['ad_id'].nunique()}")
        print(f"   Unique brands: {df['brand'].nunique()}")
        print(f"   With start dates: {df['start_date_string'].notna().sum()}")
        print(f"   With end dates: {df['end_date_string'].notna().sum()}")
        print(f"   With platform data: {df['publisher_platforms'].notna().sum()}")
        print(f"   Active ads: {df['is_active'].sum()}")
        print(f"   Historical ads: {(~df['is_active']).sum()}")
        
        # Date range
        if df['start_timestamp'].notna().any():
            print(f"   Date range: {df['start_timestamp'].min()} to {df['end_timestamp'].max()}")
        
        # Load to BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"  # Replace existing data
        )
        
        try:
            job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
            job.result()
            
            print(f"✅ Successfully loaded to {TABLE_ID}")
            
            # Verify the load
            verify_query = f"SELECT COUNT(*) as total_rows FROM `{TABLE_ID}`"
            result = client.query(verify_query).result()
            for row in result:
                print(f"🔍 Verification: {row.total_rows} rows in BigQuery")
                
        except Exception as e:
            print(f"❌ Error loading to BigQuery: {e}")
            return 1
    
    else:
        print("❌ No data collected!")
        return 1
    
    print("\n🎉 INGESTION COMPLETE!")
    print("✅ Ready for comprehensive time-series testing")
    return 0

if __name__ == "__main__":
    exit(main())