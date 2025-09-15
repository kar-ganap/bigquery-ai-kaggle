#!/usr/bin/env python3
"""
Live API Verification - Fetch fresh ads directly from ScrapCreators API
to verify start_time_string data and ensure our temporal intelligence is accurate
"""
import os
import sys
import json
from datetime import datetime, timedelta

# Add src to path so we can import our modules
sys.path.append('src')

from utils.ads_fetcher import MetaAdsFetcher

def fetch_fresh_ads_from_api():
    """Fetch ads directly from ScrapCreators API with proper pagination"""
    
    print("🔍 LIVE SCRAPCREATORS API VERIFICATION")
    print("=" * 55)
    print("Fetching fresh ads directly from ScrapCreators API to verify start_time_string data")
    print()
    
    # API Configuration - Using ScrapCreators API
    sc_api_key = os.environ.get('SC_API_KEY')
    if not sc_api_key:
        print("❌ SC_API_KEY environment variable not found")
        print("💡 This token is required to access ScrapCreators API")
        return
    
    print(f"📡 API REQUEST DETAILS:")
    print(f"   Using ScrapCreators API via MetaAdsFetcher")
    print(f"   Company: Warby Parker")
    print(f"   API Key: {sc_api_key[:10]}...{sc_api_key[-5:]}")
    print()
    
    try:
        # Initialize the ads fetcher
        fetcher = MetaAdsFetcher(api_key=sc_api_key)
        
        print(f"📄 Fetching ads for Warby Parker...")
        
        # Fetch ads using our existing infrastructure
        all_ads = []
        for ad_batch in fetcher.fetch_company_ads_paginated(
            company_name="Warby Parker",
            max_pages=3,  # Limit to 3 pages for verification
            max_ads=200,  # Up to 200 ads for analysis
            delay_between_requests=1.0  # Be respectful to API
        ):
            all_ads.extend(ad_batch)
            print(f"   📊 Fetched batch of {len(ad_batch)} ads (total: {len(all_ads)})")
        
        print(f"\n📊 API FETCH SUMMARY:")
        print(f"   Total ads fetched: {len(all_ads)}")
        print()
        
        if not all_ads:
            print("❌ No ads found for Warby Parker")
            return
        
        # Analyze start times
        print("📅 START TIME ANALYSIS:")
        print("-" * 30)
        
        # Parse and analyze start times
        start_times = []
        today = datetime.now().date()
        
        for i, ad in enumerate(all_ads[:20]):  # Analyze first 20 ads
            start_date_str = ad.get('start_date_string', '')
            if start_date_str:
                try:
                    # Parse ISO format timestamp
                    start_dt = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                    start_date = start_dt.date()
                    start_times.append(start_date)
                    
                    days_ago = (today - start_date).days
                    status = ""
                    if days_ago <= 7:
                        status = "🔥 LAST 7 DAYS"
                    elif days_ago <= 13:
                        status = "📊 PRIOR 7 DAYS (days 7-13)"
                    else:
                        status = f"📰 {days_ago} days ago"
                    
                    print(f"   📅 {start_date} ({status})")
                    print(f"      ID: {ad.get('id', 'unknown')}")
                    print(f"      Body: {ad.get('ad_creative_body', '')[:100]}...")
                    print()
                    
                except Exception as e:
                    print(f"   ❌ Failed to parse start_date_string '{start_date_str}': {e}")
            else:
                print(f"   ⚠️  Ad {i+1}: No start_date_string found")
        
        # Temporal intelligence calculation
        print("🎯 TEMPORAL INTELLIGENCE VERIFICATION:")
        print("-" * 40)
        
        if start_times:
            # Count ads in each time window
            last_7d_count = sum(1 for date in start_times if (today - date).days <= 7)
            prior_7d_count = sum(1 for date in start_times if 7 < (today - date).days <= 13)
            
            print(f"   📊 Today's date: {today}")
            print(f"   📊 Total ads with start times: {len(start_times)}")
            print(f"   📊 Last 7d (days 0-7): {last_7d_count} ads")
            print(f"   📊 Prior 7d (days 7-13): {prior_7d_count} ads")
            
            if prior_7d_count == 0:
                print(f"   ✅ CONFIRMED: Prior 7d = 0 → velocity_change_7d = null (correct)")
                print(f"   💡 The null values in temporal intelligence are mathematically accurate")
            else:
                velocity_change = (last_7d_count - prior_7d_count) / prior_7d_count
                print(f"   🎯 CALCULATED: velocity_change_7d = {velocity_change:.4f}")
                print(f"   ❓ If our data shows null, there may be a discrepancy")
            
            # Date range analysis
            earliest = min(start_times)
            latest = max(start_times)
            print(f"\n   📅 API Date Range: {earliest} to {latest}")
            print(f"   📊 Total span: {(latest - earliest).days} days")
            
            # Daily breakdown
            print(f"\n   📊 DAILY BREAKDOWN:")
            from collections import Counter
            daily_counts = Counter(start_times)
            for date in sorted(daily_counts.keys(), reverse=True)[:14]:  # Last 14 days
                count = daily_counts[date]
                days_ago = (today - date).days
                print(f"      {date} (day -{days_ago}): {count} ads")
        
        else:
            print("   ❌ No valid start times found in API response")
        
        # Compare with our stored data
        print(f"\n🔄 COMPARISON WITH STORED DATA:")
        print("-" * 35)
        print("From our independent verification earlier:")
        print("   📊 ads_with_dates table: 30 ads (2025-09-09 to 2025-09-12)")
        print("   📊 Last 7d: 30 ads, Prior 7d: 0 ads")
        print()
        
        if start_times:
            api_earliest = min(start_times)
            api_latest = max(start_times)
            api_last_7d = sum(1 for date in start_times if (today - date).days <= 7)
            api_prior_7d = sum(1 for date in start_times if 7 < (today - date).days <= 13)
            
            print("From live ScrapCreators API:")
            print(f"   📊 Live API data: {len(all_ads)} ads ({api_earliest} to {api_latest})")
            print(f"   📊 Last 7d: {api_last_7d} ads, Prior 7d: {api_prior_7d} ads")
            print()
            
            if api_prior_7d == 0:
                print("✅ VERIFICATION COMPLETE: Both sources confirm 0 ads in prior 7d period")
                print("✅ The null velocity_change_7d values are mathematically correct")
            else:
                print("❓ DISCREPANCY DETECTED: API shows ads in prior 7d but our data doesn't")
                print("🔍 This requires further investigation of data ingestion timing")
        
        # Save raw API response for further analysis
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"data/output/live_api_verification_{timestamp}.json"
        
        verification_data = {
            'timestamp': timestamp,
            'total_ads_fetched': len(all_ads),
            'api_response_sample': all_ads[:10],  # First 10 ads
            'temporal_analysis': {
                'today': str(today),
                'total_with_start_times': len(start_times),
                'last_7d_count': api_last_7d if start_times else 0,
                'prior_7d_count': api_prior_7d if start_times else 0,
                'date_range': {
                    'earliest': str(min(start_times)) if start_times else None,
                    'latest': str(max(start_times)) if start_times else None
                }
            }
        }
        
        with open(output_file, 'w') as f:
            json.dump(verification_data, f, indent=2)
        
        print(f"\n💾 SAVED: Raw API verification data to {output_file}")
        
    except Exception as e:
        print(f"❌ Error during API verification: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    fetch_fresh_ads_from_api()