#!/usr/bin/env python3
"""
Test Nike ads with different status parameters to match GUI count of 440
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

def test_nike_with_different_statuses():
    """Test Nike with different status parameters"""
    
    print("🔍 TESTING NIKE ADS WITH DIFFERENT STATUS PARAMETERS")
    print("=" * 70)
    print("GUI shows: 440 ads")
    print("Testing different API parameters...\n")
    
    fetcher = MetaAdsFetcher()
    
    # Test different status values
    statuses_to_test = [
        ("ACTIVE", "Active ads only"),
        ("INACTIVE", "Inactive ads only"), 
        ("ALL", "All ads (active + inactive)"),
    ]
    
    results = {}
    
    for status, description in statuses_to_test:
        print(f"📱 Testing status='{status}' ({description})")
        
        ads, result = fetcher.fetch_company_ads_list(
            company_name="Nike",
            max_ads=500,  # Higher limit
            max_pages=20,
            status=status,
            delay_between_requests=0.3
        )
        
        results[status] = {
            'ads_count': len(ads),
            'pages_fetched': result.pages_fetched if result else 0,
            'success': result.success if result else False,
            'error': result.error if result else None,
            'fetch_time': result.fetch_time if result else 0
        }
        
        if result and result.success:
            print(f"   ✅ Success: {len(ads)} ads in {result.pages_fetched} pages ({result.fetch_time:.1f}s)")
        else:
            print(f"   ❌ Failed: {result.error if result else 'Unknown error'}")
        
        print()
        
        # Add delay between tests to avoid rate limiting
        time.sleep(1)
    
    # Summary
    print(f"📊 SUMMARY")
    print("=" * 70)
    
    total_unique_ads = 0
    for status, data in results.items():
        print(f"{status:10}: {data['ads_count']:3} ads | Success: {data['success']}")
        if status == "ALL":
            total_unique_ads = data['ads_count']
    
    print(f"\nComparison with GUI:")
    print(f"GUI count:    440 ads")
    if "ALL" in results:
        all_count = results["ALL"]["ads_count"]
        print(f"API 'ALL':    {all_count} ads")
        diff = 440 - all_count
        if diff == 0:
            print("🎯 Perfect match!")
        elif abs(diff) <= 5:
            print(f"✅ Very close ({diff:+} ads)")
        else:
            print(f"🔍 Still {abs(diff)} ads difference")
    
    # Show active vs inactive breakdown
    if results.get("ACTIVE", {}).get("success") and results.get("INACTIVE", {}).get("success"):
        active_count = results["ACTIVE"]["ads_count"]
        inactive_count = results["INACTIVE"]["ads_count"]
        calculated_total = active_count + inactive_count
        
        print(f"\nBreakdown:")
        print(f"Active:   {active_count}")
        print(f"Inactive: {inactive_count}")
        print(f"Sum:      {calculated_total}")
        
        if "ALL" in results:
            all_actual = results["ALL"]["ads_count"]
            if calculated_total != all_actual:
                print(f"Note: Sum ({calculated_total}) ≠ ALL ({all_actual}) - possible API overlap handling")

if __name__ == "__main__":
    test_nike_with_different_statuses()