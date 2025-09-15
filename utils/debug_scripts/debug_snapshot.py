#!/usr/bin/env python3
"""
Quick check of snapshot field in API response
"""
import sys
import json
sys.path.append('src')

import requests
import os

# Simple direct API call to check snapshot field
SC_API_KEY = os.environ.get("SC_API_KEY")
ADS_URL = "https://api.scrapecreators.com/v1/facebook/adLibrary/company/ads"

def main():
    print("🔍 DIRECT API CALL TO CHECK SNAPSHOT FIELD")
    print("=" * 50)

    # Make direct API call (bypassing our wrapper)
    params = {
        "country": "US",
        "status": "ALL",
        "pageId": "212448944782",  # EyeBuyDirect page ID
        "limit": 1
    }

    response = requests.get(ADS_URL, params=params, headers={"x-api-key": SC_API_KEY})

    if response.status_code == 200:
        data = response.json()
        results = data.get('results', [])

        print(f"✅ API Response successful")
        print(f"📊 Results array length: {len(results)}")

        if results:
            ad = results[0]
            print(f"\n📋 FIRST AD STRUCTURE:")
            print(f"Top-level keys: {list(ad.keys())}")

            if 'snapshot' in ad:
                snapshot = ad['snapshot']
                print(f"\n✅ Snapshot field found: {type(snapshot)}")
                if snapshot is None:
                    print("   ❌ Snapshot is null")
                elif isinstance(snapshot, dict):
                    print(f"   ✅ Snapshot keys: {list(snapshot.keys())}")

                    # Check specific fields you mentioned
                    if 'body' in snapshot:
                        body = snapshot['body']
                        print(f"   📝 Body: {type(body)} = {body}")
                        if isinstance(body, dict) and 'text' in body:
                            print(f"      ✅ Body text: '{body['text'][:100]}...'")

                    if 'cta_text' in snapshot:
                        print(f"   🎯 CTA: '{snapshot['cta_text']}'")

                    if 'cards' in snapshot:
                        cards = snapshot['cards']
                        print(f"   📇 Cards: {len(cards)} items")
                        if cards:
                            first_card = cards[0]
                            print(f"      First card keys: {list(first_card.keys()) if isinstance(first_card, dict) else type(first_card)}")
                            if isinstance(first_card, dict) and 'title' in first_card:
                                print(f"      First card title: '{first_card['title']}'")
                else:
                    print(f"   ❌ Snapshot is not a dict: {snapshot}")
            else:
                print(f"❌ No snapshot field")

        else:
            print("❌ No results in response")

    else:
        print(f"❌ API call failed: {response.status_code}")
        print(f"Response: {response.text[:200]}")

if __name__ == "__main__":
    main()