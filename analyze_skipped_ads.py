#!/usr/bin/env python3
"""
Analyze what types of ads are being skipped to understand if filtering is appropriate
"""
import os
import tempfile
from src.utils.ads_fetcher import MetaAdsFetcher

def analyze_skipped_ads():
    """Analyze a few skipped ads to understand why they're being filtered"""

    print("🔍 ANALYZING SKIPPED ADS TO UNDERSTAND FILTERING LOGIC")
    print("=" * 60)

    fetcher = MetaAdsFetcher()

    # Test with EyeBuyDirect (had 29 skipped ads)
    print("\n📊 Testing EyeBuyDirect (had 29 skipped ads in pipeline run)...")

    # Create a custom version that logs skipped ads instead of filtering them
    company_name = "EyeBuyDirect"

    try:
        # Use the paginated method to get raw ads
        ads = []
        result = None

        for ad in fetcher.fetch_company_ads_paginated(company_name=company_name, max_ads=50, max_pages=2):
            if hasattr(ad, 'company_name') and hasattr(ad, 'success'):  # It's an AdsFetchResult
                result = ad
                break
            else:
                ads.append(ad)

        print(f"✅ Retrieved {len(ads)} raw ads from API")

        # Now analyze what would be skipped
        total_ads = len(ads)
        skipped_ads = []
        kept_ads = []

        for ad in ads:
            # Apply the same filtering logic as the main method
            snapshot = ad.get("snapshot", {}) or {}
            cards = snapshot.get("cards", []) or []

            # Check essential fields
            ad_id = ad.get("ad_archive_id")
            start_date = ad.get("start_date_string")

            skip_reason = None

            if not ad_id:
                skip_reason = "missing_ad_id"
            elif not start_date:
                skip_reason = "missing_start_date"
            else:
                # Check media URLs
                original_url = None
                resized_url = None
                video_preview_url = None

                if cards and len(cards) > 0:
                    first_card = cards[0] if isinstance(cards[0], dict) else {}
                    original_url = first_card.get("original_image_url")
                    resized_url = first_card.get("resized_image_url")
                    video_preview_url = first_card.get("video_preview_image_url")

                if not original_url and not resized_url and not video_preview_url:
                    skip_reason = "missing_all_media_urls"

            if skip_reason:
                skipped_ads.append({
                    'ad_id': ad_id,
                    'start_date': start_date,
                    'skip_reason': skip_reason,
                    'has_cards': len(cards) > 0 if cards else False,
                    'creative_text': str(snapshot.get("body", {}) or {}).get("text", "")[:100],
                    'title': str(snapshot.get("title", ""))[:50],
                    'raw_ad_keys': list(ad.keys())
                })
            else:
                kept_ads.append(ad_id)

        print(f"\n📈 FILTERING ANALYSIS:")
        print(f"   Total raw ads: {total_ads}")
        print(f"   Would keep: {len(kept_ads)}")
        print(f"   Would skip: {len(skipped_ads)}")
        print(f"   Skip rate: {len(skipped_ads)/total_ads*100:.1f}%")

        # Analyze skip reasons
        skip_reasons = {}
        for skipped in skipped_ads:
            reason = skipped['skip_reason']
            if reason not in skip_reasons:
                skip_reasons[reason] = []
            skip_reasons[reason].append(skipped)

        print(f"\n📋 SKIP REASONS BREAKDOWN:")
        for reason, ads_list in skip_reasons.items():
            print(f"   • {reason}: {len(ads_list)} ads")

            # Show examples
            print(f"     Examples:")
            for ad in ads_list[:3]:
                print(f"       - ID: {ad['ad_id']}")
                print(f"         Start: {ad['start_date']}")
                print(f"         Has cards: {ad['has_cards']}")
                print(f"         Text preview: '{ad['creative_text'][:50]}...'")
                print(f"         Title: '{ad['title']}'")
                print()

        # Check if any skipped ads have meaningful content
        meaningful_skipped = [ad for ad in skipped_ads
                            if len(ad['creative_text']) > 20 or len(ad['title']) > 10]

        if meaningful_skipped:
            print(f"🚨 POTENTIAL DATA LOSS:")
            print(f"   {len(meaningful_skipped)} skipped ads have meaningful text content")
            print(f"   Examples of potentially valuable skipped ads:")
            for ad in meaningful_skipped[:5]:
                print(f"     • ID: {ad['ad_id']} - '{ad['creative_text'][:80]}...'")
        else:
            print(f"✅ FILTERING APPEARS APPROPRIATE:")
            print(f"   All skipped ads lack meaningful content or essential fields")

    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_skipped_ads()