#!/usr/bin/env python3
"""
Test creative content extraction fix
"""
import sys
sys.path.append('src')

from utils.ads_fetcher import MetaAdsFetcher

def main():
    print("🧪 TESTING CREATIVE CONTENT EXTRACTION FIX")
    print("=" * 60)

    # Test with a smaller fetch to avoid too much API usage
    fetcher = MetaAdsFetcher()

    print("🎯 Testing with EyeBuyDirect (should have rich content)...")

    try:
        ads, result = fetcher.fetch_company_ads_with_metadata(
            company_name="EyeBuyDirect",
            max_ads=5,  # Small test sample
            max_pages=1
        )

        if result.get('success') and ads:
            print(f"✅ Fetched {len(ads)} ads successfully")

            print("\n🔍 ANALYZING EXTRACTED FIELDS:")
            for i, ad in enumerate(ads[:3], 1):  # Show first 3 ads
                print(f"\n📋 AD #{i} ({ad.get('ad_archive_id', 'No ID')}):")
                print(f"   Creative Text: '{ad.get('creative_text', '')}' ({len(ad.get('creative_text', ''))} chars)")
                print(f"   Title: '{ad.get('title', '')}' ({len(ad.get('title', ''))} chars)")
                print(f"   CTA Text: '{ad.get('cta_text', '')}' ({len(ad.get('cta_text', ''))} chars)")
                print(f"   Body Text: '{ad.get('body_text', '')}' ({len(ad.get('body_text', ''))} chars)")
                print(f"   Card Titles: '{ad.get('card_titles', '')}' ({len(ad.get('card_titles', ''))} chars)")

                # Check if we have any content
                has_content = any([
                    ad.get('creative_text', ''),
                    ad.get('title', ''),
                    ad.get('cta_text', ''),
                    ad.get('body_text', ''),
                    ad.get('card_titles', '')
                ])

                if has_content:
                    print(f"   ✅ HAS CONTENT!")
                else:
                    print(f"   ❌ NO CONTENT EXTRACTED")

            # Summary
            total_with_creative = sum(1 for ad in ads if ad.get('creative_text', ''))
            total_with_title = sum(1 for ad in ads if ad.get('title', ''))
            total_with_cta = sum(1 for ad in ads if ad.get('cta_text', ''))
            total_with_body = sum(1 for ad in ads if ad.get('body_text', ''))
            total_with_cards = sum(1 for ad in ads if ad.get('card_titles', ''))

            print(f"\n📊 EXTRACTION SUMMARY ({len(ads)} ads):")
            print(f"   Creative Text: {total_with_creative}/{len(ads)} ads ({total_with_creative/len(ads)*100:.1f}%)")
            print(f"   Title: {total_with_title}/{len(ads)} ads ({total_with_title/len(ads)*100:.1f}%)")
            print(f"   CTA Text: {total_with_cta}/{len(ads)} ads ({total_with_cta/len(ads)*100:.1f}%)")
            print(f"   Body Text: {total_with_body}/{len(ads)} ads ({total_with_body/len(ads)*100:.1f}%)")
            print(f"   Card Titles: {total_with_cards}/{len(ads)} ads ({total_with_cards/len(ads)*100:.1f}%)")

        else:
            print(f"❌ Failed to fetch ads: {result.get('error', 'Unknown error')}")

    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()