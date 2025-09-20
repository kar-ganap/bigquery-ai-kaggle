#!/usr/bin/env python3
"""
Test the enhanced ads_fetcher with multimodal capabilities
"""
import sys
import os
sys.path.append('src')

from utils.ads_fetcher import MetaAdsFetcher

def test_ads_fetcher_enhancements():
    """Test the new deduplication and multimodal features"""

    print("🧪 Testing Enhanced Ads Fetcher with Multimodal Support")
    print("=" * 60)

    # Initialize fetcher
    fetcher = MetaAdsFetcher()

    # Test with Warby Parker (small sample)
    print("\n📱 Testing with Warby Parker (5 ads)...")
    ads, result = fetcher.fetch_company_ads_with_metadata("Warby Parker", max_ads=5)

    if result["success"]:
        print(f"✅ Fetched {len(ads)} ads successfully")

        # Test first ad for new fields
        if ads:
            ad = ads[0]

            print(f"\n🔍 Analyzing first ad (ID: {ad.get('ad_archive_id', 'N/A')}):")
            print(f"   📝 Creative text length: {len(ad.get('creative_text', ''))}")
            print(f"   🔄 Total unique text parts: {ad.get('total_unique_text_parts', 0)}")
            print(f"   🔁 Has duplicate content: {ad.get('has_duplicate_content', False)}")
            print(f"   🎯 Dedup details: {ad.get('dedup_details', {})}")
            print(f"   🖼️  Image URLs count: {len(ad.get('image_urls', []))}")
            print(f"   🎥 Video URLs count: {len(ad.get('video_urls', []))}")
            print(f"   📋 Card titles: {ad.get('card_titles', 'None')}")
            print(f"   📄 Card bodies: {ad.get('card_bodies', 'None')}")
            print(f"   🎬 Media type: {ad.get('media_type', 'unknown')}")

            # Show image URLs if available
            image_urls = ad.get('image_urls', [])
            if image_urls:
                print(f"\n🖼️  Image URLs found:")
                for i, url in enumerate(image_urls[:3]):  # Show first 3
                    print(f"   {i+1}. {url}")
                if len(image_urls) > 3:
                    print(f"   ... and {len(image_urls) - 3} more")

            # Show creative text sample
            creative_text = ad.get('creative_text', '')
            if creative_text:
                print(f"\n📝 Creative text sample (first 200 chars):")
                print(f"   {creative_text[:200]}{'...' if len(creative_text) > 200 else ''}")

        # Summary statistics
        total_images = sum(len(ad.get('image_urls', [])) for ad in ads)
        total_videos = sum(len(ad.get('video_urls', [])) for ad in ads)
        ads_with_duplicates = sum(1 for ad in ads if ad.get('has_duplicate_content', False))

        print(f"\n📊 Summary Statistics:")
        print(f"   Total ads: {len(ads)}")
        print(f"   Total image URLs: {total_images}")
        print(f"   Total video URLs: {total_videos}")
        print(f"   Ads with duplicate content: {ads_with_duplicates}")
        print(f"   Multimodal coverage: {(total_images + total_videos) / len(ads):.1f} URLs per ad")

    else:
        print(f"❌ Failed to fetch ads: {result.get('error', 'Unknown error')}")

    print("\n" + "=" * 60)
    print("🎯 Test completed!")

if __name__ == "__main__":
    test_ads_fetcher_enhancements()