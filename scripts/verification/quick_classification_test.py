"""
Quick Media Type Classification Test

Test our fixed classification logic by directly using the MetaAdsFetcher
on a small sample to see the corrected distribution immediately.
"""
from src.utils.ads_fetcher import MetaAdsFetcher
import json

def quick_classification_test():
    print('⚡ Quick Media Type Classification Test')
    print('=' * 50)

    # Initialize fetcher with our fixed logic
    fetcher = MetaAdsFetcher()

    # Test with a small sample from Warby Parker
    print('\n📊 Testing Fixed Classification Logic')
    print('   Fetching 20 ads from Warby Parker...')

    try:
        # fetch_company_ads_with_metadata returns (ads_list, fetch_result_dict)
        ads_list, result_dict = fetcher.fetch_company_ads_with_metadata(
            company_name="warby parker",
            page_id="154835581336063",
            max_pages=1  # Just first page for quick test
        )

        if ads_list and len(ads_list) > 0:
            print(f'   ✅ Fetched {len(ads_list)} ads successfully')

            # Analyze classification distribution
            video_count = 0
            image_count = 0
            carousel_count = 0
            unknown_count = 0

            video_with_images = 0
            image_with_images = 0

            print('\n   📋 Classification Results:')

            for i, ad in enumerate(ads_list[:15]):  # Show first 15
                media_type = ad.get("media_type", "unknown")
                image_count_ad = len(ad.get("image_urls", []))
                video_count_ad = len(ad.get("video_urls", []))

                # Count by type
                if media_type == "video":
                    video_count += 1
                    if image_count_ad > 0:
                        video_with_images += 1
                    indicator = "🎬"
                elif media_type == "image":
                    image_count += 1
                    if image_count_ad > 0:
                        image_with_images += 1
                    indicator = "🖼️"
                elif media_type == "carousel":
                    carousel_count += 1
                    indicator = "📸"
                else:
                    unknown_count += 1
                    indicator = "❓"

                print(f'      {indicator} Ad {i+1}: {media_type} ({image_count_ad} imgs, {video_count_ad} vids)')

            total_ads = len(ads_list)

            print(f'\n   📊 Distribution Summary ({total_ads} ads):')
            print(f'      🎬 Video: {video_count} ({video_count/total_ads*100:.1f}%)')
            print(f'      🖼️  Image: {image_count} ({image_count/total_ads*100:.1f}%)')
            print(f'      📸 Carousel: {carousel_count} ({carousel_count/total_ads*100:.1f}%)')
            print(f'      ❓ Unknown: {unknown_count} ({unknown_count/total_ads*100:.1f}%)')

            print(f'\n   🖼️  Visual Content Analysis:')
            print(f'      Video ads with images: {video_with_images}/{video_count} ({video_with_images/video_count*100 if video_count > 0 else 0:.1f}%)')
            print(f'      Image ads with images: {image_with_images}/{image_count} ({image_with_images/image_count*100 if image_count > 0 else 0:.1f}%)')

            # Validation
            print(f'\n   🎯 Fix Validation:')

            video_percentage = video_count / total_ads * 100
            if video_percentage < 80:
                print(f'   ✅ More balanced classification: {video_percentage:.1f}% video (was 84.3%)')
                print('   ✅ Fix appears to be working!')
            else:
                print(f'   ⚠️  Still heavily video: {video_percentage:.1f}% (investigate further)')

            return {
                'total_ads': total_ads,
                'video_percentage': video_percentage,
                'image_percentage': image_count / total_ads * 100,
                'unknown_percentage': unknown_count / total_ads * 100,
                'classification_improved': video_percentage < 80
            }

        else:
            print(f'   ❌ Failed to fetch ads: {result.error if hasattr(result, "error") else "Unknown error"}')
            return None

    except Exception as e:
        print(f'   ❌ Test failed with error: {str(e)}')
        return None

if __name__ == "__main__":
    results = quick_classification_test()
    if results:
        if results['classification_improved']:
            print('\n🎉 SUCCESS: Media type classification fix is working!')
        else:
            print('\n🤔 Results still showing high video percentage - may need further investigation')