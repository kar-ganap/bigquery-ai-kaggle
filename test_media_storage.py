#!/usr/bin/env python3
"""
Quick test of media storage functionality
"""

from src.utils.media_storage import MediaStorageManager

# Mock ad data with different media types
test_ads = [
    {
        'ad_id': 'test_image_ad',
        'original_image_url': None,
        'resized_image_url': 'https://scontent.fbos1-2.fna.fbcdn.net/v/t45.1600-4/469039336_120212853846040924_1234567890123456789_n.jpg?_nc_cat=100&ccb=1-7&_nc_sid=c02e9b&_nc_ohc=abcd1234&_nc_ht=scontent.fbos1-2.fna&oh=00_abcdef&oe=abcdef12',
        'video_preview_image_url': None
    },
    {
        'ad_id': 'test_video_ad',
        'original_image_url': None,
        'resized_image_url': None,
        'video_preview_image_url': 'https://scontent.fbos1-2.fna.fbcdn.net/v/t45.1600-4/video_preview_456789_n.jpg?_nc_cat=100&ccb=1-7&_nc_sid=c02e9b&_nc_ohc=efgh5678&_nc_ht=scontent.fbos1-2.fna&oh=00_ghijkl&oe=ghijkl34'
    },
    {
        'ad_id': 'test_no_media_ad',
        'original_image_url': None,
        'resized_image_url': None,
        'video_preview_image_url': None
    }
]

def test_media_storage():
    """Test media storage classification and download"""

    print("🧪 Testing Media Storage Manager...")

    try:
        manager = MediaStorageManager()
        print("✅ MediaStorageManager initialized successfully")

        for ad in test_ads:
            print(f"\n📱 Testing ad: {ad['ad_id']}")
            try:
                media_type, storage_path = manager.classify_and_store_media(ad, "test_brand")
                print(f"   📊 Media type: {media_type}")
                print(f"   💾 Storage path: {storage_path}")

                if media_type == 'unknown' and storage_path is None:
                    print("   ⚠️  No media detected (expected for no_media_ad)")
                elif storage_path:
                    print("   ✅ Media classified and stored successfully!")
                else:
                    print("   ⚠️  Classification succeeded but storage failed")

            except Exception as e:
                print(f"   ❌ Failed: {str(e)}")

    except Exception as e:
        print(f"❌ MediaStorageManager initialization failed: {str(e)}")
        return False

    return True

if __name__ == "__main__":
    success = test_media_storage()
    if success:
        print("\n🎉 Media storage test completed!")
    else:
        print("\n💥 Media storage test failed!")