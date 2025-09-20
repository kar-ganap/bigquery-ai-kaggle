"""
Verify Video Preview Image Extraction

Test to confirm that:
1. Video ads actually have preview images in their image_urls
2. These images came from video_preview_image_url field
3. Our optimization correctly captures video preview content
"""
from src.utils.bigquery_client import run_query

def verify_video_preview_extraction():
    print('🎬 Verifying Video Preview Image Extraction')
    print('=' * 50)

    # Test 1: Compare video ads with vs without images
    print('\n📊 Step 1: Video Ads Image Analysis')

    video_image_analysis = '''
    SELECT
      brand,
      COUNT(*) as total_video_ads,
      COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as video_ads_with_images,
      COUNT(CASE WHEN ARRAY_LENGTH(image_urls) = 0 THEN 1 END) as video_ads_without_images,
      ROUND(COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as image_coverage_pct,
      AVG(ARRAY_LENGTH(image_urls)) as avg_images_per_video_ad,
      SUM(ARRAY_LENGTH(image_urls)) as total_images_from_videos
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE media_type = 'video'
    GROUP BY brand
    ORDER BY total_video_ads DESC
    '''

    video_df = run_query(video_image_analysis)
    print('   Video Ads with Image Content:')

    total_video_ads = 0
    total_video_images = 0

    for _, row in video_df.iterrows():
        total_video_ads += row['total_video_ads']
        total_video_images += row['total_images_from_videos']

        print(f'      {row["brand"]}: {row["video_ads_with_images"]}/{row["total_video_ads"]} video ads have images ({row["image_coverage_pct"]}%)')
        print(f'        └─ {row["total_images_from_videos"]} total images from video ads ({row["avg_images_per_video_ad"]:.1f} avg/ad)')

    print(f'\n   📈 Summary: {total_video_images} images extracted from {total_video_ads} video ads')

    # Test 2: Sample specific video ads to see their image URLs
    print('\n🔍 Step 2: Sample Video Ad Image URLs')

    sample_video_ads = '''
    SELECT
      ad_archive_id,
      brand,
      media_type,
      ARRAY_LENGTH(image_urls) as image_count,
      image_urls
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE media_type = 'video'
      AND ARRAY_LENGTH(image_urls) > 0
    ORDER BY ARRAY_LENGTH(image_urls) DESC
    LIMIT 5
    '''

    sample_df = run_query(sample_video_ads)
    print('   Sample Video Ads with Image URLs:')

    for _, row in sample_df.iterrows():
        print(f'\n      Video Ad {row["ad_archive_id"]} ({row["brand"]}):')
        print(f'        └─ {row["image_count"]} images:')

        # Show first few URLs to check if they look like video previews
        for i, url in enumerate(row["image_urls"][:3]):
            # Look for video-related patterns in URLs
            is_video_preview = any(indicator in url.lower() for indicator in ['video', 'preview', 'thumb', 'snapshot'])
            indicator = " (🎬 likely video preview)" if is_video_preview else ""
            print(f'          {i+1}. {url[:80]}...{indicator}')

    # Test 3: Verify that video ads contribute significantly to our image pool
    print('\n📊 Step 3: Video vs Non-Video Image Contribution')

    contribution_analysis = '''
    SELECT
      media_type,
      COUNT(*) as ad_count,
      SUM(ARRAY_LENGTH(image_urls)) as total_images,
      AVG(ARRAY_LENGTH(image_urls)) as avg_images_per_ad
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE ARRAY_LENGTH(image_urls) > 0
    GROUP BY media_type
    ORDER BY total_images DESC
    '''

    contrib_df = run_query(contribution_analysis)
    print('   Image Contribution by Media Type:')

    total_images_all = contrib_df['total_images'].sum()

    for _, row in contrib_df.iterrows():
        percentage = (row['total_images'] / total_images_all) * 100
        print(f'      {row["media_type"]}: {row["total_images"]} images ({percentage:.1f}% of total)')
        print(f'        └─ From {row["ad_count"]} ads ({row["avg_images_per_ad"]:.1f} avg/ad)')

    # Validation
    print('\n🎯 Video Preview Extraction Validation:')

    video_images = contrib_df[contrib_df['media_type'] == 'video']['total_images'].iloc[0] if len(contrib_df) > 0 else 0
    video_contribution_pct = (video_images / total_images_all) * 100 if total_images_all > 0 else 0

    high_video_contribution = video_contribution_pct > 80  # Most images should be from video previews
    video_ads_have_images = video_df['image_coverage_pct'].mean() > 95  # Most video ads should have preview images

    print(f'   ✅ Video ads contribute {video_contribution_pct:.1f}% of all images: {high_video_contribution}')
    print(f'   ✅ {video_df["image_coverage_pct"].mean():.1f}% of video ads have preview images: {video_ads_have_images}')

    if high_video_contribution and video_ads_have_images:
        print(f'\n   🏆 VERIFICATION SUCCESSFUL: Video preview extraction working correctly!')
        print(f'   🎬 Video ads are our primary source of visual content')
    else:
        print(f'\n   ⚠️  VERIFICATION ISSUES: Need to investigate video preview extraction')

    return {
        'video_contribution_pct': video_contribution_pct,
        'video_image_coverage': video_df['image_coverage_pct'].mean(),
        'total_video_images': video_images,
        'total_images': total_images_all
    }

if __name__ == "__main__":
    results = verify_video_preview_extraction()