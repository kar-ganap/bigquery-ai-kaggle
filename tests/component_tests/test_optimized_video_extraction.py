"""
Test Optimized Video URL Extraction Logic

Verify that our updated logic correctly:
1. Video URLs are eliminated (empty arrays)
2. Video preview images are captured as visual content
3. Total visual assets are maximized for Phase 3
"""
from src.utils.bigquery_client import run_query

def test_optimized_video_extraction():
    print('🎯 Testing Optimized Video URL Extraction Logic')
    print('=' * 60)

    # Test 1: Check current BigQuery data to see video vs image breakdown
    print(f'\n📊 Step 1: BigQuery Visual Content Analysis')

    visual_analysis_query = '''
    SELECT
      media_type,
      COUNT(*) as total_ads,
      COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as ads_with_images,
      SUM(ARRAY_LENGTH(image_urls)) as total_images,
      AVG(ARRAY_LENGTH(image_urls)) as avg_images_per_ad,
      COUNT(CASE WHEN ARRAY_LENGTH(video_urls) > 0 THEN 1 END) as ads_with_videos,
      SUM(ARRAY_LENGTH(video_urls)) as total_videos
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    GROUP BY media_type
    ORDER BY total_ads DESC
    '''

    try:
        visual_df = run_query(visual_analysis_query)
        print('   Current BigQuery Visual Content Breakdown:')

        total_visual_assets = 0

        for _, row in visual_df.iterrows():
            images = int(row['total_images']) if row['total_images'] else 0
            videos = int(row['total_videos']) if row['total_videos'] else 0
            total_visual_assets += images

            print(f'      {row["media_type"]}: {row["total_ads"]} ads')
            print(f'        └─ Images: {images} total ({row["avg_images_per_ad"]:.1f} avg/ad)')
            print(f'        └─ Videos: {videos} total (should be 0 with optimization)')

        print(f'\n   📈 Total Visual Assets Available: {total_visual_assets}')
        print(f'   💰 Potential Phase 3 Budget (at $0.10/image): ${total_visual_assets * 0.10:.2f}')

        # Test 2: Verify video preview images are captured
        print(f'\n🎬 Step 2: Video Preview Image Verification')

        video_preview_query = '''
        SELECT
          brand,
          COUNT(*) as video_ads,
          COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as video_ads_with_preview_images,
          ROUND(COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as preview_coverage_pct
        FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
        WHERE media_type = 'video'
        GROUP BY brand
        ORDER BY video_ads DESC
        '''

        preview_df = run_query(video_preview_query)
        print('   Video Preview Image Coverage by Brand:')

        for _, row in preview_df.iterrows():
            coverage = row['preview_coverage_pct']
            status = "✅" if coverage > 80 else "⚠️" if coverage > 50 else "❌"
            print(f'      {status} {row["brand"]}: {row["video_ads_with_preview_images"]}/{row["video_ads"]} ({coverage}%)')

        avg_coverage = preview_df['preview_coverage_pct'].mean()
        print(f'\n   📊 Average Video Preview Coverage: {avg_coverage:.1f}%')

        # Validation Results
        print(f'\n🎯 Optimization Validation Results:')

        all_videos_zero = visual_df['total_videos'].sum() == 0
        high_preview_coverage = avg_coverage > 70
        sufficient_visual_content = total_visual_assets > 500

        print(f'   ✅ Video URLs eliminated (as expected): {all_videos_zero}')
        print(f'   ✅ High video preview coverage: {high_preview_coverage} ({avg_coverage:.1f}%)')
        print(f'   ✅ Sufficient visual content for Phase 3: {sufficient_visual_content} ({total_visual_assets} assets)')

        overall_success = all_videos_zero and high_preview_coverage and sufficient_visual_content
        status = "✅ OPTIMIZATION SUCCESSFUL" if overall_success else "❌ Issues detected"
        print(f'\n   🏆 Overall Status: {status}')

        return overall_success

    except Exception as e:
        print(f'   ❌ BigQuery analysis failed: {str(e)}')
        return False

if __name__ == "__main__":
    success = test_optimized_video_extraction()
    if success:
        print(f'\n🚀 Ready to proceed with Phase 3 implementation!')
    else:
        print(f'\n⚠️  Need to address issues before Phase 3')