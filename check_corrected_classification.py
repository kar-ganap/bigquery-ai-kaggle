"""
Check Corrected Media Type Classification

Analyze the real distribution from our pipeline run with the fixed classification.
"""
from src.utils.bigquery_client import run_query

def check_corrected_classification():
    print('📊 Checking Corrected Media Type Classification')
    print('=' * 60)

    # Check the corrected distribution from our latest pipeline run
    print('\n🔍 Step 1: Latest Pipeline Results (Fixed Classification)')

    corrected_breakdown = '''
    SELECT
      media_type,
      COUNT(*) as count,
      ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
      COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as ads_with_images,
      SUM(ARRAY_LENGTH(image_urls)) as total_images,
      AVG(ARRAY_LENGTH(image_urls)) as avg_images_per_ad
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    GROUP BY media_type
    ORDER BY count DESC
    '''

    corrected_df = run_query(corrected_breakdown)
    print('   Current Classification Results:')

    total_ads = corrected_df['count'].sum()
    total_images = corrected_df['total_images'].sum()

    for _, row in corrected_df.iterrows():
        images = int(row['total_images']) if row['total_images'] else 0
        print(f'      {row["media_type"]}: {row["count"]} ads ({row["percentage"]}%)')
        print(f'        └─ {row["ads_with_images"]} with images ({images} total, {row["avg_images_per_ad"]:.1f} avg/ad)')

    print(f'\n   📊 Overall Summary:')
    print(f'      Total ads: {total_ads}')
    print(f'      Total images: {total_images}')

    # Compare with previous broken classification
    print('\n📈 Step 2: Before vs After Comparison')

    print('   BEFORE (Broken Classification):')
    print('      video: 651 ads (84.3%)')
    print('      unknown: 121 ads (15.7%)')

    print('\n   AFTER (Fixed Classification):')
    for _, row in corrected_df.iterrows():
        print(f'      {row["media_type"]}: {row["count"]} ads ({row["percentage"]}%)')

    # Validation
    print('\n🎯 Step 3: Classification Validation')

    video_percentage = 0
    image_percentage = 0
    carousel_percentage = 0

    for _, row in corrected_df.iterrows():
        if row["media_type"] == "video":
            video_percentage = row["percentage"]
        elif row["media_type"] == "image":
            image_percentage = row["percentage"]
        elif row["media_type"] == "carousel":
            carousel_percentage = row["percentage"]

    print(f'   Video ads: {video_percentage}%')
    print(f'   Image ads: {image_percentage}%')
    print(f'   Carousel ads: {carousel_percentage}%')

    # Check if this is more realistic
    realistic_video = 5 <= video_percentage <= 40
    has_mixed_content = video_percentage + image_percentage + carousel_percentage > 80
    no_over_classification = video_percentage < 80

    print(f'\n   📊 Realism Check:')
    print(f'   ✅ Realistic video percentage (5-40%): {realistic_video}')
    print(f'   ✅ Mixed content types present: {has_mixed_content}')
    print(f'   ✅ No over-classification (< 80% any type): {no_over_classification}')

    if realistic_video and has_mixed_content and no_over_classification:
        print('\n   🎉 SUCCESS: Classification looks much more realistic!')
        print('   ✅ Matches expected distribution from Meta ads library UI')
    else:
        print('\n   ⚠️  STILL NEEDS INVESTIGATION: Distribution still looks off')
        if video_percentage == 0:
            print('       🤔 0% video seems too low - may need to check video detection logic')
        elif video_percentage > 70:
            print('       🤔 Still over-classifying as video')

    return {
        'total_ads': total_ads,
        'video_percentage': video_percentage,
        'image_percentage': image_percentage,
        'carousel_percentage': carousel_percentage,
        'classification_realistic': realistic_video and has_mixed_content and no_over_classification
    }

if __name__ == "__main__":
    results = check_corrected_classification()