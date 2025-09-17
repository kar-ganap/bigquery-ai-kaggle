"""
Investigate Media Type Classification Discrepancy

The user's manual examination of Meta ads UI shows mixed content,
but our data shows 100% video ads have images. Let's investigate:

1. Are we missing regular image ads?
2. Is media_type classification wrong?
3. What does the raw data actually look like?
"""
from src.utils.bigquery_client import run_query

def investigate_media_type_discrepancy():
    print('🔍 Investigating Media Type Classification Discrepancy')
    print('=' * 60)

    # Step 1: Check ALL ads breakdown
    print('\n📊 Step 1: Complete Ads Breakdown')

    total_breakdown = '''
    SELECT
      media_type,
      COUNT(*) as total_ads,
      COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as ads_with_images,
      COUNT(CASE WHEN ARRAY_LENGTH(image_urls) = 0 THEN 1 END) as ads_without_images,
      ROUND(COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as image_coverage_pct
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    GROUP BY media_type
    ORDER BY total_ads DESC
    '''

    breakdown_df = run_query(total_breakdown)
    print('   Complete Media Type Breakdown:')

    total_all_ads = breakdown_df['total_ads'].sum()

    for _, row in breakdown_df.iterrows():
        pct_of_total = (row['total_ads'] / total_all_ads) * 100
        print(f'      {row["media_type"]}: {row["total_ads"]} ads ({pct_of_total:.1f}% of all ads)')
        print(f'        ├─ With images: {row["ads_with_images"]} ({row["image_coverage_pct"]}%)')
        print(f'        └─ Without images: {row["ads_without_images"]}')

    print(f'\n   📊 Total ads in dataset: {total_all_ads}')

    # Step 2: Check what "unknown" media type actually contains
    print('\n🤔 Step 2: Unknown Media Type Investigation')

    unknown_sample = '''
    SELECT
      ad_archive_id,
      brand,
      media_type,
      creative_text,
      title,
      ARRAY_LENGTH(image_urls) as image_count,
      ARRAY_LENGTH(video_urls) as video_count
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE media_type = 'unknown'
    LIMIT 10
    '''

    unknown_df = run_query(unknown_sample)
    print('   Sample "Unknown" Media Type Ads:')

    for _, row in unknown_df.iterrows():
        print(f'\n      Ad {row["ad_archive_id"]} ({row["brand"]}):')
        print(f'        └─ Images: {row["image_count"]}, Videos: {row["video_count"]}')
        print(f'        └─ Text: "{row["creative_text"][:50] if row["creative_text"] else "None"}..."')
        print(f'        └─ Title: "{row["title"][:50] if row["title"] else "None"}..."')

    # Step 3: Check what happens if we look at raw data before media_type classification
    print('\n📋 Step 3: Raw Data Media Type Source Check')

    raw_source_check = '''
    SELECT
      COUNT(*) as total_raw_ads,
      COUNT(CASE WHEN image_url IS NOT NULL OR image_urls_json IS NOT NULL THEN 1 END) as has_image_fields,
      COUNT(CASE WHEN video_url IS NOT NULL OR video_urls_json IS NOT NULL THEN 1 END) as has_video_fields,
      COUNT(CASE WHEN
        (image_url IS NOT NULL OR image_urls_json IS NOT NULL) AND
        (video_url IS NOT NULL OR video_urls_json IS NOT NULL)
      THEN 1 END) as has_both_fields
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_raw`
    '''

    raw_df = run_query(raw_source_check)
    print('   Raw Data Field Analysis:')

    for _, row in raw_df.iterrows():
        print(f'      Total raw ads: {row["total_raw_ads"]}')
        print(f'      With image fields: {row["has_image_fields"]}')
        print(f'      With video fields: {row["has_video_fields"]}')
        print(f'      With both fields: {row["has_both_fields"]}')

    # Step 4: Check how ads_fetcher logic classifies media_type
    print('\n🔍 Step 4: Media Type Classification Logic Check')

    classification_check = '''
    SELECT
      media_type,
      COUNT(*) as count,
      -- Sample some creative text to see patterns
      ARRAY_AGG(creative_text LIMIT 3) as sample_texts
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    GROUP BY media_type
    ORDER BY count DESC
    '''

    class_df = run_query(classification_check)
    print('   Media Type Classification Patterns:')

    for _, row in class_df.iterrows():
        print(f'\n      {row["media_type"]} ({row["count"]} ads):')
        for i, text in enumerate(row["sample_texts"]):
            if text:
                print(f'        └─ Sample {i+1}: "{text[:60]}..."')

    # Step 5: The key question - are regular image ads missing?
    print('\n❓ Step 5: Regular Image Ads Investigation')

    # Let's check if there are ads that should be classified as "image" but aren't
    potential_image_ads = '''
    SELECT
      COUNT(*) as total_with_images,
      COUNT(CASE WHEN media_type = 'video' THEN 1 END) as classified_as_video,
      COUNT(CASE WHEN media_type = 'unknown' THEN 1 END) as classified_as_unknown,
      COUNT(CASE WHEN media_type = 'image' THEN 1 END) as classified_as_image
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    WHERE ARRAY_LENGTH(image_urls) > 0
    '''

    potential_df = run_query(potential_image_ads)
    print('   Ads with Images - Classification Breakdown:')

    for _, row in potential_df.iterrows():
        total = row['total_with_images']
        print(f'      Total ads with images: {total}')
        print(f'      ├─ Classified as "video": {row["classified_as_video"]} ({row["classified_as_video"]/total*100:.1f}%)')
        print(f'      ├─ Classified as "image": {row["classified_as_image"]} ({row["classified_as_image"]/total*100:.1f}%)')
        print(f'      └─ Classified as "unknown": {row["classified_as_unknown"]} ({row["classified_as_unknown"]/total*100:.1f}%)')

    # Summary
    print('\n🎯 Investigation Summary:')

    video_dominance = breakdown_df[breakdown_df['media_type'] == 'video']['total_ads'].iloc[0] if len(breakdown_df) > 0 else 0
    total_ads = breakdown_df['total_ads'].sum()
    video_percentage = (video_dominance / total_ads) * 100

    if video_percentage > 80:
        print(f'   🚨 ISSUE FOUND: {video_percentage:.1f}% of ads classified as "video"')
        print(f'   🤔 This suggests media_type classification may be over-aggressive')
        print(f'   💡 Regular image ads might be misclassified as video ads')
    else:
        print(f'   ✅ Media type distribution looks reasonable: {video_percentage:.1f}% video')

    return {
        'total_ads': total_ads,
        'video_percentage': video_percentage,
        'unknown_ads': breakdown_df[breakdown_df['media_type'] == 'unknown']['total_ads'].iloc[0] if len(breakdown_df[breakdown_df['media_type'] == 'unknown']) > 0 else 0
    }

if __name__ == "__main__":
    results = investigate_media_type_discrepancy()