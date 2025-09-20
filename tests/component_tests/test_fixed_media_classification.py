"""
Test Fixed Media Type Classification

Show the impact of our fix and what we expect after re-ingestion.
"""
from src.utils.bigquery_client import run_query

def test_fixed_media_classification():
    print('🔧 Media Type Classification Fix Analysis')
    print('=' * 50)

    # Test 1: Check what current BigQuery data looks like before re-ingestion
    print('\n📊 Step 1: Current BigQuery Data (Before Re-ingestion)')

    current_breakdown = '''
    SELECT
      media_type,
      COUNT(*) as count,
      ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
    FROM `bigquery-ai-kaggle-469620.ads_demo.ads_with_dates`
    GROUP BY media_type
    ORDER BY count DESC
    '''

    current_df = run_query(current_breakdown)
    print('   Current Data Classification (with old bug):')

    for _, row in current_df.iterrows():
        print(f'      {row["media_type"]}: {row["count"]} ads ({row["percentage"]}%)')

    # Summary
    print('\n🎯 Media Type Classification Fix Summary:')
    print('   ✅ Removed overly broad "video" in card_str detection')
    print('   ✅ Now only video_preview_image_url determines video ads')
    print('   ✅ Only actual image fields determine image ads')
    print('   🔄 Need to re-run pipeline to see corrected classification in BigQuery')

    print('\n📋 Next Steps:')
    print('   1. Re-run pipeline ingestion with fixed classification')
    print('   2. Verify more balanced image/video distribution')
    print('   3. Proceed with Phase 3 on correctly classified data')

if __name__ == "__main__":
    test_fixed_media_classification()