#!/usr/bin/env python3
"""
Analyze corpus growth and data quality across multiple pipeline runs
"""
import os
from src.utils.bigquery_client import run_query

def analyze_corpus_growth():
    """Comprehensive analysis of corpus growth and data quality"""

    BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
    BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

    print("📈 COMPETITIVE INTELLIGENCE CORPUS GROWTH ANALYSIS")
    print("=" * 60)

    # 1. Overall corpus size and growth
    growth_query = f"""
    SELECT
        COUNT(*) as total_ads,
        COUNT(CASE WHEN media_storage_path IS NOT NULL THEN 1 END) as ads_with_media,
        COUNT(DISTINCT brand) as unique_brands,
        COUNT(DISTINCT ad_archive_id) as unique_ad_ids,
        MIN(start_date) as earliest_ad,
        MAX(start_date) as latest_ad,
        DATE_DIFF(MAX(start_date), MIN(start_date), DAY) as temporal_span_days
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    """

    growth_result = run_query(growth_query)
    if not growth_result.empty:
        row = growth_result.iloc[0]
        print(f"📊 CORPUS OVERVIEW:")
        print(f"   Total ads: {row['total_ads']}")
        print(f"   Ads with media: {row['ads_with_media']}")
        print(f"   Unique brands: {row['unique_brands']}")
        print(f"   Unique ad IDs: {row['unique_ad_ids']}")
        print(f"   Temporal span: {row['temporal_span_days']} days ({row['earliest_ad']} to {row['latest_ad']})")
        print(f"   Media coverage: {(row['ads_with_media']/row['total_ads']*100):.1f}%")

    # 2. Brand-level breakdown
    brand_query = f"""
    SELECT
        brand,
        COUNT(*) as total_ads,
        COUNT(CASE WHEN media_storage_path IS NOT NULL THEN 1 END) as ads_with_media,
        COUNT(DISTINCT ad_archive_id) as unique_ad_ids,
        MIN(start_date) as earliest_ad,
        MAX(start_date) as latest_ad,
        COUNT(DISTINCT EXTRACT(DATE FROM start_date)) as active_days
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    GROUP BY brand
    ORDER BY total_ads DESC
    """

    brand_result = run_query(brand_query)
    if not brand_result.empty:
        print(f"\n🏢 BRAND-LEVEL COMPETITIVE INTELLIGENCE:")
        for _, row in brand_result.iterrows():
            print(f"   • {row['brand']}: {row['total_ads']} ads ({row['ads_with_media']} with media)")
            print(f"     - Unique ads: {row['unique_ad_ids']}")
            print(f"     - Active period: {row['earliest_ad']} to {row['latest_ad']} ({row['active_days']} days)")
            print(f"     - Media coverage: {(row['ads_with_media']/row['total_ads']*100):.1f}%")

    # 3. Media type distribution
    media_query = f"""
    SELECT
        media_type,
        COUNT(*) as count,
        COUNT(DISTINCT brand) as brands_using,
        STRING_AGG(DISTINCT brand ORDER BY brand) as brand_list
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    WHERE media_storage_path IS NOT NULL
    GROUP BY media_type
    ORDER BY count DESC
    """

    media_result = run_query(media_query)
    if not media_result.empty:
        print(f"\n🎬 MEDIA TYPE DISTRIBUTION:")
        for _, row in media_result.iterrows():
            print(f"   • {row['media_type']}: {row['count']} ads ({row['brands_using']} brands)")
            print(f"     Brands: {row['brand_list']}")

    # 4. Temporal distribution and recency
    temporal_query = f"""
    SELECT
        EXTRACT(YEAR FROM start_date) as year,
        EXTRACT(MONTH FROM start_date) as month,
        COUNT(*) as ads_count,
        COUNT(DISTINCT brand) as active_brands,
        COUNT(CASE WHEN media_storage_path IS NOT NULL THEN 1 END) as ads_with_media
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    GROUP BY year, month
    ORDER BY year DESC, month DESC
    LIMIT 12
    """

    temporal_result = run_query(temporal_query)
    if not temporal_result.empty:
        print(f"\n📅 TEMPORAL DISTRIBUTION (Recent 12 months):")
        for _, row in temporal_result.iterrows():
            print(f"   • {int(row['year'])}-{int(row['month']):02d}: {row['ads_count']} ads ({row['active_brands']} brands, {row['ads_with_media']} with media)")

    # 5. Data quality metrics
    quality_query = f"""
    SELECT
        COUNT(*) as total_ads,
        COUNT(CASE WHEN creative_text IS NOT NULL AND LENGTH(creative_text) > 10 THEN 1 END) as meaningful_text,
        COUNT(CASE WHEN media_storage_path IS NOT NULL THEN 1 END) as has_media,
        COUNT(CASE WHEN start_date IS NOT NULL THEN 1 END) as has_start_date,
        COUNT(CASE WHEN brand IS NOT NULL THEN 1 END) as has_brand,
        COUNT(CASE WHEN ad_archive_id IS NOT NULL THEN 1 END) as has_ad_id,
        COUNT(CASE WHEN creative_text IS NOT NULL AND LENGTH(creative_text) > 10
                   AND media_storage_path IS NOT NULL THEN 1 END) as complete_ads
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    """

    quality_result = run_query(quality_query)
    if not quality_result.empty:
        row = quality_result.iloc[0]
        total = row['total_ads']
        print(f"\n✅ DATA QUALITY METRICS:")
        print(f"   Total ads: {total}")
        print(f"   Meaningful text: {row['meaningful_text']} ({row['meaningful_text']/total*100:.1f}%)")
        print(f"   Has media: {row['has_media']} ({row['has_media']/total*100:.1f}%)")
        print(f"   Has start date: {row['has_start_date']} ({row['has_start_date']/total*100:.1f}%)")
        print(f"   Has brand: {row['has_brand']} ({row['has_brand']/total*100:.1f}%)")
        print(f"   Has ad ID: {row['has_ad_id']} ({row['has_ad_id']/total*100:.1f}%)")
        print(f"   Complete ads (text + media): {row['complete_ads']} ({row['complete_ads']/total*100:.1f}%)")

    # 6. Competitive intelligence insights
    insights_query = f"""
    SELECT
        brand,
        COUNT(DISTINCT EXTRACT(DATE FROM start_date)) as campaign_days,
        COUNT(*) as total_creative_variations,
        COUNT(CASE WHEN media_type = 'image' THEN 1 END) as image_ads,
        COUNT(CASE WHEN media_type = 'video' THEN 1 END) as video_ads,
        COUNT(CASE WHEN media_type = 'carousel' THEN 1 END) as carousel_ads,
        AVG(LENGTH(creative_text)) as avg_text_length
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
    WHERE media_storage_path IS NOT NULL
    GROUP BY brand
    ORDER BY total_creative_variations DESC
    """

    insights_result = run_query(insights_query)
    if not insights_result.empty:
        print(f"\n🎯 COMPETITIVE INTELLIGENCE INSIGHTS:")
        for _, row in insights_result.iterrows():
            print(f"   • {row['brand']}:")
            print(f"     - Campaign activity: {row['campaign_days']} days")
            print(f"     - Creative variations: {row['total_creative_variations']}")
            print(f"     - Media mix: {row['image_ads']} images, {row['video_ads']} videos, {row['carousel_ads']} carousels")
            print(f"     - Avg text length: {row['avg_text_length']:.0f} chars")

    print(f"\n🎉 CORPUS ANALYSIS COMPLETE!")
    print(f"Competitive intelligence corpus ready for visual analysis, embeddings, and strategic insights.")

if __name__ == "__main__":
    analyze_corpus_growth()