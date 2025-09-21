#!/usr/bin/env python3
"""
Test Schema Propagation - Verify that computed_media_type and media_storage_path
are being included in the DataFrame sent to BigQuery.
"""

import sys
import pandas as pd
from src.pipeline.stages.ingestion import IngestionStage
from src.pipeline.core.base import PipelineContext
from src.pipeline.models.candidates import ValidatedCompetitor

def test_schema_propagation():
    """Test that new fields are propagated through the ingestion pipeline"""

    print("🧪 TESTING SCHEMA PROPAGATION")
    print("=" * 50)

    # Create a test context
    context = PipelineContext(
        brand="TestBrand",
        vertical="test",
        run_id="test_123"
    )

    # Create ingestion stage with media storage enabled
    stage = IngestionStage(context, dry_run=False)

    # Create a test ad
    test_ad = {
        'ad_id': 'test_ad_123',
        'ad_archive_id': 'test_ad_123',
        'brand': 'TestBrand',
        'creative_text': 'Test ad text',
        'title': 'Test title',
        'original_image_url': 'https://example.com/image.jpg',
        'resized_image_url': None,
        'video_preview_image_url': None,
        'media_type': 'image',
        'snapshot_url': 'https://example.com/snapshot',
        'publisher_platforms': ['facebook'],
        'impressions': {'lower_bound': 100, 'upper_bound': 1000},
        'spend': {'lower_bound': 10, 'upper_bound': 100},
        'currency': 'USD'
    }

    # Test _normalize_ad_data
    print("\n1️⃣ Testing _normalize_ad_data...")
    normalized = stage._normalize_ad_data(test_ad, 'TestBrand')

    print(f"   Normalized ad keys: {list(normalized.keys())}")
    print(f"   Has computed_media_type: {'computed_media_type' in normalized}")
    print(f"   Has media_storage_path: {'media_storage_path' in normalized}")

    if 'computed_media_type' in normalized:
        print(f"   computed_media_type value: {normalized['computed_media_type']}")
    if 'media_storage_path' in normalized:
        print(f"   media_storage_path value: {normalized['media_storage_path']}")

    # Test DataFrame creation
    print("\n2️⃣ Testing DataFrame creation...")
    all_ads = [normalized]

    # Simulate the same preparation logic as in the ingestion stage
    prepared_ads = []
    for ad in all_ads:
        prepared_ad = ad.copy()

        # Same processing as in ingestion.py
        if 'image_urls' in prepared_ad and isinstance(prepared_ad['image_urls'], list):
            prepared_ad['image_urls_json'] = str(prepared_ad['image_urls'])
            prepared_ad['image_url'] = prepared_ad['image_urls'][0] if prepared_ad['image_urls'] else None
            del prepared_ad['image_urls']

        if 'video_urls' in prepared_ad and isinstance(prepared_ad['video_urls'], list):
            prepared_ad['video_urls_json'] = str(prepared_ad['video_urls'])
            prepared_ad['video_url'] = prepared_ad['video_urls'][0] if prepared_ad['video_urls'] else None
            del prepared_ad['video_urls']

        prepared_ads.append(prepared_ad)

    ads_df = pd.DataFrame(prepared_ads)

    print(f"   DataFrame columns: {list(ads_df.columns)}")
    print(f"   Has computed_media_type column: {'computed_media_type' in ads_df.columns}")
    print(f"   Has media_storage_path column: {'media_storage_path' in ads_df.columns}")

    # Check values
    if 'computed_media_type' in ads_df.columns:
        print(f"   computed_media_type in DataFrame: {ads_df['computed_media_type'].iloc[0]}")
    if 'media_storage_path' in ads_df.columns:
        print(f"   media_storage_path in DataFrame: {ads_df['media_storage_path'].iloc[0]}")

    # Summary
    print("\n📊 SUMMARY")
    print("=" * 30)

    has_fields = ('computed_media_type' in ads_df.columns and
                  'media_storage_path' in ads_df.columns)

    if has_fields:
        print("   ✅ Schema propagation is WORKING!")
        print("   ✅ New fields are in the DataFrame")
        print("   ℹ️  The issue must be elsewhere (e.g., MediaStorageManager disabled)")
    else:
        print("   ❌ Schema propagation FAILED!")
        print("   ❌ New fields are missing from DataFrame")
        print("   🔍 Need to debug _classify_and_store_media method")

    return has_fields

if __name__ == "__main__":
    success = test_schema_propagation()
    sys.exit(0 if success else 1)