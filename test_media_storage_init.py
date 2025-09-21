#!/usr/bin/env python3
"""
Test MediaStorageManager Initialization

This script will test MediaStorageManager initialization to identify
why it's failing in the pipeline and falling back to disabled state.
"""

import sys
import os

def test_media_storage_initialization():
    """Test MediaStorageManager initialization to identify the error"""

    print("🧪 TESTING MEDIASTORAGEMANAGER INITIALIZATION")
    print("=" * 60)

    try:
        print("\n1️⃣ Testing import...")
        from src.utils.media_storage import MediaStorageManager
        print("   ✅ Import successful")

        print("\n2️⃣ Testing initialization...")
        manager = MediaStorageManager()
        print("   ✅ MediaStorageManager initialized successfully")

        print("\n3️⃣ Testing bucket access...")
        bucket_name = manager.bucket.name
        print(f"   ✅ Connected to bucket: {bucket_name}")

        print("\n4️⃣ Testing basic operations...")
        blobs = list(manager.bucket.list_blobs(prefix="ad-media/", max_results=1))
        print(f"   ✅ Bucket access working - found {len(blobs)} files in ad-media/")

        print("\n🎉 MediaStorageManager is working perfectly!")
        print("   The pipeline should not be disabling media storage.")
        print("   The issue must be elsewhere in the schema propagation.")

        return True

    except ImportError as e:
        print(f"   ❌ Import failed: {e}")
        print("   🔍 Check if src.utils.media_storage module exists")
        return False

    except Exception as e:
        print(f"   ❌ MediaStorageManager initialization failed: {e}")
        print(f"   🔍 Error type: {type(e).__name__}")

        # Check specific error types
        if "DefaultCredentialsError" in str(type(e)):
            print("   💡 Google Cloud credentials not found.")
            print("      Run: gcloud auth application-default login")
        elif "Forbidden" in str(e) or "403" in str(e):
            print("   💡 Permission denied accessing GCS bucket.")
            print("      Check if you have Storage Admin role.")
        elif "NotFound" in str(e) or "404" in str(e):
            print("   💡 GCS bucket not found.")
            print("      Check if 'ads-media-storage-bigquery-ai-kaggle' bucket exists.")
        else:
            print("   💡 Unknown error - may be network or configuration issue.")

        return False

if __name__ == "__main__":
    success = test_media_storage_initialization()
    sys.exit(0 if success else 1)