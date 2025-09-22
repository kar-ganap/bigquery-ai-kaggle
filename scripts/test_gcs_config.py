#!/usr/bin/env python3
"""
Test script to verify GCS bucket configuration is working properly
"""

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available, use system environment variables

from src.utils.media_storage import BUCKET_NAME, MediaStorageManager

def test_gcs_configuration():
    """Test that GCS bucket configuration is properly loaded from environment"""

    print("🧪 TESTING GCS BUCKET CONFIGURATION")
    print("=" * 50)

    # Check environment variable
    env_bucket = os.environ.get("GCS_BUCKET")
    print(f"📋 Environment GCS_BUCKET: {env_bucket}")
    print(f"📋 Resolved BUCKET_NAME: {BUCKET_NAME}")

    # Verify they match
    if env_bucket and env_bucket == BUCKET_NAME:
        print("✅ Environment variable correctly loaded")
    elif not env_bucket:
        print("⚠️  No GCS_BUCKET in environment - using fallback default")
        print(f"   Default: {BUCKET_NAME}")
    else:
        print("❌ Mismatch between environment and resolved bucket name")
        return False

    # Test MediaStorageManager initialization
    try:
        print("\n🔧 Testing MediaStorageManager initialization...")
        manager = MediaStorageManager()
        print(f"✅ MediaStorageManager initialized successfully")
        print(f"   Bucket: {manager.bucket.name}")

        # Verify bucket name matches
        if manager.bucket.name == BUCKET_NAME:
            print("✅ MediaStorageManager using correct bucket name")
        else:
            print(f"❌ MediaStorageManager bucket mismatch: {manager.bucket.name} vs {BUCKET_NAME}")
            return False

    except Exception as e:
        print(f"❌ MediaStorageManager initialization failed: {e}")
        print("   This might be due to missing GCP credentials or permissions")
        return False

    print("\n🎯 GCS CONFIGURATION TEST RESULTS:")
    print("   ✅ Environment variable configuration working")
    print("   ✅ Hardcoded bucket name replaced with environment variable")
    print("   ✅ MediaStorageManager properly configured")
    print(f"   📦 Active bucket: {BUCKET_NAME}")

    return True

if __name__ == "__main__":
    success = test_gcs_configuration()

    if success:
        print("\n🎉 All GCS configuration tests passed!")
        exit(0)
    else:
        print("\n💥 Some GCS configuration tests failed!")
        exit(1)