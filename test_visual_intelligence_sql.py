#!/usr/bin/env python3
"""
Test Visual Intelligence SQL Generation

Tests the SQL generation without executing to identify the LENGTH vs ARRAY_LENGTH issue.
"""

import os
from src.pipeline.core.base import PipelineContext
from src.pipeline.stages.visual_intelligence import VisualIntelligenceStage

def test_sql_generation():
    """Test SQL generation for visual intelligence"""

    print("🧪 Testing Visual Intelligence SQL Generation")
    print("=" * 50)

    # Create mock context
    context = PipelineContext("test", "test_run")

    # Create visual intelligence stage
    stage = VisualIntelligenceStage(context)

    print("📄 Testing adaptive sampling SQL generation...")
    try:
        sampling_sql = stage._generate_adaptive_sampling_sql()
        print("✅ Adaptive sampling SQL generated successfully")

        # Check for LENGTH vs ARRAY_LENGTH issues
        lines = sampling_sql.split('\n')
        for i, line in enumerate(lines, 1):
            if 'LENGTH(' in line and 'image_urls' in line:
                print(f"⚠️  Found potential issue at line {i}: {line.strip()}")
            elif 'ARRAY_LENGTH(' in line and 'image_urls' in line:
                print(f"✅ Correct ARRAY_LENGTH usage at line {i}: {line.strip()}")

    except Exception as e:
        print(f"❌ Adaptive sampling SQL generation failed: {e}")
        return False

    print("\n📄 Testing visual analysis SQL generation...")
    try:
        analysis_sql = stage._generate_visual_analysis_sql()
        print("✅ Visual analysis SQL generated successfully")

        # Check for LENGTH vs ARRAY_LENGTH issues
        lines = analysis_sql.split('\n')
        for i, line in enumerate(lines, 1):
            if 'LENGTH(' in line and 'image_urls' in line:
                print(f"⚠️  Found potential issue at line {i}: {line.strip()}")
            elif 'ARRAY_LENGTH(' in line and 'image_urls' in line:
                print(f"✅ Correct ARRAY_LENGTH usage at line {i}: {line.strip()}")

    except Exception as e:
        print(f"❌ Visual analysis SQL generation failed: {e}")
        return False

    print("\n✅ All SQL generation tests passed!")
    return True

if __name__ == "__main__":
    success = test_sql_generation()
    if success:
        print("\n🎉 Visual Intelligence SQL test completed successfully!")
    else:
        print("\n💥 Visual Intelligence SQL test failed!")