#!/usr/bin/env python3
"""
Test script for Phase 1: Core Pipeline Infrastructure
Validates all components are working correctly
"""

import subprocess
import json
import os
from pathlib import Path


def test_cli_help():
    """Test CLI help functionality"""
    print("🧪 Testing CLI help...")
    result = subprocess.run(
        ["python", "scripts/compete_intel_pipeline.py", "--help"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "--brand" in result.stdout
    assert "--vertical" in result.stdout
    print("✅ CLI help working")


def test_dry_run_execution():
    """Test dry-run mode execution"""
    print("\n🧪 Testing dry-run execution...")
    result = subprocess.run(
        ["python", "scripts/compete_intel_pipeline.py", "--brand", "TestBrand", "--dry-run"],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0
    assert "PIPELINE COMPLETE" in result.stdout
    assert "DRY RUN MODE" in result.stdout
    print("✅ Dry-run execution working")


def test_progress_tracking():
    """Test progress tracking display"""
    print("\n🧪 Testing progress tracking...")
    result = subprocess.run(
        ["python", "scripts/compete_intel_pipeline.py", "--brand", "TestBrand", "--dry-run"],
        capture_output=True,
        text=True
    )
    
    # Check all 6 stages are displayed
    for stage in range(1, 7):
        assert f"STAGE {stage}/6" in result.stdout
    
    # Check progress percentages
    assert "Progress: 0%" in result.stdout
    assert "Progress: 83%" in result.stdout
    
    print("✅ Progress tracking working")


def test_output_file_generation():
    """Test output file generation"""
    print("\n🧪 Testing output file generation...")
    
    # Clean up any existing test files
    test_brand = "TestBrand"
    output_dir = Path("data/output")
    
    # Run pipeline
    result = subprocess.run(
        ["python", "scripts/compete_intel_pipeline.py", "--brand", test_brand, "--dry-run"],
        capture_output=True,
        text=True
    )
    
    # Find generated files
    report_files = list(output_dir.glob(f"{test_brand.lower()}*_report.json"))
    sql_files = list(output_dir.glob(f"{test_brand.lower()}*_dashboards.sql"))
    
    assert len(report_files) > 0, "No report file generated"
    assert len(sql_files) > 0, "No SQL dashboard file generated"
    
    # Validate JSON structure
    with open(report_files[0]) as f:
        report = json.load(f)
    
    assert report["brand"] == test_brand
    assert "output" in report
    assert "level_1" in report["output"]
    assert "level_2" in report["output"]
    assert "level_3" in report["output"]
    assert "level_4" in report["output"]
    
    print("✅ Output file generation working")


def test_error_handling():
    """Test error handling"""
    print("\n🧪 Testing error handling...")
    
    # Test missing required argument
    result = subprocess.run(
        ["python", "scripts/compete_intel_pipeline.py"],
        capture_output=True,
        text=True
    )
    assert result.returncode != 0
    assert "required" in result.stderr.lower()
    
    print("✅ Error handling working")


def test_json_output_format():
    """Test JSON output format"""
    print("\n🧪 Testing JSON output format...")
    
    result = subprocess.run(
        ["python", "scripts/compete_intel_pipeline.py", 
         "--brand", "TestBrand", 
         "--output-format", "json", 
         "--dry-run"],
        capture_output=True,
        text=True
    )
    
    # Extract JSON from output (after the headers)
    lines = result.stdout.split('\n')
    json_start = None
    for i, line in enumerate(lines):
        if line.strip().startswith('{'):
            json_start = i
            break
    
    if json_start:
        json_lines = []
        brace_count = 0
        for line in lines[json_start:]:
            json_lines.append(line)
            brace_count += line.count('{') - line.count('}')
            if brace_count == 0 and len(json_lines) > 1:
                break
        
        json_str = '\n'.join(json_lines)
        output = json.loads(json_str)
        
        assert "level_1" in output
        assert "level_2" in output
    
    print("✅ JSON output format working")


def run_all_tests():
    """Run all infrastructure tests"""
    print("=" * 60)
    print("🚀 PHASE 1 INFRASTRUCTURE TESTS")
    print("=" * 60)
    
    tests = [
        ("CLI Help", test_cli_help),
        ("Dry-run Execution", test_dry_run_execution),
        ("Progress Tracking", test_progress_tracking),
        ("Output File Generation", test_output_file_generation),
        ("Error Handling", test_error_handling),
        ("JSON Output Format", test_json_output_format)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"❌ {test_name} failed: {e}")
            failed += 1
    
    print("\n" + "=" * 60)
    print("📊 TEST SUMMARY")
    print("=" * 60)
    print(f"✅ Passed: {passed}/{len(tests)}")
    print(f"❌ Failed: {failed}/{len(tests)}")
    
    if failed == 0:
        print("\n🎉 ALL INFRASTRUCTURE TESTS PASSED!")
        print("Phase 1 complete - ready for Phase 2 (Stage Integration)")
    else:
        print(f"\n⚠️  {failed} tests failed - fix before proceeding")
    
    return failed == 0


if __name__ == "__main__":
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)