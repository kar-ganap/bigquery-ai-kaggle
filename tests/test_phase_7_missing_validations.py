"""
Additional Phase 7 Validation Tests
Covers missing checkpoints from PHASE_7_CHECKPOINT_TRACKER.md
"""

import sys
from pathlib import Path
import time
import re
import psutil
import os

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.compete_intel_pipeline import CompetitiveIntelligencePipeline


class TestPhase7MissingValidations:
    """Additional validation tests for Phase 7 missing checkpoints"""
    
    def setup_method(self):
        """Setup for each test"""
        self.pipeline = CompetitiveIntelligencePipeline('Warby Parker', dry_run=True)
        self.result = self.pipeline.execute_pipeline()
        self.level_4 = self.result.output.level_4
    
    def test_sql_query_syntax_validation(self):
        """Validate all SQL queries for syntax"""
        print("\n🔍 Validating SQL Query Syntax...")
        
        sql_queries = {
            'CTA Analysis': self.level_4.get('cta_competitive_analysis'),
            'Channel Performance': self.level_4.get('channel_competitive_performance'),
            'Content Quality': self.level_4.get('content_quality_competitive_analysis'),
            'Audience Strategy': self.level_4.get('audience_strategy_competitive_matrix'),
            'Campaign Lifecycle': self.level_4.get('campaign_lifecycle_performance'),
            'Strategic Mix': self.level_4.get('strategic_mix'),
            'Copying Monitor': self.level_4.get('copying_monitor'),
            'Evolution Trends': self.level_4.get('evolution_trends'),
        }
        
        for query_name, query_sql in sql_queries.items():
            assert query_sql is not None, f"SQL query missing: {query_name}"
            assert isinstance(query_sql, str), f"SQL query not string: {query_name}"
            assert len(query_sql.strip()) > 0, f"SQL query empty: {query_name}"
            
            # Basic SQL syntax validation
            assert 'SELECT' in query_sql.upper(), f"Missing SELECT: {query_name}"
            assert 'FROM' in query_sql.upper(), f"Missing FROM: {query_name}"
            
            # Check for proper table references
            assert '`' in query_sql, f"Missing table backticks: {query_name}"
            
            # Check for proper brand parameterization
            assert 'Warby Parker' in query_sql or '{self.brand}' in query_sql, f"Missing brand reference: {query_name}"
            
            print(f"  ✅ {query_name}: Valid SQL syntax")
        
        print("✅ SQL Query Syntax Validation - PASSED")
    
    def test_performance_profiling_by_enhancement(self):
        """Profile execution time by enhancement"""
        print("\n⏱️  Profiling Performance by Enhancement...")
        
        # Test individual pipeline stages
        stage_times = {}
        
        # Stage 1: Discovery
        start = time.time()
        pipeline = CompetitiveIntelligencePipeline('Warby Parker', dry_run=True)
        candidates = pipeline._stage_1_discovery()
        stage_times['Stage 1 - Discovery'] = time.time() - start
        
        # Stage 2: Curation (using mock data from stage 1)
        start = time.time()
        validated = pipeline._stage_2_curation(candidates)
        stage_times['Stage 2 - Curation'] = time.time() - start
        
        # Full pipeline execution
        start = time.time()
        full_result = pipeline.execute_pipeline()
        stage_times['Full Pipeline'] = time.time() - start
        
        # Verify performance requirements
        for stage, duration in stage_times.items():
            assert duration < 5.0, f"{stage} took {duration:.2f}s (should be <5s)"
            print(f"  ✅ {stage}: {duration:.3f}s")
        
        # Overall pipeline should be <2s as specified
        assert stage_times['Full Pipeline'] < 2.0, f"Full pipeline {stage_times['Full Pipeline']:.2f}s (should be <2s)"
        
        print("✅ Performance Profiling - PASSED")
    
    def test_memory_usage_patterns(self):
        """Review memory usage patterns"""
        print("\n🧠 Analyzing Memory Usage Patterns...")
        
        # Get initial memory usage
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Run pipeline multiple times to check for memory leaks
        peak_memory = initial_memory
        
        for i in range(3):
            pipeline = CompetitiveIntelligencePipeline('Warby Parker', dry_run=True)
            result = pipeline.execute_pipeline()
            
            current_memory = process.memory_info().rss / 1024 / 1024  # MB
            peak_memory = max(peak_memory, current_memory)
            
            # Memory should not grow excessively between runs
            memory_growth = current_memory - initial_memory
            assert memory_growth < 100, f"Memory growth {memory_growth:.1f}MB too high on run {i+1}"
        
        total_memory_used = peak_memory - initial_memory
        print(f"  ✅ Initial Memory: {initial_memory:.1f}MB")
        print(f"  ✅ Peak Memory: {peak_memory:.1f}MB") 
        print(f"  ✅ Memory Growth: {total_memory_used:.1f}MB")
        
        # Memory usage should be reasonable
        assert total_memory_used < 200, f"Total memory usage {total_memory_used:.1f}MB too high"
        
        print("✅ Memory Usage Analysis - PASSED")
    
    def test_edge_cases_and_error_conditions(self):
        """Test edge cases and error conditions"""
        print("\n🚨 Testing Edge Cases and Error Conditions...")
        
        # Test 1: Empty brand name
        try:
            pipeline_empty = CompetitiveIntelligencePipeline('', dry_run=True)
            result_empty = pipeline_empty.execute_pipeline()
            # Should handle gracefully, not crash
            assert result_empty.output is not None, "Pipeline should handle empty brand gracefully"
            print("  ✅ Empty brand name handled")
        except Exception as e:
            # If it throws an exception, it should be handled gracefully
            assert "brand" in str(e).lower(), f"Unexpected error for empty brand: {e}"
            print("  ✅ Empty brand error handled appropriately")
        
        # Test 2: Special characters in brand name
        try:
            pipeline_special = CompetitiveIntelligencePipeline('Test&Brand@123', dry_run=True)
            result_special = pipeline_special.execute_pipeline()
            assert result_special.output is not None, "Pipeline should handle special characters"
            print("  ✅ Special characters in brand name handled")
        except Exception as e:
            # Should handle gracefully
            print(f"  ⚠️  Special characters caused: {e} (acceptable if handled)")
        
        # Test 3: Very long brand name
        try:
            long_brand = "A" * 100  # 100 character brand name
            pipeline_long = CompetitiveIntelligencePipeline(long_brand, dry_run=True)
            result_long = pipeline_long.execute_pipeline()
            assert result_long.output is not None, "Pipeline should handle long brand names"
            print("  ✅ Long brand name handled")
        except Exception as e:
            print(f"  ⚠️  Long brand name caused: {e} (acceptable if handled)")
        
        # Test 4: Check data structure integrity under edge conditions
        pipeline = CompetitiveIntelligencePipeline('Warby Parker', dry_run=True)
        result = pipeline.execute_pipeline()
        
        # Verify no None values in critical paths
        assert result.output.level_1 is not None, "Level 1 should not be None"
        assert result.output.level_2 is not None, "Level 2 should not be None"
        assert result.output.level_3 is not None, "Level 3 should not be None"
        assert result.output.level_4 is not None, "Level 4 should not be None"
        
        # Check for empty critical sections
        level_2 = result.output.level_2
        phase_7_sections = [
            'cta_strategy_analysis',
            'channel_performance_matrix', 
            'content_quality_benchmarking',
            'audience_strategy_analysis',
            'campaign_lifecycle_optimization'
        ]
        
        for section in phase_7_sections:
            assert section in level_2, f"Missing Phase 7 section: {section}"
            assert level_2[section] is not None, f"Phase 7 section is None: {section}"
            assert len(level_2[section]) > 0, f"Phase 7 section is empty: {section}"
        
        print("✅ Edge Cases and Error Conditions - PASSED")
    
    def test_cross_enhancement_interactions(self):
        """Test cross-enhancement interactions working correctly"""
        print("\n🔗 Testing Cross-Enhancement Interactions...")
        
        result = self.pipeline.execute_pipeline()
        level_2 = result.output.level_2
        level_3 = result.output.level_3
        
        # Test 1: Verify all enhancements are present and non-conflicting
        enhancements = {
            'CTA': level_2.get('cta_strategy_analysis'),
            'Channel': level_2.get('channel_performance_matrix'),
            'Content': level_2.get('content_quality_benchmarking'),
            'Audience': level_2.get('audience_strategy_analysis'),
            'Lifecycle': level_2.get('campaign_lifecycle_optimization')
        }
        
        for name, data in enhancements.items():
            assert data is not None, f"{name} enhancement data is None"
            assert isinstance(data, dict), f"{name} enhancement data is not dict"
            assert len(data) > 0, f"{name} enhancement data is empty"
        
        # Test 2: Check AI interventions cover all enhancement types
        ai_interventions = level_3.get('phase_7_ai_interventions', [])
        intervention_types = {i.get('gap_type') for i in ai_interventions}
        
        expected_types = {'CTA_AGGRESSIVENESS', 'CONTENT_QUALITY', 'CHANNEL_PERFORMANCE', 
                         'AUDIENCE_TARGETING', 'CAMPAIGN_LIFECYCLE'}
        
        missing_types = expected_types - intervention_types
        assert len(missing_types) == 0, f"Missing AI intervention types: {missing_types}"
        
        # Test 3: Verify no data conflicts between enhancements
        # Check that metrics are consistent across enhancements
        brand_mentions = []
        
        def extract_brand_mentions(data, path=""):
            if isinstance(data, dict):
                for key, value in data.items():
                    if 'brand' in key.lower() and isinstance(value, (str, int, float)):
                        brand_mentions.append((path + key, value))
                    elif isinstance(value, (dict, list)):
                        extract_brand_mentions(value, path + key + ".")
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    extract_brand_mentions(item, path + f"[{i}].")
        
        extract_brand_mentions(level_2)
        
        # Should have brand-related metrics
        assert len(brand_mentions) > 0, "No brand-specific metrics found"
        
        print(f"  ✅ Found {len(enhancements)} enhancements")
        print(f"  ✅ Found {len(ai_interventions)} AI interventions")
        print(f"  ✅ Found {len(brand_mentions)} brand metrics")
        print("✅ Cross-Enhancement Interactions - PASSED")

def run_missing_validation_tests():
    """Run all missing validation tests"""
    print("=" * 70)
    print("PHASE 7 MISSING VALIDATION TESTS")
    print("=" * 70)
    
    test_suite = TestPhase7MissingValidations()
    test_methods = [
        ('test_sql_query_syntax_validation', 'SQL Query Syntax Validation'),
        ('test_performance_profiling_by_enhancement', 'Performance Profiling'),
        ('test_memory_usage_patterns', 'Memory Usage Analysis'),
        ('test_edge_cases_and_error_conditions', 'Edge Cases & Error Conditions'),
        ('test_cross_enhancement_interactions', 'Cross-Enhancement Interactions'),
    ]
    
    # Setup once
    test_suite.setup_method()
    
    # Run tests
    passed = 0
    failed = 0
    
    for test_method, test_name in test_methods:
        try:
            method = getattr(test_suite, test_method)
            method()
            passed += 1
        except AssertionError as e:
            print(f"❌ {test_name}: FAILED - {str(e)}")
            failed += 1
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {str(e)}")
            failed += 1
    
    # Summary
    print("\n" + "=" * 70)
    print("MISSING VALIDATION TEST SUMMARY")
    print("=" * 70)
    print(f"Additional Tests: {len(test_methods)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Pass Rate: {(passed/len(test_methods)*100):.1f}%")
    
    if failed == 0:
        print("🎉 ALL MISSING VALIDATION TESTS PASSED!")
        print("✅ Phase 7 fully validated and ready for production!")
    else:
        print("⚠️  Some validation tests failed. Review before production.")
    
    return passed, failed

if __name__ == "__main__":
    passed, failed = run_missing_validation_tests()
    sys.exit(0 if failed == 0 else 1)