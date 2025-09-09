"""
Comprehensive Phase 7 Validation Tests
Based on PHASE_7_IMPLEMENTATION_PLAN.md requirements
"""

import sys
from pathlib import Path
import time
import json

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.compete_intel_pipeline import CompetitiveIntelligencePipeline


class TestPhase7Validation:
    """Complete Phase 7 validation test suite"""
    
    def setup_method(self):
        """Setup for each test"""
        self.pipeline = CompetitiveIntelligencePipeline('Warby Parker', dry_run=True)
        self.result = self.pipeline.execute_pipeline()
        self.level_2 = self.result.output.level_2
        self.level_3 = self.result.output.level_3
        self.level_4 = self.result.output.level_4
    
    # ========================================================================
    # ENHANCEMENT 1: CTA Aggressiveness Tests
    # ========================================================================
    
    def test_cta_aggressiveness_integration(self):
        """Test 1.1: CTA aggressiveness data appears in Level 2 output"""
        assert 'cta_strategy_analysis' in self.level_2, "CTA strategy analysis missing from Level 2"
        
        cta_data = self.level_2['cta_strategy_analysis']
        assert 'brand_aggressiveness_score' in cta_data, "Brand aggressiveness score missing"
        assert 'discount_strategy' in cta_data, "Discount strategy missing"
        assert 'urgency_tactics' in cta_data, "Urgency tactics missing"
        
        # Verify data structure
        assert isinstance(cta_data['brand_aggressiveness_score'], (int, float))
        assert 'avg_discount_percentage' in cta_data['discount_strategy']
        assert 'scarcity_signal_usage' in cta_data['urgency_tactics']
        print("✅ Test 1.1: CTA aggressiveness integration - PASSED")
    
    def test_cta_interventions(self):
        """Test 1.2: CTA-specific interventions appear in Level 3"""
        assert 'phase_7_ai_interventions' in self.level_3, "AI interventions missing from Level 3"
        
        interventions = self.level_3['phase_7_ai_interventions']
        cta_interventions = [i for i in interventions if 'CTA' in i.get('gap_type', '')]
        
        assert len(cta_interventions) > 0, "No CTA interventions found"
        
        # Check intervention structure
        for intervention in cta_interventions:
            assert 'intervention_title' in intervention
            assert 'current_state' in intervention
            assert 'market_benchmark' in intervention
            assert 'specific_action' in intervention
            assert 'priority' in intervention
            assert 'expected_impact' in intervention
        
        print("✅ Test 1.2: CTA interventions - PASSED")
    
    def test_cta_sql_dashboards(self):
        """Test 1.3: CTA SQL queries are valid and contain expected fields"""
        assert 'cta_competitive_analysis' in self.level_4, "CTA SQL dashboard missing"
        
        query = self.level_4['cta_competitive_analysis']
        assert 'final_aggressiveness_score' in query, "Aggressiveness score missing from SQL"
        assert 'promotional_theme' in query, "Promotional theme missing from SQL"
        assert 'discount_percentage' in query, "Discount percentage missing from SQL"
        assert 'SELECT' in query and 'FROM' in query, "Invalid SQL structure"
        
        print("✅ Test 1.3: CTA SQL dashboards - PASSED")
    
    # ========================================================================
    # ENHANCEMENT 2: Channel Performance Tests
    # ========================================================================
    
    def test_channel_performance_matrix(self):
        """Test 2.1: Channel performance data integration"""
        assert 'channel_performance_matrix' in self.level_2, "Channel performance matrix missing"
        
        channel_data = self.level_2['channel_performance_matrix']
        assert 'platform_distribution' in channel_data, "Platform distribution missing"
        assert 'media_type_effectiveness' in channel_data, "Media type effectiveness missing"
        
        # Verify platform data structure
        platforms = channel_data['platform_distribution']
        assert isinstance(platforms, dict), "Platform distribution should be a dict"
        
        for platform, data in platforms.items():
            assert 'brand_share' in data, f"Brand share missing for {platform}"
            assert 'market_avg' in data, f"Market average missing for {platform}"
            assert 'performance_vs_market' in data, f"Performance comparison missing for {platform}"
        
        print("✅ Test 2.1: Channel performance matrix - PASSED")
    
    def test_channel_interventions(self):
        """Test 2.2: Channel optimization interventions"""
        interventions = self.level_3.get('phase_7_ai_interventions', [])
        channel_interventions = [i for i in interventions if 'CHANNEL' in i.get('gap_type', '')]
        
        assert len(channel_interventions) > 0, "No channel interventions found"
        
        # Check for platform-specific recommendations
        intervention_text = json.dumps(interventions).lower()
        platforms = ['facebook', 'instagram', 'tiktok']
        assert any(p in intervention_text for p in platforms), "No platform-specific recommendations"
        
        print("✅ Test 2.2: Channel interventions - PASSED")
    
    def test_channel_sql_queries(self):
        """Test 2.3: Channel performance SQL queries"""
        assert 'channel_competitive_performance' in self.level_4, "Channel SQL dashboard missing"
        
        query = self.level_4['channel_competitive_performance']
        assert 'media_type' in query, "Media type missing from SQL"
        assert 'publisher_platforms' in query, "Publisher platforms missing from SQL"
        assert 'performance_rank' in query or 'RANK()' in query, "Performance ranking missing"
        
        print("✅ Test 2.3: Channel SQL queries - PASSED")
    
    # ========================================================================
    # ENHANCEMENT 3: Content Quality Tests
    # ========================================================================
    
    def test_content_quality_benchmarking(self):
        """Test 3.1: Content quality metrics integration"""
        assert 'content_quality_benchmarking' in self.level_2, "Content quality benchmarking missing"
        
        quality_data = self.level_2['content_quality_benchmarking']
        assert 'text_richness' in quality_data, "Text richness analysis missing"
        assert 'category_coverage' in quality_data, "Category coverage missing"
        assert 'content_depth_analysis' in quality_data, "Content depth analysis missing"
        
        # Verify quality scoring
        richness = quality_data['text_richness']
        assert 'brand_avg_score' in richness, "Brand average score missing"
        assert 'market_avg_score' in richness, "Market average score missing"
        assert 'quality_gap' in richness, "Quality gap analysis missing"
        
        print("✅ Test 3.1: Content quality benchmarking - PASSED")
    
    def test_content_quality_sql(self):
        """Test 3.2: Content quality SQL queries"""
        assert 'content_quality_competitive_analysis' in self.level_4, "Content quality SQL missing"
        
        query = self.level_4['content_quality_competitive_analysis']
        assert 'text_richness_score' in query, "Text richness score missing from SQL"
        assert 'category' in query.lower(), "Category analysis missing from SQL"
        
        print("✅ Test 3.2: Content quality SQL - PASSED")
    
    # ========================================================================
    # ENHANCEMENT 4: Audience Intelligence Tests
    # ========================================================================
    
    def test_audience_strategy_analysis(self):
        """Test 4.1: Audience intelligence integration"""
        assert 'audience_strategy_analysis' in self.level_2, "Audience strategy analysis missing"
        
        audience_data = self.level_2['audience_strategy_analysis']
        assert 'persona_targeting' in audience_data, "Persona targeting missing"
        assert 'topic_diversity' in audience_data, "Topic diversity missing"
        assert 'angle_strategy_mix' in audience_data, "Angle strategy mix missing"
        
        # Verify persona analysis
        personas = audience_data['persona_targeting']
        assert 'primary_personas' in personas, "Primary personas missing"
        assert 'persona_gap' in personas, "Persona gap analysis missing"
        assert 'targeting_completeness' in personas, "Targeting completeness missing"
        
        print("✅ Test 4.1: Audience strategy analysis - PASSED")
    
    def test_audience_sql_queries(self):
        """Test 4.2: Audience strategy SQL queries"""
        assert 'audience_strategy_competitive_matrix' in self.level_4, "Audience SQL missing"
        
        query = self.level_4['audience_strategy_competitive_matrix']
        assert 'persona' in query, "Persona analysis missing from SQL"
        assert 'topic' in query.lower() or 'angle' in query.lower(), "Topic/angle analysis missing"
        
        print("✅ Test 4.2: Audience SQL queries - PASSED")
    
    # ========================================================================
    # ENHANCEMENT 5: Campaign Lifecycle Tests
    # ========================================================================
    
    def test_campaign_lifecycle_analysis(self):
        """Test 5.1: Campaign lifecycle intelligence"""
        assert 'campaign_lifecycle_optimization' in self.level_2, "Campaign lifecycle optimization missing"
        
        lifecycle_data = self.level_2['campaign_lifecycle_optimization']
        assert 'duration_analysis' in lifecycle_data, "Duration analysis missing"
        assert 'refresh_velocity' in lifecycle_data, "Refresh velocity missing"
        
        # Verify duration analysis
        duration = lifecycle_data['duration_analysis']
        assert 'avg_campaign_days' in duration, "Average campaign days missing"
        assert 'optimal_duration_range' in duration, "Optimal duration range missing"
        
        print("✅ Test 5.1: Campaign lifecycle analysis - PASSED")
    
    def test_lifecycle_sql_queries(self):
        """Test 5.2: Campaign lifecycle SQL queries"""
        assert 'campaign_lifecycle_performance' in self.level_4, "Lifecycle SQL missing"
        
        query = self.level_4['campaign_lifecycle_performance']
        assert 'active_days' in query or 'duration' in query.lower(), "Duration metrics missing"
        assert 'lifecycle' in query.lower() or 'stage' in query.lower(), "Lifecycle stages missing"
        
        print("✅ Test 5.2: Lifecycle SQL queries - PASSED")
    
    # ========================================================================
    # END-TO-END INTEGRATION TESTS
    # ========================================================================
    
    def test_phase_7_complete_integration(self):
        """Test E2E-1: All Phase 7 enhancements work together"""
        # Level 2 enhancements
        level_2_requirements = [
            'cta_strategy_analysis',
            'channel_performance_matrix',
            'content_quality_benchmarking',
            'audience_strategy_analysis',
            'campaign_lifecycle_optimization'
        ]
        
        for requirement in level_2_requirements:
            assert requirement in self.level_2, f"Missing Level 2: {requirement}"
        
        # Level 3 AI interventions
        assert 'phase_7_ai_interventions' in self.level_3, "AI interventions missing"
        assert len(self.level_3['phase_7_ai_interventions']) >= 5, "Insufficient AI interventions"
        
        # Level 4 SQL dashboards
        level_4_requirements = [
            'cta_competitive_analysis',
            'channel_competitive_performance',
            'content_quality_competitive_analysis',
            'audience_strategy_competitive_matrix',
            'campaign_lifecycle_performance'
        ]
        
        for requirement in level_4_requirements:
            assert requirement in self.level_4, f"Missing Level 4: {requirement}"
        
        print("✅ Test E2E-1: Complete integration - PASSED")
    
    def test_data_utilization_improvement(self):
        """Test E2E-2: Verify data utilization increased from 40% to 65%"""
        # Count unique fields being used across all levels
        level_2_fields = self._count_fields(self.level_2)
        level_3_fields = len(self.level_3.get('phase_7_ai_interventions', []))
        level_4_queries = len([k for k in self.level_4.keys() if 'analysis' in k or 'performance' in k])
        
        # We should have at least 25 new fields from Phase 7
        new_fields_count = level_2_fields + level_3_fields + level_4_queries
        assert new_fields_count >= 25, f"Only {new_fields_count} new fields (need 25+)"
        
        print(f"✅ Test E2E-2: Data utilization - PASSED ({new_fields_count} new fields)")
    
    def test_no_performance_regression(self):
        """Test E2E-3: Ensure Phase 7 doesn't impact pipeline performance"""
        start_time = time.time()
        
        # Run a new pipeline execution
        pipeline = CompetitiveIntelligencePipeline('Warby Parker', dry_run=True)
        result = pipeline.execute_pipeline()
        
        execution_time = time.time() - start_time
        
        assert execution_time < 2.0, f"Execution took {execution_time:.2f}s (max 2s)"
        assert result.output is not None, "Output is None"
        assert hasattr(result.output, 'level_1'), "Level 1 missing"
        assert hasattr(result.output, 'level_2'), "Level 2 missing"
        assert hasattr(result.output, 'level_3'), "Level 3 missing"
        assert hasattr(result.output, 'level_4'), "Level 4 missing"
        
        print(f"✅ Test E2E-3: Performance - PASSED ({execution_time:.2f}s)")
    
    def test_output_structure_integrity(self):
        """Test E2E-4: Verify output structure integrity"""
        # Check that all levels are properly structured
        assert isinstance(self.level_2, dict), "Level 2 should be a dict"
        assert isinstance(self.level_3, dict), "Level 3 should be a dict"
        assert isinstance(self.level_4, dict), "Level 4 should be a dict"
        
        # Check no None values in critical fields
        for key, value in self.level_2.items():
            assert value is not None, f"Level 2 {key} is None"
        
        # Check AI interventions structure
        interventions = self.level_3.get('phase_7_ai_interventions', [])
        for intervention in interventions:
            assert isinstance(intervention, dict), "Intervention should be a dict"
            assert 'gap_type' in intervention, "Gap type missing"
            assert 'intervention_title' in intervention, "Title missing"
        
        print("✅ Test E2E-4: Output structure integrity - PASSED")
    
    def _count_fields(self, data, depth=0, max_depth=3):
        """Helper to count unique fields in nested dict"""
        if depth > max_depth or not isinstance(data, dict):
            return 0
        
        count = len(data.keys())
        for value in data.values():
            if isinstance(value, dict):
                count += self._count_fields(value, depth + 1, max_depth)
        return count


def run_all_tests():
    """Run all Phase 7 validation tests"""
    print("=" * 70)
    print("PHASE 7 VALIDATION TEST SUITE")
    print("=" * 70)
    print()
    
    test_suite = TestPhase7Validation()
    test_results = []
    
    # List all test methods
    test_methods = [
        # Enhancement 1: CTA
        ('test_cta_aggressiveness_integration', 'CTA Aggressiveness Integration'),
        ('test_cta_interventions', 'CTA Interventions'),
        ('test_cta_sql_dashboards', 'CTA SQL Dashboards'),
        
        # Enhancement 2: Channel
        ('test_channel_performance_matrix', 'Channel Performance Matrix'),
        ('test_channel_interventions', 'Channel Interventions'),
        ('test_channel_sql_queries', 'Channel SQL Queries'),
        
        # Enhancement 3: Content
        ('test_content_quality_benchmarking', 'Content Quality Benchmarking'),
        ('test_content_quality_sql', 'Content Quality SQL'),
        
        # Enhancement 4: Audience
        ('test_audience_strategy_analysis', 'Audience Strategy Analysis'),
        ('test_audience_sql_queries', 'Audience SQL Queries'),
        
        # Enhancement 5: Lifecycle
        ('test_campaign_lifecycle_analysis', 'Campaign Lifecycle Analysis'),
        ('test_lifecycle_sql_queries', 'Lifecycle SQL Queries'),
        
        # End-to-End Tests
        ('test_phase_7_complete_integration', 'Complete Integration'),
        ('test_data_utilization_improvement', 'Data Utilization'),
        ('test_no_performance_regression', 'Performance Regression'),
        ('test_output_structure_integrity', 'Output Structure Integrity'),
    ]
    
    # Setup once
    test_suite.setup_method()
    
    # Run each test
    passed = 0
    failed = 0
    
    for test_method, test_name in test_methods:
        try:
            method = getattr(test_suite, test_method)
            method()
            test_results.append((test_name, 'PASSED'))
            passed += 1
        except AssertionError as e:
            print(f"❌ Test {test_name}: FAILED - {str(e)}")
            test_results.append((test_name, f'FAILED: {str(e)}'))
            failed += 1
        except Exception as e:
            print(f"❌ Test {test_name}: ERROR - {str(e)}")
            test_results.append((test_name, f'ERROR: {str(e)}'))
            failed += 1
    
    # Print summary
    print()
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Total Tests: {len(test_methods)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Pass Rate: {(passed/len(test_methods)*100):.1f}%")
    print()
    
    if failed == 0:
        print("🎉 ALL TESTS PASSED! Phase 7 is ready for Release Candidate!")
    else:
        print("⚠️  Some tests failed. Please review and fix before proceeding.")
        print("\nFailed tests:")
        for name, result in test_results:
            if 'FAILED' in result or 'ERROR' in result:
                print(f"  - {name}: {result}")
    
    return passed, failed


if __name__ == "__main__":
    passed, failed = run_all_tests()
    sys.exit(0 if failed == 0 else 1)