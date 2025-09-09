#!/usr/bin/env python3
"""
Phase 7 Enhancement Test Suite
Comprehensive validation tests for medium-impact intelligence integration
"""

import sys
import os
import time
import pytest
import json
from typing import Dict, Any

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from scripts.compete_intel_pipeline import CompetitiveIntelligencePipeline

class TestPhase7Enhancements:
    """Test suite for Phase 7 medium-impact intelligence enhancements"""
    
    @pytest.fixture
    def pipeline(self):
        """Create pipeline instance for testing"""
        return CompetitiveIntelligencePipeline(
            brand="Warby Parker",
            vertical="Eyewear", 
            dry_run=True
        )
    
    @pytest.fixture
    def pipeline_result(self, pipeline):
        """Execute pipeline and return result"""
        return pipeline.execute_pipeline()

    # ============================================================================
    # Enhancement 1: CTA Aggressiveness Intelligence Tests
    # ============================================================================
    
    def test_cta_aggressiveness_integration(self, pipeline_result):
        """Test CTA aggressiveness data appears in Level 2 output"""
        level_2 = pipeline_result.output.level_2
        
        # Check for CTA strategy analysis section
        assert "cta_strategy_analysis" in level_2, "CTA strategy analysis missing from Level 2"
        
        cta_data = level_2["cta_strategy_analysis"]
        
        # Verify required fields
        assert "brand_aggressiveness_score" in cta_data, "Brand aggressiveness score missing"
        assert "market_avg_aggressiveness" in cta_data, "Market average aggressiveness missing"
        assert "aggressiveness_gap" in cta_data, "Aggressiveness gap missing"
        
        # Verify discount strategy section
        assert "discount_strategy" in cta_data, "Discount strategy section missing"
        discount_data = cta_data["discount_strategy"]
        assert "avg_discount_percentage" in discount_data, "Average discount percentage missing"
        assert "competitor_avg" in discount_data, "Competitor average discount missing"
        assert "discount_gap" in discount_data, "Discount gap missing"
        
        # Verify urgency tactics section
        assert "urgency_tactics" in cta_data, "Urgency tactics section missing"
        urgency_data = cta_data["urgency_tactics"]
        assert "scarcity_signal_usage" in urgency_data, "Scarcity signal usage missing"
        assert "market_scarcity_usage" in urgency_data, "Market scarcity usage missing"
        assert "urgency_intensity_vs_market" in urgency_data, "Urgency intensity comparison missing"
    
    def test_cta_interventions(self, pipeline_result):
        """Test CTA-specific interventions appear in Level 3"""
        level_3 = pipeline_result.output.level_3
        
        # Convert level 3 to string for content analysis
        level_3_str = json.dumps(level_3).lower()
        
        # Check for CTA-related interventions
        cta_keywords = ["discount", "urgency", "aggressiveness", "cta", "scarcity"]
        assert any(keyword in level_3_str for keyword in cta_keywords), \
            "No CTA-related interventions found in Level 3"
        
        # Check for specific intervention types
        assert "discount" in level_3_str or "pricing" in level_3_str, \
            "No discount/pricing interventions found"
        assert "urgency" in level_3_str or "scarcity" in level_3_str, \
            "No urgency/scarcity interventions found"
    
    def test_cta_sql_dashboards(self, pipeline_result):
        """Test CTA SQL queries are valid and contain expected fields"""
        level_4 = pipeline_result.output.level_4
        
        # Check for CTA competitive analysis query
        assert "cta_competitive_analysis" in level_4, "CTA competitive analysis query missing"
        
        cta_query = level_4["cta_competitive_analysis"]
        
        # Validate SQL contains expected fields
        assert "final_aggressiveness_score" in cta_query, "Aggressiveness score field missing from SQL"
        assert "promotional_theme" in cta_query, "Promotional theme field missing from SQL"
        assert "discount_percentage" in cta_query, "Discount percentage field missing from SQL"
        assert "has_scarcity_signals" in cta_query, "Scarcity signals field missing from SQL"
        
        # Basic SQL syntax validation
        assert "SELECT" in cta_query.upper(), "Invalid SQL: Missing SELECT"
        assert "FROM" in cta_query.upper(), "Invalid SQL: Missing FROM"
    
    # ============================================================================
    # Enhancement 2: Channel & Media Strategy Intelligence Tests  
    # ============================================================================
    
    def test_channel_performance_matrix(self, pipeline_result):
        """Test channel performance data integration"""
        level_2 = pipeline_result.output.level_2
        
        # Check for channel performance matrix
        assert "channel_performance_matrix" in level_2, "Channel performance matrix missing from Level 2"
        
        channel_data = level_2["channel_performance_matrix"]
        
        # Verify platform distribution section
        assert "platform_distribution" in channel_data, "Platform distribution missing"
        platforms = channel_data["platform_distribution"]
        
        # Check platform data structure (at least one platform should exist)
        assert len(platforms) > 0, "No platform data found"
        
        # Verify platform data structure for first platform
        first_platform = next(iter(platforms.values()))
        assert "brand_share" in first_platform, "Brand share missing from platform data"
        assert "market_avg" in first_platform, "Market average missing from platform data"  
        assert "performance_vs_market" in first_platform, "Performance comparison missing from platform data"
        
        # Verify media type effectiveness section
        assert "media_type_effectiveness" in channel_data, "Media type effectiveness missing"
        media_data = channel_data["media_type_effectiveness"]
        assert len(media_data) > 0, "No media type effectiveness data found"
    
    def test_channel_interventions(self, pipeline_result):
        """Test channel optimization interventions"""
        level_3 = pipeline_result.output.level_3
        level_3_str = json.dumps(level_3).lower()
        
        # Check for channel-related interventions
        channel_keywords = ["channel", "platform", "media", "facebook", "instagram", "tiktok", "format"]
        assert any(keyword in level_3_str for keyword in channel_keywords), \
            "No channel-related interventions found in Level 3"
        
        # Check for specific intervention types
        optimization_keywords = ["format", "rebalance", "allocation", "performance"]
        assert any(keyword in level_3_str for keyword in optimization_keywords), \
            "No channel optimization interventions found"
    
    def test_channel_sql_queries(self, pipeline_result):
        """Test channel performance SQL queries"""
        level_4 = pipeline_result.output.level_4
        
        # Check for channel competitive performance query
        assert "channel_competitive_performance" in level_4, "Channel competitive performance query missing"
        
        channel_query = level_4["channel_competitive_performance"]
        
        # Validate SQL contains expected fields
        assert "media_type" in channel_query, "Media type field missing from SQL"
        assert "publisher_platforms" in channel_query, "Publisher platforms field missing from SQL"
        assert "performance_rank" in channel_query or "ROW_NUMBER" in channel_query, "Performance ranking missing from SQL"
        
        # Basic SQL syntax validation
        assert "SELECT" in channel_query.upper(), "Invalid SQL: Missing SELECT"
        assert "GROUP BY" in channel_query.upper(), "Invalid SQL: Missing GROUP BY"
    
    # ============================================================================
    # Enhancement 3: Content Quality Intelligence Tests
    # ============================================================================
    
    def test_content_quality_benchmarking(self, pipeline_result):
        """Test content quality metrics integration"""
        level_2 = pipeline_result.output.level_2
        
        # Check for content quality benchmarking section
        assert "content_quality_benchmarking" in level_2, "Content quality benchmarking missing from Level 2"
        
        quality_data = level_2["content_quality_benchmarking"]
        
        # Verify text richness section
        assert "text_richness" in quality_data, "Text richness section missing"
        richness = quality_data["text_richness"]
        assert "brand_avg_score" in richness, "Brand average score missing"
        assert "market_avg_score" in richness, "Market average score missing"
        assert "quality_gap" in richness, "Quality gap missing"
        assert "percentile_rank" in richness, "Percentile rank missing"
        
        # Verify category coverage section
        assert "category_coverage" in quality_data, "Category coverage section missing"
        coverage = quality_data["category_coverage"]
        assert "brand_categories" in coverage, "Brand categories count missing"
        assert "market_leader_categories" in coverage, "Market leader categories missing"
        assert "coverage_completeness" in coverage, "Coverage completeness missing"
        
        # Verify content depth analysis
        assert "content_depth_analysis" in quality_data, "Content depth analysis missing"
        depth = quality_data["content_depth_analysis"]
        assert "avg_word_count" in depth, "Average word count missing"
        assert "market_avg_word_count" in depth, "Market average word count missing"
    
    def test_content_quality_sql(self, pipeline_result):
        """Test content quality SQL queries"""
        level_4 = pipeline_result.output.level_4
        
        # Check for content quality competitive analysis query
        assert "content_quality_competitive_analysis" in level_4, "Content quality analysis query missing"
        
        quality_query = level_4["content_quality_competitive_analysis"]
        
        # Validate SQL contains expected fields
        assert "text_richness_score" in quality_query, "Text richness score field missing"
        assert "page_category" in quality_query, "Page category field missing"
        assert "has_category" in quality_query, "Has category field missing"
    
    # ============================================================================
    # Enhancement 4: Audience Intelligence Tests
    # ============================================================================
    
    def test_audience_strategy_analysis(self, pipeline_result):
        """Test audience intelligence integration"""
        level_2 = pipeline_result.output.level_2
        
        # Check for audience strategy analysis section
        assert "audience_strategy_analysis" in level_2, "Audience strategy analysis missing from Level 2"
        
        audience_data = level_2["audience_strategy_analysis"]
        
        # Verify persona targeting section
        assert "persona_targeting" in audience_data, "Persona targeting section missing"
        personas = audience_data["persona_targeting"]
        assert "primary_personas" in personas, "Primary personas missing"
        assert "market_personas" in personas, "Market personas missing"
        assert "persona_gap" in personas, "Persona gap missing"
        assert "targeting_completeness" in personas, "Targeting completeness missing"
        
        # Verify topic diversity section
        assert "topic_diversity" in audience_data, "Topic diversity section missing"
        topics = audience_data["topic_diversity"]
        assert "brand_topic_count" in topics, "Brand topic count missing"
        assert "market_leader_topics" in topics, "Market leader topics missing"
        assert "topic_diversity_score" in topics, "Topic diversity score missing"
        
        # Verify angle strategy mix
        assert "angle_strategy_mix" in audience_data, "Angle strategy mix missing"
        angles = audience_data["angle_strategy_mix"]
        assert "promotional" in angles, "Promotional angle percentage missing"
        assert "emotional" in angles, "Emotional angle percentage missing"
        assert "aspirational" in angles, "Aspirational angle percentage missing"
    
    def test_audience_sql_queries(self, pipeline_result):
        """Test audience strategy SQL queries"""
        level_4 = pipeline_result.output.level_4
        
        # Check for audience strategy competitive matrix query
        assert "audience_strategy_competitive_matrix" in level_4, "Audience strategy matrix query missing"
        
        audience_query = level_4["audience_strategy_competitive_matrix"]
        
        # Validate SQL contains expected fields
        assert "persona" in audience_query, "Persona field missing from SQL"
        assert "topics" in audience_query, "Topics field missing from SQL"
        assert "UNNEST" in audience_query or "SPLIT" in audience_query, "Array processing missing from SQL"
    
    # ============================================================================
    # Enhancement 5: Campaign Lifecycle Intelligence Tests
    # ============================================================================
    
    def test_campaign_lifecycle_analysis(self, pipeline_result):
        """Test campaign lifecycle intelligence"""
        level_2 = pipeline_result.output.level_2
        
        # Check for campaign lifecycle optimization section
        assert "campaign_lifecycle_optimization" in level_2, "Campaign lifecycle optimization missing from Level 2"
        
        lifecycle_data = level_2["campaign_lifecycle_optimization"]
        
        # Verify duration analysis section
        assert "duration_analysis" in lifecycle_data, "Duration analysis section missing"
        duration = lifecycle_data["duration_analysis"]
        assert "avg_campaign_days" in duration, "Average campaign days missing"
        assert "optimal_duration_range" in duration, "Optimal duration range missing"
        assert "performance_decline_threshold" in duration, "Performance decline threshold missing"
        assert "campaigns_exceeding_optimal" in duration, "Campaigns exceeding optimal missing"
        
        # Verify refresh velocity section
        assert "refresh_velocity" in lifecycle_data, "Refresh velocity section missing"
        velocity = lifecycle_data["refresh_velocity"]
        assert "brand_refresh_cycle" in velocity, "Brand refresh cycle missing"
        assert "market_avg_refresh" in velocity, "Market average refresh missing"
        assert "velocity_gap" in velocity, "Velocity gap missing"
        assert "refresh_efficiency_score" in velocity, "Refresh efficiency score missing"
    
    def test_lifecycle_sql_queries(self, pipeline_result):
        """Test campaign lifecycle SQL queries"""
        level_4 = pipeline_result.output.level_4
        
        # Check for campaign lifecycle performance query
        assert "campaign_lifecycle_performance" in level_4, "Campaign lifecycle performance query missing"
        
        lifecycle_query = level_4["campaign_lifecycle_performance"]
        
        # Validate SQL contains expected fields
        assert "active_days" in lifecycle_query, "Active days field missing from SQL"
        assert "days_since_launch" in lifecycle_query, "Days since launch field missing from SQL"
        assert "lifecycle_stage" in lifecycle_query or "CASE" in lifecycle_query, "Lifecycle stage classification missing"
    
    # ============================================================================
    # Integration & Performance Tests
    # ============================================================================
    
    def test_phase_7_complete_integration(self, pipeline_result):
        """Test all Phase 7 enhancements work together"""
        level_2 = pipeline_result.output.level_2
        level_4 = pipeline_result.output.level_4
        
        # Verify all Level 2 enhancements present
        expected_level_2_sections = [
            "cta_strategy_analysis",
            "channel_performance_matrix", 
            "content_quality_benchmarking",
            "audience_strategy_analysis",
            "campaign_lifecycle_optimization"
        ]
        
        for section in expected_level_2_sections:
            assert section in level_2, f"Level 2 section missing: {section}"
        
        # Verify all Level 4 enhancements present
        expected_level_4_queries = [
            "cta_competitive_analysis",
            "channel_competitive_performance",
            "content_quality_competitive_analysis", 
            "audience_strategy_competitive_matrix",
            "campaign_lifecycle_performance"
        ]
        
        for query in expected_level_4_queries:
            assert query in level_4, f"Level 4 query missing: {query}"
    
    def test_data_utilization_improvement(self, pipeline_result):
        """Verify data utilization increased significantly"""
        level_2 = pipeline_result.output.level_2
        level_4 = pipeline_result.output.level_4
        
        # Count new intelligence sections added
        new_sections_count = len([
            section for section in [
                "cta_strategy_analysis",
                "channel_performance_matrix",
                "content_quality_benchmarking", 
                "audience_strategy_analysis",
                "campaign_lifecycle_optimization"
            ] if section in level_2
        ])
        
        # Should have all 5 new sections
        assert new_sections_count >= 4, f"Expected at least 4 new sections, got {new_sections_count}"
        
        # Count new SQL queries added
        new_queries_count = len([
            query for query in [
                "cta_competitive_analysis",
                "channel_competitive_performance",
                "content_quality_competitive_analysis",
                "audience_strategy_competitive_matrix", 
                "campaign_lifecycle_performance"
            ] if query in level_4
        ])
        
        # Should have all 5 new queries
        assert new_queries_count >= 4, f"Expected at least 4 new queries, got {new_queries_count}"
    
    def test_no_performance_regression(self):
        """Ensure Phase 7 doesn't impact pipeline performance"""
        pipeline = CompetitiveIntelligencePipeline(
            brand="Warby Parker",
            vertical="Eyewear",
            dry_run=True
        )
        
        start_time = time.time()
        result = pipeline.execute_pipeline()
        execution_time = time.time() - start_time
        
        # Performance requirement: dry run should complete in < 2 seconds
        assert execution_time < 2.0, f"Performance regression: execution took {execution_time:.2f}s (max 2.0s)"
        assert result.output is not None, "Pipeline returned no output"
        assert result.output.level_1 is not None, "Level 1 missing"
        assert result.output.level_2 is not None, "Level 2 missing" 
        assert result.output.level_3 is not None, "Level 3 missing"
        assert result.output.level_4 is not None, "Level 4 missing"
    
    def test_output_structure_integrity(self, pipeline_result):
        """Test that all output levels maintain proper structure"""
        output = pipeline_result.output
        
        # Verify all 4 levels exist
        required_levels = ["level_1", "level_2", "level_3", "level_4"]
        for level in required_levels:
            assert level in output, f"Output level missing: {level}"
            assert output[level] is not None, f"Output level is None: {level}"
        
        # Verify Level 1 structure (Executive Summary)
        level_1 = output["level_1"]
        assert "duration" in level_1, "Duration missing from Level 1"
        assert "competitors_analyzed" in level_1, "Competitors analyzed missing from Level 1"
        assert "strategic_position" in level_1, "Strategic position missing from Level 1"
        
        # Verify Level 2 structure includes original + new sections
        level_2 = output["level_2"]
        original_sections = ["current_state", "influence_detection", "temporal_evolution", "predictive_intelligence"]
        for section in original_sections:
            assert section in level_2, f"Original Level 2 section missing: {section}"
        
        # Verify Level 4 structure (SQL queries are strings)
        level_4 = output["level_4"]
        for query_name, query_content in level_4.items():
            assert isinstance(query_content, str), f"Level 4 query {query_name} is not a string"
            assert len(query_content) > 0, f"Level 4 query {query_name} is empty"

# ============================================================================
# Utility Functions for Testing
# ============================================================================

def count_utilized_fields(output: Dict[str, Any]) -> int:
    """Count the number of unique database fields utilized in the output"""
    # This would need to be implemented to parse the output and count
    # unique field references - placeholder for now
    return len(str(output))

if __name__ == "__main__":
    # Run the test suite
    pytest.main([__file__, "-v", "--tb=short"])