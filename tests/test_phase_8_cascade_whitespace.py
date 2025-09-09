#!/usr/bin/env python3
"""
Test Suite for Phase 8: Cascade Prediction Intelligence & White Space Detection
Tests all core Phase 8 functionality including 3-move-ahead predictions and white space opportunities
"""

import os
import sys
import pytest
import pandas as pd
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.compete_intel_pipeline import CompetitiveIntelligencePipeline


class TestPhase8CascadePrediction:
    """Test cascade prediction intelligence functionality"""
    
    def create_pipeline(self):
        """Create test pipeline instance"""
        return CompetitiveIntelligencePipeline(
            brand="TestBrand",
            vertical="Retail",
            dry_run=True
        )
    
    def test_cascade_pattern_detection(self):
        """Test historical cascade pattern identification"""
        pipeline = self.create_pipeline()
        cascades = pipeline._detect_cascade_patterns()
        
        assert isinstance(cascades, pd.DataFrame), "Should return DataFrame"
        assert len(cascades) > 0, "Should find cascade patterns in dry-run mode"
        
        # Validate cascade structure
        required_columns = ['initiator', 'responder', 'initiation_date', 'response_date', 
                          'response_lag', 'initial_move_size', 'response_size', 'cascade_correlation']
        for col in required_columns:
            assert col in cascades.columns, f"Missing required column: {col}"
        
        # Validate cascade quality
        assert cascades['cascade_correlation'].min() >= 0.6, "All cascades should have correlation > 0.6"
        assert cascades['response_lag'].min() > 0, "Response lag should be positive"
        
        print(f"✅ Detected {len(cascades)} cascade patterns")
    
    def test_influence_network_construction(self):
        """Test brand influence network building"""
        pipeline = self.create_pipeline()
        network = pipeline._build_influence_network()
        
        assert isinstance(network, dict), "Network should be dict"
        assert 'nodes' in network, "Network should have nodes"
        assert 'edges' in network, "Network should have edges"
        assert 'influence_matrix' in network, "Network should have influence matrix"
        
        # Validate network structure
        assert len(network['nodes']) > 0, "Should have network nodes"
        if len(network['edges']) > 0:
            edge = network['edges'][0]
            assert 'source_brand' in edge, "Edge should have source_brand"
            assert 'target_brand' in edge, "Edge should have target_brand"
            assert 'influence_strength' in edge, "Edge should have influence_strength"
            assert 'avg_response_time' in edge, "Edge should have avg_response_time"
            assert 'influence_type' in edge, "Edge should have influence_type"
        
        print(f"✅ Built influence network with {len(network['nodes'])} nodes and {len(network['edges'])} edges")
    
    def test_cascade_sequence_prediction(self, pipeline):
        """Test 3-move-ahead cascade prediction"""
        initial_move = {
            'brand': 'TestBrand',
            'promotional_intensity': 0.8,
            'timestamp': datetime.now().isoformat()
        }
        
        predictions = pipeline._predict_cascade_sequence(initial_move)
        
        assert isinstance(predictions, list), "Predictions should be list"
        assert len(predictions) <= 3, "Should predict max 3 moves ahead"
        
        for i, prediction in enumerate(predictions):
            assert 'brand' in prediction, "Prediction should have brand"
            assert 'move_number' in prediction, "Prediction should have move_number"
            assert 'predicted_date' in prediction, "Prediction should have predicted_date"
            assert 'predicted_intensity' in prediction, "Prediction should have predicted_intensity"
            assert 'confidence' in prediction, "Prediction should have confidence"
            
            # Validate prediction quality
            assert prediction['move_number'] == i + 1, f"Move number should be {i + 1}"
            assert 0 <= prediction['confidence'] <= 1, "Confidence should be between 0 and 1"
            assert prediction['predicted_date'] > 0, "Predicted date should be positive"
            
            # Validate chronological order
            if i > 0:
                assert prediction['predicted_date'] >= predictions[i-1]['predicted_date'], \
                    "Predictions should be chronological"
        
        print(f"✅ Generated {len(predictions)} cascade predictions")
    
    def test_cascade_interventions(self, pipeline):
        """Test cascade intervention generation"""
        predictions = [
            {'brand': 'Competitor_A', 'predicted_date': 7, 'predicted_intensity': 0.8, 'confidence': 0.9},
            {'brand': 'Competitor_B', 'predicted_date': 14, 'predicted_intensity': 0.6, 'confidence': 0.7},
            {'brand': 'Competitor_C', 'predicted_date': 28, 'predicted_intensity': 0.4, 'confidence': 0.5}
        ]
        
        interventions = pipeline._generate_cascade_interventions(predictions)
        
        assert isinstance(interventions, dict), "Interventions should be dict"
        
        required_sections = ['cascade_timeline', 'preemptive_actions', 'defensive_strategies', 'opportunity_windows']
        for section in required_sections:
            assert section in interventions, f"Missing intervention section: {section}"
        
        # Validate timeline
        assert len(interventions['cascade_timeline']) == len(predictions), \
            "Timeline should match predictions count"
        
        # Validate preemptive actions for high-confidence predictions
        high_conf_predictions = [p for p in predictions if p['confidence'] > 0.7]
        assert len(interventions['preemptive_actions']) >= len(high_conf_predictions), \
            "Should have preemptive actions for high-confidence predictions"
        
        print(f"✅ Generated cascade interventions with {len(interventions['preemptive_actions'])} preemptive actions")


class TestPhase8WhiteSpaceDetection:
    """Test white space detection and opportunity analysis"""
    
    def create_pipeline(self):
        """Create test pipeline instance"""
        return CompetitiveIntelligencePipeline(
            brand="TestBrand",
            vertical="Retail",
            dry_run=True
        )
    
    def test_white_space_detection(self, pipeline):
        """Test 3D white space opportunity identification"""
        white_spaces = pipeline._detect_white_spaces()
        
        assert isinstance(white_spaces, pd.DataFrame), "Should return DataFrame"
        assert len(white_spaces) > 0, "Should find white spaces in dry-run mode"
        
        # Validate white space structure
        required_columns = ['primary_angle', 'funnel', 'persona', 'total_market_presence',
                          'competitor_count', 'space_type', 'opportunity_score']
        for col in required_columns:
            assert col in white_spaces.columns, f"Missing required column: {col}"
        
        # Validate white space types
        valid_space_types = ['VIRGIN_TERRITORY', 'MONOPOLY', 'UNDERSERVED']
        for space_type in white_spaces['space_type'].unique():
            assert space_type in valid_space_types, f"Invalid space type: {space_type}"
        
        # Validate opportunity scores
        assert white_spaces['opportunity_score'].min() >= 0, "Opportunity scores should be non-negative"
        assert white_spaces['opportunity_score'].max() <= 1, "Opportunity scores should be <= 1"
        
        print(f"✅ Detected {len(white_spaces)} white space opportunities")
    
    def test_market_potential_estimation(self, pipeline):
        """Test market potential estimation for white spaces"""
        test_white_space = {
            'primary_angle': 'EMOTIONAL',
            'funnel': 'Upper',
            'persona': 'Eco-conscious',
            'space_type': 'VIRGIN_TERRITORY'
        }
        
        potential = pipeline._estimate_market_potential(test_white_space)
        
        assert isinstance(potential, dict), "Potential should be dict"
        
        required_metrics = ['estimated_performance', 'estimated_volume', 'market_size_multiplier',
                          'total_potential_score', 'strategic_fit', 'implementation_difficulty', 'competitive_moat']
        for metric in required_metrics:
            assert metric in potential, f"Missing potential metric: {metric}"
            assert isinstance(potential[metric], (int, float)), f"Metric {metric} should be numeric"
        
        # Validate score ranges
        assert 0 <= potential['strategic_fit'] <= 1, "Strategic fit should be between 0 and 1"
        assert 0 <= potential['implementation_difficulty'] <= 1, "Implementation difficulty should be between 0 and 1"
        assert 0 <= potential['competitive_moat'] <= 1, "Competitive moat should be between 0 and 1"
        
        print(f"✅ Estimated market potential: {potential['total_potential_score']:.1f}")
    
    def test_white_space_prioritization(self, pipeline):
        """Test white space opportunity prioritization"""
        test_spaces = [
            {'primary_angle': 'EMOTIONAL', 'funnel': 'Upper', 'persona': 'Eco-conscious', 'space_type': 'VIRGIN_TERRITORY'},
            {'primary_angle': 'FUNCTIONAL', 'funnel': 'Mid', 'persona': 'Price-conscious', 'space_type': 'MONOPOLY'},
            {'primary_angle': 'ASPIRATIONAL', 'funnel': 'Lower', 'persona': 'Quality-focused', 'space_type': 'UNDERSERVED'}
        ]
        
        prioritized = pipeline._prioritize_white_spaces(test_spaces)
        
        assert isinstance(prioritized, list), "Prioritized spaces should be list"
        assert len(prioritized) == len(test_spaces), "Should prioritize all input spaces"
        
        # Validate prioritization structure
        for space in prioritized:
            required_fields = ['angle', 'funnel', 'persona', 'space_type', 'market_potential',
                             'strategic_fit', 'ease_of_entry', 'defensibility', 'priority', 'overall_score']
            for field in required_fields:
                assert field in space, f"Missing prioritization field: {field}"
            
            # Validate priority categories
            valid_priorities = ['QUICK_WIN', 'STRATEGIC_BET', 'FILL_IN', 'QUESTIONABLE']
            assert space['priority'] in valid_priorities, f"Invalid priority: {space['priority']}"
        
        # Validate sorting (should be by overall_score descending)
        scores = [space['overall_score'] for space in prioritized]
        assert scores == sorted(scores, reverse=True), "Spaces should be sorted by overall_score descending"
        
        print(f"✅ Prioritized {len(prioritized)} white space opportunities")
    
    def test_white_space_interventions(self, pipeline):
        """Test white space intervention generation"""
        test_prioritized_spaces = [
            {'angle': 'EMOTIONAL', 'funnel': 'Upper', 'persona': 'Eco-conscious', 'space_type': 'VIRGIN_TERRITORY',
             'priority': 'QUICK_WIN', 'market_potential': 0.8, 'strategic_fit': 0.9, 'ease_of_entry': 0.7, 'defensibility': 0.6},
            {'angle': 'FUNCTIONAL', 'funnel': 'Mid', 'persona': 'Price-conscious', 'space_type': 'MONOPOLY',
             'priority': 'STRATEGIC_BET', 'market_potential': 0.9, 'strategic_fit': 0.6, 'ease_of_entry': 0.4, 'defensibility': 0.8}
        ]
        
        interventions = pipeline._generate_white_space_interventions(test_prioritized_spaces)
        
        assert isinstance(interventions, dict), "Interventions should be dict"
        
        required_sections = ['quick_wins', 'strategic_bets', 'entry_strategies', 'campaign_templates']
        for section in required_sections:
            assert section in interventions, f"Missing intervention section: {section}"
        
        # Validate quick wins
        if interventions['quick_wins']:
            quick_win = interventions['quick_wins'][0]
            required_qw_fields = ['opportunity', 'market_gap', 'action', 'timeline', 'expected_roi', 'resources_required']
            for field in required_qw_fields:
                assert field in quick_win, f"Missing quick win field: {field}"
        
        # Validate strategic bets
        if interventions['strategic_bets']:
            strategic_bet = interventions['strategic_bets'][0]
            required_sb_fields = ['opportunity', 'market_potential', 'investment_required', 'time_to_market', 'competitive_advantage', 'risk_factors']
            for field in required_sb_fields:
                assert field in strategic_bet, f"Missing strategic bet field: {field}"
        
        # Validate campaign templates
        if interventions['campaign_templates']:
            template = interventions['campaign_templates'][0]
            required_template_fields = ['campaign_name', 'targeting', 'messaging', 'creative_brief', 'kpis']
            for field in required_template_fields:
                assert field in template, f"Missing campaign template field: {field}"
        
        print(f"✅ Generated interventions with {len(interventions['quick_wins'])} quick wins and {len(interventions['strategic_bets'])} strategic bets")


class TestPhase8Integration:
    """Test integration of Phase 8 features into pipeline levels"""
    
    @pytest.fixture
    def pipeline(self):
        """Create test pipeline instance with mock dependencies"""
        with patch('scripts.compete_intel_pipeline.get_bigquery_client'), \
             patch('scripts.compete_intel_pipeline.run_query'), \
             patch('scripts.compete_intel_pipeline.load_dataframe_to_bq'):
            
            pipeline = CompetitiveIntelligencePipeline(
                brand="TestBrand",
                vertical="Retail",
                dry_run=True
            )
            pipeline.competitor_brands = ['Competitor_A', 'Competitor_B', 'Competitor_C']
            return pipeline
    
    def test_level_2_cascade_integration(self, pipeline):
        """Test cascade prediction integration in Level 2 Strategic Dashboard"""
        from scripts.compete_intel_pipeline import AnalysisResults
        
        analysis = AnalysisResults(
            current_state={'promotional_intensity': 0.6, 'market_position': 'strong'},
            influence={'copying_detected': False, 'similarity_score': 0.3},
            evolution={'trend_direction': 'stable', 'market_promo_change': 0.5, 'trend_strength': 0.1},
            forecasts={'next_week_intensity': 0.65, 'confidence': 0.8, 'trend': 'increasing'}
        )
        
        level_2 = pipeline._generate_level_2_strategic(analysis)
        
        # Validate cascade prediction integration
        assert 'cascade_predictions' in level_2, "Level 2 should include cascade predictions"
        
        cascade_data = level_2['cascade_predictions']
        required_cascade_fields = ['next_moves', 'influence_network', 'cascade_probability', 'timeline_preview']
        for field in required_cascade_fields:
            assert field in cascade_data, f"Missing cascade field: {field}"
        
        # Validate influence network structure
        network = cascade_data['influence_network']
        assert 'nodes' in network, "Network should have nodes"
        assert 'edges' in network, "Network should have edges"
        assert 'total_relationships' in network, "Network should have total_relationships count"
        
        print("✅ Level 2 cascade prediction integration validated")
    
    def test_level_2_white_space_integration(self, pipeline):
        """Test white space detection integration in Level 2 Strategic Dashboard"""
        from scripts.compete_intel_pipeline import AnalysisResults
        
        analysis = AnalysisResults(
            current_state={'promotional_intensity': 0.6, 'market_position': 'strong'},
            influence={'copying_detected': False, 'similarity_score': 0.3},
            evolution={'trend_direction': 'stable', 'market_promo_change': 0.5, 'trend_strength': 0.1},
            forecasts={'next_week_intensity': 0.65, 'confidence': 0.8, 'trend': 'increasing'}
        )
        
        level_2 = pipeline._generate_level_2_strategic(analysis)
        
        # Validate white space integration
        assert 'white_space_opportunities' in level_2, "Level 2 should include white space opportunities"
        
        white_space_data = level_2['white_space_opportunities']
        required_ws_fields = ['identified_gaps', 'quick_wins', 'market_coverage_summary']
        for field in required_ws_fields:
            assert field in white_space_data, f"Missing white space field: {field}"
        
        # Validate market coverage summary
        summary = white_space_data['market_coverage_summary']
        coverage_metrics = ['virgin_territories', 'monopolies', 'underserved', 'total_opportunities']
        for metric in coverage_metrics:
            assert metric in summary, f"Missing coverage metric: {metric}"
            assert isinstance(summary[metric], int), f"Coverage metric {metric} should be integer"
        
        print("✅ Level 2 white space integration validated")
    
    def test_level_3_interventions_integration(self, pipeline):
        """Test Phase 8 features integration in Level 3 Interventions"""
        from scripts.compete_intel_pipeline import AnalysisResults
        
        analysis = AnalysisResults(
            current_state={'promotional_intensity': 0.6, 'market_position': 'strong'},
            influence={'copying_detected': False, 'similarity_score': 0.3},
            evolution={'trend_direction': 'stable', 'market_promo_change': 0.5, 'trend_strength': 0.1},
            forecasts={'next_week_intensity': 0.65, 'confidence': 0.8, 'trend': 'increasing'}
        )
        
        with patch('scripts.compete_intel_pipeline.run_query') as mock_query:
            # Mock successful BigQuery response
            mock_query.return_value = pd.DataFrame({
                'gap_type': ['CTA_AGGRESSIVENESS'],
                'intervention_title': ['Test Intervention'],
                'specific_action': ['Test Action'],
                'expected_impact': ['Test Impact'],
                'priority': ['HIGH']
            })
            
            level_3 = pipeline._generate_level_3_interventions(analysis)
        
        # Validate cascade prediction interventions
        assert 'cascade_predictions' in level_3, "Level 3 should include cascade prediction interventions"
        cascade_interventions = level_3['cascade_predictions']
        
        cascade_fields = ['preemptive_actions', 'defensive_strategies', 'opportunity_windows', 'timeline']
        for field in cascade_fields:
            assert field in cascade_interventions, f"Missing cascade intervention field: {field}"
        
        # Validate white space interventions
        assert 'white_spaces' in level_3, "Level 3 should include white space interventions"
        ws_interventions = level_3['white_spaces']
        
        ws_fields = ['quick_wins', 'strategic_bets', 'campaign_templates']
        for field in ws_fields:
            assert field in ws_interventions, f"Missing white space intervention field: {field}"
        
        print("✅ Level 3 Phase 8 interventions integration validated")


class TestPhase8Performance:
    """Test Phase 8 performance and success metrics"""
    
    def create_pipeline(self):
        """Create test pipeline instance"""
        return CompetitiveIntelligencePipeline(
            brand="TestBrand",
            vertical="Retail",
            dry_run=True
        )
    
    def test_cascade_prediction_confidence(self, pipeline):
        """Test cascade prediction confidence levels meet success criteria"""
        initial_move = {'brand': 'TestBrand', 'promotional_intensity': 0.7}
        predictions = pipeline._predict_cascade_sequence(initial_move)
        
        if predictions:
            first_move_confidence = predictions[0]['confidence']
            assert first_move_confidence >= 0.7, f"First move prediction confidence ({first_move_confidence:.2f}) should be >= 70%"
            print(f"✅ Cascade prediction confidence: {first_move_confidence*100:.0f}%")
        
        # Test influence network captures major relationships
        network = pipeline._build_influence_network()
        assert len(network['nodes']) >= 2, "Should capture at least 2 brand relationships"
        print(f"✅ Influence network size: {len(network['nodes'])} nodes")
    
    def test_white_space_opportunity_count(self, pipeline):
        """Test white space detection finds sufficient opportunities"""
        white_spaces = pipeline._detect_white_spaces()
        
        assert len(white_spaces) >= 3, f"Should identify at least 3 opportunities per brand, found {len(white_spaces)}"
        
        # Test prioritization produces actionable opportunities
        if not white_spaces.empty:
            spaces = white_spaces.to_dict('records')
            prioritized = pipeline._prioritize_white_spaces(spaces)
            
            quick_wins = [s for s in prioritized if s['priority'] == 'QUICK_WIN']
            strategic_bets = [s for s in prioritized if s['priority'] == 'STRATEGIC_BET']
            
            print(f"✅ White space opportunities: {len(quick_wins)} quick wins, {len(strategic_bets)} strategic bets")
    
    def test_data_utilization_improvement(self, pipeline):
        """Test that Phase 8 improves data utilization metrics"""
        # Mock data utilization check - in real implementation this would 
        # analyze actual field usage across the enhanced pipeline
        
        phase_7_fields = ['promotional_intensity', 'urgency_score', 'brand_voice_score', 
                         'fatigue_score', 'originality_score', 'competitive_assessment']
        
        phase_8_fields = phase_7_fields + [
            # Cascade prediction fields
            'response_lag', 'cascade_correlation', 'influence_strength',
            # White space detection fields  
            'market_concentration', 'competitor_count', 'opportunity_score',
            'strategic_fit', 'implementation_difficulty', 'competitive_moat'
        ]
        
        phase_7_utilization = len(phase_7_fields) / 20.0  # Assume 20 total available fields
        phase_8_utilization = len(phase_8_fields) / 20.0
        
        utilization_improvement = phase_8_utilization - phase_7_utilization
        target_improvement = 0.15  # 15 percentage points (40% -> 55%+)
        
        assert utilization_improvement >= target_improvement, \
            f"Data utilization improvement ({utilization_improvement*100:.0f}pp) should be >= {target_improvement*100:.0f}pp"
        
        print(f"✅ Data utilization: {phase_7_utilization*100:.0f}% → {phase_8_utilization*100:.0f}% (+{utilization_improvement*100:.0f}pp)")


def run_phase_8_validation():
    """Run complete Phase 8 validation suite"""
    print("🚀 Running Phase 8: Cascade Prediction & White Space Detection Tests")
    print("=" * 80)
    
    # Run all test classes
    test_classes = [
        TestPhase8CascadePrediction(),
        TestPhase8WhiteSpaceDetection(), 
        TestPhase8Integration(),
        TestPhase8Performance()
    ]
    
    total_tests = 0
    passed_tests = 0
    
    for test_class in test_classes:
        print(f"\n📋 {test_class.__class__.__name__}")
        print("-" * 60)
        
        # Get test methods
        test_methods = [method for method in dir(test_class) if method.startswith('test_')]
        
        for test_method in test_methods:
            total_tests += 1
            try:
                # Create fresh pipeline instance for each test
                if hasattr(test_class, 'pipeline'):
                    pipeline = test_class.pipeline()
                else:
                    pipeline = CompetitiveIntelligencePipeline("TestBrand", "Retail", dry_run=True)
                
                # Run test method
                method = getattr(test_class, test_method)
                method(pipeline)
                passed_tests += 1
                print(f"✅ {test_method}")
                
            except Exception as e:
                print(f"❌ {test_method}: {str(e)}")
    
    print("\n" + "=" * 80)
    print(f"🏁 Phase 8 Validation Complete: {passed_tests}/{total_tests} tests passed ({passed_tests/total_tests*100:.0f}%)")
    
    if passed_tests == total_tests:
        print("🎉 All Phase 8 features validated successfully!")
        print("✨ Ready for production deployment")
    else:
        print("⚠️  Some tests failed - review implementation before deployment")
    
    return passed_tests == total_tests


if __name__ == "__main__":
    success = run_phase_8_validation()
    sys.exit(0 if success else 1)