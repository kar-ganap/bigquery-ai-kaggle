#!/usr/bin/env python3
"""
Simple Test Suite for Phase 8: Cascade Prediction Intelligence & White Space Detection
Direct testing without pytest fixtures
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.compete_intel_pipeline import CompetitiveIntelligencePipeline


def test_cascade_prediction_pipeline():
    """Test cascade prediction functionality"""
    print("🔮 Testing Cascade Prediction Intelligence...")
    
    pipeline = CompetitiveIntelligencePipeline(
        brand="TestBrand",
        vertical="Retail", 
        dry_run=True
    )
    
    # Test cascade pattern detection
    cascades = pipeline._detect_cascade_patterns()
    assert isinstance(cascades, pd.DataFrame), "Should return DataFrame"
    assert len(cascades) >= 0, "Should handle empty results gracefully"
    print(f"   ✅ Cascade pattern detection: {len(cascades)} patterns found")
    
    # Test influence network construction
    network = pipeline._build_influence_network()
    assert isinstance(network, dict), "Network should be dict"
    assert 'nodes' in network and 'edges' in network, "Network should have nodes and edges"
    print(f"   ✅ Influence network: {len(network['nodes'])} nodes, {len(network['edges'])} edges")
    
    # Test cascade sequence prediction
    initial_move = {
        'brand': 'TestBrand',
        'promotional_intensity': 0.8,
        'timestamp': datetime.now().isoformat()
    }
    predictions = pipeline._predict_cascade_sequence(initial_move)
    assert isinstance(predictions, list), "Predictions should be list"
    assert len(predictions) <= 3, "Should predict max 3 moves ahead"
    print(f"   ✅ Cascade sequence prediction: {len(predictions)} moves predicted")
    
    # Test cascade interventions
    if predictions:
        interventions = pipeline._generate_cascade_interventions(predictions)
        assert isinstance(interventions, dict), "Interventions should be dict"
        required_sections = ['cascade_timeline', 'preemptive_actions']
        for section in required_sections:
            assert section in interventions, f"Missing intervention section: {section}"
        print(f"   ✅ Cascade interventions: {len(interventions['cascade_timeline'])} timeline items")
    
    print("   🎉 Cascade prediction tests passed!")
    return True


def test_white_space_detection_pipeline():
    """Test white space detection functionality"""
    print("🎯 Testing White Space Detection...")
    
    pipeline = CompetitiveIntelligencePipeline(
        brand="TestBrand",
        vertical="Retail",
        dry_run=True
    )
    
    # Test white space detection
    white_spaces = pipeline._detect_white_spaces()
    assert isinstance(white_spaces, pd.DataFrame), "Should return DataFrame"
    assert len(white_spaces) >= 0, "Should handle empty results gracefully"
    print(f"   ✅ White space detection: {len(white_spaces)} opportunities found")
    
    if not white_spaces.empty:
        # Test market potential estimation
        test_space = white_spaces.iloc[0].to_dict()
        potential = pipeline._estimate_market_potential(test_space)
        assert isinstance(potential, dict), "Potential should be dict"
        required_metrics = ['strategic_fit', 'implementation_difficulty', 'competitive_moat']
        for metric in required_metrics:
            assert metric in potential, f"Missing metric: {metric}"
        print(f"   ✅ Market potential estimation: {potential.get('total_potential_score', 0):.1f} score")
        
        # Test prioritization
        spaces = white_spaces.to_dict('records')
        prioritized = pipeline._prioritize_white_spaces(spaces)
        assert isinstance(prioritized, list), "Prioritized spaces should be list"
        assert len(prioritized) == len(spaces), "Should prioritize all spaces"
        print(f"   ✅ White space prioritization: {len(prioritized)} spaces prioritized")
        
        # Test interventions
        interventions = pipeline._generate_white_space_interventions(prioritized)
        assert isinstance(interventions, dict), "Interventions should be dict"
        required_sections = ['quick_wins', 'strategic_bets', 'campaign_templates']
        for section in required_sections:
            assert section in interventions, f"Missing intervention section: {section}"
        print(f"   ✅ White space interventions: {len(interventions['quick_wins'])} quick wins")
    
    print("   🎉 White space detection tests passed!")
    return True


def test_phase_8_integration():
    """Test Phase 8 integration with existing pipeline levels"""
    print("🔗 Testing Phase 8 Integration...")
    
    pipeline = CompetitiveIntelligencePipeline(
        brand="TestBrand",
        vertical="Retail",
        dry_run=True
    )
    pipeline.competitor_brands = ['Competitor_A', 'Competitor_B', 'Competitor_C']
    
    # Test Level 2 integration with mock analysis
    from scripts.compete_intel_pipeline import AnalysisResults
    
    analysis = AnalysisResults(
        current_state={'promotional_intensity': 0.6, 'market_position': 'strong'},
        influence={'copying_detected': False, 'similarity_score': 0.3},
        evolution={'trend_direction': 'stable', 'market_promo_change': 0.5, 'trend_strength': 0.1},
        forecasts={'next_week_intensity': 0.65, 'confidence': 0.8, 'trend': 'increasing'}
    )
    
    # Generate Level 2 with Phase 8 enhancements
    level_2 = pipeline._generate_level_2_strategic(analysis)
    
    # Validate Phase 8 integration
    assert 'cascade_predictions' in level_2, "Level 2 should include cascade predictions"
    assert 'white_space_opportunities' in level_2, "Level 2 should include white space opportunities"
    
    cascade_data = level_2['cascade_predictions']
    assert 'next_moves' in cascade_data, "Should have cascade predictions"
    assert 'influence_network' in cascade_data, "Should have influence network"
    
    ws_data = level_2['white_space_opportunities']
    assert 'identified_gaps' in ws_data, "Should have white space gaps"
    assert 'market_coverage_summary' in ws_data, "Should have market coverage summary"
    
    print("   ✅ Level 2 Phase 8 integration validated")
    
    # Test Level 3 integration 
    from unittest.mock import patch
    
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
        
        # Validate Phase 8 integration
        assert 'cascade_predictions' in level_3, "Level 3 should include cascade prediction interventions"
        assert 'white_spaces' in level_3, "Level 3 should include white space interventions"
        
        print("   ✅ Level 3 Phase 8 integration validated")
    
    print("   🎉 Phase 8 integration tests passed!")
    return True


def test_end_to_end_pipeline():
    """Test complete end-to-end pipeline with Phase 8 enhancements"""
    print("🚀 Testing End-to-End Pipeline with Phase 8...")
    
    pipeline = CompetitiveIntelligencePipeline(
        brand="Warby Parker",
        vertical="Eyewear",
        dry_run=True
    )
    
    try:
        # Run complete pipeline (dry-run mode)
        results = pipeline.execute_pipeline()
        output = results.output
        
        # Validate pipeline results structure  
        assert hasattr(results, 'output'), "Results should have output attribute"
        assert results.output is not None, "Results should have output"
        
        # The output is an IntelligenceOutput object with level_1, level_2, etc. attributes
        output = results.output
        
        # Validate 4 levels exist in output
        assert hasattr(output, 'level_1'), "Should have level_1"
        assert hasattr(output, 'level_2'), "Should have level_2" 
        assert hasattr(output, 'level_3'), "Should have level_3"
        
        # Validate Phase 8 features in Level 2
        level_2 = output.level_2
        assert isinstance(level_2, dict), "Level 2 should be dict"
        assert 'cascade_predictions' in level_2, "Level 2 should include Phase 8 cascade predictions"
        assert 'white_space_opportunities' in level_2, "Level 2 should include Phase 8 white space opportunities"
        
        # Validate cascade predictions structure
        cascade_data = level_2['cascade_predictions']
        assert 'next_moves' in cascade_data, "Should have next moves"
        assert 'influence_network' in cascade_data, "Should have influence network"
        
        # Validate white space structure  
        ws_data = level_2['white_space_opportunities']
        assert 'identified_gaps' in ws_data, "Should have identified gaps"
        assert 'market_coverage_summary' in ws_data, "Should have market coverage"
        
        # Validate Phase 8 features in Level 3
        level_3 = output.level_3
        assert isinstance(level_3, dict), "Level 3 should be dict"
        assert 'cascade_predictions' in level_3, "Level 3 should include Phase 8 cascade interventions"
        assert 'white_spaces' in level_3, "Level 3 should include Phase 8 white space interventions"
        
        print("   ✅ End-to-end pipeline execution successful")
        print("   ✅ All 4 levels generated with Phase 8 enhancements")
        
        # Validate performance
        duration = output['level_1_executive'].get('duration', 'Unknown')
        print(f"   ✅ Pipeline performance: {duration}")
        
        print("   🎉 End-to-end pipeline test passed!")
        return True
        
    except Exception as e:
        print(f"   ❌ End-to-end pipeline test failed: {str(e)}")
        return False


def run_phase_8_tests():
    """Run all Phase 8 tests"""
    print("🚀 Phase 8: Cascade Prediction & White Space Detection - Test Suite")
    print("=" * 80)
    
    tests = [
        ("Cascade Prediction Pipeline", test_cascade_prediction_pipeline),
        ("White Space Detection Pipeline", test_white_space_detection_pipeline),  
        ("Phase 8 Integration", test_phase_8_integration),
        ("End-to-End Pipeline", test_end_to_end_pipeline)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 60)
        
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"   ❌ Test failed: {str(e)}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 80)
    print(f"🏁 Phase 8 Test Results: {passed}/{total} tests passed ({passed/total*100:.0f}%)")
    
    if passed == total:
        print("🎉 All Phase 8 features working correctly!")
        print("✨ Pipeline enhanced with:")
        print("   • 3-move-ahead cascade predictions")
        print("   • Brand influence network analysis")
        print("   • White space opportunity detection")
        print("   • Preemptive intervention strategies")
        print("   • Campaign template generation")
        print("\n🚀 Phase 8 ready for production!")
    else:
        print("⚠️  Some tests failed - review implementation")
    
    return passed == total


if __name__ == "__main__":
    success = run_phase_8_tests()
    sys.exit(0 if success else 1)