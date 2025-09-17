#!/usr/bin/env python3
"""
Test Enhanced Visual Intelligence Stage
"""
from src.pipeline.stages.visual_intelligence import VisualIntelligenceStage
from src.pipeline.core.base import PipelineContext
from src.pipeline.models.results import AnalysisResults

def test_enhanced_visual_stage():
    """Test the enhanced Visual Intelligence stage with competitive insights"""

    print("🎨 Testing Enhanced Visual Intelligence Stage")
    print("=" * 50)

    # Create test context
    context = PipelineContext('test_enhanced_visual', 'Warby Parker', 'eyewear')
    stage = VisualIntelligenceStage(context, dry_run=False)

    try:
        print("🚀 Running enhanced visual intelligence analysis...")

        # Mock analysis results
        mock_results = AnalysisResults([], [])
        results = stage.execute(mock_results)

        print(f"✅ Enhanced Visual Intelligence completed!")
        print(f"   📊 Sampled: {results.sampled_ads} ads")
        print(f"   💡 Visual insights: {results.visual_insights}")
        print(f"   🎯 Competitive insights: {results.competitive_insights}")
        print(f"   💰 Cost estimate: ${results.cost_estimate:.2f}")
        print(f"   📋 Table ID: {results.table_id}")

        return True

    except Exception as e:
        print(f"❌ Enhanced visual intelligence failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_enhanced_visual_stage()