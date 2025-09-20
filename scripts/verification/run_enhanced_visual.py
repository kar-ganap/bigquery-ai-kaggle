#!/usr/bin/env python3
"""
Run Enhanced Visual Intelligence with Real Data
"""
from src.pipeline.stages.visual_intelligence import VisualIntelligenceStage
from src.pipeline.core.base import PipelineContext
from src.pipeline.models.results import AnalysisResults

def run_enhanced_visual_intelligence():
    """Run enhanced Visual Intelligence analysis with real data"""

    print("🎨 Running Enhanced Visual Intelligence with Real Data")
    print("=" * 60)

    # Create context with timestamp
    from datetime import datetime
    run_id = f"enhanced_visual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    context = PipelineContext(run_id, 'Warby Parker', 'eyewear')
    stage = VisualIntelligenceStage(context, dry_run=False)

    print(f"🚀 Running enhanced visual intelligence analysis...")
    print(f"   Run ID: {run_id}")
    print(f"   Target: 20 images per brand, max 200 total")

    try:
        # Mock analysis results
        mock_results = AnalysisResults([], [])
        results = stage.execute(mock_results)

        print(f"\n✅ Enhanced Visual Intelligence completed!")
        print(f"   📊 Sampled: {results.sampled_ads} ads")
        print(f"   💡 Visual insights: {results.visual_insights}")
        print(f"   🎯 Competitive insights: {results.competitive_insights}")
        print(f"   💰 Cost estimate: ${results.cost_estimate:.2f}")
        print(f"   📋 Table: visual_intelligence_{run_id}")

        # Test query the results
        if results.sampled_ads > 0:
            from src.utils.bigquery_client import run_query

            test_query = f"""
            SELECT
              brand,
              COUNT(*) as total_analyzed,
              AVG(visual_text_alignment_score) as avg_visual_alignment,
              AVG(luxury_positioning_score) as avg_luxury_pos,
              AVG(boldness_score) as avg_boldness,
              STRING_AGG(DISTINCT target_demographic, ', ') as demographics
            FROM `bigquery-ai-kaggle-469620.ads_demo.visual_intelligence_{run_id}`
            WHERE luxury_positioning_score IS NOT NULL
            GROUP BY brand
            ORDER BY total_analyzed DESC
            """

            print(f"\n📊 COMPETITIVE POSITIONING RESULTS:")
            result = run_query(test_query)
            for _, row in result.iterrows():
                print(f"   🏢 {row['brand']}: {row['total_analyzed']} ads")
                print(f"      Visual Alignment: {row['avg_visual_alignment']:.3f}")
                print(f"      Luxury Position: {row['avg_luxury_pos']:.3f} | Boldness: {row['avg_boldness']:.3f}")
                print(f"      Target Demographics: {row['demographics']}")

        return True

    except Exception as e:
        print(f"❌ Enhanced visual intelligence failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    run_enhanced_visual_intelligence()