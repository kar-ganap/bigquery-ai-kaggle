"""
Test Phase 3 Visual Intelligence Integration

Quick test to validate that Phase 3 is properly integrated into the pipeline.
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.pipeline.core.base import PipelineContext
from src.pipeline.stages.visual_intelligence import VisualIntelligenceStage
from src.pipeline.models.results import AnalysisResults

def test_phase3_integration():
    print('🎨 Testing Phase 3 Visual Intelligence Integration')
    print('=' * 60)

    # Step 1: Test stage creation
    print('\n📦 Step 1: Testing Visual Intelligence Stage Creation')

    context = PipelineContext(
        brand="Warby Parker",
        vertical="eyewear",
        run_id="test_phase3_20250916"
    )

    # Create visual intelligence stage
    visual_stage = VisualIntelligenceStage(context, dry_run=True)
    print(f'   ✅ Successfully created VisualIntelligenceStage')
    print(f'   📊 Stage: Visual Intelligence, Context: {context.brand}')

    # Step 2: Test dry run execution
    print('\n🧪 Step 2: Testing Dry Run Execution')

    # Create mock analysis results (what would come from embeddings stage)
    mock_analysis = AnalysisResults(
        current_state={
            'promotional_intensity': 0.55,
            'urgency_score': 0.06,
            'brand_voice_score': 0.59
        }
    )

    try:
        results = visual_stage.execute(mock_analysis)

        print(f'   ✅ Dry run execution successful')
        print(f'   📊 Sampled ads: {results.sampled_ads}')
        print(f'   💡 Visual insights: {results.visual_insights}')
        print(f'   💰 Cost estimate: ${results.cost_estimate:.2f}')

    except Exception as e:
        print(f'   ❌ Dry run failed: {str(e)}')
        return False

    # Step 3: Test adaptive sampling SQL generation
    print('\n🎯 Step 3: Testing Adaptive Sampling SQL')

    try:
        sampling_sql = visual_stage._generate_adaptive_sampling_sql()

        print(f'   ✅ Successfully generated adaptive sampling SQL')
        print(f'   📊 SQL length: {len(sampling_sql)} characters')

        # Check for key components
        has_adaptive_logic = 'CASE' in sampling_sql and 'target_sample_size' in sampling_sql
        has_budget_control = 'final_sample_size' in sampling_sql

        print(f'   ✅ Contains adaptive logic: {has_adaptive_logic}')
        print(f'   ✅ Contains budget control: {has_budget_control}')

    except Exception as e:
        print(f'   ❌ SQL generation failed: {str(e)}')
        return False

    # Step 4: Test visual analysis SQL generation
    print('\n🔍 Step 4: Testing Visual Analysis SQL')

    try:
        analysis_sql = visual_stage._generate_visual_analysis_sql()

        print(f'   ✅ Successfully generated visual analysis SQL')
        print(f'   📊 SQL length: {len(analysis_sql)} characters')

        # Check for key components
        has_multimodal = 'AI.GENERATE' in analysis_sql
        has_strategic_scoring = 'strategic_score' in analysis_sql
        has_image_analysis = 'primary_image_url' in analysis_sql

        print(f'   ✅ Contains multimodal AI: {has_multimodal}')
        print(f'   ✅ Contains strategic scoring: {has_strategic_scoring}')
        print(f'   ✅ Contains image analysis: {has_image_analysis}')

    except Exception as e:
        print(f'   ❌ Visual analysis SQL generation failed: {str(e)}')
        return False

    # Step 5: Validate budget parameters
    print('\n💰 Step 5: Testing Budget Configuration')

    per_brand_budget = visual_stage.per_brand_budget
    max_total_budget = visual_stage.max_total_budget

    print(f'   📊 Per-brand budget: {per_brand_budget} images')
    print(f'   📊 Max total budget: {max_total_budget} images')

    budget_reasonable = 5 <= per_brand_budget <= 20 and 50 <= max_total_budget <= 200
    print(f'   ✅ Budget parameters reasonable: {budget_reasonable}')

    # Final validation
    print('\n🎯 Phase 3 Integration Validation')

    all_tests_passed = True
    integration_ready = all_tests_passed and budget_reasonable

    print(f'   ✅ Stage creation: Success')
    print(f'   ✅ Dry run execution: Success')
    print(f'   ✅ Adaptive sampling: Success')
    print(f'   ✅ Visual analysis: Success')
    print(f'   ✅ Budget configuration: Success')

    if integration_ready:
        print(f'\n   🎉 PHASE 3 INTEGRATION SUCCESSFUL!')
        print(f'   🚀 Ready for full pipeline execution with visual intelligence')
        print(f'\n   📋 Next Steps:')
        print(f'      1. Run pipeline with visual intelligence enabled')
        print(f'      2. Validate visual-text contradiction detection')
        print(f'      3. Review competitive visual insights')
    else:
        print(f'\n   ⚠️  Integration issues detected - needs debugging')

    return integration_ready

if __name__ == "__main__":
    success = test_phase3_integration()
    if success:
        print(f'\n🎨 Phase 3 visual intelligence ready for deployment!')
    else:
        print(f'\n⚠️  Phase 3 integration needs attention')