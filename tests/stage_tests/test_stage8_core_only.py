#!/usr/bin/env python3
"""
Stage 8 Core Functionality Test - Disable expensive ML whitespace analysis
Test just the core intelligence modules without the AI.GENERATE_TEXT calls
"""

import json
from datetime import datetime
from src.pipeline.stages.multidimensional_intelligence import MultiDimensionalIntelligenceStage
from src.pipeline.models.results import AnalysisResults

def load_stage7_results():
    """Load Stage 7 results that include CTA Intelligence"""
    try:
        with open('data/output/stage_tests/stage7_cta_enhanced_test_results.json', 'r') as f:
            stage7_data = json.load(f)

        print("✅ Loaded Stage 7 results with CTA Intelligence")
        print(f"   - Status: {stage7_data.get('status')}")
        print(f"   - Business Impact Score: {stage7_data.get('forecasts', {}).get('business_impact_score')}")

        # Convert to AnalysisResults object
        analysis_results = AnalysisResults(
            status=stage7_data.get('status', 'unknown'),
            message="Stage 7 Analysis with CTA Intelligence completed",
            current_state=stage7_data.get('current_state', {}),
            influence=stage7_data.get('influence', {}),
            evolution=stage7_data.get('evolution', {}),
            forecasts=stage7_data.get('forecasts', {}),
            metadata=stage7_data.get('metadata', {})
        )

        return analysis_results

    except FileNotFoundError:
        print("❌ Stage 7 results file not found. Run Stage 7 test first.")
        return None
    except Exception as e:
        print(f"❌ Error loading Stage 7 results: {e}")
        return None

def test_individual_intelligence_modules():
    """Test each intelligence module individually to isolate any issues"""

    print("🧪 Testing Individual Stage 8 Intelligence Modules")
    print("=" * 60)

    previous_results = load_stage7_results()
    if not previous_results:
        return

    try:
        # Initialize Stage 8
        stage8 = MultiDimensionalIntelligenceStage(
            stage_name="Multi-Dimensional Intelligence",
            stage_number=8,
            run_id="stage4_test"
        )

        competitor_brands = ['Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
        stage8.competitor_brands = competitor_brands

        print("🎯 Testing Individual Intelligence Modules:")

        # Test 1: Audience Intelligence
        print("\n1️⃣ Testing Audience Intelligence...")
        try:
            audience_results = stage8._execute_audience_intelligence("stage4_test", competitor_brands)
            print(f"   ✅ Status: {audience_results.get('status')}")
            print(f"   📊 Table: {audience_results.get('table_created')}")
            print(f"   🎯 Brands: {len(audience_results.get('brands_analyzed', []))}")
        except Exception as e:
            print(f"   ❌ Audience Intelligence failed: {e}")
            import traceback
            traceback.print_exc()

        # Test 2: Creative Intelligence
        print("\n2️⃣ Testing Creative Intelligence...")
        try:
            creative_results = stage8._execute_creative_intelligence("stage4_test", competitor_brands)
            print(f"   ✅ Status: {creative_results.get('status')}")
            print(f"   📊 Table: {creative_results.get('table_created')}")
            print(f"   🎯 Brands: {len(creative_results.get('brands_analyzed', []))}")
        except Exception as e:
            print(f"   ❌ Creative Intelligence failed: {e}")
            import traceback
            traceback.print_exc()

        # Test 3: Channel Intelligence
        print("\n3️⃣ Testing Channel Intelligence...")
        try:
            channel_results = stage8._execute_channel_intelligence("stage4_test", competitor_brands)
            print(f"   ✅ Status: {channel_results.get('status')}")
            print(f"   📊 Table: {channel_results.get('table_created')}")
            print(f"   🎯 Brands: {len(channel_results.get('brands_analyzed', []))}")
        except Exception as e:
            print(f"   ❌ Channel Intelligence failed: {e}")
            import traceback
            traceback.print_exc()

        # Test 4: Data Completeness
        print("\n4️⃣ Testing Data Completeness Calculation...")
        try:
            completeness = stage8._calculate_data_completeness("stage4_test", competitor_brands)
            print(f"   ✅ Data Completeness: {completeness:.1f}%")
        except Exception as e:
            print(f"   ❌ Data Completeness failed: {e}")
            import traceback
            traceback.print_exc()

        # Test 5: Whitespace Intelligence (basic fallback only)
        print("\n5️⃣ Testing Basic Whitespace Intelligence (no ML)...")
        try:
            whitespace_results = stage8._generate_basic_whitespace_analysis("stage4_test", competitor_brands)
            print(f"   ✅ Status: {whitespace_results.get('status')}")
            print(f"   🎯 Opportunities: {whitespace_results.get('opportunities_found', 0)}")
            print(f"   📊 Quality: {whitespace_results.get('data_quality')}")
        except Exception as e:
            print(f"   ❌ Basic Whitespace failed: {e}")
            import traceback
            traceback.print_exc()

        print("\n✅ Individual module testing completed!")

    except Exception as e:
        print(f"❌ Individual module test failed: {str(e)}")
        import traceback
        traceback.print_exc()

def test_stage8_without_expensive_ml():
    """Test Stage 8 by temporarily disabling the expensive ML whitespace analysis"""

    print("\n" + "=" * 60)
    print("🧪 Testing Complete Stage 8 (Basic Whitespace Only)")
    print("=" * 60)

    previous_results = load_stage7_results()
    if not previous_results:
        return

    try:
        # Initialize Stage 8
        stage8 = MultiDimensionalIntelligenceStage(
            stage_name="Multi-Dimensional Intelligence",
            stage_number=8,
            run_id="stage4_test"
        )

        competitor_brands = ['Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']
        stage8.competitor_brands = competitor_brands

        # Temporarily override the whitespace intelligence to use basic analysis only
        original_method = stage8._execute_whitespace_intelligence

        def basic_whitespace_only(run_id, brands):
            return stage8._generate_basic_whitespace_analysis(run_id, brands)

        stage8._execute_whitespace_intelligence = basic_whitespace_only

        print(f"🎯 Testing Stage 8 with basic whitespace analysis only")
        print(f"📊 Using cached data from run_id: stage4_test")

        # Execute Stage 8
        print("\n🚀 Executing Stage 8...")
        results = stage8.execute(previous_results)

        # Restore original method
        stage8._execute_whitespace_intelligence = original_method

        # Validate results
        print(f"\n📊 Stage 8 Results:")
        print(f"   Status: {results.status}")
        print(f"   Message: {results.message}")

        if results.status == 'success':
            print(f"\n🎯 Strategic Metrics Preservation:")
            print(f"   - Promotional Intensity: {results.current_state.get('promotional_intensity', 'N/A')}")
            print(f"   - Business Impact Score: {results.forecasts.get('business_impact_score', 'N/A')}")
            print(f"   - Top Copier: {results.influence.get('top_copier', 'N/A')}")

            print(f"\n🧠 Intelligence Module Results:")
            print(f"   - Audience Intelligence: {results.audience_intelligence.get('status', 'N/A')}")
            print(f"   - Creative Intelligence: {results.creative_intelligence.get('status', 'N/A')}")
            print(f"   - Channel Intelligence: {results.channel_intelligence.get('status', 'N/A')}")
            print(f"   - Whitespace Intelligence: {results.whitespace_intelligence.get('status', 'N/A')}")
            print(f"   - Data Completeness: {results.data_completeness:.1f}%")

            print(f"\n✅ Stage 8 completed successfully without expensive ML queries!")
        else:
            print(f"\n❌ Stage 8 failed: {results.message}")

    except Exception as e:
        print(f"❌ Stage 8 test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_individual_intelligence_modules()
    test_stage8_without_expensive_ml()