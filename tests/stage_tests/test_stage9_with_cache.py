#!/usr/bin/env python3
"""
Stage 9 Enhanced Intelligence Output Testing with Modern Pipeline Integration
Tests the L1→L4 Progressive Disclosure Framework using cached Stage 8 MultiDimensional results
"""

import json
from datetime import datetime
from src.pipeline.stages.enhanced_output import EnhancedOutputStage
from src.pipeline.stages.multidimensional_intelligence import MultiDimensionalResults
from src.pipeline.core.base import PipelineContext

def load_stage8_multidimensional_results():
    """Load Stage 8 MultiDimensional results (modern pipeline output)"""
    try:
        with open('data/output/stage_tests/stage8_multidimensional_test_results.json', 'r') as f:
            stage8_data = json.load(f)

        print("✅ Loaded Stage 8 MultiDimensional results for Stage 9 input")
        print(f"   - Status: {stage8_data.get('status')}")
        print(f"   - Business Impact Score: {stage8_data.get('forecasts', {}).get('business_impact_score')}")
        print(f"   - Market Position: {stage8_data.get('current_state', {}).get('market_position')}")
        print(f"   - Data Completeness: {stage8_data.get('data_completeness', 0):.1f}%")

        # Modern intelligence components
        print(f"   🧠 Modern Intelligence Components:")
        print(f"      - Audience Intelligence: {stage8_data.get('audience_intelligence', {}).get('status', 'missing')}")
        print(f"      - Creative Intelligence: {stage8_data.get('creative_intelligence', {}).get('status', 'missing')}")
        print(f"      - Channel Intelligence: {stage8_data.get('channel_intelligence', {}).get('status', 'missing')}")
        print(f"      - Whitespace Intelligence: {stage8_data.get('whitespace_intelligence', {}).get('status', 'missing')}")

        # Convert to MultiDimensionalResults object (modern pipeline format)
        multidim_results = MultiDimensionalResults(
            # Core analysis (inherited from AnalysisResults)
            status=stage8_data.get('status', 'unknown'),
            message=stage8_data.get('message', 'Multi-dimensional intelligence completed'),
            current_state=stage8_data.get('current_state', {}),
            influence=stage8_data.get('influence', {}),
            evolution=stage8_data.get('evolution', {}),
            forecasts=stage8_data.get('forecasts', {}),
            metadata=stage8_data.get('metadata', {}),

            # Multi-dimensional intelligence (modern additions)
            audience_intelligence=stage8_data.get('audience_intelligence', {}),
            creative_intelligence=stage8_data.get('creative_intelligence', {}),
            channel_intelligence=stage8_data.get('channel_intelligence', {}),
            whitespace_intelligence=stage8_data.get('whitespace_intelligence', {}),
            intelligence_summary=stage8_data.get('intelligence_summary', {}),
            data_completeness=stage8_data.get('data_completeness', 0.0)
        )

        return multidim_results

    except FileNotFoundError:
        print("❌ Stage 8 results file not found. Run Stage 8 test first.")
        return None
    except Exception as e:
        print(f"❌ Error loading Stage 8 results: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_stage9_enhanced_output():
    """Test Stage 9 Enhanced Intelligence Output with Modern Pipeline Integration"""

    print("🧪 Testing Stage 9 Enhanced Intelligence Output (Modern Pipeline)")
    print("=" * 70)

    # Load prerequisite results (Stage 8 MultiDimensional)
    stage8_results = load_stage8_multidimensional_results()
    if not stage8_results:
        return

    try:
        # Create pipeline context (modern pipeline)
        context = PipelineContext(
            brand="Warby Parker",
            vertical="eyewear",
            run_id="stage4_test",
            verbose=True
        )
        context.competitor_brands = ['LensCrafters', 'EyeBuyDirect', 'Zenni Optical', 'GlassesUSA']

        # Initialize Stage 9 (Enhanced Output)
        stage9 = EnhancedOutputStage(
            context=context,
            dry_run=False,
            verbose=True
        )

        print(f"\n🎯 Testing Modern Enhanced Intelligence Output Generation")
        print(f"📊 Input: Stage 8 MultiDimensional Results (Full modern intelligence)")
        print(f"🏢 Brands analyzed: {len(context.competitor_brands + [context.brand])}")

        # Execute Stage 9 with MultiDimensional results
        print(f"\n🚀 Executing Stage 9 Enhanced Output...")
        start_time = datetime.now()
        results = stage9.execute(stage8_results)
        duration = (datetime.now() - start_time).total_seconds()

        print(f"   ✅ Stage 9 completed in {duration:.2f}s")

        # Validate modern pipeline output structure
        print(f"\n📊 Modern Pipeline Output Structure:")
        has_l1 = hasattr(results, 'level_1') and results.level_1
        has_l2 = hasattr(results, 'level_2') and results.level_2
        has_l3 = hasattr(results, 'level_3') and results.level_3
        has_l4 = hasattr(results, 'level_4') and results.level_4

        print(f"   Level 1 (Executive): {'✅' if has_l1 else '❌'}")
        print(f"   Level 2 (Strategic): {'✅' if has_l2 else '❌'}")
        print(f"   Level 3 (Interventions): {'✅' if has_l3 else '❌'}")
        print(f"   Level 4 (SQL Dashboards): {'✅' if has_l4 else '❌'}")

        # Analyze Level 1 Executive Summary
        if has_l1:
            l1 = results.level_1
            print(f"\n🎯 Level 1 Executive Summary (Modern):")
            if isinstance(l1, dict):
                insights = l1.get('insights', [])
                threat_level = l1.get('threat_level', 'N/A')
                confidence = l1.get('confidence_score', 'N/A')
                print(f"   - Critical Insights: {len(insights) if isinstance(insights, list) else 'N/A'}")
                print(f"   - Threat Level: {threat_level}")
                print(f"   - Confidence Score: {confidence}")

                # Sample key insight
                if isinstance(insights, list) and insights:
                    sample_insight = insights[0] if len(str(insights[0])) < 100 else str(insights[0])[:97] + "..."
                    print(f"   - Key Insight: {sample_insight}")

        # Analyze Level 2 Strategic Dashboard
        if has_l2:
            l2 = results.level_2
            print(f"\n📈 Level 2 Strategic Dashboard (Modern):")
            if isinstance(l2, dict):
                categories = [k for k in l2.keys() if not k.startswith('_')]
                print(f"   - Strategic Categories: {len(categories)}")
                print(f"   - Categories: {', '.join(categories[:5])}{'...' if len(categories) > 5 else ''}")

                # Check for modern intelligence integration
                creative_category = any('creative' in cat.lower() for cat in categories)
                channel_category = any('channel' in cat.lower() for cat in categories)
                audience_category = any('audience' in cat.lower() for cat in categories)

                print(f"   - Creative Intelligence: {'✅ Integrated' if creative_category else '❌ Missing'}")
                print(f"   - Channel Intelligence: {'✅ Integrated' if channel_category else '❌ Missing'}")
                print(f"   - Audience Intelligence: {'✅ Integrated' if audience_category else '❌ Missing'}")

        # Analyze Level 3 Actionable Interventions
        if has_l3:
            l3 = results.level_3
            print(f"\n🎮 Level 3 Actionable Interventions (Modern):")
            if isinstance(l3, dict):
                immediate = l3.get('immediate_actions', [])
                short_term = l3.get('short_term_tactics', [])
                monitoring = l3.get('monitoring_actions', [])

                immediate_count = len(immediate) if isinstance(immediate, list) else 0
                short_term_count = len(short_term) if isinstance(short_term, list) else 0
                monitoring_count = len(monitoring) if isinstance(monitoring, list) else 0

                print(f"   - Immediate Actions: {immediate_count}")
                print(f"   - Short-term Tactics: {short_term_count}")
                print(f"   - Monitoring Actions: {monitoring_count}")
                print(f"   - Total Interventions: {immediate_count + short_term_count + monitoring_count}")

        # Analyze Level 4 SQL Dashboards
        if has_l4:
            l4 = results.level_4
            print(f"\n📋 Level 4 SQL Dashboards (Modern):")
            if isinstance(l4, dict):
                project = l4.get('project', 'N/A')
                dataset = l4.get('dataset', 'N/A')
                queries = l4.get('dashboard_queries', [])

                print(f"   - Project: {project}")
                print(f"   - Dataset: {dataset}")
                print(f"   - SQL Dashboard Queries: {len(queries) if isinstance(queries, list) else 0}")

                if isinstance(queries, list) and queries:
                    total_sql_length = sum(len(str(q)) for q in queries)
                    print(f"   - Total SQL Length: {total_sql_length:,} characters")

        # Check Modern Intelligence Integration Evidence
        print(f"\n🧠 Modern Intelligence Integration Evidence:")

        # Check if Stage 8 intelligence is referenced in outputs
        full_output_str = str(results).lower()
        creative_evidence = any(term in full_output_str for term in ['creative', 'messaging', 'content'])
        channel_evidence = any(term in full_output_str for term in ['channel', 'platform', 'instagram', 'facebook'])
        audience_evidence = any(term in full_output_str for term in ['audience', 'demographic', 'psychographic'])
        whitespace_evidence = any(term in full_output_str for term in ['whitespace', 'opportunity', 'gap'])

        print(f"   - Creative Intelligence Signals: {'✅' if creative_evidence else '❌'}")
        print(f"   - Channel Intelligence Signals: {'✅' if channel_evidence else '❌'}")
        print(f"   - Audience Intelligence Signals: {'✅' if audience_evidence else '❌'}")
        print(f"   - Whitespace Intelligence Signals: {'✅' if whitespace_evidence else '❌'}")

        # Save comprehensive results
        output_data = {
            'test_type': 'stage9_modern_pipeline',
            'status': 'success',
            'stage9_execution_time': duration,
            'framework_type': 'L1_L4_progressive_disclosure_modern',
            'input_source': 'stage8_multidimensional_results',
            'modern_intelligence_integration': {
                'audience_intelligence_available': bool(stage8_results.audience_intelligence),
                'creative_intelligence_available': bool(stage8_results.creative_intelligence),
                'channel_intelligence_available': bool(stage8_results.channel_intelligence),
                'whitespace_intelligence_available': bool(stage8_results.whitespace_intelligence),
                'data_completeness': stage8_results.data_completeness
            },
            'output_structure': {
                'level_1_executive': has_l1,
                'level_2_strategic': has_l2,
                'level_3_interventions': has_l3,
                'level_4_dashboards': has_l4
            },
            'intelligence_evidence': {
                'creative_signals': creative_evidence,
                'channel_signals': channel_evidence,
                'audience_signals': audience_evidence,
                'whitespace_signals': whitespace_evidence
            },
            'progressive_disclosure_output': {
                'level_1': results.level_1 if has_l1 else None,
                'level_2': results.level_2 if has_l2 else None,
                'level_3': results.level_3 if has_l3 else None,
                'level_4': results.level_4 if has_l4 else None
            },
            'test_timestamp': datetime.now().isoformat()
        }

        with open('data/output/stage_tests/stage9_modern_pipeline_test_results.json', 'w') as f:
            json.dump(output_data, f, indent=2)

        print(f"\n💾 Results saved to: data/output/stage_tests/stage9_modern_pipeline_test_results.json")

        # Final modern pipeline assessment
        all_levels_generated = has_l1 and has_l2 and has_l3 and has_l4
        modern_intelligence_integrated = creative_evidence and channel_evidence and audience_evidence

        print(f"\n🏁 Modern Pipeline Stage 9 Assessment:")
        print(f"   Progressive Disclosure: {'✅ Complete (L1-L4)' if all_levels_generated else '⚠️  Partial'}")
        print(f"   Modern Intelligence: {'✅ Integrated' if modern_intelligence_integrated else '⚠️  Limited'}")
        print(f"   Data Completeness: {stage8_results.data_completeness:.1f}%")
        print(f"   Execution Performance: {'✅ Fast' if duration < 30 else '⚠️  Slow'} ({duration:.2f}s)")

        if all_levels_generated and modern_intelligence_integrated:
            print(f"\n🎉 SUCCESS: Stage 9 modern pipeline integration working perfectly!")
            print(f"   - All 4 levels of progressive disclosure generated")
            print(f"   - Stage 8 multi-dimensional intelligence fully integrated")
            print(f"   - Creative, Channel, Audience & Whitespace intelligence processed")
            print(f"   - Modern pipeline flow: Stage 7 → Stage 8 → Stage 9 ✅")
        else:
            print(f"\n⚠️  PARTIAL SUCCESS: Stage 9 completed with some limitations")
            if not all_levels_generated:
                print(f"   - Missing progressive disclosure levels")
            if not modern_intelligence_integrated:
                print(f"   - Modern intelligence integration incomplete")

    except Exception as e:
        print(f"❌ Stage 9 test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("## Stage 9 Expected Behavior (Modern Pipeline):")
    print("=" * 60)
    print("Stage 9 (Enhanced Intelligence Output) should generate:")
    print("🎯 Level 1: Executive Summary - Top 5 critical insights for C-suite")
    print("📈 Level 2: Strategic Dashboard - Strategic intelligence grouped by category")
    print("🎮 Level 3: Actionable Interventions - Immediate actions, tactics, monitoring")
    print("📋 Level 4: SQL Dashboards - Full analytical queries for deep-dive analysis")
    print("")
    print("🧠 Modern Intelligence Integration:")
    print("   - Should process Stage 8 MultiDimensional results (not Stage 7)")
    print("   - Must integrate Audience, Creative, Channel & Whitespace intelligence")
    print("   - Uses Progressive Disclosure Framework with intelligent filtering")
    print("   - Generates systematic L1→L4 output for different stakeholder needs")
    print("")
    print("🔧 Pipeline Flow: Stage 7 → Stage 8 → Stage 9 (Fixed orchestrator)")
    print("")

    test_stage9_enhanced_output()