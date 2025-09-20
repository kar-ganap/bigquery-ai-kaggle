"""
Stage 7: Enhanced Intelligence Output Generation

Systematic L1→L4 progressive disclosure with intelligent signal filtering.
Integrates the new ProgressiveDisclosureFramework for enhanced intelligence organization.
"""
import os
import time
import json
from typing import List, Dict, Any

from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import AnalysisResults, IntelligenceOutput
from ...intelligence.framework import (
    ProgressiveDisclosureFramework,
    create_creative_intelligence_signals,
    create_channel_intelligence_signals,
    create_audience_intelligence_signals,
    create_visual_intelligence_signals
)

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class EnhancedOutputStage(PipelineStage[AnalysisResults, IntelligenceOutput]):
    """
    Stage 7: Enhanced Intelligence Output Generation.
    
    Responsibilities:
    - Generate Level 1: Executive Summary (Critical Insights Only)
    - Generate Level 2: Strategic Dashboard (Strategic Intelligence)
    - Generate Level 3: Actionable Interventions (Tactical Detail)
    - Generate Level 4: SQL Dashboards (Full Analytical Detail)
    - Apply systematic signal filtering and thresholding
    - Integrate Creative and Channel Intelligence P2 enhancements
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Enhanced Intelligence Output", 7, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.verbose = verbose
    
    def execute(self, analysis: AnalysisResults) -> IntelligenceOutput:
        """Execute systematic intelligence output generation with L1→L4 framework"""
        
        print("   📊 Generating systematic L1→L4 intelligence framework...")
        print("   🎯 Using intelligent signal filtering and thresholding")
        
        # Initialize progressive disclosure framework
        framework = ProgressiveDisclosureFramework()
        
        # Add signals from core analysis
        self._add_core_analysis_signals(framework, analysis)
        
        # Add Creative Intelligence signals (P2 enhancements)
        if hasattr(analysis, 'creative_intelligence') and analysis.creative_intelligence:
            create_creative_intelligence_signals(framework, analysis.creative_intelligence)
            print(f"   🎨 Added Creative Intelligence signals: {len([s for s in framework.signals if 'Creative' in s.source_module])}")
        
        # Add Channel Intelligence signals (P2 enhancements)
        if hasattr(analysis, 'channel_intelligence') and analysis.channel_intelligence:
            create_channel_intelligence_signals(framework, analysis.channel_intelligence)
            print(f"   📺 Added Channel Intelligence signals: {len([s for s in framework.signals if 'Channel' in s.source_module])}")

        # Add Audience Intelligence signals (P0 Priority - fundamental analysis)
        if hasattr(analysis, 'audience_intelligence') and analysis.audience_intelligence:
            create_audience_intelligence_signals(framework, analysis.audience_intelligence)
            print(f"   👥 Added Audience Intelligence signals: {len([s for s in framework.signals if 'Audience' in s.source_module])}")

        # Add Visual Intelligence signals (Phase 3 - multimodal)
        if hasattr(analysis, 'visual_intelligence') and analysis.visual_intelligence:
            create_visual_intelligence_signals(framework, analysis.visual_intelligence)
            print(f"   👁️ Added Visual Intelligence signals: {len([s for s in framework.signals if 'Visual' in s.source_module])}")

        # Add Whitespace Intelligence signals (Phase 8 - market opportunities)
        if hasattr(analysis, 'whitespace_intelligence') and analysis.whitespace_intelligence:
            self._add_whitespace_signals(framework, analysis.whitespace_intelligence)
            print(f"   🎯 Added Whitespace Intelligence signals: {len([s for s in framework.signals if 'Whitespace' in s.source_module])}")

        # Generate systematic L1→L4 output
        output = IntelligenceOutput()
        
        print(f"   📈 Framework Stats: {framework.get_framework_stats()['total_signals']} total signals, {framework.get_framework_stats()['framework_efficiency']:.1%} efficiency")
        
        print("   🎯 Level 1: Executive Summary (Top 5 Critical Insights)")
        output.level_1 = framework.generate_level_1_executive()
        
        print("   📈 Level 2: Strategic Dashboard (Strategic Intelligence)")
        output.level_2 = framework.generate_level_2_strategic()
        
        print("   🎮 Level 3: Actionable Interventions (Tactical Detail)")
        output.level_3 = framework.generate_level_3_interventions()
        
        print("   📋 Level 4: SQL Dashboards (Full Analytical Detail)")
        output.level_4 = framework.generate_level_4_dashboards(BQ_PROJECT, BQ_DATASET)
        
        # Display output with framework stats
        self._display_systematic_output(output, framework)
        
        # Save output files
        if not self.dry_run:
            self._save_output_files(output, analysis)

        return output
    
    def _add_whitespace_signals(self, framework: ProgressiveDisclosureFramework, whitespace_data: Dict) -> None:
        """Add whitespace opportunity signals to the framework"""

        if not whitespace_data or 'opportunities' not in whitespace_data:
            return

        for opp in whitespace_data.get('opportunities', []):
            if opp.get('score', 0) > 0.3:  # Only include significant opportunities
                framework.add_signal(
                    insight=f"Whitespace opportunity: {opp.get('segment', 'Unknown')} - {opp.get('messaging_angle', 'Unknown')}",
                    value=opp.get('score', 0),
                    confidence=opp.get('confidence', 0.7),
                    business_impact=0.8,
                    actionability=0.9,
                    source_module="Whitespace Intelligence",
                    metric_name="whitespace_opportunity_score",
                    metadata={
                        'space_type': opp.get('space_type', 'Virgin Territory'),
                        'funnel_stage': opp.get('funnel_stage', 'Unknown'),
                        'campaign_ready': opp.get('campaign_ready', False),
                        'sample_headline': opp.get('sample_headline', ''),
                        'sample_cta': opp.get('sample_cta', '')
                    }
                )

    def _add_core_analysis_signals(self, framework: ProgressiveDisclosureFramework, analysis: AnalysisResults) -> None:
        """Add core analysis signals to the framework"""
        
        # Market Position Signal
        market_position = analysis.current_state.get('market_position', 'unknown')
        if market_position == 'defensive':
            framework.add_signal(
                insight=f"Brand is in defensive market position - strategic repositioning needed",
                value=market_position,
                confidence=0.8,
                business_impact=0.9,
                actionability=0.7,
                source_module="Market Analysis",
                metric_name="market_position_defensive",
                metadata={'position': market_position, 'requires': 'strategic_action'}
            )
        
        # Competitive Threat Signal
        copying_detected = analysis.influence.get('copying_detected', False)
        if copying_detected:
            top_copier = analysis.influence.get('top_copier', 'Unknown competitor')
            similarity_score = analysis.influence.get('similarity_score', 0.0)

            # Phase 5: Apply temporal enhancement to competitive intelligence
            base_insight = f"Competitive copying detected from {top_copier} (similarity: {similarity_score:.1%})"
            temporal_metadata = {
                'temporal_trend': 'increasing',  # competitive copying is increasing (threat accelerating)
                'timeframe': '6 weeks'
            }
            enhanced_insight = framework.add_temporal_context(
                base_insight,
                similarity_score,
                'competitive_copying',
                temporal_metadata
            )

            framework.add_signal(
                insight=enhanced_insight,
                value=similarity_score,
                confidence=0.9,
                business_impact=0.8,
                actionability=0.8,
                source_module="Competitive Intelligence",
                metric_name="competitive_similarity_score",
                metadata={'copier': top_copier, 'similarity': similarity_score}
            )
        
        # Momentum Signal
        momentum = analysis.evolution.get('momentum_status', 'STABLE')
        velocity_7d = analysis.evolution.get('velocity_change_7d', 0.0)
        if momentum == 'ACCELERATING' and velocity_7d > 10.0:
            framework.add_signal(
                insight=f"Positive momentum detected - {velocity_7d:.1f}% velocity increase (7-day)",
                value=velocity_7d,
                confidence=0.7,
                business_impact=0.6,
                actionability=0.5,
                source_module="Momentum Analysis",
                metric_name="momentum_velocity_positive",
                metadata={'momentum': momentum, 'velocity_7d': velocity_7d}
            )
        
        # CTA Performance Signals (if available)
        cta_aggressiveness = analysis.current_state.get('avg_cta_aggressiveness', 0.0)
        if cta_aggressiveness > 0.0:
            if cta_aggressiveness < 3.0:
                framework.add_signal(
                    insight=f"Low CTA aggressiveness detected ({cta_aggressiveness:.1f}/10) - strengthen call-to-action language",
                    value=cta_aggressiveness,
                    confidence=0.8,
                    business_impact=0.7,
                    actionability=0.9,
                    source_module="CTA Intelligence",
                    metric_name="cta_aggressiveness_low",
                    metadata={'current_score': cta_aggressiveness, 'target': 5.0}
                )
            elif cta_aggressiveness > 7.0:
                framework.add_signal(
                    insight=f"High CTA aggressiveness ({cta_aggressiveness:.1f}/10) - consider moderation for brand trust",
                    value=cta_aggressiveness,
                    confidence=0.6,
                    business_impact=0.5,
                    actionability=0.6,
                    source_module="CTA Intelligence",
                    metric_name="cta_aggressiveness_high",
                    metadata={'current_score': cta_aggressiveness, 'recommendation': 'moderate'}
                )
        
        # Forecasting Signals
        business_impact_score = analysis.forecasts.get('business_impact_score', 0)
        confidence = analysis.forecasts.get('confidence', 'MEDIUM')
        if business_impact_score >= 8:
            framework.add_signal(
                insight=f"High business impact forecast (score: {business_impact_score}/10) - significant opportunity detected",
                value=business_impact_score,
                confidence=0.7 if confidence == 'HIGH' else 0.5,
                business_impact=0.8,
                actionability=0.6,
                source_module="Forecasting Intelligence",
                metric_name="business_impact_forecast_high",
                metadata={'impact_score': business_impact_score, 'forecast_confidence': confidence}
            )
    
    def _display_systematic_output(self, output: IntelligenceOutput, framework: ProgressiveDisclosureFramework):
        """Display systematic framework output with intelligence metrics"""
        
        print("\n" + "=" * 80)
        print("🎯 SYSTEMATIC INTELLIGENCE OUTPUT (L1→L4 FRAMEWORK)")
        print("=" * 80)
        
        # Framework Statistics
        stats = framework.get_framework_stats()
        print(f"\n📊 FRAMEWORK STATISTICS")
        print("-" * 40)
        print(f"Total Signals: {stats['total_signals']}")
        print(f"Framework Efficiency: {stats['framework_efficiency']:.1%}")
        print(f"Average Confidence: {stats['avg_confidence']:.2f}")
        print(f"Average Business Impact: {stats['avg_business_impact']:.2f}")
        
        # Signal Distribution by Level
        print(f"\n📈 SIGNAL DISTRIBUTION")
        print("-" * 40)
        for level, count in stats['by_level'].items():
            print(f"{level}: {count} signals")
        
        # L1 Executive Summary
        print("\n📋 L1: EXECUTIVE SUMMARY (Critical Insights Only)")
        print("-" * 50)
        level1 = output.level_1
        if 'executive_insights' in level1:
            for i, insight in enumerate(level1['executive_insights'], 1):
                print(f"  {i}. {insight}")
        print(f"Threat Level: {level1.get('threat_level', 'Unknown')}")
        print(f"Confidence Score: {level1.get('confidence_score', 0.0):.2f}")
        if level1.get('filtered_signals', 0) > 0:
            print(f"📋 Filtered {level1['filtered_signals']} lower-priority signals from executive view")
        
        # L2 Strategic Dashboard Summary
        print("\n📊 L2: STRATEGIC DASHBOARD SUMMARY")
        print("-" * 50)
        level2 = output.level_2
        if 'strategic_intelligence' in level2:
            for module, data in level2['strategic_intelligence'].items():
                print(f"  📈 {module}: {data['signal_count']} signals (avg confidence: {data['confidence_avg']:.2f})")
        
        # L3 Interventions Summary
        print("\n🎮 L3: ACTIONABLE INTERVENTIONS SUMMARY")
        print("-" * 50)
        level3 = output.level_3
        if 'intervention_summary' in level3:
            summary = level3['intervention_summary']
            print(f"  🚨 Immediate Actions: {summary['immediate_count']}")
            print(f"  📅 Short-term Tactics: {summary['short_term_count']}")
            print(f"  👁️ Monitoring Actions: {summary['monitoring_count']}")
            print(f"  ⚡ Avg Actionability: {summary['avg_actionability']:.2f}")
        
        # L4 Dashboards Summary
        print("\n📋 L4: SQL DASHBOARDS SUMMARY")
        print("-" * 50)
        level4 = output.level_4
        print(f"Project: {level4.get('bigquery_project', 'Unknown')}")
        print(f"Dataset: {level4.get('bigquery_dataset', 'Unknown')}")
        print(f"Dashboard Queries: {len(level4.get('dashboard_queries', {}))}")
        print(f"Total Signals: {level4.get('total_signals', 0)}")
        if level4.get('filtered_noise_count', 0) > 0:
            print(f"🔇 Filtered {level4['filtered_noise_count']} noise signals")
        
        print("\n" + "=" * 80)
        print("✅ SYSTEMATIC L1→L4 INTELLIGENCE GENERATION COMPLETE")
        print("=" * 80)
    
    def _save_output_files(self, output: IntelligenceOutput, analysis: AnalysisResults = None):
        """Save intelligence output to files"""

        # Ensure output directory exists
        os.makedirs("data/output/clean_checkpoints", exist_ok=True)

        # Save systematic intelligence output
        output_path = f"data/output/clean_checkpoints/systematic_intelligence_{self.context.run_id}.json"

        # Convert output to serializable format
        output_data = {
            'brand': self.context.brand,
            'run_id': self.context.run_id,
            'timestamp': time.time(),
            'level_1': output.level_1,
            'level_2': output.level_2,
            'level_3': output.level_3,
            'level_4': output.level_4
        }

        # Add whitespace intelligence if available
        if analysis and hasattr(analysis, 'whitespace_intelligence') and analysis.whitespace_intelligence:
            output_data['whitespace_intelligence'] = analysis.whitespace_intelligence

        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)

        print(f"   💾 Saved systematic intelligence output: {output_path}")

        # Save L3 interventions as separate actionable file
        interventions_path = f"data/output/clean_checkpoints/interventions_{self.context.run_id}.json"
        with open(interventions_path, 'w') as f:
            json.dump(output.level_3, f, indent=2, default=str)

        print(f"   💾 Saved actionable interventions: {interventions_path}")

        # Save whitespace opportunities as separate file for campaign teams
        if analysis and hasattr(analysis, 'whitespace_intelligence') and analysis.whitespace_intelligence:
            whitespace_path = f"data/output/clean_checkpoints/whitespace_{self.context.run_id}.json"
            with open(whitespace_path, 'w') as f:
                json.dump(analysis.whitespace_intelligence, f, indent=2, default=str)
            print(f"   💾 Saved whitespace opportunities: {whitespace_path}")
        
        # Save L4 SQL queries as executable files
        if 'dashboard_queries' in output.level_4:
            sql_dir = f"data/output/sql_dashboards_{self.context.run_id}"
            os.makedirs(sql_dir, exist_ok=True)
            
            for query_name, query in output.level_4['dashboard_queries'].items():
                sql_path = f"{sql_dir}/{query_name}.sql"
                with open(sql_path, 'w') as f:
                    f.write(f"-- {query_name.replace('_', ' ').title()} Dashboard\n")
                    f.write(f"-- Generated by Systematic L1→L4 Intelligence Framework\n")
                    f.write(f"-- Run ID: {self.context.run_id}\n\n")
                    f.write(query)
                
            print(f"   💾 Saved {len(output.level_4['dashboard_queries'])} SQL dashboard queries: {sql_dir}/")