"""
DEPRECATED: Legacy Stage 7 Output Generation

⚠️  THIS FILE IS DEPRECATED AS OF 2025-09-18 ⚠️

This legacy output system has been replaced by the enhanced Intelligence Framework
in enhanced_output.py with proper metric naming and structured intelligence signals.

ISSUES RESOLVED BY ENHANCED FRAMEWORK:
- Fixed meaningless metric names like "Creative Intelligence_2"
- Added structured intelligence signals with business-friendly naming
- Implemented proper temporal intelligence enhancements
- Standardized on "immediate_actions" key instead of inconsistent "interventions"
- Added whitespace intelligence integration
- Improved signal filtering and thresholding

USE: src/pipeline/stages/enhanced_output.py (EnhancedOutputStage)
DEPRECATED: This file (OutputStage) - DO NOT USE

The legacy system generated inconsistent output schemas and generic metric names
that were "as good as not having anything" per user feedback. The enhanced system
provides meaningful business intelligence with proper naming conventions.
"""
import os
import time
import json
from typing import List

from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import AnalysisResults, IntelligenceOutput

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class OutputStage(PipelineStage[AnalysisResults, IntelligenceOutput]):
    """
    ⚠️  DEPRECATED: DO NOT USE THIS CLASS ⚠️

    This legacy output system has been DEPRECATED as of 2025-09-18.

    USE INSTEAD: EnhancedOutputStage from enhanced_output.py

    REASON FOR DEPRECATION:
    - Generated meaningless metric names like "Creative Intelligence_2"
    - Inconsistent output schema ("interventions" vs "immediate_actions")
    - No structured intelligence signals
    - Missing temporal intelligence enhancements
    - No whitespace intelligence integration

    The enhanced system provides business-friendly metric naming and
    structured intelligence signals that are actually useful for analysis.
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Intelligence Output", 7, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.verbose = verbose
    
    def execute(self, analysis: AnalysisResults) -> IntelligenceOutput:
        """Execute intelligence output generation"""

        # DEPRECATION WARNING
        print("   ⚠️  WARNING: USING DEPRECATED OutputStage!")
        print("   ⚠️  This legacy system generates meaningless metric names.")
        print("   ⚠️  USE EnhancedOutputStage from enhanced_output.py instead!")
        print("   ⚠️  The enhanced system has proper business-friendly naming.")
        print()

        print("   📊 Generating 4-level intelligence framework...")
        
        # DEBUG: Log what we received from Analysis stage
        print(f"   🔍 DEBUG OUTPUT STAGE: Received analysis object")
        print(f"   🔍 DEBUG: analysis.current_state = {analysis.current_state}")
        print(f"   🔍 DEBUG: current_state type = {type(analysis.current_state)}")
        
        if hasattr(analysis, 'current_state') and analysis.current_state:
            for key, value in analysis.current_state.items():
                print(f"   🔍 DEBUG: current_state['{key}'] = {value} (type: {type(value)})")
        
        output = IntelligenceOutput()
        
        # Generate all 4 levels of progressive disclosure
        print("   🎯 Level 1: Executive Summary")
        output.level_1 = self._generate_level_1_executive(analysis)
        
        print("   📈 Level 2: Strategic Dashboard")
        output.level_2 = self._generate_level_2_strategic(analysis)
        
        print("   🎮 Level 3: Actionable Interventions")
        output.level_3 = self._generate_level_3_interventions(analysis)
        
        print("   📋 Level 4: SQL Dashboards")
        output.level_4 = self._generate_level_4_dashboards(analysis)
        
        # Display output
        self._display_output(output)
        
        # Save output files
        if not self.dry_run:
            self._save_output_files(output)
        
        return output
    
    def _generate_level_1_executive(self, analysis: AnalysisResults) -> dict:
        """Generate Level 1: Executive Summary"""
        
        # Extract key insights from analysis  
        market_position = analysis.current_state.get('market_position', 'unknown')
        copying_detected = analysis.influence.get('copying_detected', False)
        trend_direction = analysis.evolution.get('trend_direction', 'stable')
        forecast_summary = analysis.forecasts.get('executive_summary', 'Stable market conditions expected')
        
        # Enhanced CTA intelligence from available data
        cta_aggressiveness = analysis.current_state.get('avg_cta_aggressiveness', 3.0)
        cta_competitive_position = 'MODERATE' if cta_aggressiveness < 4.0 else 'AGGRESSIVE'
        
        return {
            'brand': self.context.brand,
            'market_position': market_position,
            'competitive_threat_level': 'HIGH' if copying_detected else 'MEDIUM',
            'trend_momentum': trend_direction.upper(),
            'forecast_summary': forecast_summary,
            'key_insights': [
                f"{self.context.brand} is in a {market_position} market position",
                f"Competitive copying {'detected' if copying_detected else 'not detected'}",
                f"Market trend is {trend_direction}",
                f"Forecast: {forecast_summary[:60]}..."
            ]
        }
    
    def _generate_level_2_strategic(self, analysis: AnalysisResults) -> dict:
        """Generate Level 2: Strategic Dashboard"""
        
        # DEBUG: Log what values we're extracting for Level 2
        pi_val = analysis.current_state.get('promotional_intensity', 0.0)
        us_val = analysis.current_state.get('urgency_score', 0.0)
        bv_val = analysis.current_state.get('brand_voice_score', 0.0)
        
        print(f"   🔍 DEBUG LEVEL_2: promotional_intensity = {pi_val} (type: {type(pi_val)})")
        print(f"   🔍 DEBUG LEVEL_2: urgency_score = {us_val} (type: {type(us_val)})")
        print(f"   🔍 DEBUG LEVEL_2: brand_voice_score = {bv_val} (type: {type(bv_val)})")
        
        level_2_data = {
            'current_state_metrics': {
                'promotional_intensity': pi_val,
                'urgency_score': us_val,
                'brand_voice_score': bv_val,
                'market_position': analysis.current_state.get('market_position', 'unknown'),
                'promotional_volatility': analysis.current_state.get('promotional_volatility', 0.0),
                # Enhanced CTA intelligence metrics
                'cta_aggressiveness': analysis.current_state.get('avg_cta_aggressiveness', 3.0),
                'cta_diversity_score': analysis.current_state.get('cta_diversity_score', 2.5),
                'cta_competitive_position': analysis.current_state.get('cta_competitive_position', 'MODERATE')
            },
            'competitive_influence': {
                'copying_detected': analysis.influence.get('copying_detected', False),
                'top_copier': analysis.influence.get('top_copier', 'None'),
                'similarity_score': analysis.influence.get('similarity_score', 0.0),
                'lag_days': analysis.influence.get('lag_days', 0)
            },
            'temporal_intelligence': {
                'momentum_status': analysis.evolution.get('momentum_status', 'STABLE'),
                'velocity_change_7d': analysis.evolution.get('velocity_change_7d', 0.0),
                'velocity_change_30d': analysis.evolution.get('velocity_change_30d', 0.0),
                'trend_direction': analysis.evolution.get('trend_direction', 'stable')
            },
            'forecasting': {
                'business_impact_score': analysis.forecasts.get('business_impact_score', 2),
                'confidence': analysis.forecasts.get('confidence', 'MEDIUM'),
                'next_7_days': analysis.forecasts.get('next_7_days', 'stable_short_term_outlook'),
                'next_14_days': analysis.forecasts.get('next_14_days', 'stable_competitive_positioning'),
                'next_30_days': analysis.forecasts.get('next_30_days', 'stable_market_continuation'),
                'progressive_timeline': {
                    'short_term_tactical': analysis.forecasts.get('next_7_days', 'stable_short_term_outlook'),
                    'intermediate_strategic': analysis.forecasts.get('next_14_days', 'stable_competitive_positioning'), 
                    'long_term_positioning': analysis.forecasts.get('next_30_days', 'stable_market_continuation')
                }
            },
            'channel_intelligence': {
                # Vertical-agnostic channel metrics
                'dominant_strategy': analysis.channel_intelligence.get('dominant_platform_strategy', 'CROSS_PLATFORM_SYNERGY'),
                'platform_diversity': analysis.channel_intelligence.get('platform_diversity_score', 2.0),
                'cross_platform_rate': analysis.channel_intelligence.get('cross_platform_synergy_rate', 50.0),
                'instagram_focus': analysis.channel_intelligence.get('instagram_focused_rate', 20.0),
                'facebook_focus': analysis.channel_intelligence.get('facebook_focused_rate', 20.0),
                'channel_efficiency': analysis.channel_intelligence.get('avg_content_richness', 0.5),
                # Enhanced metrics from multidimensional stage
                'platform_concentration': analysis.channel_intelligence.get('platform_concentration', 'BALANCED'),
                'temporal_strategy': analysis.channel_intelligence.get('temporal_strategy', 'CONTINUOUS'),
                'competitive_positioning': analysis.channel_intelligence.get('competitive_positioning', 0.5),
                'optimization_potential': analysis.channel_intelligence.get('optimization_potential', 0.3)
            }
        }
        
        print(f"   🔍 DEBUG LEVEL_2: Final level_2_data current_state_metrics = {level_2_data['current_state_metrics']}")
        return level_2_data
    
    def _generate_level_3_interventions(self, analysis: AnalysisResults) -> dict:
        """Generate Level 3: Actionable Interventions"""
        
        interventions = []
        
        # Generate interventions based on analysis
        market_position = analysis.current_state.get('market_position', 'unknown')
        if market_position == 'defensive':
            interventions.append({
                'priority': 'HIGH',
                'action': 'Increase promotional intensity',
                'rationale': 'Currently in defensive position - need to be more aggressive',
                'timeline': '2-4 weeks'
            })

        # Enhanced CTA intelligence interventions
        cta_aggressiveness = analysis.current_state.get('avg_cta_aggressiveness', 3.0)
        cta_diversity = analysis.current_state.get('cta_diversity_score', 2.5)
        cta_position = analysis.current_state.get('cta_competitive_position', 'MODERATE')
        
        if cta_aggressiveness < 2.5:
            interventions.append({
                'priority': 'HIGH',
                'action': 'Strengthen call-to-action language',
                'rationale': f'Low CTA aggressiveness ({cta_aggressiveness:.1f}/10) - missing conversion opportunities',
                'timeline': '1-2 weeks'
            })
        
        if cta_diversity < 2.0:
            interventions.append({
                'priority': 'MEDIUM',
                'action': 'Diversify call-to-action strategies',
                'rationale': 'Limited CTA variety - test different conversion approaches',
                'timeline': '2-3 weeks'
            })
            
        if cta_position == 'WEAK' and cta_aggressiveness < 4.0:
            interventions.append({
                'priority': 'HIGH',
                'action': 'Competitive CTA enhancement campaign',
                'rationale': 'Weak competitive CTA positioning - immediate improvement needed',
                'timeline': '1-2 weeks'
            })
        
        if analysis.influence.get('copying_detected', False):
            top_copier = analysis.influence.get('top_copier', 'Unknown')
            interventions.append({
                'priority': 'MEDIUM',
                'action': f'Monitor and differentiate from {top_copier}',
                'rationale': 'Competitive copying detected - need differentiation strategy',
                'timeline': '1-2 weeks'
            })
        
        # Channel-based interventions (enhanced with multidimensional intelligence)
        cross_platform_rate = analysis.channel_intelligence.get('cross_platform_synergy_rate', 50.0)
        if cross_platform_rate < 30.0:
            interventions.append({
                'priority': 'MEDIUM',
                'action': 'Expand cross-platform campaign integration',
                'rationale': f'Only {cross_platform_rate:.1f}% cross-platform synergy - missing reach opportunities',
                'timeline': '2-3 weeks'
            })
        
        platform_diversity = analysis.channel_intelligence.get('platform_diversity_score', 2.0)
        if platform_diversity < 2.0:
            interventions.append({
                'priority': 'LOW',
                'action': 'Test additional channel diversification',
                'rationale': 'Single-channel dependency risk detected',
                'timeline': '4-6 weeks'
            })

        # Enhanced channel intelligence interventions
        platform_concentration = analysis.channel_intelligence.get('platform_concentration', 'BALANCED')
        if platform_concentration == 'CONCENTRATED':
            interventions.append({
                'priority': 'MEDIUM',
                'action': 'Diversify platform allocation to reduce risk',
                'rationale': 'Over-concentrated on single platform - vulnerable to policy changes',
                'timeline': '3-4 weeks'
            })
        
        temporal_strategy = analysis.channel_intelligence.get('temporal_strategy', 'CONTINUOUS')
        optimization_potential = analysis.channel_intelligence.get('optimization_potential', 0.3)
        if optimization_potential > 0.6:
            interventions.append({
                'priority': 'HIGH' if temporal_strategy == 'BURST' else 'MEDIUM',
                'action': 'Optimize channel performance and timing',
                'rationale': f'High optimization potential ({optimization_potential:.1f}) - significant efficiency gains available',
                'timeline': '1-2 weeks' if temporal_strategy == 'BURST' else '2-4 weeks'
            })
        
        momentum = analysis.evolution.get('momentum_status', 'STABLE')
        if momentum == 'ACCELERATING':
            interventions.append({
                'priority': 'LOW',
                'action': 'Maintain current strategy',
                'rationale': 'Positive momentum detected - continue current approach',
                'timeline': 'Ongoing'
            })
        
        return {
            'immediate_actions': [i for i in interventions if i['priority'] == 'HIGH'],
            'short_term_actions': [i for i in interventions if i['priority'] == 'MEDIUM'],
            'monitoring_actions': [i for i in interventions if i['priority'] == 'LOW'],
            'strategic_recommendations': [
                'Implement competitive monitoring dashboard',
                'Set up automated alerts for market position changes',
                'Establish weekly competitive intelligence reviews'
            ]
        }
    
    def _generate_level_4_dashboards(self, analysis: AnalysisResults) -> dict:
        """Generate Level 4: SQL Dashboards"""
        
        return {
            'bigquery_project': BQ_PROJECT,
            'bigquery_dataset': BQ_DATASET,
            'key_tables': {
                'ads_raw': f'{BQ_PROJECT}.{BQ_DATASET}.ads_raw_{self.context.run_id}',
                'ads_embeddings': f'{BQ_PROJECT}.{BQ_DATASET}.ads_embeddings',
                'strategic_labels': f'{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates'
            },
            'dashboard_queries': {
                'competitive_overview': f'''
                    SELECT 
                        brand,
                        COUNT(*) as ad_count,
                        AVG(promotional_intensity) as avg_promo_intensity,
                        AVG(urgency_score) as avg_urgency
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
                    WHERE brand IN ('{self.context.brand}', {', '.join([f"'{b}'" for b in getattr(self.context, 'competitor_brands', [])])})
                    GROUP BY brand
                    ORDER BY avg_promo_intensity DESC
                ''',
                'trend_analysis': f'''
                    SELECT 
                        DATE(start_timestamp) as date,
                        brand,
                        AVG(promotional_intensity) as daily_promo_intensity
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
                    WHERE brand = '{self.context.brand}'
                        AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
                    GROUP BY date, brand
                    ORDER BY date DESC
                '''
            },
            'visualization_suggestions': [
                'Time series chart for promotional intensity trends',
                'Competitive positioning scatter plot',
                'Market share analysis pie chart',
                'Copying detection heatmap'
            ]
        }
    
    def _display_output(self, output: IntelligenceOutput):
        """Display formatted output to console"""
        
        print("\n" + "=" * 80)
        print("🎯 COMPETITIVE INTELLIGENCE OUTPUT")
        print("=" * 80)
        
        # Level 1: Executive Summary
        print("\n📋 LEVEL 1: EXECUTIVE SUMMARY")
        print("-" * 40)
        level1 = output.level_1
        print(f"Brand: {level1.get('brand', 'Unknown')}")
        print(f"Market Position: {level1.get('market_position', 'Unknown')}")
        print(f"Threat Level: {level1.get('competitive_threat_level', 'Unknown')}")
        print(f"Trend: {level1.get('trend_momentum', 'Unknown')}")
        print(f"Forecast: {level1.get('forecast_summary', 'No forecast available')}")
        
        # Level 2: Key Metrics
        print("\n📊 LEVEL 2: KEY METRICS")
        print("-" * 40)
        level2 = output.level_2
        current_state = level2.get('current_state_metrics', {})
        print(f"Promotional Intensity: {current_state.get('promotional_intensity', 0):.2f}")
        print(f"Market Position: {current_state.get('market_position', 'Unknown')}")
        
        influence = level2.get('competitive_influence', {})
        if influence.get('copying_detected'):
            print(f"⚠️  Copying detected from: {influence.get('top_copier', 'Unknown')}")
        
        # Level 3: Actions (abbreviated)
        print("\n🎮 LEVEL 3: TOP ACTIONS")
        print("-" * 40)
        level3 = output.level_3
        immediate = level3.get('immediate_actions', [])
        if immediate:
            for action in immediate[:2]:  # Show top 2
                print(f"• {action.get('action', 'No action')} ({action.get('priority', 'Unknown')} priority)")
        else:
            print("• No immediate actions required")
        
        print("\n" + "=" * 80)
    
    def _save_output_files(self, output: IntelligenceOutput):
        """Save output to files"""
        
        try:
            # Ensure output directory exists
            output_dir = "data/output"
            os.makedirs(output_dir, exist_ok=True)
            
            # Save JSON output
            output_file = f"{output_dir}/intelligence_{self.context.run_id}.json"
            
            # DEBUG: Log what we're about to save to JSON
            json_data = {
                'brand': self.context.brand,
                'run_id': self.context.run_id,
                'level_1': output.level_1,
                'level_2': output.level_2,
                'level_3': output.level_3,
                'level_4': output.level_4
            }
            
            print(f"   🔍 DEBUG JSON SAVE: About to write to {output_file}")
            print(f"   🔍 DEBUG JSON SAVE: level_2 current_state_metrics = {json_data['level_2']['current_state_metrics']}")
            
            with open(output_file, 'w') as f:
                json.dump(json_data, f, indent=2)
            
            print(f"   💾 Output saved to: {output_file}")
            
        except Exception as e:
            print(f"   ⚠️  Could not save output files: {e}")