#!/usr/bin/env python3
"""
L1→L4 Progressive Disclosure Intelligence Framework
Systematic structure for organizing all intelligence outputs with intelligent thresholding.

🎯 Key Innovation: Transforms information overload into actionable intelligence hierarchy
- L1 Executive: 3-5 critical insights (80%+ confidence)
- L2 Strategic: 10-15 strategic signals (60%+ confidence) 
- L3 Interventions: 20-25 actionable tactics (must be actionable)
- L4 Dashboards: Full analytical detail (above noise threshold)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union
from enum import Enum
import numpy as np


class SignalStrength(Enum):
    """Signal strength classification for intelligent filtering"""
    CRITICAL = "CRITICAL"      # Always surface - immediate action needed
    HIGH = "HIGH"              # Surface in L1-L2 - strategic importance
    MEDIUM = "MEDIUM"          # Surface in L2-L3 - tactical opportunities  
    LOW = "LOW"                # Surface in L3-L4 - monitoring/context
    NOISE = "NOISE"            # Filter out - insufficient confidence/impact


class IntelligenceLevel(Enum):
    """Progressive disclosure levels"""
    L1_EXECUTIVE = 1           # Executive summary - 3-5 key insights
    L2_STRATEGIC = 2           # Strategic dashboard - 10-15 metrics
    L3_INTERVENTIONS = 3       # Actionable interventions - detailed tactics
    L4_DASHBOARDS = 4          # SQL dashboards - full data exploration


@dataclass
class IntelligenceSignal:
    """Individual intelligence signal with metadata"""
    insight: str                                    # The actual insight text
    value: Union[float, int, str, Dict]            # Quantitative or qualitative value
    confidence: float                              # 0-1 confidence score
    business_impact: float                         # 0-1 potential business impact
    actionability: float                           # 0-1 how actionable this is
    source_module: str                             # Which intelligence module generated this
    signal_strength: SignalStrength                # Computed strength classification
    recommended_levels: List[IntelligenceLevel]    # Which L1-L4 levels to surface in
    metric_name: str = ""                          # Descriptive name for this metric (e.g., 'competitive_similarity_threat')
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IntelligenceThresholds:
    """Configurable thresholds for signal filtering"""
    # L1 Executive Summary - only the most critical insights
    l1_min_confidence: float = 0.8        # High confidence required
    l1_min_business_impact: float = 0.7   # High business impact required
    l1_max_signals: int = 5               # Maximum 5 insights for executives
    
    # L2 Strategic Dashboard - important strategic signals  
    l2_min_confidence: float = 0.6        # Medium-high confidence
    l2_min_business_impact: float = 0.5   # Medium business impact
    l2_max_signals: int = 15              # Maximum 15 insights for strategy
    
    # L3 Interventions - actionable opportunities
    l3_min_confidence: float = 0.4        # Medium confidence acceptable
    l3_min_actionability: float = 0.6     # Must be actionable
    l3_max_signals: int = 25              # More tactical detail allowed
    
    # L4 Dashboards - all data above noise threshold
    l4_min_confidence: float = 0.2        # Low confidence for exploration
    l4_max_signals: int = 50              # Full detail for analysts


class ProgressiveDisclosureFramework:
    """Systematic L1→L4 framework with intelligent signal filtering"""
    
    def __init__(self, thresholds: Optional[IntelligenceThresholds] = None):
        self.thresholds = thresholds or IntelligenceThresholds()
        self.signals: List[IntelligenceSignal] = []
    
    def add_signal(
        self,
        insight: str,
        value: Union[float, int, str, Dict],
        confidence: float,
        business_impact: float,
        actionability: float,
        source_module: str,
        metric_name: str = "",
        metadata: Optional[Dict] = None
    ) -> IntelligenceSignal:
        """Add a new intelligence signal with automatic classification"""

        # Normalize numerical values to exactly 2 decimal places
        normalized_value = self._normalize_value(value)

        signal = IntelligenceSignal(
            insight=insight,
            value=normalized_value,
            confidence=confidence,
            business_impact=business_impact,
            actionability=actionability,
            source_module=source_module,
            metric_name=metric_name,
            signal_strength=self._classify_signal_strength(confidence, business_impact, actionability),
            recommended_levels=self._determine_disclosure_levels(confidence, business_impact, actionability),
            metadata=metadata or {}
        )

        self.signals.append(signal)
        return signal

    def _normalize_value(self, value: Union[float, int, str, Dict]) -> Union[float, int, str, Dict]:
        """Normalize numerical values to exactly 2 decimal places for consistency"""
        if isinstance(value, float):
            # Round to exactly 2 decimal places
            return round(value, 2)
        elif isinstance(value, int):
            # Keep integers as integers
            return value
        else:
            # Keep strings, dicts, and other types unchanged
            return value

    def _classify_signal_strength(
        self, 
        confidence: float, 
        business_impact: float, 
        actionability: float
    ) -> SignalStrength:
        """Classify signal strength based on confidence, impact, and actionability"""
        
        # Weighted composite score
        composite_score = (
            confidence * 0.4 +           # Confidence is most important
            business_impact * 0.4 +      # Business impact critical  
            actionability * 0.2          # Actionability important for tactics
        )
        
        # Classification thresholds
        if composite_score >= 0.8 and confidence >= 0.7:
            return SignalStrength.CRITICAL
        elif composite_score >= 0.6 and confidence >= 0.5:
            return SignalStrength.HIGH  
        elif composite_score >= 0.4 and confidence >= 0.3:
            return SignalStrength.MEDIUM
        elif composite_score >= 0.2:
            return SignalStrength.LOW
        else:
            return SignalStrength.NOISE
    
    def _determine_disclosure_levels(
        self,
        confidence: float,
        business_impact: float, 
        actionability: float
    ) -> List[IntelligenceLevel]:
        """Determine which L1-L4 levels this signal should appear in"""
        
        levels = []
        
        # L1 Executive Summary - highest bar
        if (confidence >= self.thresholds.l1_min_confidence and 
            business_impact >= self.thresholds.l1_min_business_impact):
            levels.append(IntelligenceLevel.L1_EXECUTIVE)
        
        # L2 Strategic Dashboard  
        if (confidence >= self.thresholds.l2_min_confidence and
            business_impact >= self.thresholds.l2_min_business_impact):
            levels.append(IntelligenceLevel.L2_STRATEGIC)
        
        # L3 Interventions - must be actionable
        if (confidence >= self.thresholds.l3_min_confidence and
            actionability >= self.thresholds.l3_min_actionability):
            levels.append(IntelligenceLevel.L3_INTERVENTIONS)
        
        # L4 Dashboards - everything above noise
        if confidence >= self.thresholds.l4_min_confidence:
            levels.append(IntelligenceLevel.L4_DASHBOARDS)
        
        return levels
    
    def generate_level_1_executive(self) -> Dict[str, Any]:
        """Generate L1 Executive Summary with top signals only"""
        
        # Filter signals for L1
        l1_signals = [s for s in self.signals if IntelligenceLevel.L1_EXECUTIVE in s.recommended_levels]
        
        # Sort by composite importance score
        l1_signals.sort(
            key=lambda s: (s.confidence * 0.4 + s.business_impact * 0.6), 
            reverse=True
        )
        
        # Take top signals only
        top_signals = l1_signals[:self.thresholds.l1_max_signals]
        
        return {
            'executive_insights': [s.insight for s in top_signals],
            'critical_metrics': {
                s.metric_name or f"{s.source_module}_{i}": s.value
                for i, s in enumerate(top_signals)
                if isinstance(s.value, (int, float))
            },
            'threat_level': self._assess_overall_threat_level(top_signals),
            'confidence_score': np.mean([s.confidence for s in top_signals]) if top_signals else 0.0,
            'signal_count': len(top_signals),
            'filtered_signals': len(l1_signals) - len(top_signals)
        }
    
    def generate_level_2_strategic(self) -> Dict[str, Any]:
        """Generate L2 Strategic Dashboard with strategic signals"""
        
        # Filter signals for L2
        l2_signals = [s for s in self.signals if IntelligenceLevel.L2_STRATEGIC in s.recommended_levels]
        
        # Group by source module for organized display
        by_module = {}
        for signal in l2_signals:
            if signal.source_module not in by_module:
                by_module[signal.source_module] = []
            by_module[signal.source_module].append(signal)
        
        # Sort each module's signals by importance
        for module_signals in by_module.values():
            module_signals.sort(
                key=lambda s: s.confidence * s.business_impact,
                reverse=True
            )
        
        return {
            'strategic_intelligence': {
                module: {
                    'insights': [s.insight for s in signals[:5]],  # Top 5 per module
                    'key_metrics': {
                        f"{module.lower().replace(' ', '_')}_{i}": s.value 
                        for i, s in enumerate(signals[:3])  # Top 3 metrics per module
                        if isinstance(s.value, (int, float))
                    },
                    'confidence_avg': np.mean([s.confidence for s in signals]),
                    'business_impact_avg': np.mean([s.business_impact for s in signals]),
                    'signal_count': len(signals)
                }
                for module, signals in by_module.items()
            },
            'cross_module_patterns': self._identify_cross_module_patterns(l2_signals),
            'strategic_priorities': self._rank_strategic_priorities(l2_signals),
            'total_signals': len(l2_signals),
            'modules_active': len(by_module)
        }
    
    def generate_level_3_interventions(self) -> Dict[str, Any]:
        """Generate L3 Actionable Interventions with tactical detail"""
        
        # Filter signals for L3 - must be actionable
        l3_signals = [s for s in self.signals if IntelligenceLevel.L3_INTERVENTIONS in s.recommended_levels]
        
        # Categorize interventions by priority
        interventions = {
            'immediate_actions': [],  # CRITICAL signals
            'short_term_tactics': [], # HIGH signals  
            'monitoring_actions': []  # MEDIUM signals
        }
        
        for signal in l3_signals:
            intervention = {
                'action': signal.insight,
                'rationale': f"Based on {signal.source_module} analysis",
                'confidence': signal.confidence,
                'business_impact': signal.business_impact,
                'actionability': signal.actionability,
                'timeline': self._suggest_timeline(signal),
                'success_metrics': self._suggest_success_metrics(signal),
                'source_module': signal.source_module,
                'value': signal.value if isinstance(signal.value, (int, float, str)) else str(signal.value)
            }
            
            if signal.signal_strength == SignalStrength.CRITICAL:
                intervention['priority'] = 'CRITICAL'
                interventions['immediate_actions'].append(intervention)
            elif signal.signal_strength == SignalStrength.HIGH:
                intervention['priority'] = 'HIGH' 
                interventions['short_term_tactics'].append(intervention)
            else:
                intervention['priority'] = 'MEDIUM'
                interventions['monitoring_actions'].append(intervention)
        
        # Sort each category by actionability
        for category in interventions.values():
            category.sort(key=lambda x: x['actionability'], reverse=True)
        
        return {
            **interventions,
            'intervention_summary': {
                'total_interventions': len(l3_signals),
                'immediate_count': len(interventions['immediate_actions']),
                'short_term_count': len(interventions['short_term_tactics']), 
                'monitoring_count': len(interventions['monitoring_actions']),
                'avg_confidence': np.mean([s.confidence for s in l3_signals]) if l3_signals else 0.0,
                'avg_actionability': np.mean([s.actionability for s in l3_signals]) if l3_signals else 0.0
            }
        }
    
    def generate_level_4_dashboards(self, project_id: str, dataset_id: str) -> Dict[str, Any]:
        """Generate L4 SQL Dashboards with full analytical detail"""
        
        # All signals above noise threshold
        l4_signals = [s for s in self.signals if IntelligenceLevel.L4_DASHBOARDS in s.recommended_levels]
        
        # Group by module for dashboard organization
        dashboard_queries = {}
        
        active_modules = set(s.source_module for s in l4_signals)
        
        for module in active_modules:
            module_key = module.lower().replace(' ', '_').replace('-', '_')
            dashboard_queries[f"{module_key}_analysis"] = self._generate_module_sql(
                module, project_id, dataset_id
            )

            # Add specialized Visual Style Competitive Matrix for Visual Intelligence
            if "Visual" in module:
                dashboard_queries["visual_style_competitive_matrix"] = self._generate_visual_style_matrix_sql(
                    project_id, dataset_id
                )

        # Phase 2: Add Competitive Velocity Analysis Dashboard (covers all intelligence dimensions)
        if active_modules:  # Only add if we have at least one active module
            dashboard_queries["competitive_velocity_analysis"] = self._generate_competitive_velocity_sql(
                project_id, dataset_id
            )

        # Phase 3: Add Strategic Inflection Detection Dashboard
        if active_modules:
            dashboard_queries["strategic_inflection_detection"] = self._generate_strategic_inflection_sql(
                project_id, dataset_id
            )

        # Phase 4: Add Predictive Integration Dashboard (ML.FORECAST models)
        if active_modules:
            dashboard_queries["predictive_competitive_forecast"] = self._generate_predictive_integration_sql(
                project_id, dataset_id
            )

        return {
            'bigquery_project': project_id,
            'bigquery_dataset': dataset_id,
            'dashboard_queries': dashboard_queries,
            'signal_explorer_query': self._generate_signal_explorer_sql(l4_signals, project_id, dataset_id),
            'confidence_analysis_query': self._generate_confidence_analysis_sql(project_id, dataset_id),
            'module_performance_metrics': {
                module: {
                    'signal_count': len([s for s in l4_signals if s.source_module == module]),
                    'avg_confidence': np.mean([s.confidence for s in l4_signals if s.source_module == module]) if [s for s in l4_signals if s.source_module == module] else 0.0,
                    'avg_business_impact': np.mean([s.business_impact for s in l4_signals if s.source_module == module]) if [s for s in l4_signals if s.source_module == module] else 0.0
                }
                for module in active_modules
            },
            'total_signals': len(l4_signals),
            'filtered_noise_count': len([s for s in self.signals if s.signal_strength == SignalStrength.NOISE])
        }
    
    def _assess_overall_threat_level(self, signals: List[IntelligenceSignal]) -> str:
        """Assess overall competitive threat level"""
        if not signals:
            return "UNKNOWN"
        
        critical_count = len([s for s in signals if s.signal_strength == SignalStrength.CRITICAL])
        high_count = len([s for s in signals if s.signal_strength == SignalStrength.HIGH])
        
        if critical_count >= 2:
            return "CRITICAL"
        elif critical_count >= 1 or high_count >= 3:
            return "HIGH"
        elif high_count >= 1:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _identify_cross_module_patterns(self, signals: List[IntelligenceSignal]) -> List[str]:
        """Identify patterns across different intelligence modules"""
        patterns = []
        
        modules = set(s.source_module for s in signals)
        if len(modules) >= 2:
            patterns.append(f"Multi-module intelligence available from {len(modules)} sources")
        
        # Check for consistent high-confidence signals across modules
        high_conf_modules = [s.source_module for s in signals if s.confidence >= 0.7]
        if len(set(high_conf_modules)) >= 2:
            patterns.append("Consistent high-confidence insights across multiple intelligence modules")
        
        # Check for complementary signals
        creative_signals = [s for s in signals if 'Creative' in s.source_module]
        channel_signals = [s for s in signals if 'Channel' in s.source_module]
        visual_signals = [s for s in signals if 'Visual' in s.source_module]

        if creative_signals and channel_signals:
            patterns.append("Creative and Channel intelligence provide complementary insights")

        if visual_signals and creative_signals:
            patterns.append("Visual and Creative intelligence show multimodal consistency patterns")

        if len([creative_signals, channel_signals, visual_signals]) >= 2:
            active_modules = []
            if creative_signals: active_modules.append("Creative")
            if channel_signals: active_modules.append("Channel")
            if visual_signals: active_modules.append("Visual")
            patterns.append(f"Multi-dimensional intelligence active: {', '.join(active_modules)} modules provide comprehensive competitive analysis")
        
        return patterns
    
    def _rank_strategic_priorities(self, signals: List[IntelligenceSignal]) -> List[str]:
        """Rank strategic priorities based on signal analysis"""
        # Group by business impact and confidence
        high_impact = [s for s in signals if s.business_impact >= 0.7]
        
        priorities = []
        for signal in sorted(high_impact, key=lambda s: s.business_impact, reverse=True)[:3]:
            priorities.append(f"{signal.source_module}: {signal.insight[:100]}...")
        
        return priorities
    
    def _suggest_timeline(self, signal: IntelligenceSignal) -> str:
        """Suggest implementation timeline based on signal characteristics"""
        if signal.signal_strength == SignalStrength.CRITICAL:
            return "Immediate (1-2 weeks)"
        elif signal.signal_strength == SignalStrength.HIGH:
            return "Short-term (2-4 weeks)"
        else:
            return "Medium-term (4-8 weeks)"
    
    def _suggest_success_metrics(self, signal: IntelligenceSignal) -> List[str]:
        """Suggest success metrics for intervention"""
        metrics = ["Implementation completion rate"]
        
        if "Creative" in signal.source_module:
            if "text length" in signal.insight.lower():
                metrics.extend(["Average creative text length", "Content engagement rate"])
            elif "emotional intensity" in signal.insight.lower():
                metrics.extend(["AI emotional intensity score", "Emotional engagement rate", "Brand resonance metrics"])
            elif "emotional" in signal.insight.lower():
                metrics.extend(["Emotional keyword usage rate", "Brand sentiment score"])
            elif "industry relevance" in signal.insight.lower():
                metrics.extend(["Industry relevance score", "Eyewear context alignment", "Message effectiveness"])
            elif "positive/aspirational" in signal.insight.lower():
                metrics.extend(["Positive sentiment rate", "Aspirational messaging score", "Brand uplift metrics"])
            elif "lifestyle" in signal.insight.lower() or "premium" in signal.insight.lower():
                metrics.extend(["Brand positioning score", "Lifestyle appeal metrics", "Premium perception index"])
            else:
                metrics.extend(["Creative performance score", "Brand message consistency"])
        elif "Channel" in signal.source_module:
            if "cross-platform" in signal.insight.lower():
                metrics.extend(["Cross-platform synergy rate", "Multi-channel reach"])
            elif "diversification" in signal.insight.lower():
                metrics.extend(["Platform diversity score", "Risk concentration metric"])
            else:
                metrics.extend(["Channel efficiency score", "Platform optimization rate"])
        elif "Visual" in signal.source_module:
            if "visual-text alignment" in signal.insight.lower():
                metrics.extend(["Visual-text alignment score", "Multimodal consistency rate"])
            elif "brand consistency" in signal.insight.lower():
                metrics.extend(["Brand visual consistency score", "Brand recognition rate"])
            elif "positioning" in signal.insight.lower():
                metrics.extend(["Competitive positioning score", "Market differentiation rate"])
            elif "differentiation" in signal.insight.lower():
                metrics.extend(["Visual differentiation score", "Competitive advantage index"])
            elif "fatigue" in signal.insight.lower():
                metrics.extend(["Creative freshness score", "Visual pattern diversity"])
            else:
                metrics.extend(["Visual intelligence score", "Multimodal effectiveness"])
        else:
            metrics.extend(["Performance improvement %", "Strategic goal achievement"])
        
        return metrics
    
    def _generate_module_sql(self, module: str, project_id: str, dataset_id: str) -> str:
        """Generate SQL query for module-specific dashboard"""
        base_table = f"`{project_id}.{dataset_id}.ads_with_dates`"
        
        if "Creative" in module:
            return f"""
            -- Creative Intelligence Dashboard with Temporal Analysis (Phase 1)
            WITH weekly_creative_metrics AS (
              SELECT
                brand,
                DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) as week_start,
                COUNT(*) as weekly_ads,
                AVG(LENGTH(COALESCE(creative_text, ''))) as avg_creative_length,
                COUNT(DISTINCT REGEXP_EXTRACT(creative_text, r'\\b(\\w+)\\b')) as unique_words,
                SUM(CASE WHEN creative_text LIKE '%!%' THEN 1 ELSE 0 END) as exclamation_ads,
                AVG(CASE WHEN LENGTH(creative_text) > 100 THEN 1.0 ELSE 0.0 END) as long_form_rate,
                AVG(CASE
                  WHEN LENGTH(COALESCE(creative_text, '')) > 150 THEN 3
                  WHEN LENGTH(COALESCE(creative_text, '')) > 75 THEN 2
                  ELSE 1
                END) as text_length_score,
                AVG(
                  (LENGTH(UPPER(COALESCE(creative_text, ''))) - LENGTH(REPLACE(UPPER(COALESCE(creative_text, '')), brand, ''))) /
                  GREATEST(LENGTH(brand), 1)
                ) as brand_mention_density
              FROM {base_table}
              WHERE creative_text IS NOT NULL
                AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 WEEK)
              GROUP BY brand, week_start
            ),
            temporal_analysis AS (
              SELECT
                brand,
                week_start,
                weekly_ads,
                avg_creative_length,
                text_length_score,
                brand_mention_density,
                -- Week-over-week changes
                LAG(weekly_ads) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_ads,
                LAG(avg_creative_length) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_length,
                LAG(text_length_score) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_text_score,
                -- Competitive ranking
                RANK() OVER (PARTITION BY week_start ORDER BY weekly_ads DESC) as weekly_volume_rank,
                RANK() OVER (PARTITION BY week_start ORDER BY avg_creative_length DESC) as weekly_length_rank,
                LAG(RANK() OVER (PARTITION BY week_start ORDER BY weekly_ads DESC)) OVER (PARTITION BY brand ORDER BY week_start) as prev_volume_rank
              FROM weekly_creative_metrics
            ),
            creative_summary AS (
              SELECT
                brand,
                COUNT(DISTINCT week_start) as weeks_active,
                SUM(weekly_ads) as total_ads,
                AVG(avg_creative_length) as avg_creative_length,
                AVG(text_length_score) as avg_text_score,
                AVG(brand_mention_density) as avg_brand_density,
                -- Temporal insights
                AVG(weekly_ads - COALESCE(prev_week_ads, weekly_ads)) as avg_weekly_volume_change,
                AVG(avg_creative_length - COALESCE(prev_week_length, avg_creative_length)) as avg_weekly_length_change,
                AVG(weekly_volume_rank) as avg_volume_rank,
                MIN(weekly_volume_rank) as best_volume_rank,
                MAX(weekly_volume_rank) as worst_volume_rank,
                -- Trend indicators
                CASE
                  WHEN AVG(weekly_ads - COALESCE(prev_week_ads, weekly_ads)) > 2 THEN 'INCREASING_VOLUME'
                  WHEN AVG(weekly_ads - COALESCE(prev_week_ads, weekly_ads)) < -2 THEN 'DECREASING_VOLUME'
                  ELSE 'STABLE_VOLUME'
                END as volume_trend,
                CASE
                  WHEN AVG(avg_creative_length - COALESCE(prev_week_length, avg_creative_length)) > 10 THEN 'LENGTHENING_CONTENT'
                  WHEN AVG(avg_creative_length - COALESCE(prev_week_length, avg_creative_length)) < -10 THEN 'SHORTENING_CONTENT'
                  ELSE 'STABLE_LENGTH'
                END as content_trend
              FROM temporal_analysis
              GROUP BY brand
            )
            SELECT
              brand,
              weeks_active,
              total_ads,
              ROUND(avg_creative_length, 1) as avg_creative_length,
              ROUND(avg_text_score, 2) as text_optimization_score,
              ROUND(avg_brand_density, 3) as brand_mention_density,
              -- Temporal intelligence
              ROUND(avg_weekly_volume_change, 1) as avg_weekly_volume_change,
              ROUND(avg_weekly_length_change, 1) as avg_weekly_length_change,
              ROUND(avg_volume_rank, 1) as avg_competitive_rank,
              volume_trend,
              content_trend,
              -- Competitive positioning
              CASE
                WHEN avg_volume_rank <= 2 THEN 'MARKET_LEADER'
                WHEN avg_volume_rank <= 4 THEN 'STRONG_PLAYER'
                ELSE 'CHALLENGER'
              END as competitive_position,
              -- Movement indicator
              CASE
                WHEN best_volume_rank < worst_volume_rank THEN 'IMPROVING_POSITION'
                WHEN best_volume_rank > worst_volume_rank THEN 'DECLINING_POSITION'
                ELSE 'STABLE_POSITION'
              END as competitive_movement
            FROM creative_summary
            ORDER BY total_ads DESC
            """
        elif "Channel" in module:
            return f"""
            -- Channel Intelligence Dashboard with Temporal Analysis (Phase 1)
            WITH weekly_channel_metrics AS (
              SELECT
                brand,
                DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) as week_start,
                COUNT(*) as weekly_ads,
                COUNT(DISTINCT publisher_platforms) as unique_platforms,
                AVG(CASE WHEN publisher_platforms LIKE '%,%' THEN 1.0 ELSE 0.0 END) as cross_platform_rate,
                AVG(CASE
                  WHEN REGEXP_CONTAINS(publisher_platforms, 'Facebook') AND
                       REGEXP_CONTAINS(publisher_platforms, 'Instagram') AND
                       REGEXP_CONTAINS(publisher_platforms, 'Messenger') THEN 3
                  WHEN REGEXP_CONTAINS(publisher_platforms, 'Facebook') AND
                       REGEXP_CONTAINS(publisher_platforms, 'Instagram') THEN 2
                  WHEN publisher_platforms LIKE '%,%' THEN 1
                  ELSE 0
                END) as platform_diversification_score
              FROM {base_table}
              WHERE start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 WEEK)
              GROUP BY brand, week_start
            ),
            temporal_analysis AS (
              SELECT
                brand, week_start, weekly_ads, unique_platforms, cross_platform_rate, platform_diversification_score,
                -- Week-over-week changes
                LAG(weekly_ads) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_ads,
                LAG(unique_platforms) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_platforms,
                LAG(cross_platform_rate) OVER (PARTITION BY brand ORDER BY week_start) as prev_cross_platform_rate,
                -- Competitive ranking
                RANK() OVER (PARTITION BY week_start ORDER BY weekly_ads DESC) as weekly_volume_rank,
                RANK() OVER (PARTITION BY week_start ORDER BY unique_platforms DESC) as weekly_platform_diversity_rank,
                RANK() OVER (PARTITION BY week_start ORDER BY cross_platform_rate DESC) as weekly_cross_platform_rank
              FROM weekly_channel_metrics
            ),
            channel_summary AS (
              SELECT
                brand,
                COUNT(*) as weeks_active,
                SUM(weekly_ads) as total_ads,
                AVG(unique_platforms) as avg_platforms,
                AVG(cross_platform_rate) as avg_cross_platform_rate,
                AVG(platform_diversification_score) as avg_diversification_score,
                -- Temporal insights
                AVG(weekly_ads - COALESCE(prev_week_ads, weekly_ads)) as avg_weekly_volume_change,
                AVG(unique_platforms - COALESCE(prev_week_platforms, unique_platforms)) as avg_platform_expansion_change,
                AVG(cross_platform_rate - COALESCE(prev_cross_platform_rate, cross_platform_rate)) as avg_cross_platform_change,
                AVG(weekly_volume_rank) as avg_volume_rank,
                AVG(weekly_platform_diversity_rank) as avg_platform_diversity_rank,
                -- Trend classification
                CASE
                  WHEN AVG(weekly_ads - COALESCE(prev_week_ads, weekly_ads)) > 2 THEN 'INCREASING_VOLUME'
                  WHEN AVG(weekly_ads - COALESCE(prev_week_ads, weekly_ads)) < -2 THEN 'DECREASING_VOLUME'
                  ELSE 'STABLE_VOLUME'
                END as volume_trend,
                CASE
                  WHEN AVG(unique_platforms - COALESCE(prev_week_platforms, unique_platforms)) > 0.5 THEN 'EXPANDING_CHANNELS'
                  WHEN AVG(unique_platforms - COALESCE(prev_week_platforms, unique_platforms)) < -0.5 THEN 'CONSOLIDATING_CHANNELS'
                  ELSE 'STABLE_CHANNELS'
                END as channel_expansion_trend,
                CASE
                  WHEN AVG(cross_platform_rate - COALESCE(prev_cross_platform_rate, cross_platform_rate)) > 0.1 THEN 'INCREASING_CROSS_PLATFORM'
                  WHEN AVG(cross_platform_rate - COALESCE(prev_cross_platform_rate, cross_platform_rate)) < -0.1 THEN 'DECREASING_CROSS_PLATFORM'
                  ELSE 'STABLE_CROSS_PLATFORM'
                END as cross_platform_trend,
                -- Competitive positioning
                CASE
                  WHEN AVG(weekly_volume_rank) <= 2 THEN 'CHANNEL_LEADER'
                  WHEN AVG(weekly_volume_rank) <= 4 THEN 'STRONG_CHANNEL_PLAYER'
                  ELSE 'CHANNEL_CHALLENGER'
                END as competitive_position,
                -- Movement indicators
                CASE
                  WHEN AVG(weekly_volume_rank) - LAG(AVG(weekly_volume_rank)) OVER (ORDER BY brand) < -0.5 THEN 'IMPROVING_POSITION'
                  WHEN AVG(weekly_volume_rank) - LAG(AVG(weekly_volume_rank)) OVER (ORDER BY brand) > 0.5 THEN 'DECLINING_POSITION'
                  ELSE 'STABLE_POSITION'
                END as competitive_movement
              FROM temporal_analysis
              GROUP BY brand
            )
            SELECT
              brand,
              weeks_active,
              total_ads,
              ROUND(avg_platforms, 1) as avg_platforms,
              ROUND(avg_cross_platform_rate * 100, 1) as avg_cross_platform_pct,
              ROUND(avg_diversification_score, 1) as avg_diversification_score,
              ROUND(avg_weekly_volume_change, 1) as avg_weekly_volume_change,
              ROUND(avg_platform_expansion_change, 1) as avg_platform_expansion_change,
              ROUND(avg_cross_platform_change * 100, 1) as avg_cross_platform_change_pct,
              volume_trend,
              channel_expansion_trend,
              cross_platform_trend,
              competitive_position,
              competitive_movement,
              ROUND(avg_volume_rank, 1) as avg_volume_rank,
              ROUND(avg_platform_diversity_rank, 1) as avg_platform_diversity_rank
            FROM channel_summary
            ORDER BY total_ads DESC
            """
        elif "Visual" in module:
            return f"""
            -- Visual Intelligence Dashboard with Temporal Analysis (Phase 1)
            WITH weekly_visual_metrics AS (
              SELECT
                brand,
                DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) as week_start,
                COUNT(*) as weekly_visual_ads,
                AVG(visual_text_alignment_score) as avg_visual_alignment,
                AVG(brand_consistency_score) as avg_brand_consistency,
                AVG(luxury_positioning_score) as avg_luxury_position,
                AVG(boldness_score) as avg_boldness,
                AVG(visual_differentiation_level) as avg_differentiation,
                AVG(creative_pattern_risk) as avg_pattern_risk,
                -- Visual health score per week
                (AVG(visual_text_alignment_score) * 0.3 +
                 AVG(brand_consistency_score) * 0.3 +
                 AVG(visual_differentiation_level) * 0.2 +
                 (1 - AVG(creative_pattern_risk)) * 0.2) as weekly_visual_health_score
              FROM (
                SELECT * FROM {base_table.replace('.ads_with_dates', '.visual_intelligence_*')}
                WHERE luxury_positioning_score IS NOT NULL
                  AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 WEEK)
              )
              GROUP BY brand, week_start
            ),
            temporal_analysis AS (
              SELECT
                brand, week_start, weekly_visual_ads, avg_visual_alignment, avg_brand_consistency,
                avg_luxury_position, avg_boldness, avg_differentiation, avg_pattern_risk, weekly_visual_health_score,
                -- Week-over-week changes
                LAG(weekly_visual_ads) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_ads,
                LAG(avg_luxury_position) OVER (PARTITION BY brand ORDER BY week_start) as prev_luxury_position,
                LAG(avg_boldness) OVER (PARTITION BY brand ORDER BY week_start) as prev_boldness,
                LAG(avg_brand_consistency) OVER (PARTITION BY brand ORDER BY week_start) as prev_brand_consistency,
                LAG(avg_pattern_risk) OVER (PARTITION BY brand ORDER BY week_start) as prev_pattern_risk,
                -- Competitive ranking
                RANK() OVER (PARTITION BY week_start ORDER BY weekly_visual_ads DESC) as weekly_volume_rank,
                RANK() OVER (PARTITION BY week_start ORDER BY avg_luxury_position DESC) as weekly_luxury_rank,
                RANK() OVER (PARTITION BY week_start ORDER BY avg_boldness DESC) as weekly_boldness_rank,
                RANK() OVER (PARTITION BY week_start ORDER BY weekly_visual_health_score DESC) as weekly_health_rank
              FROM weekly_visual_metrics
            ),
            visual_summary AS (
              SELECT
                brand,
                COUNT(*) as weeks_active,
                SUM(weekly_visual_ads) as total_visual_ads,
                AVG(avg_visual_alignment) as overall_visual_alignment,
                AVG(avg_brand_consistency) as overall_brand_consistency,
                AVG(avg_luxury_position) as overall_luxury_position,
                AVG(avg_boldness) as overall_boldness,
                AVG(avg_differentiation) as overall_differentiation,
                AVG(avg_pattern_risk) as overall_pattern_risk,
                AVG(weekly_visual_health_score) as overall_visual_health_score,
                -- Temporal insights
                AVG(weekly_visual_ads - COALESCE(prev_week_ads, weekly_visual_ads)) as avg_weekly_volume_change,
                AVG(avg_luxury_position - COALESCE(prev_luxury_position, avg_luxury_position)) as avg_luxury_shift,
                AVG(avg_boldness - COALESCE(prev_boldness, avg_boldness)) as avg_boldness_shift,
                AVG(avg_brand_consistency - COALESCE(prev_brand_consistency, avg_brand_consistency)) as avg_consistency_shift,
                AVG(avg_pattern_risk - COALESCE(prev_pattern_risk, avg_pattern_risk)) as avg_fatigue_trend,
                AVG(weekly_volume_rank) as avg_volume_rank,
                AVG(weekly_luxury_rank) as avg_luxury_rank,
                AVG(weekly_boldness_rank) as avg_boldness_rank,
                -- Visual strategy evolution trends
                CASE
                  WHEN AVG(avg_luxury_position - COALESCE(prev_luxury_position, avg_luxury_position)) > 0.05 THEN 'MOVING_UPMARKET'
                  WHEN AVG(avg_luxury_position - COALESCE(prev_luxury_position, avg_luxury_position)) < -0.05 THEN 'MOVING_ACCESSIBLE'
                  ELSE 'STABLE_POSITIONING'
                END as luxury_positioning_trend,
                CASE
                  WHEN AVG(avg_boldness - COALESCE(prev_boldness, avg_boldness)) > 0.05 THEN 'INCREASING_BOLDNESS'
                  WHEN AVG(avg_boldness - COALESCE(prev_boldness, avg_boldness)) < -0.05 THEN 'DECREASING_BOLDNESS'
                  ELSE 'STABLE_BOLDNESS'
                END as boldness_trend,
                CASE
                  WHEN AVG(avg_pattern_risk - COALESCE(prev_pattern_risk, avg_pattern_risk)) > 0.1 THEN 'INCREASING_FATIGUE_RISK'
                  WHEN AVG(avg_pattern_risk - COALESCE(prev_pattern_risk, avg_pattern_risk)) < -0.1 THEN 'DECREASING_FATIGUE_RISK'
                  ELSE 'STABLE_FATIGUE'
                END as creative_fatigue_trend,
                -- Positioning quadrant classification
                CASE
                  WHEN AVG(avg_luxury_position) > 0.7 AND AVG(avg_boldness) > 0.7 THEN 'Luxury-Bold'
                  WHEN AVG(avg_luxury_position) > 0.7 AND AVG(avg_boldness) < 0.4 THEN 'Luxury-Subtle'
                  WHEN AVG(avg_luxury_position) < 0.4 AND AVG(avg_boldness) > 0.7 THEN 'Accessible-Bold'
                  ELSE 'Mid-Balanced'
                END as positioning_quadrant,
                -- Competitive positioning
                CASE
                  WHEN AVG(weekly_volume_rank) <= 2 THEN 'VISUAL_LEADER'
                  WHEN AVG(weekly_volume_rank) <= 4 THEN 'STRONG_VISUAL_PLAYER'
                  ELSE 'VISUAL_CHALLENGER'
                END as competitive_position
              FROM temporal_analysis
              GROUP BY brand
            )
            SELECT
              brand,
              weeks_active,
              total_visual_ads,
              ROUND(overall_visual_alignment, 3) as visual_alignment_score,
              ROUND(overall_brand_consistency, 3) as brand_consistency_score,
              ROUND(overall_luxury_position, 3) as luxury_positioning,
              ROUND(overall_boldness, 3) as boldness_score,
              ROUND(overall_differentiation, 3) as differentiation_level,
              ROUND(overall_pattern_risk, 3) as fatigue_risk,
              ROUND(overall_visual_health_score, 3) as overall_visual_health,
              ROUND(avg_weekly_volume_change, 1) as avg_weekly_volume_change,
              ROUND(avg_luxury_shift, 3) as avg_luxury_shift,
              ROUND(avg_boldness_shift, 3) as avg_boldness_shift,
              ROUND(avg_consistency_shift, 3) as avg_consistency_shift,
              ROUND(avg_fatigue_trend, 3) as avg_fatigue_trend,
              luxury_positioning_trend,
              boldness_trend,
              creative_fatigue_trend,
              positioning_quadrant,
              competitive_position,
              ROUND(avg_volume_rank, 1) as avg_volume_rank,
              ROUND(avg_luxury_rank, 1) as avg_luxury_rank,
              ROUND(avg_boldness_rank, 1) as avg_boldness_rank
            FROM visual_summary
            ORDER BY total_visual_ads DESC
            """
        else:
            return f"""
            -- General Intelligence Dashboard with Temporal Analysis (Phase 1)
            WITH weekly_general_metrics AS (
              SELECT
                brand,
                DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) as week_start,
                COUNT(*) as weekly_signals,
                COUNT(DISTINCT DATE(start_timestamp)) as active_days_in_week,
                AVG(LENGTH(COALESCE(creative_text, ''))) as avg_content_length,
                COUNT(DISTINCT publisher_platforms) as platform_count_in_week,
                COUNT(DISTINCT ad_archive_id) as unique_campaigns_in_week
              FROM {base_table}
              WHERE start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 WEEK)
              GROUP BY brand, week_start
            ),
            temporal_analysis AS (
              SELECT
                brand, week_start, weekly_signals, active_days_in_week, avg_content_length,
                platform_count_in_week, unique_campaigns_in_week,
                -- Week-over-week changes
                LAG(weekly_signals) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_signals,
                LAG(avg_content_length) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_content_length,
                LAG(platform_count_in_week) OVER (PARTITION BY brand ORDER BY week_start) as prev_week_platforms,
                -- Competitive ranking
                RANK() OVER (PARTITION BY week_start ORDER BY weekly_signals DESC) as weekly_activity_rank,
                RANK() OVER (PARTITION BY week_start ORDER BY platform_count_in_week DESC) as weekly_platform_reach_rank
              FROM weekly_general_metrics
            ),
            general_summary AS (
              SELECT
                brand,
                COUNT(*) as weeks_active,
                SUM(weekly_signals) as total_signals,
                SUM(active_days_in_week) as total_active_days,
                AVG(avg_content_length) as overall_avg_content_length,
                AVG(platform_count_in_week) as avg_platform_count,
                AVG(unique_campaigns_in_week) as avg_campaigns_per_week,
                MIN(week_start) as first_seen_week,
                MAX(week_start) as last_seen_week,
                -- Temporal insights
                AVG(weekly_signals - COALESCE(prev_week_signals, weekly_signals)) as avg_weekly_activity_change,
                AVG(avg_content_length - COALESCE(prev_week_content_length, avg_content_length)) as avg_content_length_change,
                AVG(platform_count_in_week - COALESCE(prev_week_platforms, platform_count_in_week)) as avg_platform_expansion_change,
                AVG(weekly_activity_rank) as avg_activity_rank,
                AVG(weekly_platform_reach_rank) as avg_platform_reach_rank,
                -- Activity trends
                CASE
                  WHEN AVG(weekly_signals - COALESCE(prev_week_signals, weekly_signals)) > 2 THEN 'INCREASING_ACTIVITY'
                  WHEN AVG(weekly_signals - COALESCE(prev_week_signals, weekly_signals)) < -2 THEN 'DECREASING_ACTIVITY'
                  ELSE 'STABLE_ACTIVITY'
                END as activity_trend,
                CASE
                  WHEN AVG(avg_content_length - COALESCE(prev_week_content_length, avg_content_length)) > 10 THEN 'EXPANDING_CONTENT'
                  WHEN AVG(avg_content_length - COALESCE(prev_week_content_length, avg_content_length)) < -10 THEN 'SHORTENING_CONTENT'
                  ELSE 'STABLE_CONTENT_LENGTH'
                END as content_evolution_trend,
                CASE
                  WHEN AVG(platform_count_in_week - COALESCE(prev_week_platforms, platform_count_in_week)) > 0.5 THEN 'EXPANDING_REACH'
                  WHEN AVG(platform_count_in_week - COALESCE(prev_week_platforms, platform_count_in_week)) < -0.5 THEN 'CONSOLIDATING_REACH'
                  ELSE 'STABLE_REACH'
                END as platform_reach_trend,
                -- Competitive positioning
                CASE
                  WHEN AVG(weekly_activity_rank) <= 2 THEN 'MARKET_LEADER'
                  WHEN AVG(weekly_activity_rank) <= 4 THEN 'STRONG_PLAYER'
                  ELSE 'CHALLENGER'
                END as competitive_position
              FROM temporal_analysis
              GROUP BY brand
            )
            SELECT
              brand,
              weeks_active,
              total_signals,
              total_active_days,
              ROUND(overall_avg_content_length, 1) as avg_content_length,
              ROUND(avg_platform_count, 1) as avg_platform_count,
              ROUND(avg_campaigns_per_week, 1) as avg_campaigns_per_week,
              first_seen_week,
              last_seen_week,
              ROUND(avg_weekly_activity_change, 1) as avg_weekly_activity_change,
              ROUND(avg_content_length_change, 1) as avg_content_length_change,
              ROUND(avg_platform_expansion_change, 1) as avg_platform_expansion_change,
              activity_trend,
              content_evolution_trend,
              platform_reach_trend,
              competitive_position,
              ROUND(avg_activity_rank, 1) as avg_activity_rank,
              ROUND(avg_platform_reach_rank, 1) as avg_platform_reach_rank
            FROM general_summary
            ORDER BY total_signals DESC
            """

    def _generate_visual_style_matrix_sql(self, project_id: str, dataset_id: str) -> str:
        """Generate Visual Style Competitive Matrix SQL for enhanced Visual Intelligence analysis"""
        base_table = f"`{project_id}.{dataset_id}.ads_with_dates`"

        return f"""
        -- Visual Style Competitive Matrix
        -- Enhanced Visual Intelligence Dashboard for Style Analysis
        WITH style_matrix AS (
          SELECT
            brand,
            COUNT(*) as total_ads,
            COUNTIF(visual_style = 'MINIMALIST') * 100.0 / COUNT(*) as minimalist_pct,
            COUNTIF(visual_style = 'LUXURY') * 100.0 / COUNT(*) as luxury_pct,
            COUNTIF(visual_style = 'BOLD') * 100.0 / COUNT(*) as bold_pct,
            COUNTIF(visual_style = 'CASUAL') * 100.0 / COUNT(*) as casual_pct,
            COUNTIF(visual_style = 'PROFESSIONAL') * 100.0 / COUNT(*) as professional_pct,
            AVG(brand_consistency_score) as avg_brand_consistency,
            AVG(luxury_positioning_score) as avg_luxury_score,
            AVG(boldness_score) as avg_boldness_score
          FROM {base_table}
          WHERE visual_style IS NOT NULL
          GROUP BY brand
        ),
        competitive_gaps AS (
          SELECT
            'MINIMALIST' as style,
            AVG(minimalist_pct) as market_avg,
            MAX(minimalist_pct) - MIN(minimalist_pct) as market_spread,
            STDDEV(minimalist_pct) as market_volatility
          FROM style_matrix
          UNION ALL
          SELECT
            'LUXURY' as style,
            AVG(luxury_pct) as market_avg,
            MAX(luxury_pct) - MIN(luxury_pct) as market_spread,
            STDDEV(luxury_pct) as market_volatility
          FROM style_matrix
          UNION ALL
          SELECT
            'BOLD' as style,
            AVG(bold_pct) as market_avg,
            MAX(bold_pct) - MIN(bold_pct) as market_spread,
            STDDEV(bold_pct) as market_volatility
          FROM style_matrix
          UNION ALL
          SELECT
            'CASUAL' as style,
            AVG(casual_pct) as market_avg,
            MAX(casual_pct) - MIN(casual_pct) as market_spread,
            STDDEV(casual_pct) as market_volatility
          FROM style_matrix
          UNION ALL
          SELECT
            'PROFESSIONAL' as style,
            AVG(professional_pct) as market_avg,
            MAX(professional_pct) - MIN(professional_pct) as market_spread,
            STDDEV(professional_pct) as market_volatility
          FROM style_matrix
        ),
        competitive_advantage AS (
          SELECT
            sm.brand,
            sm.total_ads,
            ROUND(sm.minimalist_pct, 1) as minimalist_pct,
            ROUND(sm.luxury_pct, 1) as luxury_pct,
            ROUND(sm.bold_pct, 1) as bold_pct,
            ROUND(sm.casual_pct, 1) as casual_pct,
            ROUND(sm.professional_pct, 1) as professional_pct,
            ROUND(sm.avg_brand_consistency, 3) as brand_consistency_score,
            ROUND(sm.avg_luxury_score, 3) as luxury_positioning_score,
            ROUND(sm.avg_boldness_score, 3) as boldness_score,
            -- Competitive advantage indicators
            CASE
              WHEN sm.minimalist_pct > (SELECT market_avg FROM competitive_gaps WHERE style = 'MINIMALIST') + 20 THEN 'DOMINATES_MINIMALIST'
              WHEN sm.luxury_pct > (SELECT market_avg FROM competitive_gaps WHERE style = 'LUXURY') + 20 THEN 'DOMINATES_LUXURY'
              WHEN sm.bold_pct > (SELECT market_avg FROM competitive_gaps WHERE style = 'BOLD') + 20 THEN 'DOMINATES_BOLD'
              WHEN sm.casual_pct > (SELECT market_avg FROM competitive_gaps WHERE style = 'CASUAL') + 20 THEN 'DOMINATES_CASUAL'
              WHEN sm.professional_pct > (SELECT market_avg FROM competitive_gaps WHERE style = 'PROFESSIONAL') + 20 THEN 'DOMINATES_PROFESSIONAL'
              ELSE 'BALANCED_PORTFOLIO'
            END as style_advantage,
            -- Style diversification score (lower = more focused, higher = more diverse)
            (sm.minimalist_pct * sm.minimalist_pct + sm.luxury_pct * sm.luxury_pct +
             sm.bold_pct * sm.bold_pct + sm.casual_pct * sm.casual_pct +
             sm.professional_pct * sm.professional_pct) / 10000.0 as style_concentration_index
          FROM style_matrix sm
        )
        SELECT
          brand,
          total_ads,
          minimalist_pct,
          luxury_pct,
          bold_pct,
          casual_pct,
          professional_pct,
          brand_consistency_score,
          luxury_positioning_score,
          boldness_score,
          style_advantage,
          ROUND(style_concentration_index, 3) as style_focus_score,
          CASE
            WHEN style_concentration_index > 0.4 THEN 'HIGHLY_FOCUSED'
            WHEN style_concentration_index > 0.25 THEN 'MODERATELY_FOCUSED'
            ELSE 'DIVERSIFIED'
          END as style_strategy
        FROM competitive_advantage
        ORDER BY total_ads DESC
        """

    def _generate_signal_explorer_sql(self, signals: List[IntelligenceSignal], project_id: str, dataset_id: str) -> str:
        """Generate SQL for exploring all intelligence signals"""
        if not signals:
            return f"SELECT 'No signals available' as message"
            
        # Create signal metadata rows  
        signal_rows = []
        for s in signals[:20]:  # Limit for SQL readability
            signal_rows.append(f"""
            SELECT 
                '{s.source_module}' as module,
                '{s.insight[:100].replace("'", "''")}...' as insight_preview,
                {s.confidence} as confidence,
                {s.business_impact} as business_impact,
                '{s.signal_strength.value}' as strength""")
        
        union_clause = " UNION ALL ".join(signal_rows)
        
        return f"""
        -- Intelligence Signal Explorer
        WITH signal_metadata AS (
            {union_clause}
        )
        SELECT 
            module,
            COUNT(*) as signal_count,
            ROUND(AVG(confidence), 3) as avg_confidence,
            ROUND(AVG(business_impact), 3) as avg_business_impact,
            COUNT(CASE WHEN strength = 'CRITICAL' THEN 1 END) as critical_signals,
            COUNT(CASE WHEN strength = 'HIGH' THEN 1 END) as high_signals
        FROM signal_metadata  
        GROUP BY module
        ORDER BY signal_count DESC
        """
    
    def _generate_confidence_analysis_sql(self, project_id: str, dataset_id: str) -> str:
        """Generate SQL for confidence analysis across data sources"""
        return f"""
        -- Confidence Analysis Across Data Sources
        SELECT
            brand,
            COUNT(*) as total_data_points,
            COUNT(CASE WHEN creative_text IS NOT NULL AND LENGTH(creative_text) > 10 THEN 1 END) as creative_data_points,
            COUNT(CASE WHEN publisher_platforms IS NOT NULL THEN 1 END) as channel_data_points,
            ROUND(COUNT(CASE WHEN creative_text IS NOT NULL AND LENGTH(creative_text) > 10 THEN 1 END) * 100.0 / COUNT(*), 2) as creative_coverage_pct,
            ROUND(COUNT(CASE WHEN publisher_platforms IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as channel_coverage_pct,
            -- Data quality indicators
            AVG(LENGTH(COALESCE(creative_text, ''))) as avg_text_richness,
            COUNT(DISTINCT publisher_platforms) as platform_diversity,
            ROUND((COUNT(CASE WHEN creative_text IS NOT NULL AND LENGTH(creative_text) > 10 THEN 1 END) + 
                   COUNT(CASE WHEN publisher_platforms IS NOT NULL THEN 1 END)) / (COUNT(*) * 2.0) * 100, 1) as overall_data_quality_score
        FROM `{project_id}.{dataset_id}.ads_with_dates`
        GROUP BY brand
        ORDER BY total_data_points DESC
        """
    
    def get_framework_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics about the framework"""
        return {
            'total_signals': len(self.signals),
            'by_strength': {
                strength.value: len([s for s in self.signals if s.signal_strength == strength])
                for strength in SignalStrength
            },
            'by_module': {
                module: len([s for s in self.signals if s.source_module == module])
                for module in set(s.source_module for s in self.signals) if self.signals
            },
            'by_level': {
                f"L{level.value}": len([s for s in self.signals if level in s.recommended_levels])
                for level in IntelligenceLevel
            },
            'avg_confidence': np.mean([s.confidence for s in self.signals]) if self.signals else 0.0,
            'avg_business_impact': np.mean([s.business_impact for s in self.signals]) if self.signals else 0.0,
            'avg_actionability': np.mean([s.actionability for s in self.signals]) if self.signals else 0.0,
            'framework_efficiency': (
                len([s for s in self.signals if s.signal_strength != SignalStrength.NOISE]) / 
                len(self.signals) if self.signals else 0.0
            )
        }

    def _generate_competitive_velocity_sql(self, project_id: str, dataset_id: str) -> str:
        """Generate Competitive Velocity Analysis SQL for Phase 2 Temporal Intelligence"""
        base_table = f"`{project_id}.{dataset_id}.ads_with_dates`"

        return f"""
        -- Competitive Velocity Analysis Dashboard (Phase 2)
        -- Measures acceleration and deceleration across all intelligence dimensions

        WITH weekly_intelligence_metrics AS (
          SELECT
            brand,
            DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) as week_start,

            -- Creative Velocity Metrics
            COUNT(*) as weekly_ads,
            AVG(LENGTH(COALESCE(creative_text, ''))) as avg_creative_length,
            COUNT(DISTINCT REGEXP_EXTRACT(creative_text, r'\\b(\\w+)\\b')) as unique_creative_words,

            -- Channel Momentum Metrics
            COUNT(DISTINCT media_type) as channel_diversity,
            AVG(ARRAY_LENGTH(platforms_array)) as avg_platform_reach,
            SUM(CASE WHEN media_type = 'VIDEO' THEN 1 ELSE 0 END) as video_ads,

            -- Visual Evolution Metrics
            AVG(CASE WHEN LENGTH(creative_text) > 100 THEN 1.0 ELSE 0.0 END) as long_form_rate,
            AVG(duration_quality_weight) as avg_quality_score

          FROM {base_table}
          WHERE creative_text IS NOT NULL
            AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 16 WEEK)
          GROUP BY brand, week_start
        ),

        velocity_calculations AS (
          SELECT
            brand,
            week_start,
            weekly_ads,
            avg_creative_length,
            unique_creative_words,
            channel_diversity,
            avg_platform_reach,
            video_ads,
            long_form_rate,
            avg_quality_score,

            -- Creative Velocity: Rate of change in creative output
            (weekly_ads - LAG(weekly_ads, 1) OVER (PARTITION BY brand ORDER BY week_start)) as creative_volume_velocity_1w,
            (weekly_ads - LAG(weekly_ads, 4) OVER (PARTITION BY brand ORDER BY week_start)) / 4.0 as creative_volume_velocity_4w,

            -- Creative Sophistication Velocity
            (avg_creative_length - LAG(avg_creative_length, 1) OVER (PARTITION BY brand ORDER BY week_start)) as creative_length_velocity_1w,
            (unique_creative_words - LAG(unique_creative_words, 1) OVER (PARTITION BY brand ORDER BY week_start)) as creative_diversity_velocity_1w,

            -- Channel Momentum: Rate of platform expansion
            (avg_platform_reach - LAG(avg_platform_reach, 1) OVER (PARTITION BY brand ORDER BY week_start)) as platform_momentum_1w,
            (channel_diversity - LAG(channel_diversity, 1) OVER (PARTITION BY brand ORDER BY week_start)) as channel_expansion_velocity_1w,

            -- Visual Evolution Speed
            (video_ads - LAG(video_ads, 1) OVER (PARTITION BY brand ORDER BY week_start)) as video_adoption_velocity_1w,
            (long_form_rate - LAG(long_form_rate, 1) OVER (PARTITION BY brand ORDER BY week_start)) as content_sophistication_velocity_1w

          FROM weekly_intelligence_metrics
        ),

        competitive_velocity_ranking AS (
          SELECT
            *,

            -- Current week competitive velocity rankings
            RANK() OVER (PARTITION BY week_start ORDER BY ABS(COALESCE(creative_volume_velocity_1w, 0)) DESC) as creative_velocity_rank,
            RANK() OVER (PARTITION BY week_start ORDER BY ABS(COALESCE(platform_momentum_1w, 0)) DESC) as platform_momentum_rank,
            RANK() OVER (PARTITION BY week_start ORDER BY ABS(COALESCE(channel_expansion_velocity_1w, 0)) DESC) as channel_velocity_rank,
            RANK() OVER (PARTITION BY week_start ORDER BY ABS(COALESCE(video_adoption_velocity_1w, 0)) DESC) as visual_velocity_rank,

            -- Velocity classification
            CASE
              WHEN COALESCE(creative_volume_velocity_1w, 0) >= 5 THEN 'ACCELERATING_FAST'
              WHEN COALESCE(creative_volume_velocity_1w, 0) >= 2 THEN 'ACCELERATING'
              WHEN COALESCE(creative_volume_velocity_1w, 0) <= -5 THEN 'DECELERATING_FAST'
              WHEN COALESCE(creative_volume_velocity_1w, 0) <= -2 THEN 'DECELERATING'
              ELSE 'STABLE_VELOCITY'
            END as creative_velocity_classification,

            CASE
              WHEN COALESCE(platform_momentum_1w, 0) >= 1.0 THEN 'EXPANDING_FAST'
              WHEN COALESCE(platform_momentum_1w, 0) >= 0.5 THEN 'EXPANDING'
              WHEN COALESCE(platform_momentum_1w, 0) <= -1.0 THEN 'CONTRACTING_FAST'
              WHEN COALESCE(platform_momentum_1w, 0) <= -0.5 THEN 'CONTRACTING'
              ELSE 'STABLE_REACH'
            END as platform_momentum_classification

          FROM velocity_calculations
        ),

        velocity_summary AS (
          SELECT
            brand,
            week_start,

            -- Current metrics
            weekly_ads,
            ROUND(avg_creative_length, 1) as avg_creative_length,
            unique_creative_words,
            channel_diversity,
            ROUND(avg_platform_reach, 1) as avg_platform_reach,
            video_ads,
            ROUND(long_form_rate * 100, 1) as long_form_pct,
            ROUND(avg_quality_score, 3) as avg_quality_score,

            -- Velocity metrics (1-week)
            ROUND(COALESCE(creative_volume_velocity_1w, 0), 1) as creative_volume_velocity_1w,
            ROUND(COALESCE(creative_length_velocity_1w, 0), 1) as creative_length_velocity_1w,
            ROUND(COALESCE(creative_diversity_velocity_1w, 0), 1) as creative_diversity_velocity_1w,
            ROUND(COALESCE(platform_momentum_1w, 0), 2) as platform_momentum_1w,
            ROUND(COALESCE(channel_expansion_velocity_1w, 0), 1) as channel_expansion_velocity_1w,
            ROUND(COALESCE(video_adoption_velocity_1w, 0), 1) as video_adoption_velocity_1w,
            ROUND(COALESCE(content_sophistication_velocity_1w, 0), 3) as content_sophistication_velocity_1w,

            -- Velocity metrics (4-week trend)
            ROUND(COALESCE(creative_volume_velocity_4w, 0), 2) as creative_volume_velocity_4w,

            -- Competitive rankings
            creative_velocity_rank,
            platform_momentum_rank,
            channel_velocity_rank,
            visual_velocity_rank,

            -- Classifications
            creative_velocity_classification,
            platform_momentum_classification,

            -- Overall velocity score (composite of all dimensions)
            ROUND((
              ABS(COALESCE(creative_volume_velocity_1w, 0)) * 0.3 +
              ABS(COALESCE(platform_momentum_1w, 0)) * 10 * 0.3 +  -- Scale platform momentum
              ABS(COALESCE(channel_expansion_velocity_1w, 0)) * 0.2 +
              ABS(COALESCE(video_adoption_velocity_1w, 0)) * 0.2
            ), 2) as overall_velocity_score,

            -- Strategic velocity insight
            CASE
              WHEN creative_velocity_rank <= 2 AND platform_momentum_rank <= 2 THEN 'MULTI_DIMENSIONAL_LEADER'
              WHEN creative_velocity_rank <= 2 THEN 'CREATIVE_VELOCITY_LEADER'
              WHEN platform_momentum_rank <= 2 THEN 'PLATFORM_MOMENTUM_LEADER'
              WHEN channel_velocity_rank <= 2 THEN 'CHANNEL_EXPANSION_LEADER'
              WHEN visual_velocity_rank <= 2 THEN 'VISUAL_EVOLUTION_LEADER'
              ELSE 'STABLE_STRATEGIC_POSITION'
            END as velocity_leadership_type

          FROM competitive_velocity_ranking
        )

        SELECT *
        FROM velocity_summary
        WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)  -- Last 12 weeks
        ORDER BY week_start DESC, overall_velocity_score DESC
        """

    def _generate_strategic_inflection_sql(self, project_id: str, dataset_id: str) -> str:
        """Generate Strategic Inflection Detection SQL for Phase 3 Temporal Intelligence"""
        base_table = f"`{project_id}.{dataset_id}.ads_with_dates`"

        return f"""
        -- Strategic Inflection Detection Dashboard (Phase 3)
        -- Identifies when competitive dynamics shifted significantly

        WITH weekly_metrics AS (
          SELECT
            brand,
            DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) as week_start,

            -- Core strategic metrics
            COUNT(*) as weekly_ads,
            AVG(LENGTH(COALESCE(creative_text, ''))) as avg_creative_length,
            COUNT(DISTINCT media_type) as media_diversity,
            AVG(ARRAY_LENGTH(platforms_array)) as avg_platform_reach,
            SUM(CASE WHEN media_type = 'VIDEO' THEN 1 ELSE 0 END) / COUNT(*) as video_ratio,
            AVG(duration_quality_weight) as avg_quality,

            -- Text sophistication metrics
            AVG(CASE WHEN creative_text LIKE '%limited%' OR creative_text LIKE '%exclusive%'
                     OR creative_text LIKE '%sale%' THEN 1 ELSE 0 END) as urgency_ratio,
            AVG(CASE WHEN creative_text LIKE '%free%' OR creative_text LIKE '%discount%'
                     OR creative_text LIKE '%off%' THEN 1 ELSE 0 END) as promo_ratio

          FROM {base_table}
          WHERE creative_text IS NOT NULL
            AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 WEEK)
          GROUP BY brand, week_start
        ),

        inflection_detection AS (
          SELECT
            brand,
            week_start,
            weekly_ads,
            avg_creative_length,
            media_diversity,
            avg_platform_reach,
            video_ratio,
            avg_quality,
            urgency_ratio,
            promo_ratio,

            -- Calculate rolling averages for stability baseline
            AVG(weekly_ads) OVER (
              PARTITION BY brand
              ORDER BY week_start
              ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_ads_4w,

            STDDEV(weekly_ads) OVER (
              PARTITION BY brand
              ORDER BY week_start
              ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) as rolling_stddev_ads_4w,

            -- Media strategy baseline
            AVG(video_ratio) OVER (
              PARTITION BY brand
              ORDER BY week_start
              ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_video_ratio_4w,

            -- Platform strategy baseline
            AVG(avg_platform_reach) OVER (
              PARTITION BY brand
              ORDER BY week_start
              ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_platform_4w,

            -- Creative strategy baseline
            AVG(avg_creative_length) OVER (
              PARTITION BY brand
              ORDER BY week_start
              ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
            ) as rolling_avg_creative_length_4w

          FROM weekly_metrics
        ),

        anomaly_detection AS (
          SELECT
            *,

            -- Volume anomaly detection (2 standard deviations)
            CASE
              WHEN ABS(weekly_ads - COALESCE(rolling_avg_ads_4w, weekly_ads)) >
                   2 * COALESCE(rolling_stddev_ads_4w, 0.001) THEN 1
              ELSE 0
            END as volume_anomaly,

            -- Media strategy shift detection (>30% change)
            CASE
              WHEN ABS(video_ratio - COALESCE(rolling_avg_video_ratio_4w, video_ratio)) > 0.3 THEN 1
              ELSE 0
            END as media_strategy_shift,

            -- Platform expansion detection (>1.5 platform change)
            CASE
              WHEN ABS(avg_platform_reach - COALESCE(rolling_avg_platform_4w, avg_platform_reach)) > 1.5 THEN 1
              ELSE 0
            END as platform_strategy_shift,

            -- Creative evolution detection (>50 char average change)
            CASE
              WHEN ABS(avg_creative_length - COALESCE(rolling_avg_creative_length_4w, avg_creative_length)) > 50 THEN 1
              ELSE 0
            END as creative_strategy_shift,

            -- Calculate Z-scores for magnitude
            SAFE_DIVIDE(
              weekly_ads - COALESCE(rolling_avg_ads_4w, weekly_ads),
              COALESCE(rolling_stddev_ads_4w, 1)
            ) as volume_z_score

          FROM inflection_detection
        ),

        competitive_responses AS (
          SELECT
            a.brand,
            a.week_start,
            a.weekly_ads,
            a.volume_anomaly,
            a.media_strategy_shift,
            a.platform_strategy_shift,
            a.creative_strategy_shift,
            a.volume_z_score,

            -- Check if competitors had inflections in the same or following week
            SUM(CASE
              WHEN b.brand != a.brand AND b.volume_anomaly = 1
                   AND b.week_start BETWEEN a.week_start AND DATE_ADD(a.week_start, INTERVAL 2 WEEK)
              THEN 1 ELSE 0
            END) as competitive_response_count,

            -- Identify market-wide shifts
            SUM(CASE
              WHEN b.week_start = a.week_start AND (
                b.volume_anomaly = 1 OR
                b.media_strategy_shift = 1 OR
                b.platform_strategy_shift = 1
              ) THEN 1 ELSE 0
            END) as market_shift_breadth

          FROM anomaly_detection a
          CROSS JOIN anomaly_detection b
          GROUP BY 1,2,3,4,5,6,7,8
        ),

        inflection_summary AS (
          SELECT
            brand,
            week_start,
            weekly_ads,
            ROUND(volume_z_score, 2) as volume_z_score,

            -- Strategic shift indicators
            volume_anomaly,
            media_strategy_shift,
            platform_strategy_shift,
            creative_strategy_shift,

            -- Total strategic changes this week
            (volume_anomaly + media_strategy_shift + platform_strategy_shift + creative_strategy_shift) as total_shifts,

            -- Competitive dynamics
            competitive_response_count,
            market_shift_breadth,

            -- Classify inflection type
            CASE
              WHEN (volume_anomaly + media_strategy_shift + platform_strategy_shift + creative_strategy_shift) >= 3
                THEN 'MAJOR_STRATEGIC_PIVOT'
              WHEN (volume_anomaly + media_strategy_shift + platform_strategy_shift + creative_strategy_shift) >= 2
                THEN 'SIGNIFICANT_ADJUSTMENT'
              WHEN volume_anomaly = 1 AND volume_z_score > 3
                THEN 'VOLUME_SURGE'
              WHEN volume_anomaly = 1 AND volume_z_score < -3
                THEN 'VOLUME_COLLAPSE'
              WHEN media_strategy_shift = 1
                THEN 'MEDIA_STRATEGY_CHANGE'
              WHEN platform_strategy_shift = 1
                THEN 'PLATFORM_EXPANSION'
              WHEN creative_strategy_shift = 1
                THEN 'CREATIVE_EVOLUTION'
              ELSE 'STABLE_STRATEGY'
            END as inflection_type,

            -- Competitive response pattern
            CASE
              WHEN competitive_response_count >= 3 THEN 'TRIGGERED_MARKET_RESPONSE'
              WHEN competitive_response_count >= 1 THEN 'PROMPTED_COMPETITOR_ACTION'
              WHEN market_shift_breadth >= 4 THEN 'PART_OF_MARKET_SHIFT'
              WHEN market_shift_breadth >= 2 THEN 'ALIGNED_WITH_TREND'
              ELSE 'INDEPENDENT_MOVE'
            END as competitive_context,

            -- Strategic importance score
            ROUND(
              ABS(COALESCE(volume_z_score, 0)) * 0.3 +
              (volume_anomaly + media_strategy_shift + platform_strategy_shift + creative_strategy_shift) * 2 +
              LEAST(competitive_response_count, 3) * 0.5 +
              LEAST(market_shift_breadth, 5) * 0.2
            , 2) as inflection_importance_score

          FROM competitive_responses
        )

        SELECT
          brand,
          week_start,
          weekly_ads,
          volume_z_score,
          inflection_type,
          competitive_context,
          inflection_importance_score,
          total_shifts,
          competitive_response_count,
          market_shift_breadth,

          -- Strategic shift details
          CASE WHEN volume_anomaly = 1 THEN 'VOLUME' ELSE NULL END as volume_shift,
          CASE WHEN media_strategy_shift = 1 THEN 'MEDIA' ELSE NULL END as media_shift,
          CASE WHEN platform_strategy_shift = 1 THEN 'PLATFORM' ELSE NULL END as platform_shift,
          CASE WHEN creative_strategy_shift = 1 THEN 'CREATIVE' ELSE NULL END as creative_shift

        FROM inflection_summary
        WHERE week_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 WEEK)
          AND inflection_type != 'STABLE_STRATEGY'  -- Only show actual inflections
        ORDER BY inflection_importance_score DESC, week_start DESC
        """

    def _generate_predictive_integration_sql(self, project_id: str, dataset_id: str) -> str:
        """Generate Predictive Integration SQL for Phase 4 Temporal Intelligence"""

        return f"""
        -- Predictive Competitive Forecast Dashboard (Phase 4)
        -- Integrates ML.FORECAST models with current competitive intelligence

        WITH current_competitive_state AS (
          SELECT
            brand,
            DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) as week_start,
            COUNT(*) as current_weekly_ads,
            AVG(duration_quality_weight) as current_avg_quality,
            COUNT(DISTINCT media_type) as current_media_diversity,
            AVG(ARRAY_LENGTH(platforms_array)) as current_platform_reach,
            SUM(CASE WHEN media_type = 'VIDEO' THEN 1 ELSE 0 END) / COUNT(*) * 100 as current_video_pct

          FROM `{project_id}.{dataset_id}.ads_with_dates`
          WHERE creative_text IS NOT NULL
            AND start_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 WEEK)
          GROUP BY brand, week_start
        ),

        -- Get latest actual values for comparison baseline
        latest_actuals AS (
          SELECT
            brand,
            MAX(week_start) as latest_week,
            ARRAY_AGG(current_weekly_ads ORDER BY week_start DESC LIMIT 1)[OFFSET(0)] as latest_weekly_ads,
            ARRAY_AGG(current_avg_quality ORDER BY week_start DESC LIMIT 1)[OFFSET(0)] as latest_avg_quality,
            ARRAY_AGG(current_platform_reach ORDER BY week_start DESC LIMIT 1)[OFFSET(0)] as latest_platform_reach,
            ARRAY_AGG(current_video_pct ORDER BY week_start DESC LIMIT 1)[OFFSET(0)] as latest_video_pct
          FROM current_competitive_state
          GROUP BY brand
        ),

        -- ML.FORECAST Volume Predictions (4-week horizon)
        volume_forecasts AS (
          SELECT
            forecast_timestamp,
            brand_id as brand,
            forecast_value as predicted_weekly_ads,
            prediction_interval_lower_bound as ads_lower_bound,
            prediction_interval_upper_bound as ads_upper_bound,
            confidence_level
          FROM ML.FORECAST(MODEL `{project_id}.{dataset_id}.forecast_ad_volume`,
                          STRUCT(4 as horizon))
          WHERE forecast_timestamp > CURRENT_DATE()
        ),

        -- ML.FORECAST Aggressiveness Predictions
        aggressiveness_forecasts AS (
          SELECT
            forecast_timestamp,
            brand_id as brand,
            forecast_value as predicted_aggressiveness,
            prediction_interval_lower_bound as aggressiveness_lower,
            prediction_interval_upper_bound as aggressiveness_upper
          FROM ML.FORECAST(MODEL `{project_id}.{dataset_id}.forecast_aggressiveness`,
                          STRUCT(4 as horizon))
          WHERE forecast_timestamp > CURRENT_DATE()
        ),

        -- ML.FORECAST Cross-Platform Predictions
        platform_forecasts AS (
          SELECT
            forecast_timestamp,
            brand_id as brand,
            forecast_value as predicted_cross_platform_pct,
            prediction_interval_lower_bound as platform_lower,
            prediction_interval_upper_bound as platform_upper
          FROM ML.FORECAST(MODEL `{project_id}.{dataset_id}.forecast_cross_platform`,
                          STRUCT(4 as horizon))
          WHERE forecast_timestamp > CURRENT_DATE()
        ),

        -- Combine all forecasts with current state
        integrated_forecasts AS (
          SELECT
            v.forecast_timestamp,
            v.brand,

            -- Current baseline (latest actuals)
            l.latest_weekly_ads,
            l.latest_avg_quality,
            l.latest_platform_reach,
            l.latest_video_pct,

            -- Volume forecasts
            ROUND(v.predicted_weekly_ads, 1) as predicted_weekly_ads,
            ROUND(v.ads_lower_bound, 1) as ads_lower_bound,
            ROUND(v.ads_upper_bound, 1) as ads_upper_bound,
            v.confidence_level,

            -- Aggressiveness forecasts
            ROUND(COALESCE(a.predicted_aggressiveness, l.latest_avg_quality), 3) as predicted_aggressiveness,
            ROUND(COALESCE(a.aggressiveness_lower, l.latest_avg_quality * 0.9), 3) as aggressiveness_lower,
            ROUND(COALESCE(a.aggressiveness_upper, l.latest_avg_quality * 1.1), 3) as aggressiveness_upper,

            -- Platform forecasts
            ROUND(COALESCE(p.predicted_cross_platform_pct, l.latest_platform_reach), 2) as predicted_platform_reach,
            ROUND(COALESCE(p.platform_lower, l.latest_platform_reach * 0.8), 2) as platform_lower,
            ROUND(COALESCE(p.platform_upper, l.latest_platform_reach * 1.2), 2) as platform_upper,

            -- Calculate predicted changes from current state
            ROUND(v.predicted_weekly_ads - l.latest_weekly_ads, 1) as predicted_volume_change,
            ROUND((v.predicted_weekly_ads - l.latest_weekly_ads) / NULLIF(l.latest_weekly_ads, 0) * 100, 1) as predicted_volume_change_pct,

            -- Strategic prediction categories
            CASE
              WHEN v.predicted_weekly_ads > l.latest_weekly_ads * 1.2 THEN 'SIGNIFICANT_EXPANSION'
              WHEN v.predicted_weekly_ads > l.latest_weekly_ads * 1.1 THEN 'MODERATE_GROWTH'
              WHEN v.predicted_weekly_ads < l.latest_weekly_ads * 0.8 THEN 'SIGNIFICANT_CONTRACTION'
              WHEN v.predicted_weekly_ads < l.latest_weekly_ads * 0.9 THEN 'MODERATE_DECLINE'
              ELSE 'STABLE_VOLUME'
            END as predicted_volume_trend,

            -- Competitive forecast insights
            RANK() OVER (PARTITION BY v.forecast_timestamp ORDER BY v.predicted_weekly_ads DESC) as predicted_volume_rank,
            RANK() OVER (PARTITION BY v.forecast_timestamp ORDER BY COALESCE(a.predicted_aggressiveness, l.latest_avg_quality) DESC) as predicted_aggressiveness_rank

          FROM volume_forecasts v
          LEFT JOIN latest_actuals l ON v.brand = l.brand
          LEFT JOIN aggressiveness_forecasts a ON v.brand = a.brand AND v.forecast_timestamp = a.forecast_timestamp
          LEFT JOIN platform_forecasts p ON v.brand = p.brand AND v.forecast_timestamp = p.forecast_timestamp
        ),

        -- Add week-ahead competitive dynamics
        competitive_forecast_analysis AS (
          SELECT
            *,

            -- Forecast horizon analysis
            DATE_DIFF(forecast_timestamp, CURRENT_DATE(), DAY) as days_ahead,
            CASE
              WHEN DATE_DIFF(forecast_timestamp, CURRENT_DATE(), DAY) <= 7 THEN 'WEEK_1'
              WHEN DATE_DIFF(forecast_timestamp, CURRENT_DATE(), DAY) <= 14 THEN 'WEEK_2'
              WHEN DATE_DIFF(forecast_timestamp, CURRENT_DATE(), DAY) <= 21 THEN 'WEEK_3'
              ELSE 'WEEK_4'
            END as forecast_horizon,

            -- Strategic forecast alerts
            CASE
              WHEN predicted_volume_change_pct > 25 THEN 'HIGH_EXPANSION_ALERT'
              WHEN predicted_volume_change_pct < -25 THEN 'HIGH_CONTRACTION_ALERT'
              WHEN predicted_volume_rank <= 2 AND predicted_volume_change_pct > 10 THEN 'EMERGING_LEADER_ALERT'
              WHEN predicted_aggressiveness_rank <= 2 THEN 'AGGRESSIVENESS_LEADER_ALERT'
              ELSE 'NORMAL_FORECAST'
            END as strategic_alert,

            -- Confidence in predictions
            CASE
              WHEN confidence_level >= 0.95 THEN 'HIGH_CONFIDENCE'
              WHEN confidence_level >= 0.80 THEN 'MEDIUM_CONFIDENCE'
              ELSE 'LOW_CONFIDENCE'
            END as forecast_confidence_level

          FROM integrated_forecasts
        )

        SELECT
          brand,
          forecast_timestamp,
          days_ahead,
          forecast_horizon,

          -- Current state
          latest_weekly_ads as current_weekly_ads,
          latest_avg_quality as current_avg_quality,
          latest_platform_reach as current_platform_reach,

          -- Predicted state
          predicted_weekly_ads,
          predicted_aggressiveness,
          predicted_platform_reach,

          -- Prediction intervals
          ads_lower_bound,
          ads_upper_bound,
          aggressiveness_lower,
          aggressiveness_upper,
          platform_lower,
          platform_upper,

          -- Changes and trends
          predicted_volume_change,
          predicted_volume_change_pct,
          predicted_volume_trend,

          -- Competitive positioning
          predicted_volume_rank,
          predicted_aggressiveness_rank,

          -- Strategic insights
          strategic_alert,
          forecast_confidence_level,

          -- Forecast metadata
          ROUND(confidence_level * 100, 1) as confidence_pct

        FROM competitive_forecast_analysis
        ORDER BY brand, forecast_timestamp
        """

    def add_temporal_context(self, base_insight: str, current_value: float, metric_name: str, metadata: Dict = None) -> str:
        """
        Add temporal context to L1-L3 insights for Phase 5 enhancement
        Transforms static insights into temporal intelligence narratives

        Args:
            base_insight: Original insight text
            current_value: Current metric value
            metric_name: Name of the metric for temporal lookup
            metadata: Additional context for temporal analysis

        Returns:
            Enhanced insight with temporal framing
        """
        # For Phase 5 implementation, we'll enhance insights with temporal patterns
        # This function provides the "where we came from/going" framing

        # Simulate temporal data (in production, this would query historical trends)
        # Mock historical progression for demonstration
        if metadata and 'temporal_trend' in metadata:
            trend = metadata['temporal_trend']
            timeframe = metadata.get('timeframe', '6 weeks')

            if trend == 'increasing':
                if 'competitor' in base_insight.lower() or 'copying' in base_insight.lower():
                    return f"{base_insight} - threat accelerating over {timeframe}, competitive pressure increasing"
                else:
                    return f"{base_insight} - improving trend over {timeframe}"
            elif trend == 'decreasing':
                if 'competitor' in base_insight.lower() or 'copying' in base_insight.lower():
                    return f"{base_insight} - threat diminishing over {timeframe}, competitive pressure decreasing"
                elif 'optimization' in base_insight.lower():
                    return f"{base_insight} - declining performance over {timeframe}, immediate action required"
                else:
                    return f"{base_insight} - concerning downward trend over {timeframe}"
            elif trend == 'volatile':
                return f"{base_insight} - unstable pattern over {timeframe}, requires monitoring"
            else:
                return f"{base_insight} - stable pattern over {timeframe}"

        # Default temporal enhancement based on value patterns
        if current_value < 0.3:
            return f"{base_insight} - requires immediate attention based on recent decline"
        elif current_value > 0.8:
            return f"{base_insight} - building on recent strong performance"
        else:
            return f"{base_insight} - shows emerging competitive dynamics"


def create_creative_intelligence_signals(framework: ProgressiveDisclosureFramework, creative_data: Dict) -> None:
    """Create Creative Intelligence signals from P2 enhanced data"""

    # Text Length Analysis Signal with Phase 5 Temporal Enhancement
    avg_length = creative_data.get('avg_text_length', 0)
    if avg_length < 30:
        base_insight = "Creative text length optimization needed - content is too brief for engagement"
        temporal_metadata = {
            'temporal_trend': 'decreasing',
            'timeframe': '4 weeks',
            'metric': 'text_length',
            'threshold': 30
        }
        enhanced_insight = framework.add_temporal_context(
            base_insight, avg_length / 100, 'text_length', temporal_metadata
        )
        framework.add_signal(
            insight=enhanced_insight,
            value=avg_length,
            confidence=0.8,
            business_impact=0.7,
            actionability=0.9,
            source_module="Creative Intelligence",
            metric_name="creative_text_length_short",
            metadata=temporal_metadata
        )
    elif avg_length > 200:
        base_insight = "Creative text may be too long - consider brevity for better performance"
        temporal_metadata = {
            'temporal_trend': 'increasing',
            'timeframe': '3 weeks',
            'metric': 'text_length',
            'threshold': 200
        }
        enhanced_insight = framework.add_temporal_context(
            base_insight, avg_length / 300, 'text_length', temporal_metadata
        )
        framework.add_signal(
            insight=enhanced_insight,
            value=avg_length,
            confidence=0.7,
            business_impact=0.6,
            actionability=0.8,
            source_module="Creative Intelligence",
            metric_name="creative_text_length_long",
            metadata=temporal_metadata
        )
    
    # Brand Mention Frequency Signal with Phase 5 Temporal Enhancement
    brand_mentions = creative_data.get('avg_brand_mentions', 0)
    if brand_mentions < 0.5:
        base_insight = "Low brand mention frequency detected - increase brand presence in creative content"
        temporal_metadata = {
            'temporal_trend': 'decreasing',
            'timeframe': '6 weeks',
            'metric': 'brand_mentions'
        }
        enhanced_insight = framework.add_temporal_context(
            base_insight, brand_mentions, 'brand_mentions', temporal_metadata
        )
        framework.add_signal(
            insight=enhanced_insight,
            value=brand_mentions,
            confidence=0.7,
            business_impact=0.8,
            actionability=0.8,
            source_module="Creative Intelligence",
            metric_name="brand_mention_frequency_low",
            metadata={'metric': 'brand_mentions', 'current': brand_mentions, 'target': 1.0}
        )
    
    # AI-Enhanced Emotional Intelligence Signals
    ai_emotional_intensity = creative_data.get('avg_ai_emotional_intensity', 0)
    ai_industry_relevance = creative_data.get('avg_ai_industry_relevance', 0)

    # AI Emotional Intensity Signal (0-10 scale)
    if ai_emotional_intensity > 0:  # AI analysis available
        if ai_emotional_intensity < 3.0:
            framework.add_signal(
                insight="Low emotional intensity detected through AI analysis - strengthen emotional appeal for eyewear marketing",
                value=round(ai_emotional_intensity, 2),
                confidence=0.8,
                business_impact=0.7,
                actionability=0.8,
                source_module="Creative Intelligence",
                metric_name="emotional_intensity_score",
                metadata={'metric': 'ai_emotional_intensity', 'scale': '0-10', 'analysis_type': 'ai_generated'}
            )
        elif ai_emotional_intensity > 8.0:
            framework.add_signal(
                insight="Very high emotional intensity detected - consider balanced approach for broader audience appeal",
                value=round(ai_emotional_intensity, 2),
                confidence=0.7,
                business_impact=0.6,
                actionability=0.7,
                source_module="Creative Intelligence",
                metric_name="emotional_intensity_score",
                metadata={'metric': 'ai_emotional_intensity', 'scale': '0-10', 'analysis_type': 'ai_generated'}
            )
    else:
        # Fallback to regex-based analysis if AI not available
        emotional_score = creative_data.get('avg_emotional_keywords', 0)
        if emotional_score < 1.0:
            framework.add_signal(
                insight="Limited emotional language detected - incorporate more engaging emotional keywords",
                value=emotional_score,
                confidence=0.6,
                business_impact=0.6,
                actionability=0.7,
                source_module="Creative Intelligence",
                metric_name="emotional_keywords_limited",
                metadata={'metric': 'emotional_keywords', 'examples': ['amazing', 'perfect', 'love'], 'analysis_type': 'regex_based'}
            )

    # AI Industry Relevance Signal (0-1 scale)
    if ai_industry_relevance > 0 and ai_industry_relevance < 0.4:
        framework.add_signal(
            insight="Low eyewear industry relevance in emotional messaging - tailor content for eyewear context",
            value=round(ai_industry_relevance, 2),
            confidence=0.8,
            business_impact=0.8,
            actionability=0.9,
            source_module="Creative Intelligence",
            metric_name="industry_relevance_score",
            metadata={'metric': 'ai_industry_relevance', 'scale': '0-1', 'industry': 'eyewear'}
        )

    # AI Sentiment Category Analysis
    ai_positive_rate = creative_data.get('ai_positive_sentiment_rate', 0)
    ai_aspirational_rate = creative_data.get('ai_aspirational_sentiment_rate', 0)

    if ai_positive_rate > 0 or ai_aspirational_rate > 0:  # AI analysis available
        total_positive_aspirational = ai_positive_rate + ai_aspirational_rate
        if total_positive_aspirational < 30:  # Less than 30% positive/aspirational
            framework.add_signal(
                insight="Low positive/aspirational sentiment detected - increase uplifting messaging for eyewear brand building",
                value=round(total_positive_aspirational, 2),
                confidence=0.8,
                business_impact=0.7,
                actionability=0.8,
                source_module="Creative Intelligence",
                metric_name="positive_sentiment_rate_pct",
                metadata={'metric': 'ai_sentiment_balance', 'positive_rate': ai_positive_rate, 'aspirational_rate': ai_aspirational_rate, 'scale': 'percentage'}
            )

    # AI Persuasion Style Analysis
    ai_lifestyle_rate = creative_data.get('ai_lifestyle_style_rate', 0)
    ai_premium_rate = creative_data.get('ai_premium_style_rate', 0)

    if ai_lifestyle_rate > 0 or ai_premium_rate > 0:  # AI analysis available
        if ai_lifestyle_rate > 70:
            framework.add_signal(
                insight="Heavy lifestyle positioning detected - consider balancing with functional benefits for broader appeal",
                value=ai_lifestyle_rate,
                confidence=0.7,
                business_impact=0.6,
                actionability=0.7,
                source_module="Creative Intelligence",
                metric_name="lifestyle_positioning_heavy",
                metadata={'metric': 'ai_lifestyle_dominance', 'percentage': ai_lifestyle_rate}
            )
        elif ai_premium_rate > 60:
            framework.add_signal(
                insight="Strong premium positioning - ensure value communication resonates with target demographic",
                value=ai_premium_rate,
                confidence=0.7,
                business_impact=0.7,
                actionability=0.8,
                source_module="Creative Intelligence",
                metric_name="premium_positioning_strong",
                metadata={'metric': 'ai_premium_positioning', 'percentage': ai_premium_rate}
            )
        elif ai_lifestyle_rate + ai_premium_rate < 20:
            framework.add_signal(
                insight="Limited lifestyle/premium positioning - opportunity to enhance brand appeal through emotional storytelling",
                value=round(ai_lifestyle_rate + ai_premium_rate, 2),
                confidence=0.8,
                business_impact=0.8,
                actionability=0.9,
                source_module="Creative Intelligence",
                metric_name="emotional_positioning_rate_pct",
                metadata={'metric': 'ai_positioning_gap', 'total_rate': ai_lifestyle_rate + ai_premium_rate, 'scale': 'percentage'}
            )
    
    # Creative Density Signal
    density_score = creative_data.get('avg_creative_density', 0)
    if density_score < 10.0:  # Low word density indicates sparse content
        framework.add_signal(
            insight="Content density optimization opportunity - enrich creative messaging",
            value=density_score,
            confidence=0.5,
            business_impact=0.5,
            actionability=0.6,
            source_module="Creative Intelligence",
            metric_name="content_density_low",
            metadata={'metric': 'creative_density', 'interpretation': 'words_per_100_chars'}
        )


def create_channel_intelligence_signals(framework: ProgressiveDisclosureFramework, channel_data: Dict) -> None:
    """Create Channel Intelligence signals from P2 enhanced data"""

    # Platform Diversification Signal with Phase 5 Temporal Enhancement
    diversification_score = channel_data.get('avg_platform_diversification', 0)
    if diversification_score < 1.5:
        base_insight = "Platform diversification opportunity - expand cross-platform presence to reduce risk"
        temporal_metadata = {
            'temporal_trend': 'stable',
            'timeframe': '8 weeks',
            'metric': 'platform_diversification',
            'max_score': 3.0
        }
        enhanced_insight = framework.add_temporal_context(
            base_insight, diversification_score / 3.0, 'platform_diversification', temporal_metadata
        )
        framework.add_signal(
            insight=enhanced_insight,
            value=diversification_score,
            confidence=0.8,
            business_impact=0.7,
            actionability=0.8,
            source_module="Channel Intelligence",
            metric_name="platform_diversification_low",
            metadata=temporal_metadata
        )
    
    # Cross-Platform Synergy Signal with Phase 5 Temporal Enhancement
    synergy_rate = channel_data.get('cross_platform_synergy_rate', 0)
    if synergy_rate < 30.0:
        base_insight = "Low cross-platform synergy detected - integrate campaigns across Facebook, Instagram, and Messenger"
        temporal_metadata = {
            'temporal_trend': 'volatile',
            'timeframe': '5 weeks',
            'metric': 'cross_platform_synergy_rate'
        }
        enhanced_insight = framework.add_temporal_context(
            base_insight, synergy_rate / 100, 'cross_platform_synergy_rate', temporal_metadata
        )
        framework.add_signal(
            insight=enhanced_insight,
            value=synergy_rate,
            confidence=0.7,
            business_impact=0.8,
            actionability=0.9,
            source_module="Channel Intelligence",
            metric_name="cross_platform_synergy_low",
            metadata={'metric': 'cross_platform_synergy', 'unit': 'percentage', 'target': 60.0}
        )
    
    # Platform Content Optimization Signal
    optimization_rate = channel_data.get('platform_optimization_rate', 0)
    if optimization_rate < 50.0:
        framework.add_signal(
            insight="Platform-specific content optimization needed - tailor messaging for each channel's audience",
            value=optimization_rate,
            confidence=0.6,
            business_impact=0.6,
            actionability=0.7,
            source_module="Channel Intelligence",
            metric_name="platform_optimization_needed",
            metadata={'metric': 'platform_optimization', 'instagram_optimal': '50-150 chars', 'facebook_optimal': '100+ chars'}
        )
    
    # Channel Concentration Risk Signal
    platform_concentration = channel_data.get('platform_concentration', 'BALANCED')
    if platform_concentration == 'CONCENTRATED':
        framework.add_signal(
            insight="High platform concentration risk - diversify to reduce dependency on single channel",
            value=platform_concentration,
            confidence=0.8,
            business_impact=0.9,  # High business risk
            actionability=0.7,
            source_module="Channel Intelligence",
            metric_name="channel_concentration_risk",
            metadata={'metric': 'concentration_risk', 'risk_type': 'policy_changes', 'mitigation': 'diversification'}
        )

def create_audience_intelligence_signals(framework: ProgressiveDisclosureFramework, audience_data: Dict) -> None:
    """Create Audience Intelligence signals from platform and psychographic analysis"""

    # Cross-Platform Strategy Signal
    cross_platform_rate = audience_data.get('avg_cross_platform_rate', 0)
    if cross_platform_rate < 30.0:
        framework.add_signal(
            insight="Low cross-platform presence - expand beyond single channel to reach broader audiences",
            value=cross_platform_rate,
            confidence=0.8,
            business_impact=0.7,
            actionability=0.8,
            source_module="Audience Intelligence",
            metric_name="audience_cross_platform_low",
            metadata={'metric': 'cross_platform_rate', 'unit': 'percentage', 'target': 50.0}
        )

    # Communication Style Optimization Signal
    avg_text_length = audience_data.get('avg_text_length', 0)
    if avg_text_length < 50:
        framework.add_signal(
            insight="Very short messaging detected - consider more detailed communication for audience engagement",
            value=avg_text_length,
            confidence=0.7,
            business_impact=0.6,
            actionability=0.8,
            source_module="Audience Intelligence",
            metric_name="communication_length_short",
            metadata={'metric': 'avg_text_length', 'unit': 'characters', 'optimal_range': '75-150'}
        )
    elif avg_text_length > 200:
        framework.add_signal(
            insight="Very long messaging detected - consider more concise communication for better engagement",
            value=avg_text_length,
            confidence=0.7,
            business_impact=0.6,
            actionability=0.8,
            source_module="Audience Intelligence",
            metric_name="communication_length_long",
            metadata={'metric': 'avg_text_length', 'unit': 'characters', 'optimal_range': '75-150'}
        )

    # Price Consciousness Strategy Signal
    price_conscious_rate = audience_data.get('avg_price_conscious_rate', 0)
    if price_conscious_rate > 40.0:
        framework.add_signal(
            insight="High price-conscious audience detected - emphasize value propositions and competitive pricing",
            value=price_conscious_rate,
            confidence=0.8,
            business_impact=0.8,
            actionability=0.9,
            source_module="Audience Intelligence",
            metric_name="price_consciousness_high",
            metadata={'metric': 'price_conscious_rate', 'unit': 'percentage', 'strategy': 'value_messaging'}
        )

    # Millennial Focus Strategy Signal
    millennial_focus_rate = audience_data.get('avg_millennial_focus_rate', 0)
    if millennial_focus_rate > 60.0:
        framework.add_signal(
            insight="Strong millennial focus detected - tailor messaging for digital-native preferences and lifestyle",
            value=millennial_focus_rate,
            confidence=0.8,
            business_impact=0.7,
            actionability=0.8,
            source_module="Audience Intelligence",
            metric_name="millennial_focus_strong",
            metadata={'metric': 'millennial_focus_rate', 'unit': 'percentage', 'strategy': 'digital_lifestyle_messaging'}
        )

    # Platform Strategy Diversification Signal
    platform_strategy = audience_data.get('most_common_platform_strategy', 'UNKNOWN')
    if platform_strategy == 'INSTAGRAM_ONLY' or platform_strategy == 'FACEBOOK_ONLY':
        framework.add_signal(
            insight=f"Single-platform dependency detected ({platform_strategy}) - diversify to reduce channel risk",
            value=platform_strategy,
            confidence=0.8,
            business_impact=0.9,
            actionability=0.7,
            source_module="Audience Intelligence",
            metric_name="platform_dependency_risk",
            metadata={'metric': 'platform_strategy', 'current': platform_strategy, 'recommendation': 'CROSS_PLATFORM'}
        )


def create_visual_intelligence_signals(framework: ProgressiveDisclosureFramework, visual_data: Dict) -> None:
    """Create Visual Intelligence signals from enhanced multimodal analysis"""

    # Visual-Text Alignment Signal
    avg_alignment = visual_data.get('avg_visual_text_alignment', 0)
    if avg_alignment < 0.6:
        framework.add_signal(
            insight="Visual-text misalignment detected - ensure images and copy work together cohesively",
            value=avg_alignment,
            confidence=0.8,
            business_impact=0.8,
            actionability=0.9,
            source_module="Visual Intelligence",
            metric_name="visual_text_alignment_low",
            metadata={'metric': 'visual_text_alignment', 'threshold': 0.6, 'optimization': 'multimodal_consistency'}
        )
    elif avg_alignment > 0.85:
        framework.add_signal(
            insight="Excellent visual-text alignment achieved - maintain this multimodal consistency",
            value=avg_alignment,
            confidence=0.9,
            business_impact=0.6,
            actionability=0.5,
            source_module="Visual Intelligence",
            metric_name="visual_text_alignment_high",
            metadata={'metric': 'visual_text_alignment', 'status': 'strength'}
        )

    # Brand Consistency Signal
    avg_brand_consistency = visual_data.get('avg_brand_consistency', 0)
    if avg_brand_consistency < 0.7:
        framework.add_signal(
            insight="Visual brand inconsistency identified - standardize brand visual elements across campaigns",
            value=avg_brand_consistency,
            confidence=0.8,
            business_impact=0.9,  # Brand consistency is critical
            actionability=0.8,
            source_module="Visual Intelligence",
            metric_name="brand_consistency_low",
            metadata={'metric': 'brand_consistency', 'impact': 'brand_recognition', 'threshold': 0.7}
        )

    # Competitive Positioning Matrix Signal
    luxury_position = visual_data.get('avg_luxury_positioning', 0.5)
    boldness_score = visual_data.get('avg_boldness', 0.5)

    # Determine positioning quadrant
    if luxury_position > 0.7 and boldness_score > 0.7:
        positioning_quadrant = "Luxury-Bold"
        positioning_insight = "Strong luxury-bold positioning - premium market leadership opportunity"
        confidence = 0.8
        business_impact = 0.8
    elif luxury_position > 0.7 and boldness_score < 0.4:
        positioning_quadrant = "Luxury-Subtle"
        positioning_insight = "Luxury-subtle positioning - consider bolder visuals to increase market presence"
        confidence = 0.7
        business_impact = 0.6
    elif luxury_position < 0.4 and boldness_score > 0.7:
        positioning_quadrant = "Accessible-Bold"
        positioning_insight = "Accessible-bold positioning - strong mass market appeal with distinctive visuals"
        confidence = 0.8
        business_impact = 0.7
    else:
        positioning_quadrant = "Mid-Balanced"
        positioning_insight = "Balanced positioning - opportunity to differentiate through stronger visual identity"
        confidence = 0.6
        business_impact = 0.7

    framework.add_signal(
        insight=positioning_insight,
        value={'quadrant': positioning_quadrant, 'luxury': luxury_position, 'boldness': boldness_score},
        confidence=confidence,
        business_impact=business_impact,
        actionability=0.7,
        source_module="Visual Intelligence",
        metric_name="competitive_positioning_matrix",
        metadata={'metric': 'competitive_positioning', 'quadrant': positioning_quadrant, 'matrix': '2D_luxury_boldness'}
    )

    # Creative Fatigue Detection Signal
    avg_pattern_risk = visual_data.get('avg_creative_pattern_risk', 0)
    if avg_pattern_risk > 0.7:
        framework.add_signal(
            insight="High creative pattern fatigue risk - refresh visual approach to avoid stale creatives",
            value=avg_pattern_risk,
            confidence=0.7,
            business_impact=0.7,
            actionability=0.8,
            source_module="Visual Intelligence",
            metric_name="creative_fatigue_risk",
            metadata={'metric': 'creative_fatigue', 'risk_level': 'high', 'action': 'visual_refresh'}
        )

    # Visual Differentiation Signal
    avg_differentiation = visual_data.get('avg_visual_differentiation', 0)
    if avg_differentiation < 0.5:
        framework.add_signal(
            insight="Low visual differentiation detected - increase unique visual elements to stand out from competitors",
            value=avg_differentiation,
            confidence=0.7,
            business_impact=0.8,
            actionability=0.8,
            source_module="Visual Intelligence",
            metric_name="visual_differentiation_low",
            metadata={'metric': 'visual_differentiation', 'competitive_risk': 'commoditization', 'threshold': 0.5}
        )
    elif avg_differentiation > 0.8:
        framework.add_signal(
            insight="Strong visual differentiation achieved - maintain unique visual identity as competitive advantage",
            value=avg_differentiation,
            confidence=0.8,
            business_impact=0.6,
            actionability=0.5,
            source_module="Visual Intelligence",
            metric_name="visual_differentiation_high",
            metadata={'metric': 'visual_differentiation', 'status': 'competitive_advantage'}
        )

    # Target Demographic Alignment Signal
    dominant_demographic = visual_data.get('dominant_target_demographic', '')
    demographic_confidence = visual_data.get('demographic_targeting_confidence', 0)

    if demographic_confidence > 0.8:
        framework.add_signal(
            insight=f"Clear visual targeting for {dominant_demographic} demographic - optimize campaigns for this audience",
            value={'demographic': dominant_demographic, 'confidence': demographic_confidence},
            confidence=0.8,
            business_impact=0.7,
            actionability=0.8,
            source_module="Visual Intelligence",
            metric_name="demographic_targeting_clear",
            metadata={'metric': 'demographic_targeting', 'primary_audience': dominant_demographic, 'strategy': 'audience_optimization'}
        )
    elif demographic_confidence < 0.5:
        framework.add_signal(
            insight="Unclear visual demographic targeting - define target audience and align visual language accordingly",
            value={'demographic': dominant_demographic or 'undefined', 'confidence': demographic_confidence},
            confidence=0.7,
            business_impact=0.8,
            actionability=0.9,
            source_module="Visual Intelligence",
            metric_name="demographic_targeting_unclear",
            metadata={'metric': 'demographic_targeting', 'issue': 'unclear_targeting', 'action_needed': 'audience_definition'}
        )