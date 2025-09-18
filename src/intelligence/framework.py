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
        metadata: Optional[Dict] = None
    ) -> IntelligenceSignal:
        """Add a new intelligence signal with automatic classification"""
        
        signal = IntelligenceSignal(
            insight=insight,
            value=value,
            confidence=confidence,
            business_impact=business_impact,
            actionability=actionability,
            source_module=source_module,
            signal_strength=self._classify_signal_strength(confidence, business_impact, actionability),
            recommended_levels=self._determine_disclosure_levels(confidence, business_impact, actionability),
            metadata=metadata or {}
        )
        
        self.signals.append(signal)
        return signal
    
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
                f"{s.source_module}_{i}": s.value 
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
            -- Creative Intelligence Dashboard
            WITH creative_metrics AS (
              SELECT 
                brand,
                COUNT(*) as total_ads,
                AVG(LENGTH(COALESCE(creative_text, ''))) as avg_creative_length,
                COUNT(DISTINCT REGEXP_EXTRACT(creative_text, r'\\b(\\w+)\\b')) as unique_words,
                SUM(CASE WHEN creative_text LIKE '%!%' THEN 1 ELSE 0 END) as exclamation_ads,
                AVG(CASE WHEN LENGTH(creative_text) > 100 THEN 1.0 ELSE 0.0 END) as long_form_rate,
                -- P2 Enhancement metrics
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
              GROUP BY brand
            )
            SELECT 
              brand,
              total_ads,
              ROUND(avg_creative_length, 1) as avg_creative_length,
              unique_words,
              exclamation_ads,
              ROUND(long_form_rate * 100, 1) as long_form_pct,
              ROUND(text_length_score, 2) as text_optimization_score,
              ROUND(brand_mention_density, 3) as brand_mention_density
            FROM creative_metrics
            ORDER BY total_ads DESC
            """
        elif "Channel" in module:
            return f"""
            -- Channel Intelligence Dashboard
            WITH channel_metrics AS (
              SELECT
                brand,
                publisher_platforms,
                COUNT(*) as ad_count,
                COUNT(DISTINCT ad_archive_id) as unique_campaigns,
                AVG(CASE WHEN publisher_platforms LIKE '%,%' THEN 1.0 ELSE 0.0 END) as cross_platform_rate,
                -- P2 Enhancement metrics
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
              GROUP BY brand, publisher_platforms
            )
            SELECT
              brand,
              publisher_platforms,
              ad_count,
              unique_campaigns,
              ROUND(cross_platform_rate * 100, 1) as cross_platform_pct,
              ROUND(platform_diversification_score, 1) as diversification_score
            FROM channel_metrics
            ORDER BY ad_count DESC
            """
        elif "Visual" in module:
            return f"""
            -- Visual Intelligence Dashboard
            WITH visual_metrics AS (
              SELECT
                brand,
                COUNT(*) as total_visual_ads,
                AVG(visual_text_alignment_score) as avg_visual_alignment,
                AVG(brand_consistency_score) as avg_brand_consistency,
                AVG(luxury_positioning_score) as avg_luxury_position,
                AVG(boldness_score) as avg_boldness,
                AVG(visual_differentiation_level) as avg_differentiation,
                AVG(creative_pattern_risk) as avg_pattern_risk,
                STRING_AGG(DISTINCT target_demographic, ', ') as target_demographics,
                -- Positioning quadrant classification
                CASE
                  WHEN AVG(luxury_positioning_score) > 0.7 AND AVG(boldness_score) > 0.7 THEN 'Luxury-Bold'
                  WHEN AVG(luxury_positioning_score) > 0.7 AND AVG(boldness_score) < 0.4 THEN 'Luxury-Subtle'
                  WHEN AVG(luxury_positioning_score) < 0.4 AND AVG(boldness_score) > 0.7 THEN 'Accessible-Bold'
                  ELSE 'Mid-Balanced'
                END as positioning_quadrant,
                -- Visual health score
                (AVG(visual_text_alignment_score) * 0.3 +
                 AVG(brand_consistency_score) * 0.3 +
                 AVG(visual_differentiation_level) * 0.2 +
                 (1 - AVG(creative_pattern_risk)) * 0.2) as visual_health_score
              FROM (
                SELECT * FROM {base_table.replace('.ads_with_dates', '.visual_intelligence_*')}
                WHERE luxury_positioning_score IS NOT NULL
              )
              GROUP BY brand
            )
            SELECT
              brand,
              total_visual_ads,
              ROUND(avg_visual_alignment, 3) as visual_alignment_score,
              ROUND(avg_brand_consistency, 3) as brand_consistency_score,
              ROUND(avg_luxury_position, 3) as luxury_positioning,
              ROUND(avg_boldness, 3) as boldness_score,
              ROUND(avg_differentiation, 3) as differentiation_level,
              ROUND(avg_pattern_risk, 3) as fatigue_risk,
              target_demographics,
              positioning_quadrant,
              ROUND(visual_health_score, 3) as overall_visual_health
            FROM visual_metrics
            ORDER BY total_visual_ads DESC
            """
        else:
            return f"""
            -- General Intelligence Dashboard
            WITH general_metrics AS (
              SELECT
                brand,
                COUNT(*) as total_signals,
                COUNT(DISTINCT DATE(start_timestamp)) as active_days,
                MIN(start_timestamp) as first_seen,
                MAX(start_timestamp) as last_seen,
                AVG(LENGTH(COALESCE(creative_text, ''))) as avg_content_length,
                COUNT(DISTINCT publisher_platforms) as platform_count
              FROM {base_table}
              GROUP BY brand
            )
            SELECT
              brand,
              total_signals,
              active_days,
              first_seen,
              last_seen,
              ROUND(avg_content_length, 1) as avg_content_length,
              platform_count,
              ROUND(total_signals / GREATEST(active_days, 1), 2) as signals_per_day
            FROM general_metrics
            ORDER BY total_signals DESC
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


def create_creative_intelligence_signals(framework: ProgressiveDisclosureFramework, creative_data: Dict) -> None:
    """Create Creative Intelligence signals from P2 enhanced data"""
    
    # Text Length Analysis Signal
    avg_length = creative_data.get('avg_text_length', 0)
    if avg_length < 30:
        framework.add_signal(
            insight="Creative text length optimization needed - content is too brief for engagement",
            value=avg_length,
            confidence=0.8,
            business_impact=0.7,
            actionability=0.9,
            source_module="Creative Intelligence",
            metadata={'metric': 'text_length', 'threshold': 30}
        )
    elif avg_length > 200:
        framework.add_signal(
            insight="Creative text may be too long - consider brevity for better performance",
            value=avg_length,
            confidence=0.7,
            business_impact=0.6,
            actionability=0.8,
            source_module="Creative Intelligence",
            metadata={'metric': 'text_length', 'threshold': 200}
        )
    
    # Brand Mention Frequency Signal
    brand_mentions = creative_data.get('avg_brand_mentions', 0)
    if brand_mentions < 0.5:
        framework.add_signal(
            insight="Low brand mention frequency detected - increase brand presence in creative content",
            value=brand_mentions,
            confidence=0.7,
            business_impact=0.8,
            actionability=0.8,
            source_module="Creative Intelligence",
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
                value=ai_emotional_intensity,
                confidence=0.8,
                business_impact=0.7,
                actionability=0.8,
                source_module="Creative Intelligence",
                metadata={'metric': 'ai_emotional_intensity', 'scale': '0-10', 'analysis_type': 'ai_generated'}
            )
        elif ai_emotional_intensity > 8.0:
            framework.add_signal(
                insight="Very high emotional intensity detected - consider balanced approach for broader audience appeal",
                value=ai_emotional_intensity,
                confidence=0.7,
                business_impact=0.6,
                actionability=0.7,
                source_module="Creative Intelligence",
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
                metadata={'metric': 'emotional_keywords', 'examples': ['amazing', 'perfect', 'love'], 'analysis_type': 'regex_based'}
            )

    # AI Industry Relevance Signal (0-1 scale)
    if ai_industry_relevance > 0 and ai_industry_relevance < 0.4:
        framework.add_signal(
            insight="Low eyewear industry relevance in emotional messaging - tailor content for eyewear context",
            value=ai_industry_relevance,
            confidence=0.8,
            business_impact=0.8,
            actionability=0.9,
            source_module="Creative Intelligence",
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
                value=total_positive_aspirational,
                confidence=0.8,
                business_impact=0.7,
                actionability=0.8,
                source_module="Creative Intelligence",
                metadata={'metric': 'ai_sentiment_balance', 'positive_rate': ai_positive_rate, 'aspirational_rate': ai_aspirational_rate}
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
                metadata={'metric': 'ai_premium_positioning', 'percentage': ai_premium_rate}
            )
        elif ai_lifestyle_rate + ai_premium_rate < 20:
            framework.add_signal(
                insight="Limited lifestyle/premium positioning - opportunity to enhance brand appeal through emotional storytelling",
                value=ai_lifestyle_rate + ai_premium_rate,
                confidence=0.8,
                business_impact=0.8,
                actionability=0.9,
                source_module="Creative Intelligence",
                metadata={'metric': 'ai_positioning_gap', 'total_rate': ai_lifestyle_rate + ai_premium_rate}
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
            metadata={'metric': 'creative_density', 'interpretation': 'words_per_100_chars'}
        )


def create_channel_intelligence_signals(framework: ProgressiveDisclosureFramework, channel_data: Dict) -> None:
    """Create Channel Intelligence signals from P2 enhanced data"""
    
    # Platform Diversification Signal
    diversification_score = channel_data.get('avg_platform_diversification', 0)
    if diversification_score < 1.5:
        framework.add_signal(
            insight="Platform diversification opportunity - expand cross-platform presence to reduce risk",
            value=diversification_score,
            confidence=0.8,
            business_impact=0.7,
            actionability=0.8,
            source_module="Channel Intelligence",
            metadata={'metric': 'platform_diversification', 'max_score': 3.0}
        )
    
    # Cross-Platform Synergy Signal
    synergy_rate = channel_data.get('cross_platform_synergy_rate', 0)
    if synergy_rate < 30.0:
        framework.add_signal(
            insight="Low cross-platform synergy detected - integrate campaigns across Facebook, Instagram, and Messenger",
            value=synergy_rate,
            confidence=0.7,
            business_impact=0.8,
            actionability=0.9,
            source_module="Channel Intelligence",
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
            metadata={'metric': 'demographic_targeting', 'issue': 'unclear_targeting', 'action_needed': 'audience_definition'}
        )