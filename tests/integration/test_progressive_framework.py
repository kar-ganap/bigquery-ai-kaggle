#!/usr/bin/env python3
"""
Test and Demonstration Script for L1→L4 Progressive Disclosure Framework

Shows how the systematic intelligence framework works with real data simulation.
"""

import sys
import os
sys.path.append('src')

from src.intelligence.framework import (
    ProgressiveDisclosureFramework,
    create_creative_intelligence_signals,
    create_channel_intelligence_signals
)


def simulate_creative_intelligence_data():
    """Simulate P2 Creative Intelligence data"""
    return {
        'avg_text_length': 25,  # Low - will trigger signal
        'avg_brand_mentions': 0.3,  # Low - will trigger signal 
        'avg_emotional_keywords': 0.5,  # Low - will trigger signal
        'avg_creative_density': 8.5  # Low - will trigger signal
    }


def simulate_channel_intelligence_data():
    """Simulate P2 Channel Intelligence data"""
    return {
        'avg_platform_diversification': 1.2,  # Low - will trigger signal
        'cross_platform_synergy_rate': 25.0,  # Low - will trigger signal
        'platform_optimization_rate': 45.0,  # Low - will trigger signal
        'platform_concentration': 'CONCENTRATED'  # Risk - will trigger signal
    }


def add_manual_intelligence_signals(framework):
    """Add some manual intelligence signals for demonstration"""
    
    # High-impact competitive threat
    framework.add_signal(
        insight="Competitor launched aggressive price war - immediate response needed",
        value=0.85,
        confidence=0.9,
        business_impact=0.95,
        actionability=0.8,
        source_module="Competitive Intelligence",
        metadata={'threat_type': 'pricing', 'competitor': 'LensCrafters'}
    )
    
    # Medium-impact market opportunity
    framework.add_signal(
        insight="Instagram engagement rates 40% higher than Facebook - shift budget allocation",
        value=0.4,
        confidence=0.7,
        business_impact=0.6,
        actionability=0.8,
        source_module="Social Media Intelligence",
        metadata={'platform': 'Instagram', 'engagement_lift': 0.4}
    )
    
    # Low-confidence speculative insight
    framework.add_signal(
        insight="Potential trend toward sustainable eyewear materials detected",
        value="sustainability_trend",
        confidence=0.3,
        business_impact=0.4,
        actionability=0.2,
        source_module="Trend Analysis",
        metadata={'trend_type': 'sustainability', 'evidence': 'weak'}
    )
    
    # High-confidence but low-actionability insight
    framework.add_signal(
        insight="Brand awareness in 18-24 demographic shows declining trend",
        value=0.12,
        confidence=0.8,
        business_impact=0.7,
        actionability=0.3,
        source_module="Brand Health Intelligence",
        metadata={'demographic': '18-24', 'trend': 'declining'}
    )


def main():
    """Test the Progressive Disclosure Framework"""
    
    print("=" * 80)
    print("🎯 L1→L4 PROGRESSIVE DISCLOSURE FRAMEWORK DEMONSTRATION")
    print("=" * 80)
    print("\n🔬 Testing systematic intelligence filtering and organization...\n")
    
    # Initialize framework
    framework = ProgressiveDisclosureFramework()
    
    # Add Creative Intelligence signals (P2 enhancements)
    print("🎨 Adding Creative Intelligence P2 signals...")
    creative_data = simulate_creative_intelligence_data()
    create_creative_intelligence_signals(framework, creative_data)
    print(f"   Added {len([s for s in framework.signals if 'Creative' in s.source_module])} Creative Intelligence signals")
    
    # Add Channel Intelligence signals (P2 enhancements)
    print("📺 Adding Channel Intelligence P2 signals...")
    channel_data = simulate_channel_intelligence_data()
    create_channel_intelligence_signals(framework, channel_data)
    print(f"   Added {len([s for s in framework.signals if 'Channel' in s.source_module])} Channel Intelligence signals")
    
    # Add manual intelligence signals
    print("🧠 Adding manual intelligence signals...")
    add_manual_intelligence_signals(framework)
    print(f"   Added {len([s for s in framework.signals if s.source_module not in ['Creative Intelligence', 'Channel Intelligence']])} manual signals")
    
    # Show framework statistics
    stats = framework.get_framework_stats()
    print(f"\n📊 FRAMEWORK STATISTICS:")
    print(f"   Total Signals: {stats['total_signals']}")
    print(f"   Framework Efficiency: {stats['framework_efficiency']:.1%}")
    print(f"   Average Confidence: {stats['avg_confidence']:.2f}")
    print(f"   Average Business Impact: {stats['avg_business_impact']:.2f}")
    
    print(f"\n   Signal Distribution by Strength:")
    for strength, count in stats['by_strength'].items():
        print(f"   {strength}: {count} signals")
    
    print(f"\n   Signal Distribution by Level:")
    for level, count in stats['by_level'].items():
        print(f"   {level}: {count} signals")
    
    # Generate L1→L4 outputs
    print("\n" + "=" * 80)
    print("🎯 GENERATING L1→L4 PROGRESSIVE DISCLOSURE")
    print("=" * 80)
    
    # L1 Executive Summary
    print("\n📋 L1: EXECUTIVE SUMMARY")
    print("-" * 50)
    l1_output = framework.generate_level_1_executive()
    print(f"Threat Level: {l1_output['threat_level']}")
    print(f"Confidence Score: {l1_output['confidence_score']:.2f}")
    print(f"Critical Insights ({l1_output['signal_count']} of {l1_output['signal_count'] + l1_output['filtered_signals']}):")
    for i, insight in enumerate(l1_output['executive_insights'], 1):
        print(f"  {i}. {insight}")
    if l1_output['filtered_signals'] > 0:
        print(f"📋 Filtered {l1_output['filtered_signals']} lower-priority signals")
    
    # L2 Strategic Dashboard
    print("\n📈 L2: STRATEGIC DASHBOARD")
    print("-" * 50)
    l2_output = framework.generate_level_2_strategic()
    print(f"Active Intelligence Modules: {l2_output['modules_active']}")
    print(f"Total Strategic Signals: {l2_output['total_signals']}")
    print(f"Strategic Intelligence by Module:")
    for module, data in l2_output['strategic_intelligence'].items():
        print(f"  📊 {module}: {data['signal_count']} signals (confidence: {data['confidence_avg']:.2f})")
        for insight in data['insights'][:2]:  # Show top 2 insights per module
            print(f"      • {insight}")
    
    if l2_output['cross_module_patterns']:
        print(f"Cross-Module Patterns:")
        for pattern in l2_output['cross_module_patterns']:
            print(f"  🔍 {pattern}")
    
    # L3 Actionable Interventions
    print("\n🎮 L3: ACTIONABLE INTERVENTIONS")
    print("-" * 50)
    l3_output = framework.generate_level_3_interventions()
    summary = l3_output['intervention_summary']
    print(f"Total Interventions: {summary['total_interventions']}")
    print(f"Average Actionability: {summary['avg_actionability']:.2f}")
    
    if l3_output['immediate_actions']:
        print(f"\n🚨 IMMEDIATE ACTIONS ({summary['immediate_count']}):")
        for action in l3_output['immediate_actions'][:3]:  # Show top 3
            print(f"  ⚡ {action['action']}")
            print(f"     Timeline: {action['timeline']} | Confidence: {action['confidence']:.2f}")
    
    if l3_output['short_term_tactics']:
        print(f"\n📅 SHORT-TERM TACTICS ({summary['short_term_count']}):")
        for action in l3_output['short_term_tactics'][:3]:  # Show top 3
            print(f"  📈 {action['action']}")
            print(f"     Timeline: {action['timeline']} | Actionability: {action['actionability']:.2f}")
    
    if l3_output['monitoring_actions']:
        print(f"\n👁️ MONITORING ACTIONS ({summary['monitoring_count']}):")
        for action in l3_output['monitoring_actions'][:2]:  # Show top 2
            print(f"  🔍 {action['action']}")
    
    # L4 SQL Dashboards
    print("\n📋 L4: SQL DASHBOARDS")
    print("-" * 50)
    l4_output = framework.generate_level_4_dashboards("bigquery-ai-kaggle-469620", "ads_demo")
    print(f"Project: {l4_output['bigquery_project']}")
    print(f"Dataset: {l4_output['bigquery_dataset']}")
    print(f"Dashboard Queries: {len(l4_output['dashboard_queries'])}")
    print(f"Total Signals: {l4_output['total_signals']}")
    print(f"Filtered Noise: {l4_output['filtered_noise_count']} signals")
    
    print(f"\nAvailable Dashboards:")
    for dashboard_name in l4_output['dashboard_queries'].keys():
        print(f"  📊 {dashboard_name.replace('_', ' ').title()}")
    
    print(f"\nModule Performance:")
    for module, metrics in l4_output['module_performance_metrics'].items():
        print(f"  📈 {module}: {metrics['signal_count']} signals (confidence: {metrics['avg_confidence']:.2f})")
    
    print("\n" + "=" * 80)
    print("✅ L1→L4 PROGRESSIVE DISCLOSURE FRAMEWORK DEMONSTRATION COMPLETE")
    print("=" * 80)
    
    # Show key benefits
    print(f"\n🎯 KEY FRAMEWORK BENEFITS:")
    print(f"  • Intelligent Filtering: {stats['framework_efficiency']:.1%} of signals above noise threshold")
    print(f"  • Executive Focus: Only {l1_output['signal_count']} critical insights for L1")
    print(f"  • Strategic Organization: {l2_output['modules_active']} intelligence modules systematically organized")
    print(f"  • Actionable Prioritization: {summary['immediate_count']} immediate + {summary['short_term_count']} short-term actions")
    print(f"  • Full Analytical Detail: {len(l4_output['dashboard_queries'])} SQL dashboards for analysts")
    
    print(f"\n💡 INTELLIGENCE TRANSFORMATION:")
    print(f"   Before: {stats['total_signals']} mixed signals → Information overload")  
    print(f"   After: Systematic L1→L4 hierarchy → Actionable intelligence")


if __name__ == "__main__":
    main()