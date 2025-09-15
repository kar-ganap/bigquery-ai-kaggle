"""
Intelligence Framework Module

Systematic L1→L4 progressive disclosure framework for organizing competitive intelligence outputs.
"""

from .framework import (
    ProgressiveDisclosureFramework,
    IntelligenceSignal,
    IntelligenceThresholds,
    SignalStrength,
    IntelligenceLevel,
    create_creative_intelligence_signals,
    create_channel_intelligence_signals
)

__all__ = [
    'ProgressiveDisclosureFramework',
    'IntelligenceSignal', 
    'IntelligenceThresholds',
    'SignalStrength',
    'IntelligenceLevel',
    'create_creative_intelligence_signals',
    'create_channel_intelligence_signals'
]