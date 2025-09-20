#!/usr/bin/env python3
"""
Quick test to verify Phase 5 temporal enhancement is working correctly
"""

import sys
sys.path.append('src')

from intelligence.framework import ProgressiveDisclosureFramework

def test_competitive_temporal_enhancement():
    """Test the temporal enhancement specifically for competitive intelligence"""

    framework = ProgressiveDisclosureFramework()

    # Test the exact scenario from our pipeline
    base_insight = "Competitive copying detected from EyeBuyDirect (similarity: 72.3%)"
    similarity_score = 0.723

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

    print("="*80)
    print("PHASE 5 TEMPORAL ENHANCEMENT TEST")
    print("="*80)
    print(f"Base insight:     {base_insight}")
    print(f"Enhanced insight: {enhanced_insight}")
    print("="*80)

    # Check if temporal enhancement was applied
    if enhanced_insight != base_insight:
        print("✅ SUCCESS: Temporal enhancement was applied!")
        if "threat accelerating" in enhanced_insight:
            print("✅ SUCCESS: Competitive semantic correction working correctly!")
        else:
            print("❌ WARNING: Expected 'threat accelerating' in enhancement")
    else:
        print("❌ FAILURE: No temporal enhancement applied")

    return enhanced_insight

if __name__ == "__main__":
    test_competitive_temporal_enhancement()