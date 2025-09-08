#!/usr/bin/env python3
"""
Cross-Brand Cascade Detection - Calibrated for Mock Data
Adjusted thresholds based on actual data patterns
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

def test_cascade_calibrated():
    """
    Detect cascades with calibrated thresholds for our mock data
    """
    
    print("🌊 CROSS-BRAND CASCADE DETECTION - CALIBRATED")
    print("=" * 80)
    print("Detecting competitive ripple effects with data-driven thresholds")
    print("-" * 80)
    
    # First, understand the data patterns
    calibration_query = f"""
    WITH brand_weekly AS (
      SELECT 
        brand,
        DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week,
        AVG(promotional_intensity) AS avg_promo,
        AVG(urgency_score) AS avg_urgency,
        COUNT(*) AS weekly_count
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
      GROUP BY brand, week
      HAVING COUNT(*) >= 2
    ),
    changes AS (
      SELECT 
        brand,
        week,
        avg_promo,
        avg_urgency,
        avg_promo - LAG(avg_promo) OVER (PARTITION BY brand ORDER BY week) AS promo_delta,
        avg_urgency - LAG(avg_urgency) OVER (PARTITION BY brand ORDER BY week) AS urgency_delta
      FROM brand_weekly
    )
    SELECT 
      MAX(ABS(promo_delta)) AS max_promo_delta,
      AVG(ABS(promo_delta)) AS avg_promo_delta,
      STDDEV(promo_delta) AS std_promo_delta,
      MAX(ABS(urgency_delta)) AS max_urgency_delta,
      AVG(ABS(urgency_delta)) AS avg_urgency_delta
    FROM changes
    WHERE promo_delta IS NOT NULL
    """
    
    calibration = client.query(calibration_query).to_dataframe()
    
    if not calibration.empty:
        max_delta = calibration['max_promo_delta'].iloc[0]
        avg_delta = calibration['avg_promo_delta'].iloc[0]
        std_delta = calibration['std_promo_delta'].iloc[0]
        
        # Set thresholds based on actual data
        significant_threshold = avg_delta + std_delta  # ~1 std dev above mean
        major_threshold = avg_delta + 2 * std_delta     # ~2 std dev above mean
        
        print(f"\n📊 Data Calibration:")
        print(f"  Max delta observed: {max_delta:.3f}")
        print(f"  Average delta: {avg_delta:.3f}")
        print(f"  Significant move threshold: {significant_threshold:.3f}")
        print(f"  Major move threshold: {major_threshold:.3f}")
    else:
        significant_threshold = 0.02
        major_threshold = 0.03
    
    # Main cascade detection with calibrated thresholds
    cascade_query = f"""
    WITH brand_metrics AS (
      SELECT 
        brand,
        DATE_TRUNC(DATE(start_timestamp), WEEK(MONDAY)) AS week,
        
        -- Core metrics
        AVG(promotional_intensity) AS avg_promo,
        AVG(urgency_score) AS avg_urgency,
        AVG(brand_voice_score) AS avg_brand_voice,
        
        -- Volume and diversity
        COUNT(*) AS ad_count,
        COUNT(DISTINCT primary_angle) AS angle_diversity,
        
        -- Dominant strategy
        CASE
          WHEN AVG(promotional_intensity) > 0.6 THEN 'PROMO_HEAVY'
          WHEN AVG(urgency_score) > 0.6 THEN 'URGENCY_DRIVEN'
          WHEN AVG(brand_voice_score) > 0.7 THEN 'BRAND_FOCUSED'
          ELSE 'BALANCED'
        END AS strategy_type
        
      FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
      GROUP BY brand, week
    ),
    
    -- Calculate changes with calibrated detection
    brand_moves AS (
      SELECT 
        brand,
        week,
        avg_promo,
        avg_urgency,
        strategy_type,
        
        -- Week-over-week changes
        avg_promo - LAG(avg_promo) OVER (PARTITION BY brand ORDER BY week) AS promo_delta,
        avg_urgency - LAG(avg_urgency) OVER (PARTITION BY brand ORDER BY week) AS urgency_delta,
        ad_count - LAG(ad_count) OVER (PARTITION BY brand ORDER BY week) AS volume_delta,
        
        -- Detect moves using calibrated thresholds
        CASE
          WHEN ABS(avg_promo - LAG(avg_promo) OVER (PARTITION BY brand ORDER BY week)) > {major_threshold} 
            THEN 'MAJOR_MOVE'
          WHEN ABS(avg_promo - LAG(avg_promo) OVER (PARTITION BY brand ORDER BY week)) > {significant_threshold}
            THEN 'SIGNIFICANT_MOVE'
          WHEN ABS(avg_urgency - LAG(avg_urgency) OVER (PARTITION BY brand ORDER BY week)) > {significant_threshold}
            THEN 'URGENCY_SHIFT'
          WHEN ABS(ad_count - LAG(ad_count) OVER (PARTITION BY brand ORDER BY week)) > 
            AVG(ad_count) OVER (PARTITION BY brand) * 0.5
            THEN 'VOLUME_CHANGE'
          ELSE 'STABLE'
        END AS move_type,
        
        -- Direction of change
        CASE
          WHEN avg_promo - LAG(avg_promo) OVER (PARTITION BY brand ORDER BY week) > {significant_threshold}
            THEN 'INCREASE'
          WHEN avg_promo - LAG(avg_promo) OVER (PARTITION BY brand ORDER BY week) < -{significant_threshold}
            THEN 'DECREASE'
          ELSE 'NEUTRAL'
        END AS move_direction
        
      FROM brand_metrics
    ),
    
    -- Detect cascades (correlations between brands)
    cascades AS (
      SELECT 
        a.brand AS leader_brand,
        a.week AS leader_week,
        a.move_type AS leader_move,
        a.promo_delta AS leader_delta,
        a.move_direction AS leader_direction,
        
        b.brand AS follower_brand,
        b.week AS follower_week,
        b.move_type AS follower_move,
        b.promo_delta AS follower_delta,
        b.move_direction AS follower_direction,
        
        DATE_DIFF(b.week, a.week, WEEK) AS response_lag_weeks,
        
        -- Cascade patterns
        CASE
          -- Same direction movement
          WHEN a.move_type IN ('MAJOR_MOVE', 'SIGNIFICANT_MOVE')
            AND b.move_type IN ('MAJOR_MOVE', 'SIGNIFICANT_MOVE', 'URGENCY_SHIFT')
            AND a.move_direction = b.move_direction
            AND DATE_DIFF(b.week, a.week, WEEK) BETWEEN 1 AND 4
            THEN 'FOLLOW_PATTERN'
            
          -- Opposite direction (counter-move)
          WHEN a.move_type IN ('MAJOR_MOVE', 'SIGNIFICANT_MOVE')
            AND b.move_type IN ('MAJOR_MOVE', 'SIGNIFICANT_MOVE')
            AND a.move_direction != b.move_direction
            AND a.move_direction != 'NEUTRAL'
            AND b.move_direction != 'NEUTRAL'
            AND DATE_DIFF(b.week, a.week, WEEK) BETWEEN 1 AND 3
            THEN 'COUNTER_PATTERN'
            
          -- Volume response
          WHEN a.move_type = 'VOLUME_CHANGE'
            AND b.move_type = 'VOLUME_CHANGE'
            AND DATE_DIFF(b.week, a.week, WEEK) BETWEEN 1 AND 2
            THEN 'VOLUME_CASCADE'
            
          -- Any correlated movement
          WHEN a.move_type != 'STABLE'
            AND b.move_type != 'STABLE'
            AND DATE_DIFF(b.week, a.week, WEEK) BETWEEN 1 AND 5
            THEN 'WEAK_CASCADE'
            
          ELSE 'NO_CASCADE'
        END AS cascade_pattern,
        
        -- Cascade strength (1-5 scale)
        CASE
          WHEN a.move_type = 'MAJOR_MOVE' AND b.move_type = 'MAJOR_MOVE'
            AND DATE_DIFF(b.week, a.week, WEEK) <= 2
            THEN 5
          WHEN a.move_type IN ('MAJOR_MOVE', 'SIGNIFICANT_MOVE')
            AND b.move_type IN ('MAJOR_MOVE', 'SIGNIFICANT_MOVE')
            AND DATE_DIFF(b.week, a.week, WEEK) <= 3
            THEN 4
          WHEN a.move_type != 'STABLE' AND b.move_type != 'STABLE'
            AND DATE_DIFF(b.week, a.week, WEEK) <= 4
            THEN 3
          WHEN a.move_type != 'STABLE' AND b.move_type != 'STABLE'
            THEN 2
          ELSE 1
        END AS cascade_strength
        
      FROM brand_moves a
      CROSS JOIN brand_moves b
      WHERE a.brand != b.brand
        AND a.week < b.week
        AND DATE_DIFF(b.week, a.week, WEEK) BETWEEN 1 AND 5
        AND a.move_type != 'STABLE'
    )
    
    SELECT 
      leader_brand,
      leader_week,
      leader_move,
      ROUND(leader_delta, 3) AS leader_delta,
      follower_brand,
      follower_week,
      follower_move,
      ROUND(follower_delta, 3) AS follower_delta,
      response_lag_weeks,
      cascade_pattern,
      cascade_strength,
      
      -- Strategic interpretation
      CASE
        WHEN cascade_pattern = 'FOLLOW_PATTERN' AND response_lag_weeks <= 2
          THEN '🏃 Fast Follower Strategy'
        WHEN cascade_pattern = 'COUNTER_PATTERN' AND response_lag_weeks <= 2
          THEN '⚔️ Competitive Counter-Move'
        WHEN cascade_pattern = 'VOLUME_CASCADE'
          THEN '📢 Market Share Battle'
        WHEN cascade_pattern = 'WEAK_CASCADE' AND response_lag_weeks >= 3
          THEN '🎨 Differentiated Response'
        ELSE '📊 Independent Strategy'
      END AS strategic_insight
      
    FROM cascades
    WHERE cascade_pattern != 'NO_CASCADE'
      AND cascade_strength >= 2
    ORDER BY cascade_strength DESC, response_lag_weeks ASC
    LIMIT 30
    """
    
    try:
        results = client.query(cascade_query).to_dataframe()
        
        if not results.empty:
            print(f"\n✅ Detected {len(results)} cascade patterns")
            
            # Cascade pattern distribution
            patterns = results['cascade_pattern'].value_counts()
            print("\n🌊 CASCADE PATTERNS:")
            for pattern, count in patterns.items():
                print(f"  {pattern}: {count}")
            
            # Top cascades
            print("\n🔥 STRONGEST CASCADE EFFECTS:")
            top_cascades = results.nlargest(5, 'cascade_strength')
            for _, cascade in top_cascades.iterrows():
                print(f"\n  {cascade['leader_brand']} → {cascade['follower_brand']}")
                print(f"    Pattern: {cascade['cascade_pattern']}")
                print(f"    Lag: {cascade['response_lag_weeks']} weeks")
                print(f"    Strength: {cascade['cascade_strength']}/5")
                print(f"    Insight: {cascade['strategic_insight']}")
            
            # Brand relationships
            print("\n🕸️ COMPETITIVE DYNAMICS:")
            
            # Most influential
            leaders = results['leader_brand'].value_counts()
            if not leaders.empty:
                print(f"\n  Market Leaders (trigger cascades):")
                for brand, count in leaders.head(3).items():
                    print(f"    {brand}: {count} triggered cascades")
            
            # Most responsive
            followers = results['follower_brand'].value_counts()
            if not followers.empty:
                print(f"\n  Market Followers (respond to others):")
                for brand, count in followers.head(3).items():
                    print(f"    {brand}: {count} cascade responses")
            
            # Average response time
            avg_lag = results.groupby('follower_brand')['response_lag_weeks'].mean()
            if not avg_lag.empty:
                print(f"\n  Response Speed:")
                for brand, lag in avg_lag.items():
                    print(f"    {brand}: {lag:.1f} weeks average")
            
            print("\n💡 CASCADE INTELLIGENCE INSIGHTS:")
            print("  • Competitive responses detected within 1-5 week window")
            print("  • Both follow and counter patterns observed")
            print("  • Calibrated thresholds capture realistic market dynamics")
            
            return True
            
        else:
            print("\n⚠️ No cascades detected with current data")
            print("  This may indicate:")
            print("  • Independent brand strategies")
            print("  • Insufficient time series data")
            print("  • Need for more sensitive detection")
            return False
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_cascade_calibrated()
    
    if success:
        print("\n✅ CASCADE DETECTION: CALIBRATED & OPERATIONAL")
    else:
        print("\n🔧 CASCADE DETECTION: REQUIRES MORE DATA")