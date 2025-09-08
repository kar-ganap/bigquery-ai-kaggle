#!/usr/bin/env python3
"""
Competitive Uniqueness Detection - Signal-Agnostic Implementation
Identifies unique vs common signals across brands to prioritize competitive advantages
Works with any signal type (text, visual, multimodal)
"""

import os
import pandas as pd
import numpy as np
from google.cloud import bigquery
import time
from datetime import datetime

PROJECT_ID = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
DATASET_ID = os.environ.get("BQ_DATASET", "ads_demo")

client = bigquery.Client(project=PROJECT_ID)

class CompetitiveUniquenessDetector:
    def __init__(self):
        self.uniqueness_scores = {}
        self.industry_patterns = {}
        self.brand_specific_signals = {}
        
    def detect_signal_uniqueness(self):
        """Detect uniqueness of signals across competitive landscape"""
        print("🎯 COMPETITIVE UNIQUENESS DETECTION")
        print("="*60)
        print("Identifying brand-specific signals vs industry-wide patterns")
        print("-"*60)
        
        try:
            # Signal-agnostic uniqueness detection
            uniqueness_query = f"""
            WITH signal_extraction AS (
              -- Extract all types of signals (works with any signal type)
              SELECT 
                brand,
                week_start,
                
                -- Strategic signals (Tier 1)
                AVG(promotional_intensity) AS signal_promo_intensity,
                AVG(urgency_score) AS signal_urgency,
                AVG(brand_voice_score) AS signal_brand_voice,
                
                -- Angle signals (can be replaced with visual signals later)
                STRING_AGG(DISTINCT primary_angle) AS signal_angles,
                COUNTIF(primary_angle = 'PROMOTIONAL') / COUNT(*) AS signal_promo_angle_pct,
                COUNTIF(primary_angle = 'EMOTIONAL') / COUNT(*) AS signal_emotional_angle_pct,
                
                -- Funnel signals
                COUNTIF(funnel = 'Upper') / COUNT(*) AS signal_upper_funnel_pct,
                COUNTIF(funnel = 'Lower') / COUNT(*) AS signal_lower_funnel_pct,
                
                -- Volume and diversity signals
                COUNT(*) AS signal_volume,
                COUNT(DISTINCT media_type) AS signal_media_diversity,
                
                -- Temporal concentration
                COUNT(DISTINCT DATE(start_timestamp)) AS signal_active_days
                
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, week_start
            ),
            
            -- Calculate signal distributions across all brands
            industry_distributions AS (
              SELECT 
                -- For each signal, calculate industry-wide statistics
                AVG(signal_promo_intensity) AS industry_avg_promo,
                STDDEV(signal_promo_intensity) AS industry_std_promo,
                
                AVG(signal_urgency) AS industry_avg_urgency,
                STDDEV(signal_urgency) AS industry_std_urgency,
                
                AVG(signal_brand_voice) AS industry_avg_brand_voice,
                STDDEV(signal_brand_voice) AS industry_std_brand_voice,
                
                AVG(signal_promo_angle_pct) AS industry_avg_promo_angle,
                AVG(signal_emotional_angle_pct) AS industry_avg_emotional_angle,
                
                AVG(signal_upper_funnel_pct) AS industry_avg_upper_funnel,
                AVG(signal_volume) AS industry_avg_volume,
                
                -- Percentiles for robust comparison
                APPROX_QUANTILES(signal_promo_intensity, 100)[OFFSET(25)] AS industry_p25_promo,
                APPROX_QUANTILES(signal_promo_intensity, 100)[OFFSET(75)] AS industry_p75_promo,
                APPROX_QUANTILES(signal_urgency, 100)[OFFSET(25)] AS industry_p25_urgency,
                APPROX_QUANTILES(signal_urgency, 100)[OFFSET(75)] AS industry_p75_urgency
                
              FROM signal_extraction
            ),
            
            -- Calculate uniqueness scores for each brand-week
            brand_uniqueness AS (
              SELECT 
                s.brand,
                s.week_start,
                
                -- Raw signals
                s.signal_promo_intensity,
                s.signal_urgency,
                s.signal_brand_voice,
                s.signal_volume,
                
                -- Z-scores (how many std devs from industry mean)
                ABS((s.signal_promo_intensity - i.industry_avg_promo) / NULLIF(i.industry_std_promo, 0)) AS z_score_promo,
                ABS((s.signal_urgency - i.industry_avg_urgency) / NULLIF(i.industry_std_urgency, 0)) AS z_score_urgency,
                ABS((s.signal_brand_voice - i.industry_avg_brand_voice) / NULLIF(i.industry_std_brand_voice, 0)) AS z_score_brand_voice,
                
                -- Percentile-based uniqueness (robust to outliers)
                CASE
                  WHEN s.signal_promo_intensity > i.industry_p75_promo THEN 
                    (s.signal_promo_intensity - i.industry_p75_promo) / (1 - i.industry_p75_promo)
                  WHEN s.signal_promo_intensity < i.industry_p25_promo THEN
                    (i.industry_p25_promo - s.signal_promo_intensity) / i.industry_p25_promo
                  ELSE 0
                END AS uniqueness_promo_percentile,
                
                -- Angle uniqueness (different from industry mix)
                ABS(s.signal_promo_angle_pct - i.industry_avg_promo_angle) AS uniqueness_promo_angle,
                ABS(s.signal_emotional_angle_pct - i.industry_avg_emotional_angle) AS uniqueness_emotional_angle,
                
                -- Funnel uniqueness
                ABS(s.signal_upper_funnel_pct - i.industry_avg_upper_funnel) AS uniqueness_funnel,
                
                -- Volume uniqueness (outlier detection)
                CASE
                  WHEN s.signal_volume > i.industry_avg_volume * 2 THEN 1.0
                  WHEN s.signal_volume < i.industry_avg_volume * 0.5 THEN 0.5
                  ELSE 0
                END AS uniqueness_volume,
                
                -- Composite uniqueness score (0-1 scale)
                (
                  COALESCE(ABS((s.signal_promo_intensity - i.industry_avg_promo) / NULLIF(i.industry_std_promo, 0)), 0) * 0.25 +
                  COALESCE(ABS((s.signal_urgency - i.industry_avg_urgency) / NULLIF(i.industry_std_urgency, 0)), 0) * 0.25 +
                  COALESCE(ABS((s.signal_brand_voice - i.industry_avg_brand_voice) / NULLIF(i.industry_std_brand_voice, 0)), 0) * 0.25 +
                  ABS(s.signal_upper_funnel_pct - i.industry_avg_upper_funnel) * 0.25
                ) / 2.0 AS composite_uniqueness_score
                
              FROM signal_extraction s
              CROSS JOIN industry_distributions i
            ),
            
            -- Classify uniqueness patterns
            uniqueness_classification AS (
              SELECT 
                brand,
                week_start,
                signal_promo_intensity,
                signal_urgency,
                signal_brand_voice,
                composite_uniqueness_score,
                
                -- Classification of uniqueness type
                CASE
                  WHEN composite_uniqueness_score > 0.7 THEN 'HIGHLY_UNIQUE'
                  WHEN composite_uniqueness_score > 0.4 THEN 'MODERATELY_UNIQUE'
                  WHEN composite_uniqueness_score > 0.2 THEN 'SLIGHTLY_UNIQUE'
                  ELSE 'INDUSTRY_STANDARD'
                END AS uniqueness_level,
                
                -- Identify which signals drive uniqueness
                CASE
                  WHEN z_score_promo > 2 THEN 'PROMO_OUTLIER'
                  WHEN z_score_urgency > 2 THEN 'URGENCY_OUTLIER'
                  WHEN z_score_brand_voice > 2 THEN 'BRAND_VOICE_OUTLIER'
                  WHEN uniqueness_funnel > 0.3 THEN 'FUNNEL_UNIQUE'
                  WHEN uniqueness_volume > 0.5 THEN 'VOLUME_UNIQUE'
                  ELSE 'BALANCED_UNIQUE'
                END AS uniqueness_driver,
                
                -- Strategic value of uniqueness
                CASE
                  WHEN composite_uniqueness_score > 0.5 AND signal_brand_voice > 0.7 THEN 'DIFFERENTIATION_OPPORTUNITY'
                  WHEN composite_uniqueness_score > 0.5 AND signal_promo_intensity > 0.7 THEN 'PRICING_ADVANTAGE'
                  WHEN composite_uniqueness_score < 0.2 THEN 'COMMODITIZED_POSITION'
                  ELSE 'COMPETITIVE_PARITY'
                END AS strategic_implication
                
              FROM brand_uniqueness
            )
            
            -- Final aggregation and insights
            SELECT 
              brand,
              COUNT(*) AS weeks_analyzed,
              
              -- Uniqueness metrics
              AVG(composite_uniqueness_score) AS avg_uniqueness_score,
              MAX(composite_uniqueness_score) AS max_uniqueness_score,
              MIN(composite_uniqueness_score) AS min_uniqueness_score,
              STDDEV(composite_uniqueness_score) AS uniqueness_volatility,
              
              -- Uniqueness distribution
              COUNTIF(uniqueness_level = 'HIGHLY_UNIQUE') / COUNT(*) AS pct_highly_unique,
              COUNTIF(uniqueness_level = 'MODERATELY_UNIQUE') / COUNT(*) AS pct_moderately_unique,
              COUNTIF(uniqueness_level = 'INDUSTRY_STANDARD') / COUNT(*) AS pct_industry_standard,
              
              -- Common uniqueness drivers
              STRING_AGG(DISTINCT uniqueness_driver ORDER BY uniqueness_driver) AS uniqueness_drivers,
              
              -- Strategic implications
              STRING_AGG(DISTINCT strategic_implication ORDER BY strategic_implication) AS strategic_positions,
              
              -- Competitive advantage score (uniqueness + performance)
              AVG(composite_uniqueness_score) * AVG(signal_brand_voice) AS competitive_advantage_score
              
            FROM uniqueness_classification
            GROUP BY brand
            ORDER BY avg_uniqueness_score DESC
            """
            
            start_time = time.time()
            uniqueness_results = client.query(uniqueness_query).to_dataframe()
            query_time = time.time() - start_time
            
            if not uniqueness_results.empty:
                print("📊 Competitive Uniqueness Analysis:")
                
                for _, row in uniqueness_results.iterrows():
                    print(f"\n🏢 {row['brand']} UNIQUENESS PROFILE:")
                    print(f"  Weeks Analyzed: {row['weeks_analyzed']}")
                    
                    print(f"\n  📈 Uniqueness Metrics:")
                    print(f"    Average Score: {row['avg_uniqueness_score']:.3f}")
                    print(f"    Max Score: {row['max_uniqueness_score']:.3f}")
                    print(f"    Volatility: {row['uniqueness_volatility']:.3f}")
                    print(f"    Competitive Advantage: {row['competitive_advantage_score']:.3f}")
                    
                    print(f"\n  📊 Uniqueness Distribution:")
                    print(f"    Highly Unique: {row['pct_highly_unique']:.1%}")
                    print(f"    Moderately Unique: {row['pct_moderately_unique']:.1%}")
                    print(f"    Industry Standard: {row['pct_industry_standard']:.1%}")
                    
                    print(f"\n  🎯 Uniqueness Drivers:")
                    print(f"    {row['uniqueness_drivers']}")
                    
                    print(f"\n  💡 Strategic Positions:")
                    print(f"    {row['strategic_positions']}")
                
                # Overall industry analysis
                avg_uniqueness = uniqueness_results['avg_uniqueness_score'].mean()
                max_advantage = uniqueness_results['competitive_advantage_score'].max()
                
                print(f"\n📈 INDUSTRY-WIDE INSIGHTS:")
                print(f"  Average Uniqueness: {avg_uniqueness:.3f}")
                print(f"  Max Competitive Advantage: {max_advantage:.3f}")
                print(f"  Query Time: {query_time:.2f}s")
                
                self.uniqueness_scores = uniqueness_results.to_dict('records')
                return True
                
            else:
                print("❌ No uniqueness results")
                return False
                
        except Exception as e:
            print(f"❌ Uniqueness detection error: {str(e)}")
            return False
    
    def detect_emerging_differentiation(self):
        """Detect emerging differentiation opportunities"""
        print(f"\n🚀 EMERGING DIFFERENTIATION DETECTION")
        print("="*60)
        
        try:
            emerging_query = f"""
            WITH temporal_signals AS (
              SELECT 
                brand,
                week_start,
                week_offset,
                
                -- Current week signals
                AVG(promotional_intensity) AS current_promo,
                AVG(urgency_score) AS current_urgency,
                AVG(brand_voice_score) AS current_brand_voice,
                
                -- Signal changes
                AVG(promotional_intensity) - LAG(AVG(promotional_intensity)) OVER (
                  PARTITION BY brand ORDER BY week_start
                ) AS promo_delta,
                
                AVG(urgency_score) - LAG(AVG(urgency_score)) OVER (
                  PARTITION BY brand ORDER BY week_start
                ) AS urgency_delta,
                
                -- Angle shifts
                STRING_AGG(DISTINCT primary_angle) AS current_angles,
                COUNT(DISTINCT primary_angle) AS angle_diversity
                
              FROM `{PROJECT_ID}.{DATASET_ID}.ads_strategic_labels_mock`
              GROUP BY brand, week_start, week_offset
            ),
            
            -- Detect differentiation moves
            differentiation_detection AS (
              SELECT 
                brand,
                week_start,
                
                -- Differentiation signals
                CASE
                  WHEN ABS(promo_delta) > 0.2 AND current_promo > 0.7 THEN 'AGGRESSIVE_PROMO_DIFFERENTIATION'
                  WHEN ABS(urgency_delta) > 0.2 AND current_urgency > 0.7 THEN 'URGENCY_DIFFERENTIATION'
                  WHEN current_brand_voice > 0.8 THEN 'BRAND_PREMIUM_DIFFERENTIATION'
                  WHEN angle_diversity >= 3 THEN 'MULTI_ANGLE_DIFFERENTIATION'
                  ELSE 'NO_CLEAR_DIFFERENTIATION'
                END AS differentiation_type,
                
                -- Differentiation strength
                GREATEST(
                  ABS(COALESCE(promo_delta, 0)),
                  ABS(COALESCE(urgency_delta, 0)),
                  (current_brand_voice - 0.5) * 2
                ) AS differentiation_strength,
                
                -- First-mover advantage detection
                CASE
                  WHEN ABS(promo_delta) > 0.2 AND week_offset = MIN(week_offset) OVER (
                    PARTITION BY ABS(promo_delta) > 0.2
                  ) THEN 'FIRST_MOVER'
                  ELSE 'FOLLOWER'
                END AS mover_status
                
              FROM temporal_signals
              WHERE promo_delta IS NOT NULL
            )
            
            SELECT 
              brand,
              COUNT(*) AS differentiation_attempts,
              
              -- Differentiation patterns
              STRING_AGG(DISTINCT differentiation_type ORDER BY differentiation_type) AS differentiation_types,
              AVG(differentiation_strength) AS avg_differentiation_strength,
              
              -- First-mover metrics
              COUNTIF(mover_status = 'FIRST_MOVER') AS first_mover_count,
              COUNTIF(mover_status = 'FIRST_MOVER') / COUNT(*) AS first_mover_rate
              
            FROM differentiation_detection
            GROUP BY brand
            ORDER BY avg_differentiation_strength DESC
            """
            
            start_time = time.time()
            emerging_results = client.query(emerging_query).to_dataframe()
            query_time = time.time() - start_time
            
            if not emerging_results.empty:
                print("📊 Emerging Differentiation Opportunities:")
                
                for _, row in emerging_results.iterrows():
                    print(f"\n🏢 {row['brand']}:")
                    print(f"  Differentiation Attempts: {row['differentiation_attempts']}")
                    print(f"  Average Strength: {row['avg_differentiation_strength']:.3f}")
                    print(f"  First-Mover Rate: {row['first_mover_rate']:.1%}")
                    print(f"  Differentiation Types: {row['differentiation_types']}")
                
                print(f"\n⏱️ Query Time: {query_time:.2f}s")
                return True
            else:
                print("❌ No emerging differentiation detected")
                return False
                
        except Exception as e:
            print(f"❌ Emerging differentiation error: {str(e)}")
            return False
    
    def generate_uniqueness_recommendations(self):
        """Generate strategic recommendations based on uniqueness analysis"""
        print(f"\n💡 STRATEGIC UNIQUENESS RECOMMENDATIONS")
        print("="*60)
        
        if not self.uniqueness_scores:
            print("❌ No uniqueness data available")
            return
        
        for brand_data in self.uniqueness_scores:
            brand = brand_data['brand']
            avg_uniqueness = brand_data['avg_uniqueness_score']
            competitive_advantage = brand_data['competitive_advantage_score']
            
            print(f"\n🏢 {brand} RECOMMENDATIONS:")
            
            if avg_uniqueness > 0.5:
                print("  ✅ MAINTAIN DIFFERENTIATION:")
                print("    • Your unique positioning is a competitive advantage")
                print("    • Continue investing in differentiated strategies")
                print("    • Monitor competitors for copying attempts")
            elif avg_uniqueness > 0.3:
                print("  🎯 ENHANCE DIFFERENTIATION:")
                print("    • Moderate uniqueness leaves room for improvement")
                print("    • Identify underutilized differentiation opportunities")
                print("    • Consider bold moves in weak signal areas")
            else:
                print("  ⚠️ URGENT: DIFFERENTIATION NEEDED:")
                print("    • Currently following industry patterns too closely")
                print("    • Risk of commoditization")
                print("    • Explore blue ocean strategies")
            
            if competitive_advantage > 0.4:
                print("  💪 COMPETITIVE ADVANTAGE: Strong")
            elif competitive_advantage > 0.2:
                print("  📊 COMPETITIVE ADVANTAGE: Moderate")
            else:
                print("  🔴 COMPETITIVE ADVANTAGE: Weak")
        
        print(f"\n🎯 GENERAL RECOMMENDATIONS:")
        print("  1. Focus on signals with highest uniqueness scores")
        print("  2. Avoid competing on commoditized signals")
        print("  3. Time differentiation moves for maximum impact")
        print("  4. Monitor competitor responses to unique strategies")
        print("  5. Balance uniqueness with proven industry patterns")

def run_competitive_uniqueness_detection():
    """Run complete competitive uniqueness detection suite"""
    print("🚀 COMPETITIVE UNIQUENESS DETECTION SUITE")
    print("="*80)
    print("Signal-agnostic system ready for text and future multimodal signals")
    print("="*80)
    
    detector = CompetitiveUniquenessDetector()
    
    # Run detection modules
    success = True
    
    # 1. Core uniqueness detection
    if not detector.detect_signal_uniqueness():
        success = False
    
    # 2. Emerging differentiation
    if not detector.detect_emerging_differentiation():
        success = False
    
    # 3. Strategic recommendations
    detector.generate_uniqueness_recommendations()
    
    print(f"\n{'='*80}")
    if success:
        print("✅ COMPETITIVE UNIQUENESS DETECTION: OPERATIONAL")
        print("🎯 Ready to identify differentiation opportunities")
        print("🔮 Will seamlessly integrate with multimodal signals")
    else:
        print("⚠️ COMPETITIVE UNIQUENESS: PARTIAL SUCCESS")
        print("🔧 Review failed components")
    print(f"{'='*80}")
    
    return success

if __name__ == "__main__":
    success = run_competitive_uniqueness_detection()
    exit(0 if success else 1)