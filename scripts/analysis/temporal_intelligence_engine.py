#!/usr/bin/env python3
"""
Temporal Intelligence Module
Integrates historical context, current state, and predictive forecasting
Implements the 3-question framework: Where are we? Where did we come from? Where are we going?
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta
import logging

class TemporalIntelligenceEngine:
    """
    Comprehensive temporal intelligence system that answers:
    1. Where are we? (current state)
    2. Where did we come from? (historical changes)
    3. Where are we going? (predictive forecasting)
    """
    
    def __init__(self, project_id: str, dataset_id: str, run_id: str, 
                 brand: str, competitors: List[str]):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.run_id = run_id
        self.brand = brand
        self.competitors = competitors
        self.logger = logging.getLogger(__name__)
    
    def generate_temporal_analysis_sql(self) -> str:
        """Generate SQL for comprehensive temporal analysis using strategic labels table"""
        return f"""
        -- TEMPORAL INTELLIGENCE: Where Did We Come From?
        WITH temporal_movements AS (
          SELECT 
            s.brand,
            -- 7-day velocity windows
            COUNT(CASE WHEN DATE(s.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_last_7d,
            COUNT(CASE WHEN DATE(s.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY) 
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_prior_7d,
            
            -- 30-day trend analysis
            COUNT(CASE WHEN DATE(s.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
                      THEN 1 END) as ads_last_30d,
            COUNT(CASE WHEN DATE(s.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
                      THEN 1 END) as ads_prior_30d,
            
            -- Promotional intensity evolution (using urgency_score as proxy for CTA aggressiveness)
            AVG(CASE WHEN DATE(s.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                     THEN s.urgency_score END) as recent_cta_score,
            AVG(CASE WHEN DATE(s.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN s.urgency_score END) as historical_cta_score,
            
            -- Creative freshness indicators
            AVG(DATE_DIFF(CURRENT_DATE(), DATE(s.start_timestamp), DAY)) as avg_campaign_age,
            COUNT(DISTINCT s.creative_text) / NULLIF(COUNT(*), 0) as creative_diversity_ratio,
            
            -- Platform evolution
            COUNT(DISTINCT CASE WHEN DATE(s.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                               THEN s.publisher_platforms END) as recent_platforms,
            COUNT(DISTINCT CASE WHEN DATE(s.start_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                               THEN s.publisher_platforms END) as historical_platforms,
            
            -- Content type shifts - comprehensive media type analysis
            AVG(CASE WHEN DATE(s.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN CASE WHEN UPPER(s.media_type) = 'VIDEO' THEN 1.0 ELSE 0.0 END END) as recent_video_pct,
            AVG(CASE WHEN DATE(s.start_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN CASE WHEN UPPER(s.media_type) = 'VIDEO' THEN 1.0 ELSE 0.0 END END) as historical_video_pct,
            
            -- Image content analysis
            AVG(CASE WHEN DATE(s.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN CASE WHEN UPPER(s.media_type) = 'IMAGE' OR s.media_type IS NULL THEN 1.0 ELSE 0.0 END END) as recent_image_pct,
            AVG(CASE WHEN DATE(s.start_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN CASE WHEN UPPER(s.media_type) = 'IMAGE' OR s.media_type IS NULL THEN 1.0 ELSE 0.0 END END) as historical_image_pct,
                     
            -- Carousel/multi-media content
            AVG(CASE WHEN DATE(s.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN CASE WHEN UPPER(s.media_type) IN ('CAROUSEL', 'MULTI_IMAGE') THEN 1.0 ELSE 0.0 END END) as recent_carousel_pct,
            AVG(CASE WHEN DATE(s.start_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN CASE WHEN UPPER(s.media_type) IN ('CAROUSEL', 'MULTI_IMAGE') THEN 1.0 ELSE 0.0 END END) as historical_carousel_pct
            
          FROM `{self.project_id}.{self.dataset_id}.ads_with_dates` s
          WHERE s.brand IN ('{self.brand}', {', '.join(f"'{c}'" for c in self.competitors)})
            AND s.creative_text IS NOT NULL
          GROUP BY s.brand
        )
        SELECT *,
          -- Calculate momentum indicators
          SAFE_DIVIDE(ads_last_7d - ads_prior_7d, NULLIF(ads_prior_7d, 0)) as velocity_change_7d,
          SAFE_DIVIDE(ads_last_30d - ads_prior_30d, NULLIF(ads_prior_30d, 0)) as velocity_change_30d,
          (recent_cta_score - historical_cta_score) as cta_intensity_shift,
          (recent_video_pct - historical_video_pct) as video_strategy_shift,
          
          -- Categorize momentum status
          CASE 
            WHEN ads_last_7d > ads_prior_7d * 1.2 THEN 'ACCELERATING'
            WHEN ads_last_7d < ads_prior_7d * 0.8 THEN 'DECELERATING'
            ELSE 'STABLE'
          END as momentum_status,
          
          -- Creative fatigue risk assessment
          CASE
            WHEN avg_campaign_age > 30 THEN 'HIGH_FATIGUE_RISK'
            WHEN avg_campaign_age > 21 THEN 'MODERATE_FATIGUE'
            ELSE 'FRESH_CREATIVES'
          END as creative_status,
          
          -- Platform strategy changes
          CASE
            WHEN recent_platforms > historical_platforms THEN 'EXPANDING_CHANNELS'
            WHEN recent_platforms < historical_platforms THEN 'CONSOLIDATING_CHANNELS'
            ELSE 'STABLE_CHANNELS'
          END as platform_strategy
          
        FROM temporal_movements
        ORDER BY brand = '{self.brand}' DESC, ads_last_7d DESC
        """
    
    def generate_wide_net_forecasting_sql(self) -> str:
        """Generate SQL for Wide Net predictive forecasting with signal prioritization"""
        return f"""
        -- PREDICTIVE INTELLIGENCE: Where Are We Going?
        WITH weekly_aggregations AS (
          SELECT 
            s.brand,
            DATE_TRUNC(DATE(s.start_timestamp), WEEK) as week,
            
            -- Tier 1 Strategic Signals (using strategic labels data)
            AVG(s.promotional_intensity) as promotional_intensity,
            AVG(s.urgency_score) as urgency_score,
            
            -- Tier 2 Message Angles (Wide Net across 10+ types)
            AVG(CASE WHEN UPPER(s.creative_text) LIKE '%FEEL%' OR 
                         UPPER(s.creative_text) LIKE '%LOVE%' 
                    THEN 1.0 ELSE 0.0 END) as emotional_pct,
            AVG(CASE WHEN UPPER(s.creative_text) LIKE '%SAVE%' OR 
                         UPPER(s.creative_text) LIKE '%DISCOUNT%' OR
                         UPPER(s.creative_text) LIKE '%OFF%'
                    THEN 1.0 ELSE 0.0 END) as promotional_pct,
            AVG(CASE WHEN UPPER(s.creative_text) LIKE '%PREMIUM%' OR 
                         UPPER(s.creative_text) LIKE '%LUXURY%'
                    THEN 1.0 ELSE 0.0 END) as aspirational_pct,
            AVG(CASE WHEN UPPER(s.creative_text) LIKE '%TRUSTED%' OR 
                         UPPER(s.creative_text) LIKE '%GUARANTEE%'
                    THEN 1.0 ELSE 0.0 END) as trust_pct,
            
            -- Tier 3 Tactical Signals - comprehensive media type tracking
            AVG(CASE WHEN UPPER(s.media_type) = 'VIDEO' THEN 1.0 ELSE 0.0 END) as video_pct,
            AVG(CASE WHEN UPPER(s.media_type) = 'IMAGE' OR s.media_type IS NULL THEN 1.0 ELSE 0.0 END) as image_pct,
            AVG(CASE WHEN UPPER(s.media_type) IN ('CAROUSEL', 'MULTI_IMAGE') THEN 1.0 ELSE 0.0 END) as carousel_pct,
            COUNT(DISTINCT s.publisher_platforms) as platform_diversity,
            COUNT(*) as weekly_volume
            
          FROM `{self.project_id}.{self.dataset_id}.ads_with_dates` s
          WHERE s.brand IN ('{self.brand}', {', '.join(f"'{c}'" for c in self.competitors)})
            AND DATE(s.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
          GROUP BY s.brand, week
        ),
        signal_detection AS (
          SELECT 
            brand,
            week,
            promotional_intensity,
            urgency_score,
            emotional_pct,
            promotional_pct,
            video_pct,
            
            -- Calculate week-over-week changes
            LAG(promotional_intensity, 1) OVER (PARTITION BY brand ORDER BY week) as prev_promo,
            LAG(urgency_score, 1) OVER (PARTITION BY brand ORDER BY week) as prev_urgency,
            LAG(video_pct, 1) OVER (PARTITION BY brand ORDER BY week) as prev_video,
            
            -- Linear trend for simple forecasting
            (promotional_intensity - LAG(promotional_intensity, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as promo_trend,
            (urgency_score - LAG(urgency_score, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as urgency_trend,
            (video_pct - LAG(video_pct, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as video_trend
            
          FROM weekly_aggregations
        ),
        forecasting AS (
          SELECT 
            brand,
            MAX(week) as latest_week,
            
            -- Current state (last week)
            MAX(promotional_intensity) as current_promo,
            MAX(urgency_score) as current_urgency,
            MAX(video_pct) as current_video,
            
            -- 4-week forecast (simple linear projection)
            MAX(promotional_intensity) + (4 * AVG(promo_trend)) as forecast_promo,
            MAX(urgency_score) + (4 * AVG(urgency_trend)) as forecast_urgency,
            MAX(video_pct) + (4 * AVG(video_trend)) as forecast_video,
            
            -- Calculate magnitude of predicted changes
            ABS(MAX(promotional_intensity) + (4 * AVG(promo_trend)) - MAX(promotional_intensity)) as promo_change,
            ABS(MAX(urgency_score) + (4 * AVG(urgency_trend)) - MAX(urgency_score)) as urgency_change,
            ABS(MAX(video_pct) + (4 * AVG(video_trend)) - MAX(video_pct)) as video_change,
            
            -- Confidence based on trend consistency
            CASE 
              WHEN STDDEV(promo_trend) < 0.02 THEN 'HIGH'
              WHEN STDDEV(promo_trend) < 0.04 THEN 'MEDIUM'
              ELSE 'LOW'
            END as promo_confidence,
            
            -- Detect seasonal patterns
            CASE
              WHEN EXTRACT(MONTH FROM MAX(week)) IN (11, 12) THEN 'HOLIDAY_SEASON'
              WHEN EXTRACT(MONTH FROM MAX(week)) = 11 AND EXTRACT(DAY FROM MAX(week)) > 20 THEN 'BLACK_FRIDAY'
              WHEN EXTRACT(MONTH FROM MAX(week)) IN (6, 7) THEN 'SUMMER_SALES'
              ELSE 'REGULAR'
            END as seasonal_context
            
          FROM signal_detection
          WHERE week >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK)
          GROUP BY brand
        ),
        prioritized_signals AS (
          SELECT *,
            -- Business Impact Scoring (1-5 scale)
            CASE 
              WHEN promo_change >= 0.15 AND promo_confidence = 'HIGH' THEN 5
              WHEN promo_change >= 0.10 AND forecast_promo >= 0.60 THEN 4
              WHEN urgency_change >= 0.15 THEN 4
              WHEN video_change >= 0.20 THEN 3
              WHEN promo_change >= 0.08 OR video_change >= 0.15 THEN 2
              ELSE 1
            END AS business_impact_weight,
            
            -- Noise Threshold Filtering
            CASE 
              WHEN promo_change >= 0.10 OR video_change >= 0.15 OR urgency_change >= 0.12 
              THEN 'ABOVE_THRESHOLD'
              ELSE 'BELOW_THRESHOLD'
            END AS signal_quality,
            
            -- Executive Summary Generation
            CASE 
              WHEN promo_change >= 0.15 AND promo_confidence = 'HIGH' AND seasonal_context = 'BLACK_FRIDAY'
              THEN CONCAT('🚨 CRITICAL: Black Friday surge predicted - ', brand, 
                         ' (+', CAST(ROUND(promo_change * 100) AS STRING), '% promotional intensity)')
              
              WHEN promo_change >= 0.15 AND promo_confidence = 'HIGH'
              THEN CONCAT('🚨 CRITICAL: Major strategic shift predicted - ', brand,
                         ' (+', CAST(ROUND(promo_change * 100) AS STRING), '%)')
              
              WHEN video_change >= 0.20 
              THEN CONCAT('⚠️ MULTIPLE_SHIFTS: Video strategy pivot - ', brand,
                         ' (+', CAST(ROUND(video_change * 100) AS STRING), '%)')
              
              WHEN urgency_change >= 0.15
              THEN CONCAT('📊 MODERATE: Urgency escalation expected - ', brand,
                         ' (+', CAST(ROUND(urgency_change * 100) AS STRING), '%)')
              
              ELSE NULL
            END AS executive_forecast
            
          FROM forecasting
        )
        SELECT 
          brand,
          latest_week,
          seasonal_context,
          business_impact_weight,
          signal_quality,
          executive_forecast,
          
          -- Detailed signals for transparency
          ROUND(current_promo, 3) as current_promotional_intensity,
          ROUND(forecast_promo, 3) as forecast_promotional_intensity,
          ROUND(promo_change, 3) as promotional_change_magnitude,
          promo_confidence,
          
          ROUND(current_urgency, 3) as current_urgency_score,
          ROUND(forecast_urgency, 3) as forecast_urgency_score,
          ROUND(urgency_change, 3) as urgency_change_magnitude,
          
          ROUND(current_video, 3) as current_video_pct,
          ROUND(forecast_video, 3) as forecast_video_pct,
          ROUND(video_change, 3) as video_change_magnitude
          
        FROM prioritized_signals
        WHERE signal_quality = 'ABOVE_THRESHOLD'  -- Only surface meaningful signals
          AND executive_forecast IS NOT NULL
        ORDER BY business_impact_weight DESC, brand = '{self.brand}' DESC
        """
    
    def structure_temporal_intelligence(self, 
                                       temporal_data: pd.DataFrame,
                                       forecast_data: pd.DataFrame,
                                       current_state: Dict) -> Dict:
        """
        Structure intelligence using the 3-question framework
        """
        # Find our brand's data
        brand_temporal = temporal_data[temporal_data['brand'] == self.brand].iloc[0] if not temporal_data.empty else {}
        brand_forecast = forecast_data[forecast_data['brand'] == self.brand].iloc[0] if not forecast_data.empty else {}
        
        # Build the 3-question structure
        temporal_intelligence = {
            "where_we_are": {
                "market_position": current_state.get('market_position', 'unknown'),
                "cta_aggressiveness": current_state.get('cta_aggressiveness', 0),
                "active_competitors": len(self.competitors),
                "current_promotional_intensity": float(brand_forecast.get('current_promotional_intensity', 0)),
                "current_urgency_score": float(brand_forecast.get('current_urgency_score', 0))
            },
            "where_we_came_from": {
                "momentum_status": brand_temporal.get('momentum_status', 'UNKNOWN'),
                "velocity_change_7d": float(brand_temporal.get('velocity_change_7d', 0)),
                "velocity_change_30d": float(brand_temporal.get('velocity_change_30d', 0)),
                "cta_intensity_shift": float(brand_temporal.get('cta_intensity_shift', 0)),
                "creative_status": brand_temporal.get('creative_status', 'UNKNOWN'),
                "platform_strategy": brand_temporal.get('platform_strategy', 'STABLE'),
                "key_changes": self._generate_key_changes(brand_temporal)
            },
            "where_we_are_going": {
                "executive_forecast": brand_forecast.get('executive_forecast', 'No significant changes predicted'),
                "forecast_promotional_intensity": float(brand_forecast.get('forecast_promotional_intensity', 0)),
                "promotional_change_magnitude": float(brand_forecast.get('promotional_change_magnitude', 0)),
                "confidence": brand_forecast.get('promo_confidence', 'LOW'),
                "seasonal_context": brand_forecast.get('seasonal_context', 'REGULAR'),
                "top_predictions": self._generate_predictions(forecast_data),
                "recommended_action": self._generate_recommendation(brand_temporal, brand_forecast)
            }
        }
        
        return temporal_intelligence
    
    def _generate_key_changes(self, temporal_data: Dict) -> List[str]:
        """Generate human-readable key changes"""
        changes = []
        
        if temporal_data.get('velocity_change_7d', 0) > 0.1:
            changes.append(f"↑ Ad velocity +{temporal_data['velocity_change_7d']:.0%} week-over-week")
        elif temporal_data.get('velocity_change_7d', 0) < -0.1:
            changes.append(f"↓ Ad velocity {temporal_data['velocity_change_7d']:.0%} week-over-week")
        
        if temporal_data.get('cta_intensity_shift', 0) > 1.0:
            changes.append(f"↑ CTA aggressiveness +{temporal_data['cta_intensity_shift']:.1f} points")
        elif temporal_data.get('cta_intensity_shift', 0) < -1.0:
            changes.append(f"↓ CTA aggressiveness {temporal_data['cta_intensity_shift']:.1f} points")
        
        if temporal_data.get('creative_status') == 'HIGH_FATIGUE_RISK':
            changes.append("⚠️ Creative fatigue risk - campaigns averaging 30+ days old")
        
        if temporal_data.get('platform_strategy') == 'EXPANDING_CHANNELS':
            changes.append("📱 Expanding to new platforms")
        elif temporal_data.get('platform_strategy') == 'CONSOLIDATING_CHANNELS':
            changes.append("🎯 Consolidating platform focus")
        
        return changes if changes else ["No significant changes in past 30 days"]
    
    def _generate_predictions(self, forecast_data: pd.DataFrame) -> List[Dict]:
        """Generate top predictions from forecast data"""
        predictions = []
        
        # Get top 3 competitor predictions
        competitor_forecasts = forecast_data[forecast_data['brand'] != self.brand].head(3)
        
        for _, row in competitor_forecasts.iterrows():
            if row.get('executive_forecast'):
                predictions.append({
                    "brand": row['brand'],
                    "signal": row['executive_forecast'],
                    "confidence": row.get('promo_confidence', 'LOW'),
                    "timeline": "4 weeks",
                    "impact": row.get('business_impact_weight', 1)
                })
        
        return predictions if predictions else [{"signal": "Market stability expected", "confidence": "MEDIUM", "timeline": "4 weeks"}]
    
    def _generate_recommendation(self, temporal: Dict, forecast: Dict) -> str:
        """Generate actionable recommendation based on temporal and forecast data"""
        
        # High urgency recommendations
        if forecast.get('promotional_change_magnitude', 0) > 0.15:
            return "URGENT: Prepare defensive pricing strategy for competitor promotional surge"
        
        if temporal.get('creative_status') == 'HIGH_FATIGUE_RISK':
            return "IMMEDIATE: Refresh creative assets - high fatigue risk detected"
        
        if temporal.get('momentum_status') == 'DECELERATING' and temporal.get('velocity_change_7d', 0) < -0.2:
            return "CRITICAL: Reverse deceleration trend - increase campaign velocity"
        
        # Strategic recommendations
        if temporal.get('platform_strategy') == 'CONSOLIDATING_CHANNELS':
            return "STRATEGIC: Consider multi-channel expansion for competitive differentiation"
        
        if forecast.get('seasonal_context') in ['BLACK_FRIDAY', 'HOLIDAY_SEASON']:
            return "SEASONAL: Accelerate holiday campaign preparation"
        
        # Default recommendation
        return "MAINTAIN: Continue current strategy with regular monitoring"

if __name__ == "__main__":
    # Example usage
    engine = TemporalIntelligenceEngine(
        project_id="bigquery-ai-kaggle-469620",
        dataset_id="ads_demo",
        run_id="warby_parker_20250910_132446",
        brand="Warby Parker",
        competitors=["EyeBuyDirect", "LensCrafters", "GlassesUSA", "Zenni Optical"]
    )
    
    print("✅ Temporal Intelligence Engine initialized")
    print("\n📊 Capabilities:")
    print("1. Historical Analysis: Where did we come from?")
    print("   - 7-day and 30-day velocity tracking")
    print("   - CTA intensity evolution")
    print("   - Creative fatigue assessment")
    print("   - Platform strategy changes")
    print("\n2. Predictive Forecasting: Where are we going?")
    print("   - Wide Net signal detection (10+ signal types)")
    print("   - Business impact scoring (1-5 scale)")
    print("   - Noise threshold filtering")
    print("   - Executive summaries with 🚨 CRITICAL alerts")
    print("\n3. Integrated 3-Question Framework")
    print("   - Structured output for all intelligence levels")
    print("   - Actionable recommendations")
    print("   - Temporal context for all metrics")