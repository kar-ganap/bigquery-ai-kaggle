#!/usr/bin/env python3
"""
Temporal Intelligence Module
Integrates historical context, current state, and predictive forecasting
Implements the 3-question framework: Where are we? Where did we come from? Where are we going?
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
from datetime import datetime, timedelta
from src.utils.sql_helpers import safe_brand_in_clause
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
        """Generate SQL for comprehensive temporal analysis using real ad timestamps"""
        return f"""
        -- TEMPORAL INTELLIGENCE: Where Did We Come From?
        WITH temporal_movements AS (
          SELECT 
            r.brand,
            -- 7-day velocity windows
            COUNT(CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_last_7d,
            COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 13 DAY) 
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) 
                      THEN 1 END) as ads_prior_7d,
            
            -- 30-day trend analysis
            COUNT(CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
                      THEN 1 END) as ads_last_30d,
            COUNT(CASE WHEN DATE(r.start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
                        AND DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
                      THEN 1 END) as ads_prior_30d,
            
            -- CTA intensity evolution (using brand-level CTA data)
            MAX(c.cta_adoption_rate) as current_cta_adoption_rate,
            MAX(c.high_urgency_ctas) as high_urgency_cta_count,
            MAX(CASE
              WHEN c.dominant_cta_strategy = 'HIGH_URGENCY' THEN 1.0
              WHEN c.dominant_cta_strategy = 'MEDIUM_ENGAGEMENT' THEN 0.7
              WHEN c.dominant_cta_strategy = 'LOW_PRESSURE' THEN 0.3
              ELSE 0.1
            END) as cta_intensity_score,
            
            -- Creative freshness indicators
            AVG(DATE_DIFF(CURRENT_DATE(), DATE(r.start_timestamp), DAY)) as avg_campaign_age,
            COUNT(DISTINCT r.creative_text) / NULLIF(COUNT(*), 0) as creative_diversity_ratio,
            
            -- Platform evolution
            COUNT(DISTINCT CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                               THEN r.publisher_platforms END) as recent_platforms,
            COUNT(DISTINCT CASE WHEN DATE(r.start_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                               THEN r.publisher_platforms END) as historical_platforms,
            
            -- Content type shifts
            AVG(CASE WHEN DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN CASE WHEN r.media_type = 'video' THEN 1.0 ELSE 0.0 END END) as recent_video_pct,
            AVG(CASE WHEN DATE(r.start_timestamp) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                     THEN CASE WHEN r.media_type = 'video' THEN 1.0 ELSE 0.0 END END) as historical_video_pct
            
          FROM `{self.project_id}.{self.dataset_id}.ads_with_dates` r
          LEFT JOIN `{self.project_id}.{self.dataset_id}.cta_aggressiveness_analysis` c
            ON r.brand = c.brand
          WHERE r.brand IN {safe_brand_in_clause(self.brand, self.competitors)}
            AND r.creative_text IS NOT NULL
          GROUP BY r.brand
        )
        SELECT *,
          -- Calculate momentum indicators
          SAFE_DIVIDE(ads_last_7d - ads_prior_7d, NULLIF(ads_prior_7d, 0)) as velocity_change_7d,
          SAFE_DIVIDE(ads_last_30d - ads_prior_30d, NULLIF(ads_prior_30d, 0)) as velocity_change_30d,
          cta_intensity_score as cta_intensity_shift,
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
    
    def generate_bigquery_ai_forecasting_sql(self) -> str:
        """Generate BigQuery AI-first forecasting using AI.FORECAST with TimesFM and AI.GENERATE_TABLE for uncertainty quantification"""
        return f"""
        -- ADVANCED BIGQUERY AI FORECASTING: Where Are We Going?
        WITH time_series_data AS (
          SELECT 
            r.brand,
            DATE(r.start_timestamp) as date,
            
            -- Core metrics for forecasting 
            AVG(COALESCE(c.final_aggressiveness_score, 5.0) / 10.0) as promotional_intensity,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%LIMITED%' OR 
                         UPPER(r.creative_text) LIKE '%TODAY%' OR
                         UPPER(r.creative_text) LIKE '%NOW%'
                    THEN 1.0 ELSE 0.0 END) as urgency_score,
            AVG(CASE WHEN r.media_type = 'video' THEN 1.0 ELSE 0.0 END) as video_pct,
            COUNT(*) as daily_volume
            
          FROM `{self.project_id}.{self.dataset_id}.ads_with_dates` r
          LEFT JOIN `{self.project_id}.{self.dataset_id}.cta_aggressiveness_analysis` c
            ON r.ad_archive_id = c.ad_archive_id
          WHERE r.brand IN {safe_brand_in_clause(self.brand, self.competitors)}
            AND DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
            AND r.creative_text IS NOT NULL
          GROUP BY r.brand, DATE(r.start_timestamp)
          HAVING COUNT(*) >= 1  -- Ensure we have data points
        ),
        -- AI.FORECAST for promotional_intensity with 95% confidence intervals
        promotional_intensity_forecast AS (
          SELECT 
            brand,
            forecast_timestamp,
            forecast_value as forecast_promotional_intensity,
            prediction_interval_lower_bound as promo_lower_95,
            prediction_interval_upper_bound as promo_upper_95,
            ABS(prediction_interval_upper_bound - prediction_interval_lower_bound) / (2 * forecast_value) as promo_uncertainty_ratio
          FROM AI.FORECAST(
            (SELECT date as forecast_timestamp, promotional_intensity as metric_value, brand 
             FROM time_series_data WHERE promotional_intensity IS NOT NULL),
            data_col => 'metric_value',
            timestamp_col => 'forecast_timestamp', 
            id_cols => ['brand'],
            model => 'TimesFM 2.0',
            horizon => 30,
            confidence_level => 0.95
          )
          WHERE DATE(forecast_timestamp) <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
        ),
        -- AI.FORECAST for urgency_score with 95% confidence intervals  
        urgency_forecast AS (
          SELECT 
            brand,
            forecast_timestamp,
            forecast_value as forecast_urgency_score,
            prediction_interval_lower_bound as urgency_lower_95,
            prediction_interval_upper_bound as urgency_upper_95,
            ABS(prediction_interval_upper_bound - prediction_interval_lower_bound) / (2 * forecast_value) as urgency_uncertainty_ratio
          FROM AI.FORECAST(
            (SELECT date as forecast_timestamp, urgency_score as metric_value, brand 
             FROM time_series_data WHERE urgency_score IS NOT NULL),
            data_col => 'metric_value',
            timestamp_col => 'forecast_timestamp',
            id_cols => ['brand'],
            model => 'TimesFM 2.0', 
            horizon => 30,
            confidence_level => 0.95
          )
          WHERE DATE(forecast_timestamp) <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
        ),
        -- AI.FORECAST for video_pct with 80% confidence intervals (higher volatility expected)
        video_forecast AS (
          SELECT 
            brand,
            forecast_timestamp,
            forecast_value as forecast_video_pct,
            prediction_interval_lower_bound as video_lower_80,
            prediction_interval_upper_bound as video_upper_80,
            ABS(prediction_interval_upper_bound - prediction_interval_lower_bound) / (2 * forecast_value) as video_uncertainty_ratio
          FROM AI.FORECAST(
            (SELECT date as forecast_timestamp, video_pct as metric_value, brand 
             FROM time_series_data WHERE video_pct IS NOT NULL),
            data_col => 'metric_value',
            timestamp_col => 'forecast_timestamp',
            id_cols => ['brand'],
            model => 'TimesFM 2.0',
            horizon => 30,
            confidence_level => 0.80  -- Lower confidence for more volatile metric
          )
          WHERE DATE(forecast_timestamp) <= DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
        ),
        -- Combine all forecasts by brand and forecast date
        combined_forecasts AS (
          SELECT 
            COALESCE(p.brand, u.brand, v.brand) as brand,
            COALESCE(p.forecast_timestamp, u.forecast_timestamp, v.forecast_timestamp) as forecast_date,
            
            -- Point forecasts
            p.forecast_promotional_intensity,
            u.forecast_urgency_score, 
            v.forecast_video_pct,
            
            -- Confidence intervals
            p.promo_lower_95,
            p.promo_upper_95,
            u.urgency_lower_95,
            u.urgency_upper_95,
            v.video_lower_80,
            v.video_upper_80,
            
            -- Uncertainty ratios (relative uncertainty)
            p.promo_uncertainty_ratio,
            u.urgency_uncertainty_ratio,
            v.video_uncertainty_ratio
            
          FROM promotional_intensity_forecast p
          FULL OUTER JOIN urgency_forecast u 
            ON p.brand = u.brand AND p.forecast_timestamp = u.forecast_timestamp
          FULL OUTER JOIN video_forecast v 
            ON COALESCE(p.brand, u.brand) = v.brand 
            AND COALESCE(p.forecast_timestamp, u.forecast_timestamp) = v.forecast_timestamp
        ),
        -- Get current baseline metrics for change calculation
        current_metrics AS (
          SELECT 
            brand,
            AVG(promotional_intensity) as current_promotional_intensity,
            AVG(urgency_score) as current_urgency_score,
            AVG(video_pct) as current_video_pct
          FROM time_series_data
          WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
          GROUP BY brand
        ),
        -- Join forecasts with current baselines to compute changes
        forecast_with_changes AS (
          SELECT 
            f.*,
            c.current_promotional_intensity,
            c.current_urgency_score,
            c.current_video_pct,
            
            -- Calculate predicted changes
            (f.forecast_promotional_intensity - c.current_promotional_intensity) as promo_change,
            (f.forecast_urgency_score - c.current_urgency_score) as urgency_change,
            (f.forecast_video_pct - c.current_video_pct) as video_change,
            
            -- Calculate change magnitudes
            ABS(f.forecast_promotional_intensity - c.current_promotional_intensity) as promo_change_magnitude,
            ABS(f.forecast_urgency_score - c.current_urgency_score) as urgency_change_magnitude,
            ABS(f.forecast_video_pct - c.current_video_pct) as video_change_magnitude
            
          FROM combined_forecasts f
          LEFT JOIN current_metrics c ON f.brand = c.brand
          WHERE f.forecast_promotional_intensity IS NOT NULL 
             OR f.forecast_urgency_score IS NOT NULL 
             OR f.forecast_video_pct IS NOT NULL
        ),
        -- Apply statistical significance filtering and uncertainty assessment
        significant_forecasts AS (
          SELECT *,
            -- Overall uncertainty score (0-1, higher = more uncertain)
            (COALESCE(promo_uncertainty_ratio, 0) * 0.4 + 
             COALESCE(urgency_uncertainty_ratio, 0) * 0.3 + 
             COALESCE(video_uncertainty_ratio, 0) * 0.3) as overall_uncertainty_score,
            
            -- Statistical significance test (is predicted change larger than uncertainty?)
            CASE 
              WHEN promo_change_magnitude > (promo_upper_95 - promo_lower_95) / 4 THEN TRUE  -- Change > 1/4 of prediction interval
              WHEN urgency_change_magnitude > (urgency_upper_95 - urgency_lower_95) / 4 THEN TRUE
              WHEN video_change_magnitude > (video_upper_80 - video_lower_80) / 4 THEN TRUE
              ELSE FALSE
            END as statistically_significant,
            
            -- Forecast reliability classification
            CASE 
              WHEN (COALESCE(promo_uncertainty_ratio, 1) * 0.4 + 
                    COALESCE(urgency_uncertainty_ratio, 1) * 0.3 + 
                    COALESCE(video_uncertainty_ratio, 1) * 0.3) <= 0.2 THEN 'HIGH'
              WHEN (COALESCE(promo_uncertainty_ratio, 1) * 0.4 + 
                    COALESCE(urgency_uncertainty_ratio, 1) * 0.3 + 
                    COALESCE(video_uncertainty_ratio, 1) * 0.3) <= 0.4 THEN 'MEDIUM'
              ELSE 'LOW'
            END as forecast_reliability
            
          FROM forecast_with_changes
        ),
        -- Generate business impact weighting and seasonal context  
        prioritized_signals AS (
          SELECT *,
            -- Business impact scoring (1-5 scale)
            CASE 
              WHEN promo_change_magnitude >= 0.15 AND statistically_significant THEN 5  -- Major promotional shift
              WHEN urgency_change_magnitude >= 0.10 AND statistically_significant THEN 4  -- Urgency messaging shift
              WHEN video_change_magnitude >= 0.20 AND statistically_significant THEN 3   -- Creative format shift
              WHEN statistically_significant THEN 2   -- Other significant changes
              ELSE 1  -- Minor or non-significant changes
            END as business_impact_weight,
            
            -- Seasonal context detection
            CASE 
              WHEN EXTRACT(MONTH FROM forecast_date) IN (11, 12) THEN 'HOLIDAY_SEASON'
              WHEN EXTRACT(MONTH FROM forecast_date) = 1 THEN 'NEW_YEAR_RESET'
              WHEN EXTRACT(MONTH FROM forecast_date) IN (3, 4) THEN 'SPRING_REFRESH' 
              WHEN EXTRACT(MONTH FROM forecast_date) IN (8, 9) THEN 'BACK_TO_SCHOOL'
              ELSE 'REGULAR'
            END as seasonal_context,
            
            -- Signal quality assessment for filtering
            CASE 
              WHEN statistically_significant AND forecast_reliability IN ('HIGH', 'MEDIUM') THEN 'ABOVE_THRESHOLD'
              WHEN forecast_reliability = 'HIGH' THEN 'ABOVE_THRESHOLD'  -- Always include high-reliability forecasts
              ELSE 'BELOW_THRESHOLD'
            END as signal_quality,
            
            -- Latest week data for current positioning
            DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), WEEK) as latest_week
            
          FROM significant_forecasts
        ),
        -- AI.GENERATE_TABLE for executive summary generation with uncertainty quantification
        executive_summaries AS (
          SELECT 
            brand,
            AI.GENERATE_TEXT(
              CONCAT(
                'Generate a concise executive forecast summary for ', brand, 
                '. Promotional intensity forecast: ', ROUND(forecast_promotional_intensity, 3),
                ' (current: ', ROUND(current_promotional_intensity, 3), 
                ', change: ', ROUND(promo_change, 3), 
                ', confidence: ', forecast_reliability, '). ',
                'Urgency score forecast: ', ROUND(forecast_urgency_score, 3),
                ' (current: ', ROUND(current_urgency_score, 3), 
                ', change: ', ROUND(urgency_change, 3), '). ',
                'Context: ', seasonal_context, ', Business impact: ', business_impact_weight, '/5. ',
                'RESPOND WITH ONE SENTENCE ONLY describing the key predicted change and business implication.'
              ),
              connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
            ) as executive_forecast
          FROM prioritized_signals 
          WHERE signal_quality = 'ABOVE_THRESHOLD'
          GROUP BY brand, forecast_promotional_intensity, current_promotional_intensity, promo_change, 
                   forecast_urgency_score, current_urgency_score, urgency_change, 
                   seasonal_context, business_impact_weight, forecast_reliability
        ),
        -- Final output with confidence thresholding and signal filtering
        final_forecast_output AS (
          SELECT 
            s.brand,
            s.latest_week,
            s.seasonal_context,
            s.business_impact_weight,
            s.signal_quality,
            e.executive_forecast,
            
            -- Current state metrics
            ROUND(s.current_promotional_intensity, 3) as current_promo,
            ROUND(s.current_urgency_score, 3) as current_urgency,
            ROUND(s.current_video_pct, 3) as current_video,
            
            -- Forecast metrics with uncertainty bounds
            ROUND(s.forecast_promotional_intensity, 3) as forecast_promo,
            ROUND(s.promo_lower_95, 3) as promo_lower_bound,
            ROUND(s.promo_upper_95, 3) as promo_upper_bound,
            ROUND(s.promo_change, 3) as promo_change,
            s.forecast_reliability as promo_confidence,
            
            ROUND(s.forecast_urgency_score, 3) as forecast_urgency,
            ROUND(s.urgency_change, 3) as urgency_change,
            
            ROUND(s.forecast_video_pct, 3) as forecast_video,
            ROUND(s.video_change, 3) as video_change,
            
            -- Statistical metadata
            s.statistically_significant,
            ROUND(s.overall_uncertainty_score, 3) as uncertainty_score
            
          FROM prioritized_signals s
          LEFT JOIN executive_summaries e ON s.brand = e.brand
          WHERE s.signal_quality = 'ABOVE_THRESHOLD'  -- Apply confidence thresholding
             OR s.business_impact_weight >= 4        -- Always include high-impact signals
             OR s.forecast_reliability = 'HIGH'      -- Always include high-confidence forecasts
          ORDER BY 
            CASE s.forecast_reliability WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
            s.overall_uncertainty_score ASC,  -- Lower uncertainty = higher priority
            s.brand = '{self.brand}' DESC     -- Prioritize target brand
        )
        SELECT 
          brand,
          latest_week,
          seasonal_context,
          business_impact_weight,
          signal_quality,
          executive_forecast,
          
          -- Detailed signals for transparency
          current_promo as current_promotional_intensity,
          forecast_promo as forecast_promotional_intensity,
          promo_change as promotional_change_magnitude,
          promo_lower_bound,
          promo_upper_bound,
          promo_confidence,
          
          current_urgency as current_urgency_score,
          forecast_urgency as forecast_urgency_score,
          urgency_change as urgency_change_magnitude,
          
          current_video as current_video_pct,
          forecast_video as forecast_video_pct,
          video_change as video_change_magnitude,
          
          -- Meta information
          statistically_significant,
          uncertainty_score
          
        FROM final_forecast_output
        WHERE signal_quality = 'ABOVE_THRESHOLD'  -- Only surface meaningful signals
          AND executive_forecast IS NOT NULL
        ORDER BY business_impact_weight DESC, brand = '{self.brand}' DESC
        """

    def generate_wide_net_forecasting_sql(self) -> str:
        """
        🎯 Enhanced Wide Net Forecasting Strategy (Best of Both Worlds)

        Combines:
        - Original 4-Tier Distribution Analysis with 5-Factor Ranking System
        - Enhanced CTA Intelligence Integration from brand-level analysis
        - Sophisticated Noise Threshold Filtering & Signal Prioritization
        - Time-series trend analysis with linear projections
        """
        return f"""
        -- ENHANCED WIDE NET FORECASTING: Where Are We Going?
        WITH weekly_aggregations AS (
          SELECT
            r.brand,
            DATE_TRUNC(DATE(r.start_timestamp), WEEK) as week,

            -- Tier 1 Strategic Signals (Core Strategic Labels)
            AVG(r.promotional_intensity) as promotional_intensity,
            AVG(r.urgency_score) as urgency_score,
            AVG(r.brand_voice_score) as brand_voice_score,

            -- Tier 2 Message Angles (Wide Net across 10+ angle types)
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%FEEL%' OR
                         UPPER(r.creative_text) LIKE '%LOVE%' OR
                         UPPER(r.creative_text) LIKE '%CONFIDENCE%'
                    THEN 1.0 ELSE 0.0 END) as emotional_pct,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%SAVE%' OR
                         UPPER(r.creative_text) LIKE '%DISCOUNT%' OR
                         UPPER(r.creative_text) LIKE '%OFF%' OR
                         UPPER(r.creative_text) LIKE '%SALE%'
                    THEN 1.0 ELSE 0.0 END) as promotional_pct,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%LIMITED%' OR
                         UPPER(r.creative_text) LIKE '%HURRY%' OR
                         UPPER(r.creative_text) LIKE '%NOW%' OR
                         UPPER(r.creative_text) LIKE '%TODAY%'
                    THEN 1.0 ELSE 0.0 END) as urgency_pct,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%TRUST%' OR
                         UPPER(r.creative_text) LIKE '%AWARD%' OR
                         UPPER(r.creative_text) LIKE '%CERTIFIED%'
                    THEN 1.0 ELSE 0.0 END) as trust_pct,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%REVIEWS%' OR
                         UPPER(r.creative_text) LIKE '%CUSTOMERS%' OR
                         UPPER(r.creative_text) LIKE '%RATED%'
                    THEN 1.0 ELSE 0.0 END) as social_proof_pct,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%ONLY%' OR
                         UPPER(r.creative_text) LIKE '%LEFT%' OR
                         UPPER(r.creative_text) LIKE '%WHILE%'
                    THEN 1.0 ELSE 0.0 END) as scarcity_pct,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%BENEFIT%' OR
                         UPPER(r.creative_text) LIKE '%IMPROVE%' OR
                         UPPER(r.creative_text) LIKE '%BETTER%'
                    THEN 1.0 ELSE 0.0 END) as benefit_focused_pct,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%FEATURE%' OR
                         UPPER(r.creative_text) LIKE '%TECHNOLOGY%' OR
                         UPPER(r.creative_text) LIKE '%ADVANCED%'
                    THEN 1.0 ELSE 0.0 END) as feature_focused_pct,
            AVG(CASE WHEN UPPER(r.creative_text) LIKE '%DREAM%' OR
                         UPPER(r.creative_text) LIKE '%LUXURY%' OR
                         UPPER(r.creative_text) LIKE '%PREMIUM%'
                    THEN 1.0 ELSE 0.0 END) as aspirational_pct,

            -- Tier 3 Tactical Metrics
            AVG(CASE WHEN r.media_type = 'video' THEN 1.0 ELSE 0.0 END) as video_pct,
            COUNT(DISTINCT r.publisher_platforms) as platform_diversity,

            -- Tier 4 Advanced: CTA Intelligence Integration (Brand-level)
            MAX(c.cta_adoption_rate) / 100.0 as cta_adoption_rate,
            MAX(CASE
              WHEN c.dominant_cta_strategy = 'HIGH_URGENCY' THEN 1.0
              WHEN c.dominant_cta_strategy = 'MEDIUM_ENGAGEMENT' THEN 0.7
              WHEN c.dominant_cta_strategy = 'LOW_PRESSURE' THEN 0.3
              ELSE 0.1
            END) as cta_aggressiveness_score,
            MAX(c.high_urgency_ctas) / NULLIF(MAX(c.total_ads), 0) as high_urgency_cta_ratio,

            -- Message complexity and audience sophistication
            AVG(LENGTH(r.creative_text) / 100.0) as message_complexity,
            COUNT(DISTINCT r.creative_text) / NULLIF(COUNT(*), 0) as creative_diversity

          FROM `{self.project_id}.{self.dataset_id}.ads_with_dates` r
          LEFT JOIN `{self.project_id}.{self.dataset_id}.cta_aggressiveness_analysis` c
            ON r.brand = c.brand
          WHERE r.brand IN {safe_brand_in_clause(self.brand, self.competitors)}
            AND r.creative_text IS NOT NULL
            AND DATE(r.start_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
          GROUP BY r.brand, week
        ),

        signal_detection AS (
          SELECT *,
            -- Time-series trend analysis with linear projections
            (promotional_intensity - LAG(promotional_intensity, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as promo_trend,
            (urgency_score - LAG(urgency_score, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as urgency_trend,
            (video_pct - LAG(video_pct, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as video_trend,
            (cta_aggressiveness_score - LAG(cta_aggressiveness_score, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as cta_trend,

            -- Message angle trend detection
            (emotional_pct - LAG(emotional_pct, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as emotional_trend,
            (promotional_pct - LAG(promotional_pct, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as promotional_angle_trend,
            (social_proof_pct - LAG(social_proof_pct, 4) OVER (PARTITION BY brand ORDER BY week)) / 4 as social_proof_trend

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
            MAX(cta_aggressiveness_score) as current_cta_aggressiveness,
            MAX(emotional_pct) as current_emotional,
            MAX(social_proof_pct) as current_social_proof,

            -- 4-week forecast (enhanced linear projection with CTA context)
            GREATEST(0, LEAST(1, MAX(promotional_intensity) + (4 * AVG(promo_trend)))) as forecast_promo,
            GREATEST(0, LEAST(1, MAX(urgency_score) + (4 * AVG(urgency_trend)))) as forecast_urgency,
            GREATEST(0, LEAST(1, MAX(video_pct) + (4 * AVG(video_trend)))) as forecast_video,
            GREATEST(0, LEAST(1, MAX(cta_aggressiveness_score) + (4 * AVG(cta_trend)))) as forecast_cta_aggressiveness,

            -- Calculate magnitude of predicted changes (15%+ distribution shift = high signal)
            ABS(MAX(promotional_intensity) + (4 * AVG(promo_trend)) - MAX(promotional_intensity)) as promo_change,
            ABS(MAX(urgency_score) + (4 * AVG(urgency_trend)) - MAX(urgency_score)) as urgency_change,
            ABS(MAX(video_pct) + (4 * AVG(video_trend)) - MAX(video_pct)) as video_change,
            ABS(MAX(cta_aggressiveness_score) + (4 * AVG(cta_trend)) - MAX(cta_aggressiveness_score)) as cta_change,
            ABS(MAX(emotional_pct) + (4 * AVG(emotional_trend)) - MAX(emotional_pct)) as emotional_change,

            -- Statistical significance (HIGH/MEDIUM/LOW confidence based on trend consistency)
            CASE
              WHEN STDDEV(promo_trend) < 0.02 THEN 'HIGH'
              WHEN STDDEV(promo_trend) < 0.04 THEN 'MEDIUM'
              ELSE 'LOW'
            END as promo_confidence,

            CASE
              WHEN STDDEV(cta_trend) < 0.03 THEN 'HIGH'
              WHEN STDDEV(cta_trend) < 0.06 THEN 'MEDIUM'
              ELSE 'LOW'
            END as cta_confidence,

            -- Seasonal context for timing criticality
            CASE
              WHEN EXTRACT(MONTH FROM MAX(week)) IN (11, 12) THEN 'HOLIDAY_SEASON'
              WHEN EXTRACT(MONTH FROM MAX(week)) = 11 AND EXTRACT(DAY FROM MAX(week)) > 20 THEN 'BLACK_FRIDAY'
              WHEN EXTRACT(MONTH FROM MAX(week)) IN (6, 7) THEN 'SUMMER_SALES'
              WHEN EXTRACT(MONTH FROM MAX(week)) IN (1, 2) THEN 'NEW_YEAR_RESET'
              ELSE 'REGULAR'
            END as seasonal_context

          FROM signal_detection
          WHERE week >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK)
          GROUP BY brand
        ),

        prioritized_signals AS (
          SELECT *,
            -- 🧠 Enhanced 5-Factor Ranking System (1-5 scale) with CTA Intelligence
            CASE
              -- Factor 1: Magnitude + Factor 2: Statistical Significance
              WHEN promo_change >= 0.15 AND promo_confidence = 'HIGH' THEN 5  -- 15%+ distribution shift with high confidence
              WHEN cta_change >= 0.20 AND cta_confidence = 'HIGH' THEN 5      -- CTA strategy major shift

              -- Factor 3: Business Impact Weight (promotional_intensity = 5, CTA = 4, platform = 3)
              WHEN promo_change >= 0.10 AND forecast_promo >= 0.60 THEN 4      -- High promotional intensity threshold
              WHEN urgency_change >= 0.15 THEN 4                              -- Urgency escalation
              WHEN cta_change >= 0.12 AND seasonal_context IN ('BLACK_FRIDAY', 'HOLIDAY_SEASON') THEN 4  -- CTA + seasonal

              -- Factor 4: Competitive Uniqueness + Factor 5: Timing Criticality
              WHEN video_change >= 0.20 THEN 3                                -- Media type shifts ≥20%
              WHEN emotional_change >= 0.15 AND seasonal_context != 'REGULAR' THEN 3  -- Message angle pivots ≥15%
              WHEN promo_change >= 0.08 OR video_change >= 0.15 OR cta_change >= 0.10 THEN 2  -- Moderate changes
              ELSE 1
            END AS business_impact_weight,

            -- 🎛️ Noise Threshold Filtering (Customer-facing output requirements)
            CASE
              WHEN promo_change >= 0.10 OR                    -- Promotional intensity changes ≥10%
                   urgency_change >= 0.10 OR                  -- Brand voice positioning shifts ≥10%
                   video_change >= 0.20 OR                    -- Media type shifts ≥20%
                   emotional_change >= 0.15 OR                -- Message angle pivots ≥15%
                   cta_change >= 0.12                         -- CTA strategy changes ≥12%
              THEN 'ABOVE_THRESHOLD'
              ELSE 'BELOW_THRESHOLD'
            END AS signal_quality,

            -- Competitive uniqueness (brand diverging from competitor average)
            ABS(current_promo - AVG(current_promo) OVER()) as competitive_uniqueness_promo,
            ABS(current_cta_aggressiveness - AVG(current_cta_aggressiveness) OVER()) as competitive_uniqueness_cta,

            -- Executive Summary Generation with CTA Intelligence
            CASE
              WHEN promo_change >= 0.15 AND promo_confidence = 'HIGH' AND seasonal_context = 'BLACK_FRIDAY'
              THEN CONCAT('🚨 CRITICAL: Black Friday surge predicted - ', brand,
                         ' (+', CAST(ROUND(promo_change * 100) AS STRING), '% promotional intensity, CTA aggressiveness: ',
                         CAST(ROUND(current_cta_aggressiveness * 100) AS STRING), '%)')

              WHEN cta_change >= 0.20 AND promo_change >= 0.10
              THEN CONCAT('🚨 CRITICAL: Dual strategy shift - ', brand,
                         ' (CTA: +', CAST(ROUND(cta_change * 100) AS STRING), '%, Promo: +',
                         CAST(ROUND(promo_change * 100) AS STRING), '%)')

              WHEN promo_change >= 0.15 AND promo_confidence = 'HIGH'
              THEN CONCAT('🚨 CRITICAL: Major promotional escalation - ', brand,
                         ' (+', CAST(ROUND(promo_change * 100) AS STRING), '%)')

              WHEN video_change >= 0.20 AND cta_change >= 0.10
              THEN CONCAT('⚠️ MULTI-CHANNEL: Video + CTA strategy pivot - ', brand,
                         ' (Video: +', CAST(ROUND(video_change * 100) AS STRING), '%, CTA: +',
                         CAST(ROUND(cta_change * 100) AS STRING), '%)')

              WHEN urgency_change >= 0.15
              THEN CONCAT('📊 MODERATE: Urgency messaging escalation - ', brand,
                         ' (+', CAST(ROUND(urgency_change * 100) AS STRING), '%)')

              WHEN cta_change >= 0.12
              THEN CONCAT('🎯 TACTICAL: CTA strategy adjustment - ', brand,
                         ' (+', CAST(ROUND(cta_change * 100) AS STRING), '% aggressiveness)')

              ELSE CONCAT('🔄 STABLE: Baseline competitive positioning - ', brand)
            END AS executive_summary

          FROM forecasting
        )

        SELECT
          brand,
          latest_week,
          seasonal_context,
          business_impact_weight,
          signal_quality,
          executive_summary,

          -- Enhanced detailed signals for transparency (Tier 1-4 metrics)
          ROUND(current_promo, 3) as current_promotional_intensity,
          ROUND(forecast_promo, 3) as forecast_promotional_intensity,
          ROUND(promo_change, 3) as promotional_change_magnitude,
          promo_confidence,

          ROUND(current_urgency, 3) as current_urgency_score,
          ROUND(forecast_urgency, 3) as forecast_urgency_score,
          ROUND(urgency_change, 3) as urgency_change_magnitude,

          ROUND(current_video, 3) as current_video_pct,
          ROUND(forecast_video, 3) as forecast_video_pct,
          ROUND(video_change, 3) as video_change_magnitude,

          -- CTA Intelligence metrics
          ROUND(current_cta_aggressiveness, 3) as current_cta_aggressiveness,
          ROUND(forecast_cta_aggressiveness, 3) as forecast_cta_aggressiveness,
          ROUND(cta_change, 3) as cta_change_magnitude,
          cta_confidence,

          -- Message angle metrics
          ROUND(current_emotional, 3) as current_emotional_pct,
          ROUND(current_social_proof, 3) as current_social_proof_pct,

          -- Competitive positioning
          ROUND(competitive_uniqueness_promo, 3) as competitive_uniqueness_promo,
          ROUND(competitive_uniqueness_cta, 3) as competitive_uniqueness_cta,

          -- Meta analysis
          CASE WHEN business_impact_weight >= 2 THEN TRUE ELSE FALSE END as statistically_significant,
          ROUND(1.0 - (business_impact_weight / 5.0), 2) as uncertainty_score

        FROM prioritized_signals
        WHERE signal_quality = 'ABOVE_THRESHOLD'  -- Only surface meaningful signals (≥2 business impact weight)
          AND executive_summary IS NOT NULL
        ORDER BY business_impact_weight DESC, brand = '{self.brand}' DESC
        LIMIT 5  -- Top 5 signals per brand maximum to avoid information overload
        """
