#!/usr/bin/env python3
"""
Channel Performance Intelligence Module - Phase 8 P0 Implementation  
Analyzes publisher platforms, delivery timing, and channel effectiveness
Utilizes publisher_platforms and delivery timing data from ads_with_dates
"""

from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import os

# Global BigQuery constants - consistent with main pipeline
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

@dataclass
class ChannelInsight:
    """Channel performance analysis insight"""
    platform: str  # Facebook, Instagram, Audience_Network, etc.
    brand: str
    campaign_intensity: float  # 0-1 scale of usage
    performance_efficiency: float  # Spend/impression ratio analysis
    temporal_strategy: str  # STEADY, BURST, SEASONAL, OPPORTUNISTIC
    competitive_positioning: float  # 0-1 relative to competitors
    audience_alignment: str  # BROAD, TARGETED, NICHE
    optimization_potential: float  # 0-1 improvement opportunity
    strategic_recommendations: List[str]

@dataclass
class TimingPattern:
    """Temporal advertising pattern analysis"""
    pattern_type: str  # WEEKDAY_FOCUS, WEEKEND_PUSH, CONTINUOUS, BURST
    peak_hours: List[int]  # Hours of peak activity
    seasonal_variations: Dict[str, float]  # Month -> intensity
    campaign_duration: str  # SHORT, MEDIUM, LONG, ALWAYS_ON
    competitive_timing: str  # EARLY_MOVER, FOLLOWER, CONTRARIAN

class ChannelPerformanceEngine:
    """Advanced channel and timing analysis using BigQuery ML"""
    
    def __init__(self, project_id: str = BQ_PROJECT, dataset_id: str = BQ_DATASET):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.logger = logging.getLogger(__name__)
        
    def create_channel_analysis_sql(self, run_id: str, brands: List[str]) -> str:
        """Generate SQL for comprehensive channel performance analysis"""
        brands_filter = "', '".join(brands)
        
        return f"""
        -- Channel Performance Intelligence - Phase 8 Implementation
        CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.channel_performance_{run_id}` AS
        
        WITH platform_extraction AS (
          SELECT 
            ad_archive_id,
            brand,
            creative_text,
            start_date_string,
            end_date_string,
            impressions_lower,
            impressions_upper,
            spend_lower,
            spend_upper,
            currency,
            
            -- Extract individual platforms from array
            platform,
            -- Calculate campaign metrics
            CASE
              WHEN start_date_string IS NOT NULL AND end_date_string IS NOT NULL 
              THEN DATE_DIFF(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', end_date_string), SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string), DAY)
              WHEN start_date_string IS NOT NULL
              THEN DATE_DIFF(CURRENT_TIMESTAMP(), SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string), DAY)
              ELSE 0
            END as campaign_duration_days,
            COALESCE(CAST(impressions_upper AS INT64), CAST(impressions_lower AS INT64), 1000) as estimated_impressions,
            COALESCE(CAST(spend_upper AS FLOAT64), CAST(spend_lower AS FLOAT64), 100.0) as estimated_spend,
            
            -- Time-based features  
            CASE
              WHEN start_date_string IS NOT NULL 
              THEN EXTRACT(MONTH FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))
              ELSE 1
            END as start_month,
            CASE
              WHEN start_date_string IS NOT NULL 
              THEN EXTRACT(DAYOFWEEK FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))
              ELSE 1
            END as start_day_of_week,
            CASE
              WHEN start_date_string IS NOT NULL 
              THEN EXTRACT(HOUR FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))
              ELSE 0
            END as start_hour,
            CASE
              WHEN start_date_string IS NOT NULL 
              THEN EXTRACT(QUARTER FROM SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', start_date_string))
              ELSE 1
            END as start_quarter,
            
            -- Platform categorization
            CASE 
              WHEN platform LIKE '%facebook%' OR platform LIKE '%Facebook%' THEN 'Facebook'
              WHEN platform LIKE '%instagram%' OR platform LIKE '%Instagram%' THEN 'Instagram' 
              WHEN platform LIKE '%audience%' OR platform LIKE '%Audience%' THEN 'Audience_Network'
              WHEN platform LIKE '%messenger%' OR platform LIKE '%Messenger%' THEN 'Messenger'
              ELSE 'Other_Platform'
            END as platform_category
            
          FROM (
            SELECT *,
              -- Parse publisher_platforms string to extract individual platforms
              CASE 
                WHEN publisher_platforms LIKE '%Facebook%' THEN 'Facebook'
                WHEN publisher_platforms LIKE '%Instagram%' THEN 'Instagram'
                WHEN publisher_platforms LIKE '%Audience%' THEN 'Audience_Network'
                WHEN publisher_platforms LIKE '%Messenger%' THEN 'Messenger'
                ELSE COALESCE(publisher_platforms, 'Other_Platform')
              END as platform
            FROM `{self.project_id}.{self.dataset_id}.ads_raw_{run_id}`
          )
          WHERE brand IN ('{brands_filter}')
            AND start_date_string IS NOT NULL
            AND publisher_platforms IS NOT NULL
        ),
        
        channel_ml_analysis AS (
          SELECT 
            *,
            -- Channel strategy classification using AI.GENERATE
            AI.GENERATE(
              CONCAT(
                'Analyze this ', brand, ' ad channel strategy. ',
                'Platform: ', platform_category, '. ',
                'Campaign Duration: ', CAST(campaign_duration_days AS STRING), ' days. ',
                'Start Month: ', CAST(start_month AS STRING), '. ',
                'Estimated Impressions: ', CAST(estimated_impressions AS STRING), '. ',
                'What is the primary channel strategy? ',
                'Return ONLY one of: BROAD_REACH, TARGETED_PRECISION, COST_EFFICIENT, PREMIUM_PLACEMENT'
              ),
              connection_id => '{self.project_id}.us.vertex-ai'
            ) as channel_strategy_raw,
            
            -- Temporal pattern analysis using AI.GENERATE
            AI.GENERATE(
              CONCAT(
                'Analyze the temporal pattern of this ad campaign. ',
                'Start: Month ', CAST(start_month AS STRING), ', Day ', CAST(start_day_of_week AS STRING), ', Hour ', CAST(start_hour AS STRING), '. ',
                'Duration: ', CAST(campaign_duration_days AS STRING), ' days. ',
                'What timing strategy does this represent? ',
                'Return ONLY one of: STEADY_CONTINUOUS, BURST_CAMPAIGN, WEEKEND_FOCUSED, WEEKDAY_BUSINESS, SEASONAL_PUSH'
              ),
              connection_id => '{self.project_id}.us.vertex-ai'
            ) as temporal_pattern_raw,
            
            -- Audience targeting assessment using AI.GENERATE
            AI.GENERATE(
              CONCAT(
                'Based on this ad placement and content, assess audience targeting approach. ',
                'Platform: ', platform_category, '. ',
                'Content preview: "', SUBSTR(COALESCE(creative_text, ''), 1, 200), '". ',
                'Impressions: ', CAST(estimated_impressions AS STRING), '. ',
                'What targeting strategy is likely? ',
                'Return ONLY one of: BROAD_MASS_MARKET, DEMOGRAPHIC_TARGETED, INTEREST_BASED, BEHAVIORAL_PRECISE, LOOKALIKE_EXPANSION'
              ),
              connection_id => '{self.project_id}.us.vertex-ai'
            ) as targeting_strategy_raw,
            
            -- Campaign intensity scoring using AI.GENERATE_DOUBLE
            AI.GENERATE_DOUBLE(
              CONCAT(
                'Rate the campaign intensity/aggressiveness of this ad. ',
                'Impressions: ', CAST(estimated_impressions AS STRING), '. ',
                'Spend: ', CAST(estimated_spend AS STRING), '. ',
                'Duration: ', CAST(campaign_duration_days AS STRING), ' days. ',
                'Platform: ', platform_category, '. ',
                'Consider spend velocity and impression targets. ',
                'Return ONLY a score from 0.0 to 1.0 (e.g., 0.7)'
              ),
              connection_id => '{self.project_id}.us.vertex-ai'
            ) as campaign_intensity_raw,
            
            -- Performance efficiency prediction using AI.GENERATE_DOUBLE
            AI.GENERATE_DOUBLE(
              CONCAT(
                'Predict the performance efficiency of this channel strategy. ',
                'Platform: ', platform_category, '. ',
                'Strategy: Based on impressions ', CAST(estimated_impressions AS STRING), ' and spend ', CAST(estimated_spend AS STRING), '. ',
                'Content type: "', SUBSTR(COALESCE(creative_text, ''), 1, 100), '". ',
                'Rate likely cost-effectiveness and engagement. ',
                'Return ONLY a score from 0.0 to 1.0 (e.g., 0.6)'
              ),
              connection_id => '{self.project_id}.us.vertex-ai'
            ) as performance_efficiency_raw
            
          FROM platform_extraction
        ),
        
        channel_metrics AS (
          SELECT 
            *,
            -- Clean and validate ML outputs
            CASE 
              WHEN UPPER(TRIM(channel_strategy_raw.result)) IN ('BROAD_REACH', 'TARGETED_PRECISION', 'COST_EFFICIENT', 'PREMIUM_PLACEMENT')
              THEN UPPER(TRIM(channel_strategy_raw.result))
              ELSE 'BROAD_REACH'  -- Default
            END as channel_strategy_final,
            
            CASE 
              WHEN UPPER(TRIM(temporal_pattern_raw.result)) IN ('STEADY_CONTINUOUS', 'BURST_CAMPAIGN', 'WEEKEND_FOCUSED', 'WEEKDAY_BUSINESS', 'SEASONAL_PUSH')
              THEN UPPER(TRIM(temporal_pattern_raw.result))
              ELSE 'STEADY_CONTINUOUS'  -- Default
            END as temporal_pattern_final,
            
            CASE 
              WHEN UPPER(TRIM(targeting_strategy_raw.result)) IN ('BROAD_MASS_MARKET', 'DEMOGRAPHIC_TARGETED', 'INTEREST_BASED', 'BEHAVIORAL_PRECISE', 'LOOKALIKE_EXPANSION')
              THEN UPPER(TRIM(targeting_strategy_raw.result))
              ELSE 'DEMOGRAPHIC_TARGETED'  -- Default
            END as targeting_strategy_final,
            
            -- Convert string scores to validated floats
            LEAST(1.0, GREATEST(0.0,
              COALESCE(SAFE_CAST(campaign_intensity_raw.result AS FLOAT64), 0.5)
            )) as campaign_intensity_score,
            
            LEAST(1.0, GREATEST(0.0,
              COALESCE(SAFE_CAST(performance_efficiency_raw.result AS FLOAT64), 0.5)
            )) as performance_efficiency_score,
            
            -- Calculate spend efficiency
            CASE 
              WHEN estimated_impressions > 0 AND estimated_spend > 0 
              THEN LEAST(1.0, (estimated_impressions / estimated_spend) / 1000.0)  -- Normalize to 0-1
              ELSE 0.5  -- Default efficiency
            END as spend_efficiency,
            
            -- Temporal efficiency scoring
            CASE 
              WHEN campaign_duration_days BETWEEN 1 AND 7 THEN 0.9  -- Short burst
              WHEN campaign_duration_days BETWEEN 8 AND 30 THEN 0.8  -- Medium campaign
              WHEN campaign_duration_days BETWEEN 31 AND 90 THEN 0.7  -- Long campaign  
              ELSE 0.6  -- Always-on
            END as duration_efficiency
            
          FROM channel_ml_analysis
        ),
        
        competitive_benchmarking AS (
          SELECT 
            *,
            -- Competitive channel positioning using AI.GENERATE
            AI.GENERATE(
              CONCAT(
                'How does this ', brand, ' channel strategy differentiate in the eyewear market? ',
                'Platform: ', platform_category, '. ',
                'Strategy: ', channel_strategy_final, '. ',
                'Timing: ', temporal_pattern_final, '. ',
                'What makes this approach unique vs competitors? ',
                'Return a brief competitive advantage (e.g., "Early mobile adopter", "Cross-platform synergy", "Precise retargeting")'
              ),
              connection_id => '{self.project_id}.us.vertex-ai'
            ) as competitive_advantage,
            
            -- Channel optimization recommendations using AI.GENERATE
            AI.GENERATE(
              CONCAT(
                'Suggest 3 channel optimization recommendations for this campaign. ',
                'Current: Platform ', platform_category, ', Strategy ', channel_strategy_final, ', Pattern ', temporal_pattern_final, '. ',
                'Performance: Intensity ', CAST(COALESCE(campaign_intensity_score, 0.5) AS STRING), ', Efficiency ', CAST(COALESCE(performance_efficiency_score, 0.5) AS STRING), '. ',
                'Focus on actionable channel and timing improvements. ',
                'Return as comma-separated list (e.g., "Test Instagram Stories, Optimize weekend timing, A/B test audiences")'
              ),
              connection_id => '{self.project_id}.us.vertex-ai'
            ) as optimization_recommendations
            
          FROM channel_metrics
        )
        
        SELECT 
          ad_archive_id,
          brand,
          platform_category as channel,
          start_date_string,
          end_date_string,
          estimated_impressions,
          estimated_spend,
          campaign_duration_days,
          
          -- Channel Intelligence Metrics
          channel_strategy_final as channel_strategy,
          temporal_pattern_final as temporal_pattern,
          targeting_strategy_final as targeting_strategy,
          
          -- Performance Scores
          COALESCE(campaign_intensity_score, 0.5) as campaign_intensity,
          COALESCE(performance_efficiency_score, 0.5) as performance_efficiency,
          spend_efficiency,
          duration_efficiency,
          
          -- Strategic Insights
          TRIM(competitive_advantage.result) as competitive_advantage,
          TRIM(optimization_recommendations.result) as optimization_recommendations,
          
          -- Temporal Features
          start_month,
          start_day_of_week,
          start_hour,
          start_quarter,
          
          -- Meta Information
          CURRENT_TIMESTAMP() as analysis_timestamp,
          '{run_id}' as run_id
          
        FROM competitive_benchmarking
        ORDER BY brand, channel, performance_efficiency DESC;
        """
        
    def create_timing_patterns_sql(self, run_id: str) -> str:
        """Generate SQL for temporal pattern analysis across brands"""
        return f"""
        -- Channel Timing Pattern Analysis - Cross-Brand Intelligence
        CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.timing_patterns_{run_id}` AS
        
        WITH temporal_aggregation AS (
          SELECT 
            brand,
            channel,
            temporal_pattern,
            start_month,
            start_day_of_week,
            start_hour,
            start_quarter,
            COUNT(*) as campaign_count,
            AVG(performance_efficiency) as avg_performance,
            AVG(campaign_intensity) as avg_intensity,
            AVG(spend_efficiency) as avg_spend_efficiency,
            SUM(estimated_impressions) as total_impressions,
            SUM(estimated_spend) as total_spend
          FROM `{self.project_id}.{self.dataset_id}.channel_performance_{run_id}`
          GROUP BY brand, channel, temporal_pattern, start_month, start_day_of_week, start_hour, start_quarter
        ),
        
        pattern_effectiveness AS (
          SELECT 
            temporal_pattern,
            channel,
            COUNT(DISTINCT brand) as brands_using_pattern,
            COUNT(*) as total_campaigns,
            AVG(avg_performance) as pattern_performance,
            AVG(avg_intensity) as pattern_intensity,
            AVG(avg_spend_efficiency) as pattern_spend_efficiency,
            STDDEV(avg_performance) as performance_consistency,
            
            -- Peak timing analysis
            -- Use APPROX_TOP_COUNT for most frequent timing patterns
            APPROX_TOP_COUNT(start_hour, 1)[OFFSET(0)].value as peak_hour,
            APPROX_TOP_COUNT(start_day_of_week, 1)[OFFSET(0)].value as peak_day,
            APPROX_TOP_COUNT(start_month, 1)[OFFSET(0)].value as peak_month,
            
            -- Calculate pattern prevalence
            COUNT(*) / (SELECT COUNT(*) FROM temporal_aggregation) as market_share,
            
            -- Seasonal strength
            STDDEV(CASE WHEN start_quarter = 1 THEN avg_performance END) as q1_variance,
            STDDEV(CASE WHEN start_quarter = 2 THEN avg_performance END) as q2_variance,
            STDDEV(CASE WHEN start_quarter = 3 THEN avg_performance END) as q3_variance,
            STDDEV(CASE WHEN start_quarter = 4 THEN avg_performance END) as q4_variance
            
          FROM temporal_aggregation
          GROUP BY temporal_pattern, channel
        ),
        
        timing_recommendations AS (
          SELECT 
            *,
            -- Timing opportunity score
            CASE 
              WHEN pattern_performance > 0.7 AND market_share < 0.3 THEN 0.9  -- High performance, low adoption
              WHEN pattern_performance > 0.6 AND market_share < 0.5 THEN 0.7  -- Good performance, moderate adoption
              WHEN pattern_performance > 0.5 AND performance_consistency < 0.1 THEN 0.6  -- Consistent results
              ELSE 0.3  -- Saturated or poor performing
            END as timing_opportunity_score,
            
            -- Seasonal reliability
            CASE 
              WHEN COALESCE(q1_variance, 0) + COALESCE(q2_variance, 0) + COALESCE(q3_variance, 0) + COALESCE(q4_variance, 0) < 0.2 THEN 'YEAR_ROUND_STABLE'
              WHEN COALESCE(q4_variance, 0) > 0.3 THEN 'HOLIDAY_SENSITIVE'  
              WHEN COALESCE(q3_variance, 0) > 0.3 THEN 'BACK_TO_SCHOOL_PEAK'
              WHEN COALESCE(q2_variance, 0) > 0.3 THEN 'SPRING_SURGE'
              ELSE 'WINTER_STRONG'
            END as seasonal_pattern
            
          FROM pattern_effectiveness
        )
        
        SELECT 
          temporal_pattern as pattern_name,
          channel,
          brands_using_pattern,
          total_campaigns,
          ROUND(pattern_performance, 3) as avg_performance,
          ROUND(pattern_intensity, 3) as avg_intensity,
          ROUND(pattern_spend_efficiency, 3) as avg_spend_efficiency,
          ROUND(performance_consistency, 3) as consistency_score,
          ROUND(market_share, 3) as market_adoption,
          ROUND(timing_opportunity_score, 3) as opportunity_score,
          peak_hour,
          peak_day,  
          peak_month,
          seasonal_pattern,
          CURRENT_TIMESTAMP() as analysis_timestamp,
          '{run_id}' as run_id
        FROM timing_recommendations
        ORDER BY opportunity_score DESC, avg_performance DESC;
        """
        
    def create_channel_summary_sql(self, run_id: str) -> str:
        """Generate executive summary of channel performance intelligence"""
        return f"""
        -- Channel Performance Executive Summary
        CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.v_channel_performance_summary_{run_id}` AS
        
        WITH brand_channel_profiles AS (
          SELECT 
            brand,
            COUNT(DISTINCT channel) as channels_used,
            COUNT(*) as total_campaigns,
            
            -- Channel distribution
            COUNT(CASE WHEN channel = 'Facebook' THEN 1 END) / COUNT(*) as facebook_share,
            COUNT(CASE WHEN channel = 'Instagram' THEN 1 END) / COUNT(*) as instagram_share,
            COUNT(CASE WHEN channel = 'Audience_Network' THEN 1 END) / COUNT(*) as audience_network_share,
            
            -- Strategy preferences
            -- Use APPROX_TOP_COUNT for dominant channel strategy
            APPROX_TOP_COUNT(channel_strategy, 1)[OFFSET(0)].value as primary_channel_strategy,
            CASE APPROX_TOP_COUNT(channel_strategy, 1)[OFFSET(0)].value
              WHEN 'BROAD_REACH' THEN 1 WHEN 'TARGETED_PRECISION' THEN 2
              WHEN 'COST_EFFICIENT' THEN 3 ELSE 4 END as primary_strategy,
            -- Use APPROX_TOP_COUNT for dominant temporal pattern
            APPROX_TOP_COUNT(temporal_pattern, 1)[OFFSET(0)].value as primary_temporal_pattern,
            CASE APPROX_TOP_COUNT(temporal_pattern, 1)[OFFSET(0)].value
              WHEN 'STEADY_CONTINUOUS' THEN 1 WHEN 'BURST_CAMPAIGN' THEN 2 ELSE 3 END as primary_timing,
            
            -- Performance metrics
            AVG(performance_efficiency) as avg_channel_performance,
            AVG(campaign_intensity) as avg_campaign_intensity,
            AVG(spend_efficiency) as avg_spend_efficiency,
            AVG(duration_efficiency) as avg_duration_efficiency,
            
            -- Investment metrics
            SUM(estimated_spend) as total_ad_spend,
            SUM(estimated_impressions) as total_impressions,
            AVG(campaign_duration_days) as avg_campaign_length,
            
            -- Temporal insights
            COUNT(CASE WHEN start_day_of_week IN (1,2,3,4,5) THEN 1 END) / COUNT(*) as weekday_focus,
            COUNT(CASE WHEN start_day_of_week IN (6,7) THEN 1 END) / COUNT(*) as weekend_focus
            
          FROM `{self.project_id}.{self.dataset_id}.channel_performance_{run_id}`
          GROUP BY brand
        ),
        
        competitive_rankings AS (
          SELECT 
            brand,
            -- Performance rankings
            RANK() OVER (ORDER BY avg_channel_performance DESC) as performance_rank,
            RANK() OVER (ORDER BY avg_spend_efficiency DESC) as efficiency_rank,
            RANK() OVER (ORDER BY channels_used DESC) as diversification_rank,
            RANK() OVER (ORDER BY total_impressions DESC) as reach_rank,
            
            -- Calculate overall channel excellence
            (avg_channel_performance * 0.4 + avg_spend_efficiency * 0.3 + 
             avg_campaign_intensity * 0.2 + avg_duration_efficiency * 0.1) as channel_excellence_score
             
          FROM brand_channel_profiles
        )
        
        SELECT 
          p.brand,
          p.channels_used,
          p.total_campaigns,
          
          -- Channel mix
          ROUND(p.facebook_share * 100, 1) as facebook_percentage,
          ROUND(p.instagram_share * 100, 1) as instagram_percentage,
          ROUND(p.audience_network_share * 100, 1) as audience_network_percentage,
          
          -- Strategic profile
          p.primary_channel_strategy as dominant_strategy,
          p.primary_temporal_pattern as dominant_timing,
          ROUND(p.weekday_focus * 100, 1) as weekday_focus_pct,
          
          -- Performance metrics
          ROUND(p.avg_channel_performance, 3) as channel_performance,
          ROUND(p.avg_spend_efficiency, 3) as spend_efficiency,
          ROUND(p.avg_campaign_intensity, 3) as campaign_intensity,
          ROUND(p.avg_campaign_length, 1) as avg_campaign_days,
          
          -- Investment scale
          CAST(p.total_ad_spend AS INT64) as total_spend,
          CAST(p.total_impressions AS INT64) as total_impressions,
          
          -- Competitive positioning
          r.performance_rank,
          r.efficiency_rank,
          r.diversification_rank,
          ROUND(r.channel_excellence_score, 3) as overall_channel_score,
          
          CASE 
            WHEN r.channel_excellence_score >= 0.8 THEN 'CHANNEL_LEADER'
            WHEN r.channel_excellence_score >= 0.6 THEN 'STRONG_CHANNEL_MIX'
            WHEN r.channel_excellence_score >= 0.4 THEN 'AVERAGE_PERFORMANCE'
            ELSE 'CHANNEL_OPPORTUNITY'
          END as channel_tier
          
        FROM brand_channel_profiles p
        JOIN competitive_rankings r ON p.brand = r.brand
        ORDER BY r.channel_excellence_score DESC;
        """
        
    def execute_channel_performance_analysis(self, run_id: str, brands: List[str]) -> Dict[str, any]:
        """Execute complete channel performance analysis"""
        try:
            from src.utils.bigquery_client import run_query
            
            self.logger.info(f"📺 Starting Channel Performance Analysis for {len(brands)} brands")
            
            # Step 1: Core channel analysis
            channel_sql = self.create_channel_analysis_sql(run_id, brands)
            self.logger.info("   Analyzing channel strategies and timing patterns...")
            run_query(channel_sql)
            
            # Step 2: Timing pattern analysis
            timing_sql = self.create_timing_patterns_sql(run_id)
            self.logger.info("   Identifying optimal timing patterns...")
            run_query(timing_sql)
            
            # Step 3: Executive summary
            summary_sql = self.create_channel_summary_sql(run_id)
            self.logger.info("   Generating channel performance summary...")
            run_query(summary_sql)
            
            self.logger.info("✅ Channel Performance Analysis completed successfully")
            
            return {
                'status': 'success',
                'analysis_type': 'channel_performance',
                'run_id': run_id,
                'brands_analyzed': brands,
                'tables_created': [
                    f'channel_performance_{run_id}',
                    f'timing_patterns_{run_id}',
                    f'v_channel_performance_summary_{run_id}'
                ],
                'insights': [
                    'Platform distribution analysis',
                    'Channel strategy classification',
                    'Temporal pattern optimization',
                    'Audience targeting assessment',
                    'Campaign intensity scoring',
                    'Performance efficiency benchmarking',
                    'Competitive channel positioning',
                    'Timing opportunity identification'
                ]
            }
            
        except Exception as e:
            self.logger.error(f"❌ Channel Performance Analysis failed: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'analysis_type': 'channel_performance'
            }

def main():
    """Test Channel Performance module"""
    engine = ChannelPerformanceEngine()
    
    test_brands = ['Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical']
    test_run_id = f"channel_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    result = engine.execute_channel_performance_analysis(test_run_id, test_brands)
    print(f"Channel Performance Test Result: {result}")

if __name__ == "__main__":
    main()