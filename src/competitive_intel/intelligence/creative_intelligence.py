#!/usr/bin/env python3
"""
Creative Intelligence Module - Phase 8 P0 Implementation
Analyzes visual creative content using existing ML.GENERATE_TEXT infrastructure
Leverages white space detection patterns for comprehensive creative analysis
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
class CreativeInsight:
    """Structured creative analysis insight"""
    creative_type: str  # TEXT_HEAVY, VISUAL_FOCUS, BALANCED, MINIMAL
    emotional_appeal: str  # JOY, TRUST, FEAR, SURPRISE, DESIRE, URGENCY
    visual_hierarchy: str  # HEADLINE_DOMINANT, CTA_PROMINENT, BRAND_FOCUSED, PRODUCT_CENTERED
    complexity_level: str  # SIMPLE, MODERATE, COMPLEX, OVERWHELMING
    brand_integration: float  # 0-1 scale
    readability_score: float  # 0-1 scale
    creative_freshness: float  # 0-1 scale based on uniqueness
    performance_predictor: float  # 0-1 predicted engagement
    recommended_optimizations: List[str]
    competitive_differentiation: str

@dataclass
class CreativeThemeAnalysis:
    """Cross-brand creative theme analysis"""
    theme_name: str
    prevalence_score: float  # How common this theme is
    effectiveness_score: float  # How well it performs
    brand_adoption: Dict[str, float]  # Brand -> adoption rate
    seasonal_trends: Dict[str, float]  # Month -> usage
    emerging_variations: List[str]
    saturation_risk: float  # 0-1 risk of theme becoming stale

class CreativeIntelligenceEngine:
    """Advanced creative content analysis using BigQuery ML"""
    
    def __init__(self, project_id: str = BQ_PROJECT, dataset_id: str = BQ_DATASET):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.logger = logging.getLogger(__name__)
        
    def create_creative_analysis_sql(self, run_id: str, brands: List[str]) -> str:
        """Generate SQL for comprehensive creative intelligence analysis"""
        brands_filter = "', '".join(brands)
        
        return f"""
        -- Creative Intelligence Analysis - Phase 8 Implementation
        CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.creative_intelligence_{run_id}` AS
        
        WITH base_creative_data AS (
          SELECT 
            ad_archive_id,
            brand,
            creative_text,
            title,
            cta_text,
            media_type,
            start_date_string,
            end_date_string,
            publisher_platforms,
            impressions_lower,
            impressions_upper,
            -- Calculate content richness
            LENGTH(COALESCE(creative_text, '')) as text_length,
            LENGTH(COALESCE(title, '')) as title_length,
            LENGTH(COALESCE(cta_text, '')) as cta_length,
            CASE 
              WHEN LENGTH(COALESCE(creative_text, '')) > 200 THEN 'TEXT_HEAVY'
              WHEN LENGTH(COALESCE(creative_text, '')) < 50 THEN 'MINIMAL'  
              WHEN COALESCE(title, '') != '' AND LENGTH(COALESCE(creative_text, '')) < 100 THEN 'VISUAL_FOCUS'
              ELSE 'BALANCED'
            END as content_type_basic
          FROM `{self.project_id}.{self.dataset_id}.ads_raw_{run_id}`
          WHERE brand IN ('{brands_filter}')
            AND creative_text IS NOT NULL
            AND LENGTH(creative_text) > 0
        ),
        
        creative_rule_analysis AS (
          SELECT 
            *,
            -- Advanced creative type classification (rule-based)
            CASE 
              WHEN LENGTH(COALESCE(creative_text, '')) > 300 AND LENGTH(COALESCE(title, '')) > 50 THEN 'TEXT_HEAVY'
              WHEN LENGTH(COALESCE(creative_text, '')) < 80 AND COALESCE(title, '') = '' THEN 'MINIMAL'
              WHEN LENGTH(COALESCE(creative_text, '')) < 100 AND LENGTH(COALESCE(title, '')) > 20 THEN 'VISUAL_FOCUS'
              ELSE 'BALANCED'
            END as creative_type_ml,
            
            -- Emotional appeal detection (rule-based keyword analysis)
            CASE 
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(NEW|FRESH|DISCOVER|LAUNCH|REVEAL|INTRODUCING)\b') THEN 'SURPRISE'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(LIMITED|HURRY|NOW|TODAY|SALE|OFFER|DEAL)\b') THEN 'URGENCY'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(LOVE|ENJOY|HAPPY|SMILE|FUN|CELEBRATE)\b') THEN 'JOY'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(DREAM|ACHIEVE|SUCCESS|PERFECT|IDEAL|PREMIUM)\b') THEN 'ASPIRATION'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(WANT|NEED|GET|SHOP|BUY|CHOOSE)\b') THEN 'DESIRE'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(SAFE|SECURE|PROTECT|GUARANTEE|QUALITY|RELIABLE)\b') THEN 'TRUST'
              ELSE 'COMFORT'
            END as emotional_appeal,
            
            -- Visual hierarchy analysis (rule-based)
            CASE 
              WHEN COALESCE(cta_text, '') != '' AND LENGTH(COALESCE(cta_text, '')) > 15 THEN 'CTA_PROMINENT'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(title, '')), brand) THEN 'BRAND_FOCUSED'
              WHEN LENGTH(COALESCE(title, '')) > LENGTH(COALESCE(creative_text, '')) THEN 'HEADLINE_DOMINANT'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(GLASSES|FRAMES|LENSES|EYEWEAR|VISION)\b') THEN 'PRODUCT_CENTERED'
              ELSE 'BENEFIT_FOCUSED'
            END as visual_hierarchy,
            
            -- Complexity assessment (rule-based)
            CASE 
              WHEN (text_length + title_length + cta_length) > 400 THEN 'COMPLEX'
              WHEN (text_length + title_length + cta_length) < 80 THEN 'SIMPLE'
              WHEN REGEXP_CONTAINS(COALESCE(creative_text, '') || ' ' || COALESCE(title, ''), r'[,;:]{3,}') THEN 'OVERWHELMING'
              ELSE 'MODERATE'
            END as complexity_level,
            
            -- Brand integration strength (rule-based scoring)
            CASE 
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(title, '')), brand) 
                   AND REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '')), brand) THEN '0.9'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(title, '')), brand) 
                   OR REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '')), brand) THEN '0.7'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   UPPER(SUBSTR(brand, 1, 4))) THEN '0.5'
              ELSE '0.3'
            END as brand_integration_raw,
            
            -- Creative freshness/uniqueness (rule-based originality scoring)
            CASE 
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(SEE THE DIFFERENCE|CRYSTAL CLEAR|PERFECT VISION|CLEAR SIGHT)\b') THEN '0.2'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(AFFORDABLE|CHEAP|DISCOUNT|BUDGET)\b') THEN '0.4'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(STYLE|FASHION|TREND|MODERN|CONTEMPORARY)\b') THEN '0.6'
              WHEN LENGTH(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) > 200 
                   AND NOT REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                           r'\b(GLASSES|EYEWEAR|FRAMES|VISION)\b') THEN '0.8'
              ELSE '0.5'
            END as creative_freshness_raw,
            
            -- Performance prediction (rule-based engagement scoring)
            CASE 
              WHEN COALESCE(cta_text, '') != '' 
                   AND REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                       r'\b(NEW|FREE|SALE|LIMITED|NOW)\b') THEN '0.8'
              WHEN LENGTH(COALESCE(creative_text, '')) BETWEEN 50 AND 200 
                   AND LENGTH(COALESCE(title, '')) BETWEEN 10 AND 60 THEN '0.7'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(DISCOVER|TRY|EXPLORE|EXPERIENCE)\b') THEN '0.6'
              WHEN LENGTH(COALESCE(creative_text, '')) < 50 OR LENGTH(COALESCE(creative_text, '')) > 300 THEN '0.4'
              ELSE '0.5'
            END as performance_predictor_raw
            
          FROM base_creative_data
        ),
        
        creative_metrics AS (
          SELECT 
            *,
            -- Clean and validate ML outputs
            CASE 
              WHEN UPPER(TRIM(creative_type_ml)) IN ('TEXT_HEAVY', 'VISUAL_FOCUS', 'BALANCED', 'MINIMAL') 
              THEN UPPER(TRIM(creative_type_ml))
              ELSE content_type_basic
            END as creative_type_final,
            
            CASE 
              WHEN UPPER(TRIM(emotional_appeal)) IN ('JOY', 'TRUST', 'FEAR', 'SURPRISE', 'DESIRE', 'URGENCY', 'COMFORT', 'ASPIRATION')
              THEN UPPER(TRIM(emotional_appeal))
              ELSE 'TRUST'  -- Default for eyewear
            END as emotional_appeal_final,
            
            CASE 
              WHEN UPPER(TRIM(visual_hierarchy)) IN ('HEADLINE_DOMINANT', 'CTA_PROMINENT', 'BRAND_FOCUSED', 'PRODUCT_CENTERED', 'BENEFIT_FOCUSED')
              THEN UPPER(TRIM(visual_hierarchy))
              ELSE 'BENEFIT_FOCUSED'  -- Default
            END as visual_hierarchy_final,
            
            CASE 
              WHEN UPPER(TRIM(complexity_level)) IN ('SIMPLE', 'MODERATE', 'COMPLEX', 'OVERWHELMING')
              THEN UPPER(TRIM(complexity_level))
              ELSE 'MODERATE'  -- Default
            END as complexity_level_final,
            
            -- Convert string scores to floats with validation
            LEAST(1.0, GREATEST(0.0, 
              SAFE_CAST(REGEXP_REPLACE(TRIM(brand_integration_raw), r'[^0-9.]', '') AS FLOAT64)
            )) as brand_integration_score,
            
            LEAST(1.0, GREATEST(0.0,
              SAFE_CAST(REGEXP_REPLACE(TRIM(creative_freshness_raw), r'[^0-9.]', '') AS FLOAT64)  
            )) as creative_freshness_score,
            
            LEAST(1.0, GREATEST(0.0,
              SAFE_CAST(REGEXP_REPLACE(TRIM(performance_predictor_raw), r'[^0-9.]', '') AS FLOAT64)
            )) as performance_predictor_score,
            
            -- Calculate readability approximation
            CASE 
              WHEN text_length = 0 THEN 0.9  -- Visual-only is "readable"
              WHEN text_length < 50 THEN 0.9
              WHEN text_length < 100 THEN 0.8
              WHEN text_length < 200 THEN 0.6
              ELSE 0.4
            END as readability_score
            
          FROM creative_rule_analysis
        ),
        
        competitive_benchmarking AS (
          SELECT 
            *,
            -- Competitive differentiation analysis (rule-based)
            CASE 
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(TECH|DIGITAL|ONLINE|APP|VIRTUAL)\b') THEN 'Tech-forward messaging'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(LIFE|LIFESTYLE|EVERYDAY|LIVING|EXPERIENCE)\b') THEN 'Lifestyle integration'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(\$|PRICE|COST|AFFORDABLE|CHEAP|VALUE)\b') THEN 'Price transparency'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(QUALITY|PREMIUM|LUXURY|CRAFTED|MATERIALS)\b') THEN 'Quality emphasis'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(FASHION|STYLE|TREND|DESIGN|LOOK)\b') THEN 'Style differentiation'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                   r'\b(HOME|DELIVERY|CONVENIENT|EASY|SIMPLE)\b') THEN 'Convenience positioning'
              ELSE 'Standard eyewear messaging'
            END as competitive_differentiation,
            
            -- Creative optimization recommendations (rule-based)
            CASE 
              WHEN COALESCE(cta_text, '') = '' THEN 'Add clear CTA, Strengthen value proposition'
              WHEN LENGTH(COALESCE(creative_text, '')) > 250 THEN 'Simplify message, Focus key benefit'  
              WHEN LENGTH(COALESCE(creative_text, '')) < 50 THEN 'Add supporting details, Expand benefit description'
              WHEN NOT REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                       r'\b(NEW|SAVE|FREE|LIMITED|NOW|TODAY)\b') THEN 'Add urgency trigger, Enhance CTA'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')), 
                       brand) = FALSE THEN 'Strengthen brand integration, Add brand mention'
              ELSE 'Test emotional appeal, Optimize headline'
            END as optimization_recommendations
            
          FROM creative_metrics
        )
        
        SELECT 
          ad_archive_id,
          brand,
          creative_text,
          title,
          cta_text,
          media_type,
          start_date_string,
          publisher_platforms,
          impressions_lower,
          impressions_upper,
          
          -- Creative Intelligence Metrics
          creative_type_final as creative_type,
          emotional_appeal_final as emotional_appeal,
          visual_hierarchy_final as visual_hierarchy,  
          complexity_level_final as complexity_level,
          
          -- Quantitative Scores
          COALESCE(brand_integration_score, 0.5) as brand_integration,
          COALESCE(readability_score, 0.7) as readability_score,
          COALESCE(creative_freshness_score, 0.5) as creative_freshness,
          COALESCE(performance_predictor_score, 0.5) as performance_predictor,
          
          -- Strategic Insights
          TRIM(competitive_differentiation) as competitive_differentiation,
          TRIM(optimization_recommendations) as optimization_recommendations,
          
          -- Content Metrics
          text_length,
          title_length,
          cta_length,
          
          -- Meta Information
          CURRENT_TIMESTAMP() as analysis_timestamp,
          '{run_id}' as run_id
          
        FROM competitive_benchmarking
        ORDER BY brand, performance_predictor DESC;
        """
        
    def create_creative_themes_sql(self, run_id: str) -> str:
        """Generate SQL for cross-brand creative theme analysis"""
        return f"""
        -- Creative Theme Analysis - Cross-Brand Intelligence
        CREATE OR REPLACE TABLE `{self.project_id}.{self.dataset_id}.creative_themes_{run_id}` AS
        
        WITH theme_extraction AS (
          SELECT 
            brand,
            creative_type,
            emotional_appeal,
            visual_hierarchy,
            complexity_level,
            competitive_differentiation,
            AVG(performance_predictor) as avg_performance,
            AVG(creative_freshness) as avg_freshness,
            AVG(brand_integration) as avg_brand_integration,
            COUNT(*) as ad_count,
            COUNT(DISTINCT EXTRACT(MONTH FROM PARSE_DATETIME('%Y-%m-%d', start_date_string))) as months_active,
            -- Create theme signature
            CONCAT(creative_type, '_', emotional_appeal, '_', visual_hierarchy) as theme_signature
          FROM `{self.project_id}.{self.dataset_id}.creative_intelligence_{run_id}`
          GROUP BY brand, creative_type, emotional_appeal, visual_hierarchy, complexity_level, competitive_differentiation
        ),
        
        theme_prevalence AS (
          SELECT 
            theme_signature,
            creative_type,
            emotional_appeal, 
            visual_hierarchy,
            COUNT(DISTINCT brand) as brands_using,
            SUM(ad_count) as total_ads,
            AVG(avg_performance) as theme_performance,
            AVG(avg_freshness) as theme_freshness,
            STDDEV(avg_performance) as performance_variance,
            -- Calculate market prevalence
            COUNT(DISTINCT brand) / (SELECT COUNT(DISTINCT brand) FROM theme_extraction) as market_penetration,
            SUM(ad_count) / (SELECT SUM(ad_count) FROM theme_extraction) as volume_share
          FROM theme_extraction
          GROUP BY theme_signature, creative_type, emotional_appeal, visual_hierarchy
        ),
        
        theme_effectiveness AS (
          SELECT 
            *,
            -- Effectiveness score considering performance and freshness
            (theme_performance * 0.7 + theme_freshness * 0.3) as effectiveness_score,
            -- Saturation risk (high prevalence + low variance = risky)
            CASE 
              WHEN market_penetration > 0.7 AND performance_variance < 0.1 THEN 0.8
              WHEN market_penetration > 0.5 AND performance_variance < 0.15 THEN 0.6  
              WHEN market_penetration > 0.3 AND performance_variance < 0.2 THEN 0.4
              ELSE 0.2
            END as saturation_risk,
            -- Opportunity score (good performance + low penetration = opportunity)
            CASE 
              WHEN theme_performance > 0.7 AND market_penetration < 0.3 THEN 0.9
              WHEN theme_performance > 0.6 AND market_penetration < 0.5 THEN 0.7
              WHEN theme_performance > 0.5 AND market_penetration < 0.7 THEN 0.5  
              ELSE 0.3
            END as opportunity_score
          FROM theme_prevalence
        )
        
        SELECT 
          theme_signature as theme_name,
          creative_type,
          emotional_appeal,
          visual_hierarchy,
          brands_using,
          total_ads,
          ROUND(market_penetration, 3) as prevalence_score,
          ROUND(effectiveness_score, 3) as effectiveness_score,
          ROUND(theme_performance, 3) as avg_performance,
          ROUND(theme_freshness, 3) as avg_freshness,  
          ROUND(saturation_risk, 3) as saturation_risk,
          ROUND(opportunity_score, 3) as opportunity_score,
          CURRENT_TIMESTAMP() as analysis_timestamp,
          '{run_id}' as run_id
        FROM theme_effectiveness
        ORDER BY effectiveness_score DESC, opportunity_score DESC;
        """
        
    def create_creative_summary_sql(self, run_id: str) -> str:
        """Generate executive summary of creative intelligence"""
        return f"""
        -- Creative Intelligence Executive Summary
        CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.v_creative_intelligence_summary_{run_id}` AS
        
        WITH brand_creative_profiles AS (
          SELECT 
            brand,
            COUNT(*) as total_creatives,
            -- Creative type distribution
            COUNT(CASE WHEN creative_type = 'TEXT_HEAVY' THEN 1 END) / COUNT(*) as text_heavy_pct,
            COUNT(CASE WHEN creative_type = 'VISUAL_FOCUS' THEN 1 END) / COUNT(*) as visual_focus_pct,
            COUNT(CASE WHEN creative_type = 'BALANCED' THEN 1 END) / COUNT(*) as balanced_pct,
            -- Emotional strategy
            -- Use APPROX_TOP_COUNT to find most frequent emotional appeal
            APPROX_TOP_COUNT(emotional_appeal, 1)[OFFSET(0)].value as primary_emotion,
            -- Performance metrics
            AVG(performance_predictor) as avg_predicted_performance,
            AVG(creative_freshness) as avg_creativity,
            AVG(brand_integration) as avg_brand_strength,
            AVG(readability_score) as avg_clarity,
            -- Content characteristics
            AVG(text_length) as avg_text_length,
            COUNT(CASE WHEN complexity_level = 'SIMPLE' THEN 1 END) / COUNT(*) as simple_messaging_pct
          FROM `{self.project_id}.{self.dataset_id}.creative_intelligence_{run_id}`
          GROUP BY brand
        ),
        
        competitive_benchmarks AS (
          SELECT 
            brand,
            -- Rank brands by creative metrics
            RANK() OVER (ORDER BY avg_predicted_performance DESC) as performance_rank,
            RANK() OVER (ORDER BY avg_creativity DESC) as creativity_rank,
            RANK() OVER (ORDER BY avg_brand_strength DESC) as brand_integration_rank,
            RANK() OVER (ORDER BY avg_clarity DESC) as clarity_rank,
            -- Calculate overall creative score
            (avg_predicted_performance * 0.4 + avg_creativity * 0.3 + 
             avg_brand_strength * 0.2 + avg_clarity * 0.1) as creative_excellence_score
          FROM brand_creative_profiles
        )
        
        SELECT 
          p.brand,
          p.total_creatives,
          ROUND(p.text_heavy_pct * 100, 1) as text_heavy_percentage,
          ROUND(p.visual_focus_pct * 100, 1) as visual_focus_percentage,  
          ROUND(p.balanced_pct * 100, 1) as balanced_percentage,
          p.primary_emotion,
          ROUND(p.avg_predicted_performance, 3) as predicted_performance,
          ROUND(p.avg_creativity, 3) as creativity_score,
          ROUND(p.avg_brand_strength, 3) as brand_integration,
          ROUND(p.avg_clarity, 3) as message_clarity,
          ROUND(p.avg_text_length, 0) as avg_text_length,
          ROUND(p.simple_messaging_pct * 100, 1) as simple_messaging_pct,
          b.performance_rank,
          b.creativity_rank,
          b.brand_integration_rank,
          ROUND(b.creative_excellence_score, 3) as overall_creative_score,
          CASE 
            WHEN b.creative_excellence_score >= 0.8 THEN 'CREATIVE_LEADER'
            WHEN b.creative_excellence_score >= 0.6 THEN 'STRONG_CREATIVE'
            WHEN b.creative_excellence_score >= 0.4 THEN 'AVERAGE_CREATIVE'
            ELSE 'CREATIVE_OPPORTUNITY'
          END as creative_tier
        FROM brand_creative_profiles p
        JOIN competitive_benchmarks b ON p.brand = b.brand
        ORDER BY b.creative_excellence_score DESC;
        """
        
    def execute_creative_intelligence_analysis(self, run_id: str, brands: List[str]) -> Dict[str, any]:
        """Execute complete creative intelligence analysis"""
        try:
            from src.utils.bigquery_client import run_query
            
            self.logger.info(f"🎨 Starting Creative Intelligence Analysis for {len(brands)} brands")
            
            # Step 1: Core creative analysis
            creative_sql = self.create_creative_analysis_sql(run_id, brands)
            self.logger.info("   Analyzing creative content with ML.GENERATE_TEXT...")
            run_query(creative_sql)
            
            # Step 2: Theme analysis
            themes_sql = self.create_creative_themes_sql(run_id)
            self.logger.info("   Identifying creative themes and patterns...")
            run_query(themes_sql)
            
            # Step 3: Executive summary
            summary_sql = self.create_creative_summary_sql(run_id)
            self.logger.info("   Generating creative intelligence summary...")
            run_query(summary_sql)
            
            self.logger.info("✅ Creative Intelligence Analysis completed successfully")
            
            return {
                'status': 'success',
                'analysis_type': 'creative_intelligence',
                'run_id': run_id,
                'brands_analyzed': brands,
                'tables_created': [
                    f'creative_intelligence_{run_id}',
                    f'creative_themes_{run_id}',
                    f'v_creative_intelligence_summary_{run_id}'
                ],
                'insights': [
                    'Creative type distribution analysis',
                    'Emotional appeal strategy mapping', 
                    'Visual hierarchy assessment',
                    'Brand integration scoring',
                    'Creative freshness evaluation',
                    'Performance prediction modeling',
                    'Cross-brand theme identification',
                    'Competitive creative differentiation'
                ]
            }
            
        except Exception as e:
            self.logger.error(f"❌ Creative Intelligence Analysis failed: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'analysis_type': 'creative_intelligence'
            }

def main():
    """Test Creative Intelligence module"""
    engine = CreativeIntelligenceEngine()
    
    test_brands = ['Warby Parker', 'LensCrafters', 'EyeBuyDirect', 'Zenni Optical']
    test_run_id = f"creative_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    result = engine.execute_creative_intelligence_analysis(test_run_id, test_brands)
    print(f"Creative Intelligence Test Result: {result}")

if __name__ == "__main__":
    main()