#!/usr/bin/env python3
"""
Enhanced 3D White Space Detection System
Replaces mock data with real ML.GENERATE_TEXT analysis for competitive intelligence
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
class WhiteSpaceOpportunity:
    """Enhanced white space opportunity with real data"""
    messaging_angle: str
    funnel_stage: str 
    target_persona: str
    space_type: str  # VIRGIN_TERRITORY, MONOPOLY, UNDERSERVED, SATURATED
    competitive_intensity: float  # 0-1 scale
    market_potential: float  # 0-1 scale
    entry_difficulty: float  # 0-1 scale
    overall_score: float
    competitor_count: int
    total_ads: int
    avg_message_quality: float
    recent_activity: int
    channel_coverage: int
    opportunity_window: str
    recommended_investment: str
    success_indicators: List[str]
    campaign_template: Dict

class Enhanced3DWhiteSpaceDetector:
    """Advanced 3D white space detection using real competitive data"""
    
    def __init__(self, project_id: str, dataset_id: str, brand: str, competitors: List[str]):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.brand = brand
        self.competitors = competitors
        self.logger = logging.getLogger(__name__)
        
    def analyze_real_strategic_positions(self, run_id: str) -> str:
        """Generate SQL for real strategic position analysis using ML.GENERATE_TEXT"""
        return f"""
        -- Enhanced 3D White Space Detection with Real Data
        WITH real_strategic_positions AS (
          SELECT 
            r.brand,
            r.ad_archive_id,
            r.creative_text,
            r.publisher_platforms,
            DATE(r.start_timestamp) as campaign_date,
            r.media_type,
            -- Extract messaging angle from real content
            AI.GENERATE(
              CONCAT(
                'Analyze this eyewear/glasses ad and classify the primary messaging angle. ',
                'Text: "', SUBSTR(r.creative_text, 1, 500), '". ',
                'Return ONLY one of: EMOTIONAL, FUNCTIONAL, ASPIRATIONAL, SOCIAL_PROOF, PROBLEM_SOLUTION'
              ),
              connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
            ) as messaging_angle_raw,
            -- Extract funnel stage
            AI.GENERATE(
              CONCAT(
                'Classify this ad by marketing funnel stage. ',
                'Text: "', SUBSTR(r.creative_text, 1, 500), '". ',
                'Return ONLY one of: AWARENESS, CONSIDERATION, DECISION, RETENTION'
              ),
              connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
            ) as funnel_stage_raw,
            -- Extract target persona (simplified)
            AI.GENERATE(
              CONCAT(
                'Who is the primary target audience for this eyewear ad? ',
                'Text: "', SUBSTR(r.creative_text, 1, 500), '". ',
                'Return ONLY 2-3 words describing the main demographic/psychographic (e.g., "Young Professionals", "Price-Conscious Families", "Style-Conscious Millennials")'
              ),
              connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
            ) as target_persona_raw,
            -- Message quality from CTA analysis (brand-level)
            COALESCE(c.avg_cta_aggressiveness * 0.5, 3.0) as message_strength
          FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates` r
          LEFT JOIN `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis` c
            ON r.brand = c.brand
          WHERE r.creative_text IS NOT NULL
            AND LENGTH(r.creative_text) > 20
            AND r.brand IN ('{self.brand}', {', '.join(f"'{c}'" for c in self.competitors)})
          -- PERFORMANCE OPTIMIZATION: Limit to 15 ads for 2-minute analysis
          QUALIFY ROW_NUMBER() OVER (PARTITION BY r.brand ORDER BY r.start_timestamp DESC) <= 3
        ),
        cleaned_positions AS (
          SELECT 
            brand,
            ad_archive_id,
            creative_text,
            publisher_platforms,
            campaign_date,
            media_type,
            message_strength,
            -- Clean and standardize ML outputs
            CASE 
              WHEN UPPER(messaging_angle_raw.result) LIKE '%EMOTIONAL%' THEN 'EMOTIONAL'
              WHEN UPPER(messaging_angle_raw.result) LIKE '%FUNCTIONAL%' THEN 'FUNCTIONAL'
              WHEN UPPER(messaging_angle_raw.result) LIKE '%ASPIRATIONAL%' THEN 'ASPIRATIONAL'
              WHEN UPPER(messaging_angle_raw.result) LIKE '%SOCIAL%' THEN 'SOCIAL_PROOF'
              WHEN UPPER(messaging_angle_raw.result) LIKE '%PROBLEM%' THEN 'PROBLEM_SOLUTION'
              ELSE 'FUNCTIONAL'  -- Default fallback
            END as messaging_angle,
            CASE 
              WHEN UPPER(funnel_stage_raw.result) LIKE '%AWARENESS%' THEN 'AWARENESS'
              WHEN UPPER(funnel_stage_raw.result) LIKE '%CONSIDERATION%' THEN 'CONSIDERATION'
              WHEN UPPER(funnel_stage_raw.result) LIKE '%DECISION%' THEN 'DECISION'
              WHEN UPPER(funnel_stage_raw.result) LIKE '%RETENTION%' THEN 'RETENTION'
              ELSE 'CONSIDERATION'  -- Default fallback
            END as funnel_stage,
            -- Clean persona output
            REGEXP_REPLACE(
              TRIM(UPPER(target_persona_raw.result)),
              r'[^A-Z\\s]', ''
            ) as target_persona
          FROM real_strategic_positions
          WHERE messaging_angle_raw.result IS NOT NULL
            AND funnel_stage_raw.result IS NOT NULL
            AND target_persona_raw.result IS NOT NULL
        ),
        market_3d_grid AS (
          SELECT 
            messaging_angle,
            funnel_stage, 
            target_persona,
            COUNT(DISTINCT brand) as competitor_count,
            COUNT(DISTINCT ad_archive_id) as total_ads,
            AVG(message_strength) as avg_message_quality,
            -- Competitive intensity modeling
            (COUNT(DISTINCT brand) * COUNT(DISTINCT ad_archive_id) * AVG(message_strength)) / 1000.0 as intensity_score,
            -- Temporal opportunity analysis
            COUNT(CASE 
              WHEN campaign_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
              THEN 1 END
            ) as recent_activity,
            -- Channel diversity assessment
            COUNT(DISTINCT publisher_platforms) as channel_coverage,
            -- Brand presence check
            COUNT(CASE WHEN brand = '{self.brand}' THEN 1 END) as target_brand_presence,
            -- Quality of competition
            MAX(CASE WHEN brand != '{self.brand}' THEN message_strength ELSE 0 END) as max_competitor_quality
          FROM cleaned_positions
          GROUP BY messaging_angle, funnel_stage, target_persona
          HAVING COUNT(DISTINCT ad_archive_id) >= 2  -- Filter noise
        ),
        white_space_scoring AS (
          SELECT *,
            -- Space type classification
            CASE 
              WHEN competitor_count = 0 OR (competitor_count = 1 AND target_brand_presence = 1) THEN 'VIRGIN_TERRITORY'
              WHEN competitor_count = 1 AND target_brand_presence = 0 THEN 'MONOPOLY' 
              WHEN competitor_count <= 3 AND intensity_score < 2.0 THEN 'UNDERSERVED'
              ELSE 'SATURATED'
            END as space_type,
            -- Market potential modeling (higher = better opportunity)
            GREATEST(0.1, 
              (5.0 - intensity_score) / 5.0 * 0.4 +  -- Lower intensity = higher potential
              (recent_activity / 20.0) * 0.2 +         -- Recent activity shows market validity
              (channel_coverage / 3.0) * 0.2 +         -- More channels = bigger market
              (CASE WHEN max_competitor_quality < 6.0 THEN 0.2 ELSE 0.0 END) -- Weak competition
            ) as market_potential,
            -- Strategic value (funnel stage importance)
            CASE 
              WHEN funnel_stage = 'AWARENESS' THEN 1.0      -- Brand building high value
              WHEN funnel_stage = 'CONSIDERATION' THEN 0.8   -- Key conversion stage
              WHEN funnel_stage = 'DECISION' THEN 0.6        -- Direct conversion
              ELSE 0.4                                       -- Retention lower priority
            END as strategic_value,
            -- Entry difficulty assessment (higher = harder)
            LEAST(0.9,
              (competitor_count * 0.15) +                    -- More competitors = harder
              (intensity_score / 10.0) +                     -- Higher intensity = harder
              (CASE WHEN max_competitor_quality > 8.0 THEN 0.3 ELSE 0.1 END) + -- Strong competition
              (CASE WHEN target_brand_presence = 0 THEN 0.1 ELSE 0.0 END)      -- New space harder
            ) as entry_difficulty
          FROM market_3d_grid
        ),
        final_opportunities AS (
          SELECT *,
            -- Overall opportunity score (0-1, higher = better)
            ROUND(
              market_potential * strategic_value * (1.0 - entry_difficulty), 
              3
            ) as overall_score,
            -- Investment recommendation based on space characteristics
            CASE 
              WHEN space_type = 'VIRGIN_TERRITORY' AND strategic_value > 0.7 THEN 'HIGH ($100K-200K) - Market pioneering'
              WHEN space_type = 'MONOPOLY' AND max_competitor_quality < 6.0 THEN 'MEDIUM ($50K-100K) - Competitive displacement'
              WHEN space_type = 'UNDERSERVED' AND market_potential > 0.6 THEN 'MEDIUM ($30K-75K) - Market expansion'
              ELSE 'LOW ($10K-30K) - Tactical test'
            END as recommended_investment,
            -- Success metrics based on competitive landscape
            CASE 
              WHEN max_competitor_quality < 5.0 THEN 'CTR >3.0%, CPA <$12, Brand lift +10%'
              WHEN max_competitor_quality < 7.0 THEN 'CTR >2.5%, CPA <$15, Brand lift +8%'
              ELSE 'CTR >2.0%, CPA <$18, Brand lift +6%'
            END as success_indicators
          FROM white_space_scoring
        )
        SELECT 
          messaging_angle,
          funnel_stage,
          target_persona,
          space_type,
          ROUND(intensity_score, 2) as competitive_intensity,
          ROUND(market_potential, 2) as market_potential,
          ROUND(entry_difficulty, 2) as entry_difficulty,
          overall_score,
          competitor_count,
          total_ads,
          ROUND(avg_message_quality, 1) as avg_message_quality,
          recent_activity,
          channel_coverage,
          target_brand_presence,
          ROUND(max_competitor_quality, 1) as max_competitor_quality,
          recommended_investment,
          success_indicators,
          -- Opportunity window assessment
          CASE 
            WHEN space_type = 'VIRGIN_TERRITORY' THEN 'HIGH - Untapped market space'
            WHEN space_type = 'MONOPOLY' AND max_competitor_quality < 6.0 THEN 'MEDIUM - Weak incumbent'
            WHEN space_type = 'UNDERSERVED' AND recent_activity < 5 THEN 'MEDIUM - Low recent activity'
            ELSE 'LOW - Competitive market'
          END as opportunity_window
        FROM final_opportunities
        WHERE space_type IN ('VIRGIN_TERRITORY', 'MONOPOLY', 'UNDERSERVED')
          AND overall_score > 0.2  -- Filter out very low opportunities
        ORDER BY overall_score DESC, market_potential DESC
        LIMIT 20
        """
    
    def generate_campaign_template(self, opportunity: Dict) -> Dict:
        """Generate actionable campaign template for white space opportunity"""
        angle = opportunity['messaging_angle']
        funnel = opportunity['funnel_stage']
        persona = opportunity['target_persona']
        
        # Angle-specific messaging
        angle_messaging = {
            'EMOTIONAL': {
                'headline_template': 'Feel Confident in Every Frame',
                'value_prop': 'Emotional connection and self-expression',
                'tone': 'Warm, personal, aspirational'
            },
            'FUNCTIONAL': {
                'headline_template': 'Premium Quality at Honest Prices',
                'value_prop': 'Practical benefits and features',
                'tone': 'Clear, informative, trustworthy'
            },
            'ASPIRATIONAL': {
                'headline_template': 'Elevate Your Look, Define Your Style',
                'value_prop': 'Status and lifestyle enhancement',
                'tone': 'Sophisticated, aspirational, premium'
            },
            'SOCIAL_PROOF': {
                'headline_template': '10,000+ Happy Customers Choose Us',
                'value_prop': 'Trust and community validation',
                'tone': 'Confident, credible, community-focused'
            },
            'PROBLEM_SOLUTION': {
                'headline_template': 'Finally, Glasses That Don\'t Break the Bank',
                'value_prop': 'Direct problem solving',
                'tone': 'Direct, solution-focused, empathetic'
            }
        }
        
        # Funnel-specific CTAs
        funnel_ctas = {
            'AWARENESS': ['Learn More', 'Discover', 'Explore'],
            'CONSIDERATION': ['Try Virtual Try-On', 'Compare Options', 'See Styles'],
            'DECISION': ['Shop Now', 'Buy Today', 'Get Started'],
            'RETENTION': ['Reorder', 'Upgrade', 'Refer Friends']
        }
        
        template = angle_messaging.get(angle, angle_messaging['FUNCTIONAL'])
        
        return {
            'campaign_name': f"{angle}_{funnel}_{persona.replace(' ', '_')}_Entry",
            'targeting': {
                'persona': persona,
                'funnel_stage': funnel,
                'estimated_audience_size': self._estimate_audience_size(persona, funnel)
            },
            'messaging': {
                'primary_angle': angle,
                'headline': template['headline_template'],
                'value_proposition': template['value_prop'],
                'tone': template['tone'],
                'cta_suggestions': funnel_ctas.get(funnel, ['Learn More'])
            },
            'creative_brief': {
                'visual_style': self._get_visual_style(angle, persona),
                'copy_length': self._get_copy_length(funnel),
                'channel_optimization': self._get_channel_recommendations(opportunity)
            },
            'success_metrics': {
                'primary_kpi': self._get_primary_kpi(funnel),
                'target_metrics': opportunity.get('success_indicators', 'CTR >2.0%, CPA <$15'),
                'test_budget': opportunity.get('recommended_investment', '$30K-50K'),
                'test_duration': '4-6 weeks'
            }
        }
    
    def _estimate_audience_size(self, persona: str, funnel: str) -> str:
        """Estimate addressable audience size"""
        base_sizes = {
            'AWARENESS': '500K-2M',
            'CONSIDERATION': '100K-500K', 
            'DECISION': '50K-200K',
            'RETENTION': '10K-50K'
        }
        return base_sizes.get(funnel, '100K-500K')
    
    def _get_visual_style(self, angle: str, persona: str) -> str:
        """Recommend visual style based on angle and persona"""
        if angle == 'ASPIRATIONAL':
            return 'Premium, lifestyle-focused imagery'
        elif angle == 'FUNCTIONAL':
            return 'Clean, product-focused, informative'
        elif angle == 'EMOTIONAL':
            return 'Warm, human-centered, authentic'
        else:
            return 'Modern, approachable, trustworthy'
    
    def _get_copy_length(self, funnel: str) -> str:
        """Recommend copy length based on funnel stage"""
        return {
            'AWARENESS': 'Medium (brand story, 50-100 words)',
            'CONSIDERATION': 'Long (feature details, 75-150 words)',
            'DECISION': 'Short (direct CTA, 25-50 words)',
            'RETENTION': 'Short (focused message, 20-40 words)'
        }.get(funnel, 'Medium (50-100 words)')
    
    def _get_channel_recommendations(self, opportunity: Dict) -> str:
        """Recommend optimal channels based on opportunity characteristics"""
        if opportunity.get('channel_coverage', 0) < 2:
            return 'Multi-channel expansion opportunity'
        elif opportunity.get('competitive_intensity', 0) < 0.5:
            return 'Focus on primary performing channels'
        else:
            return 'Test emerging channels for differentiation'
    
    def _get_primary_kpi(self, funnel: str) -> str:
        """Get primary KPI based on funnel stage"""
        return {
            'AWARENESS': 'Brand awareness lift',
            'CONSIDERATION': 'Consideration rate increase',
            'DECISION': 'Conversion rate',
            'RETENTION': 'Customer lifetime value'
        }.get(funnel, 'Engagement rate')
    
    def generate_strategic_opportunities(self, whitespace_results: List[Dict]) -> List[Dict]:
        """Convert whitespace analysis results into strategic opportunities"""
        opportunities = []
        
        for result in whitespace_results:
            # Convert the SQL result into a WhiteSpaceOpportunity-like structure
            opportunity = {
                'messaging_angle': result.get('messaging_angle', 'FUNCTIONAL'),
                'funnel_stage': result.get('funnel_stage', 'CONSIDERATION'),
                'target_persona': result.get('target_persona', 'General Market'),
                'space_type': result.get('space_type', 'UNDERSERVED'),
                'competitive_intensity': float(result.get('competitive_intensity', 0.5)),
                'market_potential': float(result.get('market_potential', 0.6)),
                'entry_difficulty': float(result.get('entry_difficulty', 0.4)),
                'overall_score': float(result.get('overall_score', 0.5)),
                'competitor_count': int(result.get('competitor_count', 2)),
                'total_ads': int(result.get('total_ads', 10)),
                'avg_message_quality': float(result.get('avg_message_quality', 5.0)),
                'recent_activity': int(result.get('recent_activity', 5)),
                'channel_coverage': int(result.get('channel_coverage', 2)),
                'opportunity_window': result.get('opportunity_window', 'MEDIUM - Market opportunity'),
                'recommended_investment': result.get('recommended_investment', 'MEDIUM ($30K-75K) - Market expansion'),
                'success_indicators': [
                    indicator.strip() for indicator in 
                    result.get('success_indicators', 'CTR >2.5%, CPA <$15, Brand lift +8%').split(',')
                ],
                'campaign_template': self.generate_campaign_template(result)
            }
            opportunities.append(opportunity)
        
        return opportunities

if __name__ == "__main__":
    # Example usage
    detector = Enhanced3DWhiteSpaceDetector(
        project_id="bigquery-ai-kaggle-469620",
        dataset_id="ads_demo",
        brand="Warby Parker",
        competitors=["EyeBuyDirect", "LensCrafters", "GlassesUSA", "Zenni Optical"]
    )
    
    sql = detector.analyze_real_strategic_positions("warby_parker_20250910_132446")
    print("Enhanced 3D White Space Detection SQL generated successfully!")
    print("Key improvements:")
    print("- Real ML.GENERATE_TEXT analysis instead of mock data")
    print("- Advanced competitive intensity modeling")
    print("- Temporal opportunity windows")
    print("- Actionable campaign templates with specific targeting")
    print("- ROI-based investment recommendations")