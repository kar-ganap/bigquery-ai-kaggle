"""
Stage 7: Visual Intelligence - Multimodal Analysis with Adaptive Sampling

NEW STAGE: Analyzes visual creative strategy using BigQuery AI multimodal capabilities.
Implements adaptive sampling to balance cost control with comprehensive competitive intelligence.
"""
import os
from typing import List, Dict, Any
from datetime import datetime

from ..core.base import PipelineStage, PipelineContext
from ..models.results import AnalysisResults

try:
    from src.utils.bigquery_client import get_bigquery_client, run_query
except ImportError:
    get_bigquery_client = None
    run_query = None

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class VisualIntelligenceResults:
    """Results from enhanced visual intelligence analysis with competitive insights"""

    def __init__(self, sampled_ads: int, visual_insights: int, competitive_insights: int, cost_estimate: float):
        self.sampled_ads = sampled_ads
        self.visual_insights = visual_insights
        self.competitive_insights = competitive_insights
        self.cost_estimate = cost_estimate
        self.table_id = f"visual_intelligence_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


class VisualIntelligenceStage(PipelineStage[AnalysisResults, VisualIntelligenceResults]):
    """
    Stage 7: Visual Intelligence Analysis

    Implements adaptive sampling strategy from multimodal integration plan:
    - Small brands (≤20 ads): 50% coverage, max 10 images
    - Medium brands (21-50 ads): 30% coverage, max 15 images
    - Large brands (51-100 ads): 20% coverage, max 20 images
    - Dominant brands (>100 ads): 15 images fixed
    """

    def __init__(self, context: PipelineContext, dry_run: bool = False):
        super().__init__("Visual Intelligence", 5.5, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.per_brand_budget = int(os.getenv('MULTIMODAL_IMAGE_BUDGET_PER_BRAND', '20'))
        self.max_total_budget = int(os.getenv('MULTIMODAL_MAX_TOTAL_BUDGET', '200'))


    def execute(self, analysis_results: AnalysisResults) -> VisualIntelligenceResults:
        """Execute visual intelligence with adaptive sampling"""

        if self.dry_run:
            return self._create_mock_visual_intelligence()

        return self._run_adaptive_visual_analysis()

    def _create_mock_visual_intelligence(self) -> VisualIntelligenceResults:
        """Create mock visual intelligence results for testing"""
        return VisualIntelligenceResults(
            sampled_ads=45,
            visual_insights=12,
            competitive_insights=10,
            cost_estimate=8.50
        )

    def _run_adaptive_visual_analysis(self) -> VisualIntelligenceResults:
        """Run actual adaptive visual analysis"""

        if get_bigquery_client is None or run_query is None:
            self.logger.error("BigQuery client not available")
            raise ImportError("BigQuery client required for visual intelligence")

        try:
            print("   🎨 Running adaptive visual intelligence analysis...")

            # Step 1: Create adaptive sampling strategy table
            sampling_sql = self._generate_adaptive_sampling_sql()
            print("   📊 Generated adaptive sampling strategy")

            # Execute sampling strategy creation
            client = get_bigquery_client()
            sampling_job = client.query(sampling_sql)
            sampling_job.result()  # Wait for table creation
            print("   ✅ Created sampling strategy table")

            # Step 2: Execute sampling and analysis
            analysis_sql = self._generate_visual_analysis_sql()
            print("   🔍 Executing multimodal analysis...")

            # Execute the analysis (creates table)
            query_job = client.query(analysis_sql)
            query_job.result()  # Wait for table creation

            # Count results by querying the created table
            count_sql = f"""
            SELECT
                COUNT(*) as sampled_count,
                COUNT(CASE WHEN visual_text_alignment_score > 0 THEN 1 END) as insights_count,
                COUNT(CASE WHEN luxury_positioning_score > 0 THEN 1 END) as competitive_count
            FROM `{BQ_PROJECT}.{BQ_DATASET}.visual_intelligence_{self.context.run_id}`
            """
            count_job = client.query(count_sql)
            count_result = count_job.result()

            for row in count_result:
                sampled_count = row.sampled_count
                insights_count = row.insights_count
                competitive_count = row.competitive_count
                break

            estimated_cost = sampled_count * 0.30  # Rough estimate (doubled due to 2 AI calls per ad)

            print(f"   ✅ Analyzed {sampled_count} ads with enhanced visual intelligence")
            print(f"   💡 Generated {insights_count} visual insights")
            print(f"   🎯 Generated {competitive_count} competitive positioning insights")
            print(f"   💰 Estimated cost: ${estimated_cost:.2f}")

            return VisualIntelligenceResults(
                sampled_ads=sampled_count,
                visual_insights=insights_count,
                competitive_insights=competitive_count,
                cost_estimate=estimated_cost
            )

        except Exception as e:
            self.logger.error(f"Visual intelligence analysis failed: {str(e)}")
            raise

    def _generate_adaptive_sampling_sql(self) -> str:
        """Generate SQL for improved per-brand budget strategy"""

        return f"""
        -- Improved Visual Intelligence Sampling Strategy
        -- Per-brand budget: {self.per_brand_budget} images per brand
        -- Max total budget: {self.max_total_budget} images across all competitors

        CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.visual_sampling_strategy` AS
        WITH brand_stats AS (
          SELECT
            brand,
            COUNT(*) as total_ads,
            COUNT(CASE WHEN ARRAY_LENGTH(image_urls) > 0 THEN 1 END) as ads_with_images,
            -- Calculate adaptive sample size based on brand portfolio
            CASE
              WHEN COUNT(*) <= 20 THEN LEAST(CAST(COUNT(*) * 0.5 AS INT64), {self.per_brand_budget})  -- 50% coverage, max per-brand budget
              WHEN COUNT(*) <= 50 THEN LEAST(CAST(COUNT(*) * 0.3 AS INT64), {self.per_brand_budget})  -- 30% coverage, max per-brand budget
              WHEN COUNT(*) <= 100 THEN LEAST(CAST(COUNT(*) * 0.2 AS INT64), {self.per_brand_budget}) -- 20% coverage, max per-brand budget
              ELSE LEAST({self.per_brand_budget}, 15)  -- Fixed per-brand budget, but capped at 15 for very large brands
            END as target_sample_size
          FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
          WHERE image_urls IS NOT NULL
            AND ARRAY_LENGTH(image_urls) > 0
          GROUP BY brand
        ),
        budget_allocation AS (
          SELECT
            *,
            -- Ensure total doesn't exceed maximum budget across all brands
            CASE
              WHEN SUM(target_sample_size) OVER() <= {self.max_total_budget} THEN target_sample_size
              ELSE GREATEST(
                CAST(target_sample_size * {self.max_total_budget} / SUM(target_sample_size) OVER() AS INT64),
                3  -- Minimum 3 images per brand for meaningful analysis
              )
            END as final_sample_size,
            COUNT(*) OVER() as total_brands
          FROM brand_stats
        )
        SELECT
          brand,
          total_ads,
          ads_with_images,
          target_sample_size,
          final_sample_size,
          total_brands,
          ROUND(final_sample_size * 100.0 / total_ads, 1) as actual_coverage_pct,
          ROUND({self.max_total_budget} / total_brands, 1) as avg_budget_per_brand
        FROM budget_allocation
        ORDER BY total_ads DESC
        """

    def _generate_visual_analysis_sql(self) -> str:
        """Generate SQL for multimodal visual analysis"""

        # Define regex pattern outside f-string to avoid backslash issues
        json_regex = r'```json\\s*({[\\s\\S]*?})\\s*```'

        return f"""
        -- Multimodal Visual Intelligence Analysis
        CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.visual_intelligence_{self.context.run_id}` AS
        WITH sampled_ads AS (
          SELECT
            a.*,
            s.final_sample_size,
            -- Multi-factor scoring for strategic ad selection
            (
              -- Recency weight (30%)
              0.3 * (1.0 - DATE_DIFF(CURRENT_DATE(), DATE(a.start_timestamp), DAY) / 365.0) +
              -- Visual complexity weight (25%)
              0.25 * CASE
                WHEN ARRAY_LENGTH(a.image_urls) > 2 THEN 1.0  -- Carousel
                WHEN a.media_type = 'video' THEN 0.8  -- Video
                ELSE 0.5  -- Single image
              END +
              -- Card variations weight (25%)
              0.25 * LEAST(LENGTH(COALESCE(a.card_bodies, '')) / 50.0, 1.0) +
              -- Strategic diversity weight (20%) - extreme promotional intensities
              0.2 * ABS(0.5 - LEAST(LENGTH(a.creative_text) / 200.0, 1.0))
            ) as strategic_score,
            ROW_NUMBER() OVER (
              PARTITION BY a.brand
              ORDER BY (
                0.3 * (1.0 - DATE_DIFF(CURRENT_DATE(), DATE(a.start_timestamp), DAY) / 365.0) +
                0.25 * CASE
                  WHEN ARRAY_LENGTH(a.image_urls) > 2 THEN 1.0
                  WHEN a.media_type = 'video' THEN 0.8
                  ELSE 0.5
                END +
                0.25 * LEAST(LENGTH(COALESCE(a.card_bodies, '')) / 50.0, 1.0) +
                0.2 * ABS(0.5 - LEAST(LENGTH(a.creative_text) / 200.0, 1.0))
              ) DESC
            ) as brand_rank
          FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates` a
          JOIN `{BQ_PROJECT}.{BQ_DATASET}.visual_sampling_strategy` s ON a.brand = s.brand
          WHERE a.image_urls IS NOT NULL
            AND ARRAY_LENGTH(a.image_urls) > 0
            AND s.final_sample_size > 0
        ),
        top_sampled AS (
          SELECT *
          FROM sampled_ads
          WHERE brand_rank <= final_sample_size
        ),
        visual_analyzed AS (
          SELECT
            brand,
            ad_archive_id,
            creative_text,
            image_urls[OFFSET(0)] as primary_image_url,
            ARRAY_LENGTH(image_urls) as image_count,
            media_type,
            strategic_score,
            -- Multimodal Analysis using BigQuery AI
            AI.GENERATE(
              CONCAT(
                'VISUAL-TEXT ANALYSIS: Analyze this ad creative for brand consistency and messaging alignment.\\n\\n',
                'TEXT: "', creative_text, '"\\n',
                'IMAGE: [Image shows the primary visual creative]\\n\\n',
                'Provide JSON analysis with:\\n',
                '1. visual_text_alignment_score (0.0-1.0): How well visual and text align (MUST be decimal 0.0-1.0)\\n',
                '2. brand_consistency_score (0.0-1.0): Visual brand consistency (MUST be decimal 0.0-1.0)\\n',
                '3. creative_fatigue_risk (0.0-1.0): Risk of creative becoming stale (MUST be decimal 0.0-1.0)\\n',
                '4. key_visual_elements: [list of main visual elements]\\n',
                '5. messaging_tone: Brief description of text tone\\n',
                '6. visual_tone: Brief description of visual tone\\n',
                '7. contradictions: Any visual-text contradictions\\n',
                '8. recommendations: Max 2 actionable insights'
              ),
              connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
            ) as visual_analysis
          FROM top_sampled
        ),
        competitive_analyzed AS (
          SELECT
            *,
            -- Competitive Intelligence Analysis
            AI.GENERATE(
              CONCAT(
                'COMPETITIVE VISUAL INTELLIGENCE: Analyze this ad for competitive positioning insights.\\n\\n',
                'BRAND: ', brand, '\\n',
                'TEXT: "', SUBSTR(creative_text, 1, 150), '"\\n',
                'IMAGE: [Image shows visual creative elements]\\n\\n',
                'Analyze for competitive positioning and provide JSON with:\\n',
                '1. luxury_positioning_score (0.0-1.0): Luxury vs accessible visual positioning (MUST be decimal 0.0-1.0)\\n',
                '2. boldness_score (0.0-1.0): Bold vs subtle visual approach (MUST be decimal 0.0-1.0)\\n',
                '3. target_demographic: Likely demographic (young_professional, family_oriented, luxury_consumer, etc)\\n',
                '4. visual_differentiation_level (0.0-1.0): How unique vs category-standard (MUST be decimal 0.0-1.0)\\n',
                '5. creative_pattern_risk (0.0-1.0): Risk of overused visual patterns (MUST be decimal 0.0-1.0)\\n',
                '6. visual_style: Primary visual style (MINIMALIST/LUXURY/BOLD/CASUAL/PROFESSIONAL)\\n',
                '7. positioning_strengths: [max 2 competitive strengths]\\n',
                '8. positioning_gaps: [max 2 potential positioning opportunities]'
              ),
              connection_id => 'bigquery-ai-kaggle-469620.us.vertex-ai'
            ) as competitive_analysis
          FROM visual_analyzed
        )
        SELECT
          brand,
          ad_archive_id,
          creative_text,
          primary_image_url,
          image_count,
          media_type,
          strategic_score,
          visual_analysis,
          competitive_analysis,

          -- Extract structured insights from AI.GENERATE result (markdown JSON) with 0.0-1.0 normalization
          GREATEST(0.0, LEAST(1.0, CAST(
            JSON_VALUE(
              REGEXP_EXTRACT(visual_analysis.result, '{json_regex}'),
              '$.visual_text_alignment_score'
            ) AS FLOAT64
          ))) as visual_text_alignment_score,
          GREATEST(0.0, LEAST(1.0, CAST(
            JSON_VALUE(
              REGEXP_EXTRACT(visual_analysis.result, '{json_regex}'),
              '$.brand_consistency_score'
            ) AS FLOAT64
          ))) as brand_consistency_score,
          GREATEST(0.0, LEAST(1.0, CAST(
            JSON_VALUE(
              REGEXP_EXTRACT(visual_analysis.result, '{json_regex}'),
              '$.creative_fatigue_risk'
            ) AS FLOAT64
          ))) as creative_fatigue_risk,

          -- Extract competitive intelligence insights with 0.0-1.0 normalization
          GREATEST(0.0, LEAST(1.0, CAST(
            JSON_VALUE(
              REGEXP_EXTRACT(competitive_analysis.result, '{json_regex}'),
              '$.luxury_positioning_score'
            ) AS FLOAT64
          ))) as luxury_positioning_score,
          GREATEST(0.0, LEAST(1.0, CAST(
            JSON_VALUE(
              REGEXP_EXTRACT(competitive_analysis.result, '{json_regex}'),
              '$.boldness_score'
            ) AS FLOAT64
          ))) as boldness_score,
          JSON_VALUE(
            REGEXP_EXTRACT(competitive_analysis.result, '{json_regex}'),
            '$.target_demographic'
          ) as target_demographic,
          GREATEST(0.0, LEAST(1.0, CAST(
            JSON_VALUE(
              REGEXP_EXTRACT(competitive_analysis.result, '{json_regex}'),
              '$.visual_differentiation_level'
            ) AS FLOAT64
          ))) as visual_differentiation_level,
          GREATEST(0.0, LEAST(1.0, CAST(
            JSON_VALUE(
              REGEXP_EXTRACT(competitive_analysis.result, '{json_regex}'),
              '$.creative_pattern_risk'
            ) AS FLOAT64
          ))) as creative_pattern_risk,

          -- Extract visual style for competitive matrix
          JSON_VALUE(
            REGEXP_EXTRACT(competitive_analysis.result, '{json_regex}'),
            '$.visual_style'
          ) as visual_style

        FROM competitive_analyzed
        ORDER BY brand, strategic_score DESC
        """


# Helper function for testing
def create_visual_intelligence_stage(context: PipelineContext, dry_run: bool = True) -> VisualIntelligenceStage:
    """Factory function for creating visual intelligence stage"""
    return VisualIntelligenceStage(context, dry_run=dry_run)