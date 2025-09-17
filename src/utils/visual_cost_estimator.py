"""
Visual Intelligence Cost Estimator

Estimates costs for different image budget configurations.
Uses BigQuery INFORMATION_SCHEMA to track actual costs.
"""
import os
from typing import Dict, List, Tuple
from google.cloud import bigquery
from datetime import datetime, timedelta
import json


class VisualIntelligenceCostEstimator:
    """
    Estimates costs for Visual Intelligence image analysis at different budget levels.

    Cost Components:
    1. BigQuery compute: Negligible (~$0.001 per run for SQL)
    2. Vertex AI/Gemini multimodal: Main cost driver
    """

    # Gemini 1.5 Flash pricing (as of 2024)
    # Source: https://cloud.google.com/vertex-ai/generative-ai/pricing
    GEMINI_FLASH_PRICING = {
        'text_input_per_1k_chars': 0.00001875,  # $0.075 per 1M chars / 4
        'image_input_per_image': 0.0025,         # Per image
        'text_output_per_1k_chars': 0.000075,    # $0.30 per 1M chars / 4
    }

    def __init__(self, project_id: str = None):
        self.project_id = project_id or os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
        # Set credentials if available
        if os.path.exists("gcp-creds.json"):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "gcp-creds.json"
        self.client = bigquery.Client(project=self.project_id)

    def estimate_image_analysis_cost(
        self,
        num_images: int,
        avg_text_input_chars: int = 500,
        avg_text_output_chars: int = 800
    ) -> Dict:
        """
        Estimate cost for analyzing a specific number of images.

        Args:
            num_images: Number of images to analyze
            avg_text_input_chars: Average prompt length (chars)
            avg_text_output_chars: Average response length (chars)

        Returns:
            Dict with detailed cost breakdown
        """
        # Calculate costs
        text_input_cost = (num_images * avg_text_input_chars / 1000) * self.GEMINI_FLASH_PRICING['text_input_per_1k_chars']
        image_input_cost = num_images * self.GEMINI_FLASH_PRICING['image_input_per_image']
        text_output_cost = (num_images * avg_text_output_chars / 1000) * self.GEMINI_FLASH_PRICING['text_output_per_1k_chars']

        total_ai_cost = text_input_cost + image_input_cost + text_output_cost

        # BigQuery costs (minimal for our use case)
        # Assume 1MB per image URL in queries
        bigquery_cost = (num_images * 0.000001) * 5  # $5 per TB

        return {
            'num_images': num_images,
            'text_input_cost': round(text_input_cost, 6),
            'image_input_cost': round(image_input_cost, 6),
            'text_output_cost': round(text_output_cost, 6),
            'total_ai_cost': round(total_ai_cost, 4),
            'bigquery_cost': round(bigquery_cost, 6),
            'total_cost': round(total_ai_cost + bigquery_cost, 4),
            'cost_per_image': round((total_ai_cost + bigquery_cost) / num_images, 6) if num_images > 0 else 0
        }

    def compare_budget_scenarios(self, competitor_count: int = 5) -> List[Dict]:
        """
        Compare costs for different budget configurations.

        Args:
            competitor_count: Number of competitors to analyze

        Returns:
            List of cost scenarios
        """
        scenarios = []

        # Define budget levels to test
        budget_configs = [
            {'per_brand': 6, 'max_total': 60, 'name': 'Conservative'},
            {'per_brand': 12, 'max_total': 120, 'name': 'Current'},
            {'per_brand': 20, 'max_total': 200, 'name': 'Enhanced'},
            {'per_brand': 30, 'max_total': 300, 'name': 'Comprehensive'},
            {'per_brand': 50, 'max_total': 500, 'name': 'Maximum'},
        ]

        for config in budget_configs:
            # Calculate actual images analyzed
            total_images = min(
                config['per_brand'] * competitor_count,
                config['max_total']
            )

            # Get cost estimate
            cost = self.estimate_image_analysis_cost(total_images)

            scenario = {
                'name': config['name'],
                'per_brand_budget': config['per_brand'],
                'max_total_budget': config['max_total'],
                'competitors': competitor_count,
                'total_images_analyzed': total_images,
                'estimated_cost_usd': cost['total_cost'],
                'cost_per_image': cost['cost_per_image'],
                'ai_cost': cost['total_ai_cost'],
                'bigquery_cost': cost['bigquery_cost']
            }

            scenarios.append(scenario)

        return scenarios

    def get_historical_costs(self, days_back: int = 7) -> Dict:
        """
        Get actual historical costs from BigQuery INFORMATION_SCHEMA.

        Args:
            days_back: Number of days to look back

        Returns:
            Dict with historical cost data
        """
        try:
            query = f"""
            WITH visual_jobs AS (
                SELECT
                    DATE(creation_time) as date,
                    COUNT(*) as job_count,
                    SUM(total_bytes_billed) / POW(10, 12) as tb_billed,
                    SUM(total_bytes_billed) / POW(10, 12) * 5 as query_cost_usd,
                    -- Count AI operations
                    COUNTIF(
                        REGEXP_CONTAINS(query, r'AI\\.GENERATE|ML\\.GENERATE_TEXT')
                    ) as ai_operations
                FROM `{self.project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
                WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days_back} DAY)
                    AND (
                        REGEXP_CONTAINS(query, r'visual_intelligence')
                        OR REGEXP_CONTAINS(query, r'AI\\.GENERATE')
                        OR REGEXP_CONTAINS(query, r'ML\\.GENERATE_TEXT')
                    )
                    AND state = 'DONE'
                GROUP BY date
            )
            SELECT
                COUNT(DISTINCT date) as days_with_usage,
                SUM(job_count) as total_jobs,
                ROUND(SUM(tb_billed), 6) as total_tb_billed,
                ROUND(SUM(query_cost_usd), 4) as total_query_cost,
                SUM(ai_operations) as total_ai_operations,
                ROUND(AVG(query_cost_usd), 4) as avg_daily_cost,
                ROUND(MAX(query_cost_usd), 4) as max_daily_cost
            FROM visual_jobs
            """

            result = self.client.query(query).result()

            for row in result:
                return {
                    'period_days': days_back,
                    'days_with_usage': row.days_with_usage or 0,
                    'total_jobs': row.total_jobs or 0,
                    'total_tb_billed': float(row.total_tb_billed) if row.total_tb_billed else 0,
                    'total_query_cost': float(row.total_query_cost) if row.total_query_cost else 0,
                    'total_ai_operations': row.total_ai_operations or 0,
                    'avg_daily_cost': float(row.avg_daily_cost) if row.avg_daily_cost else 0,
                    'max_daily_cost': float(row.max_daily_cost) if row.max_daily_cost else 0,
                }

            return {'message': 'No historical data found', 'period_days': days_back}

        except Exception as e:
            return {'error': str(e), 'period_days': days_back}

    def generate_cost_report(self, competitor_count: int = 5) -> str:
        """Generate a comprehensive cost report."""
        report = []
        report.append("\n" + "=" * 70)
        report.append("💰 VISUAL INTELLIGENCE COST ANALYSIS REPORT")
        report.append("=" * 70)

        # Current pricing
        report.append("\n📊 GEMINI 1.5 FLASH PRICING (per image):")
        report.append(f"  • Image input: ${self.GEMINI_FLASH_PRICING['image_input_per_image']:.4f}")
        report.append(f"  • Text input (500 chars): ${(500/1000) * self.GEMINI_FLASH_PRICING['text_input_per_1k_chars']:.6f}")
        report.append(f"  • Text output (800 chars): ${(800/1000) * self.GEMINI_FLASH_PRICING['text_output_per_1k_chars']:.6f}")
        report.append(f"  • Total per image: ~${self.estimate_image_analysis_cost(1)['total_cost']:.4f}")

        # Budget scenarios
        report.append("\n📈 BUDGET SCENARIOS (" + str(competitor_count) + " competitors):")
        report.append("-" * 70)
        report.append(f"{'Config':<15} {'Images':<10} {'Cost/Image':<12} {'Total Cost':<12} {'vs Current'}")
        report.append("-" * 70)

        scenarios = self.compare_budget_scenarios(competitor_count)
        current_cost = scenarios[1]['estimated_cost_usd']  # 'Current' is index 1

        for scenario in scenarios:
            diff = ((scenario['estimated_cost_usd'] / current_cost) - 1) * 100 if current_cost > 0 else 0
            diff_str = f"+{diff:.0f}%" if diff > 0 else f"{diff:.0f}%"

            report.append(
                f"{scenario['name']:<15} "
                f"{scenario['total_images_analyzed']:<10} "
                f"${scenario['cost_per_image']:<11.4f} "
                f"${scenario['estimated_cost_usd']:<11.2f} "
                f"{diff_str if scenario['name'] != 'Current' else 'baseline'}"
            )

        # Historical costs
        report.append("\n📅 HISTORICAL USAGE (Last 7 days):")
        historical = self.get_historical_costs(7)

        if 'error' not in historical:
            report.append(f"  • Days with usage: {historical.get('days_with_usage', 0)}")
            report.append(f"  • Total AI operations: {historical.get('total_ai_operations', 0)}")
            report.append(f"  • Total BigQuery cost: ${historical.get('total_query_cost', 0):.4f}")
            report.append(f"  • Avg daily cost: ${historical.get('avg_daily_cost', 0):.4f}")
        else:
            report.append(f"  • No historical data available")

        # Recommendations
        report.append("\n💡 RECOMMENDATIONS:")
        report.append("  1. Main costs are from Gemini AI, not BigQuery storage/compute")
        report.append("  2. Current budget (12/brand, 120 max) costs ~$0.30-0.35 per run")
        report.append("  3. Doubling to 20/brand would cost ~$0.50-0.55 per run")
        report.append("  4. Consider adaptive sampling based on competitor importance")

        report.append("=" * 70)
        return "\n".join(report)


def main():
    """Test the cost estimator"""
    estimator = VisualIntelligenceCostEstimator()

    # Generate comprehensive report
    print(estimator.generate_cost_report(competitor_count=5))

    # Test specific scenario
    print("\n🧪 Custom Scenario Test:")
    print("What if we analyze 100 images?")
    cost = estimator.estimate_image_analysis_cost(100)
    print(f"  Total cost: ${cost['total_cost']:.2f}")
    print(f"  AI cost: ${cost['total_ai_cost']:.2f}")
    print(f"  BigQuery cost: ${cost['bigquery_cost']:.4f}")


if __name__ == "__main__":
    main()