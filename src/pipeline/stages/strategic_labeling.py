"""
Stage 4.5: Strategic Labeling

Executes the enhanced SQL script to generate strategic labels for temporal intelligence.
Integrates existing AI.GENERATE_TABLE infrastructure into the modular pipeline.
"""
import os
import time
from typing import List

from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import IngestionResults, StrategicLabelResults

try:
    from scripts.utils.bigquery_client import get_bigquery_client, run_query
except ImportError:
    get_bigquery_client = None
    run_query = None

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class StrategicLabelingStage(PipelineStage[IngestionResults, StrategicLabelResults]):
    """
    Stage 4.5: Strategic Labeling.
    
    Responsibilities:
    - Execute existing SQL script (sql/02_label_ads.sql) with dynamic project/dataset
    - Generate comprehensive strategic labels using AI.GENERATE_TABLE
    - Create ads_with_dates table with both original and temporal intelligence fields
    - Bridge between raw ad ingestion and sophisticated temporal analysis
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Strategic Labeling", 4.5, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.verbose = verbose
        # Get competitor brands from context (set by previous stages)
        self.competitor_brands = getattr(context, 'competitor_brands', [])
    
    def execute(self, ads: IngestionResults) -> StrategicLabelResults:
        """Execute strategic labeling using enhanced SQL script"""
        
        if self.dry_run:
            return self._create_mock_labels(ads)
        
        return self._run_real_labeling(ads)
    
    def _create_mock_labels(self, ads: IngestionResults) -> StrategicLabelResults:
        """Create mock strategic labels for testing"""
        return StrategicLabelResults(
            table_id=f"{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates",
            labeled_ads=ads.total_ads,
            generation_time=0.5
        )
    
    def _run_real_labeling(self, ads: IngestionResults) -> StrategicLabelResults:
        """Run actual strategic labeling using enhanced SQL script"""
        
        if get_bigquery_client is None or run_query is None:
            self.logger.error("BigQuery client not available")
            raise ImportError("BigQuery client required for strategic labeling")
        
        try:
            print("   🧠 Generating strategic labels using enhanced AI.GENERATE_TABLE script...")
            
            # Read the enhanced SQL script
            script_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 
                "sql", 
                "02_label_ads.sql"
            )
            
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Strategic labeling SQL script not found at: {script_path}")
            
            with open(script_path, 'r') as f:
                sql_template = f.read()
            
            # Replace template placeholders with actual project/dataset
            ads_table = ads.ads_table_id if ads.ads_table_id else f"{BQ_PROJECT}.{BQ_DATASET}.ads_raw"
            strategic_sql = sql_template.replace("yourproj.ads_demo.ads_raw", ads_table)
            strategic_sql = strategic_sql.replace("yourproj.ads_demo.ads_with_dates", f"{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates")
            
            # Check if strategic labels already exist
            labels_table = f"{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates"
            all_brands = self.competitor_brands + [self.context.brand]
            brand_list = ', '.join([f"'{b}'" for b in all_brands])
            
            existing_count = 0
            try:
                check_existing_sql = f"""
                SELECT COUNT(*) as existing_count
                FROM `{labels_table}`
                WHERE brand IN ({brand_list})
                  AND promotional_intensity IS NOT NULL
                  AND funnel IS NOT NULL
                """
                
                existing_result = run_query(check_existing_sql)
                existing_count = existing_result.iloc[0]['existing_count'] if not existing_result.empty else 0
            except Exception as e:
                # Table doesn't exist yet, which is fine
                print(f"   📝 No existing strategic labels found: {e}")
                existing_count = 0
            
            if existing_count > 0:
                print(f"   ✅ Found {existing_count} existing strategic labels, using those")
                labeled_count = existing_count
            else:
                print("   🔨 Executing strategic labeling SQL script...")
                labeled_count = self._execute_strategic_sql(strategic_sql, labels_table, brand_list)
            
            return StrategicLabelResults(
                table_id=labels_table,
                labeled_ads=labeled_count,
                generation_time=0.0  # Will be set by caller
            )
            
        except Exception as e:
            self.logger.error(f"Strategic labeling failed: {str(e)}")
            print(f"   ❌ Strategic labeling failed: {e}")
            
            # Fallback - just pass through without strategic labels
            return StrategicLabelResults(
                table_id="ads_with_dates_fallback", 
                labeled_ads=ads.total_ads,
                generation_time=0.0
            )
    
    def _execute_strategic_sql(self, sql: str, labels_table: str, brand_list: str) -> int:
        """Execute the strategic labeling SQL and return count"""
        
        try:
            print("   🤖 Running AI strategic analysis with comprehensive labeling...")
            run_query(sql)
            
            # Count the results
            count_result = run_query(f"SELECT COUNT(*) as count FROM `{labels_table}` WHERE brand IN ({brand_list})")
            labeled_count = count_result.iloc[0]['count'] if not count_result.empty else 0
            print(f"   ✅ Generated strategic labels for {labeled_count} ads")
            
            # Verify both original and temporal intelligence fields were generated
            verification_sql = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(promotional_intensity) as with_temporal_labels,
                COUNT(funnel) as with_funnel_labels,
                COUNT(angles) as with_angle_labels
            FROM `{labels_table}` 
            WHERE brand IN ({brand_list})
            """
            
            verification_result = run_query(verification_sql)
            if not verification_result.empty:
                row = verification_result.iloc[0]
                print(f"   📊 Verification: {row['total_records']} total, "
                      f"{row['with_temporal_labels']} with temporal labels, "
                      f"{row['with_funnel_labels']} with funnel, "
                      f"{row['with_angle_labels']} with angles")
            
            return labeled_count
            
        except Exception as e:
            print(f"   ⚠️  Strategic labeling SQL execution failed: {e}")
            # Return 0 to indicate failure
            return 0