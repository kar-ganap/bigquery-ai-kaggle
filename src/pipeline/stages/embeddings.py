"""
Stage 5: Embeddings Generation

Clean, focused implementation of embedding generation using BigQuery ML.
"""
import os
import time
from typing import List

from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import StrategicLabelResults, EmbeddingResults

try:
    from src.utils.bigquery_client import get_bigquery_client, run_query
except ImportError:
    get_bigquery_client = None
    run_query = None

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class EmbeddingsStage(PipelineStage[StrategicLabelResults, EmbeddingResults]):
    """
    Stage 6: Embeddings Generation.

    Responsibilities:
    - Generate semantic embeddings using BigQuery ML
    - Use structured content concatenation pattern
    - Check for existing embeddings to avoid regeneration
    - Fallback gracefully if embedding generation fails
    """

    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Embeddings Generation", 6, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.verbose = verbose
        # Get competitor brands from context (set by previous stages)
        self.competitor_brands = getattr(context, 'competitor_brands', [])
    
    def execute(self, labels: StrategicLabelResults) -> EmbeddingResults:
        """Execute embedding generation"""
        
        if self.dry_run:
            return self._create_mock_embeddings(labels)

        return self._run_real_embeddings(labels)
    
    def _create_mock_embeddings(self, labels: StrategicLabelResults) -> EmbeddingResults:
        """Create mock embeddings for testing"""
        return EmbeddingResults(
            table_id=f"ads_embeddings_{self.context.run_id}",
            embedding_count=labels.labeled_ads,
            dimension=768,
            generation_time=0.5
        )
    
    def _run_real_embeddings(self, labels: StrategicLabelResults) -> EmbeddingResults:
        """Run actual embedding generation with BigQuery ML"""
        
        if get_bigquery_client is None or run_query is None:
            self.logger.error("BigQuery client not available")
            raise ImportError("BigQuery client required for embedding generation")
        
        try:
            print("   🧠 Generating embeddings using existing ML.GENERATE_EMBEDDING patterns...")
            
            embedding_table = f"{BQ_PROJECT}.{BQ_DATASET}.ads_embeddings"
            
            # First, get ALL brands from the deduplicated ads_with_dates table
            print("   🔍 Discovering all brands from deduplicated data...")
            # Use the ads_with_dates table created by strategic labeling stage
            ads_table = labels.table_id if hasattr(labels, 'table_id') and labels.table_id else f"{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates"

            # Get actual brands from ingested data
            brand_discovery_sql = f"""
            SELECT DISTINCT brand
            FROM `{ads_table}`
            WHERE brand IS NOT NULL
              AND (creative_text IS NOT NULL OR title IS NOT NULL)
            ORDER BY brand
            """

            try:
                brand_result = run_query(brand_discovery_sql)
                if not brand_result.empty:
                    discovered_brands = brand_result['brand'].tolist()
                    print(f"   ✅ Discovered {len(discovered_brands)} brands in data: {', '.join(discovered_brands)}")
                    all_brands = discovered_brands
                else:
                    # Fallback to original logic
                    all_brands = self.competitor_brands + [self.context.brand]
                    print(f"   ⚠️  No brands discovered, using context: {', '.join(all_brands)}")
            except Exception as e:
                print(f"   ⚠️  Brand discovery failed ({e}), using context: {', '.join(self.competitor_brands + [self.context.brand])}")
                all_brands = self.competitor_brands + [self.context.brand]

            brand_list = ', '.join([f"'{b}'" for b in all_brands])
            print(f"   🎯 Will embed {len(all_brands)} brands: {', '.join(all_brands)}")
            
            existing_count = 0
            try:
                check_existing_sql = f"""
                SELECT COUNT(*) as existing_count,
                       COUNT(DISTINCT brand) as brands_with_embeddings
                FROM `{embedding_table}`
                WHERE brand IN ({brand_list})
                """
                
                existing_result = run_query(check_existing_sql)
                existing_count = existing_result.iloc[0]['existing_count'] if not existing_result.empty else 0
            except Exception as e:
                # Table doesn't exist yet, which is fine - we'll create embeddings
                print(f"   📝 No existing embeddings table found: {e}")
                existing_count = 0
            
            # Force fresh embeddings generation every time for accurate results
            print("   🔨 Generating fresh embeddings for accurate analysis...")
            embedding_count = self._generate_new_embeddings(labels, embedding_table, brand_list)
            
            return EmbeddingResults(
                table_id=embedding_table,
                embedding_count=embedding_count,
                dimension=768,
                generation_time=0.0  # Will be set by caller
            )
            
        except Exception as e:
            self.logger.error(f"Embedding generation failed: {str(e)}")
            print(f"   ❌ Embedding generation failed: {e}")
            
            # Fallback to mock results
            return EmbeddingResults(
                table_id=f"ads_embeddings_mock",
                embedding_count=getattr(labels, 'labeled_ads', 0),
                dimension=768,
                generation_time=0.0
            )
    
    def _generate_new_embeddings(self, labels: StrategicLabelResults, embedding_table: str, brand_list: str) -> int:
        """Generate new embeddings using BigQuery ML"""
        
        # Use the deduplicated ads_with_dates table from strategic labeling stage
        ads_table = labels.table_id if hasattr(labels, 'table_id') and labels.table_id else f"{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates"
        
        generate_embeddings_sql = f"""
        CREATE OR REPLACE TABLE `{embedding_table}` AS
        WITH structured_content AS (
          SELECT
            -- PRESERVE ALL CORE INVIOLABLE FIELDS FROM ads_with_dates
            ad_archive_id,
            brand,
            creative_text,
            title,
            cta_text,              -- CRITICAL: Preserve CTA text
            media_type,
            media_storage_path,
            start_date_string,
            end_date_string,
            publisher_platforms,   -- CRITICAL: Preserve for channel intelligence
            page_name,
            snapshot_url,

            -- ADD embedding-specific processing (don't replace core fields)
            CONCAT(
              'Title: ', COALESCE(title, ''),
              ' Content: ', COALESCE(creative_text, ''),
              ' CTA: ', COALESCE(cta_text, ''),
              ' Brand: ', COALESCE(brand, '')
            ) as structured_text,

            -- Quality indicators
            title IS NOT NULL AND LENGTH(TRIM(title)) > 0 as has_title,
            creative_text IS NOT NULL AND LENGTH(TRIM(creative_text)) > 0 as has_body,
            LENGTH(COALESCE(creative_text, '') || ' ' || COALESCE(title, '')) as content_length_chars

          FROM `{ads_table}`
          WHERE brand IN ({brand_list})
            AND (creative_text IS NOT NULL OR title IS NOT NULL)
        ),
        
        embeddings AS (
          SELECT *
          FROM ML.GENERATE_EMBEDDING(
            MODEL `{BQ_PROJECT}.{BQ_DATASET}.text_embedding_model`,
            (
              SELECT
                ad_archive_id,
                brand,
                creative_text,
                title,                 -- INCLUDE: Needed for final output
                cta_text,             -- INCLUDE: Needed for final output
                media_type,
                media_storage_path,
                start_date_string,
                end_date_string,
                publisher_platforms,
                page_name,
                snapshot_url,
                structured_text as content,
                has_title,
                has_body,
                content_length_chars
              FROM structured_content
            ),
            STRUCT('SEMANTIC_SIMILARITY' as task_type)
          )
        )
        
        SELECT
          -- PRESERVE ALL CORE INVIOLABLE FIELDS
          ad_archive_id,
          brand,
          creative_text,
          title,
          cta_text,              -- CRITICAL: Preserve CTA text
          media_type,
          media_storage_path,
          start_date_string,
          end_date_string,
          publisher_platforms,   -- CRITICAL: Preserve for channel intelligence
          page_name,
          snapshot_url,

          -- ADD embeddings and quality metrics
          content as structured_content,
          ml_generate_embedding_result as content_embedding,
          content_length_chars,
          has_title,
          has_body
        FROM embeddings
        """
        
        try:
            run_query(generate_embeddings_sql)
            
            # Count the results
            count_result = run_query(f"SELECT COUNT(*) as count FROM `{embedding_table}` WHERE brand IN ({brand_list})")
            embedding_count = count_result.iloc[0]['count'] if not count_result.empty else 0
            print(f"   ✅ Generated {embedding_count} embeddings")
            return embedding_count
            
        except Exception as e:
            print(f"   ⚠️  Embedding generation failed, checking for existing: {e}")
            # Fallback to checking existing tables
            check_existing_sql = f"""
            SELECT COUNT(*) as existing_count
            FROM `{embedding_table}`
            WHERE brand IN ({brand_list})
            """
            fallback_result = run_query(check_existing_sql)
            return fallback_result.iloc[0]['existing_count'] if not fallback_result.empty else 0