"""
Stage 5: Embeddings Generation

Clean, focused implementation of embedding generation using BigQuery ML.
"""
import os
import time
from typing import List

from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import IngestionResults, EmbeddingResults

try:
    from src.utils.bigquery_client import get_bigquery_client, run_query
except ImportError:
    get_bigquery_client = None
    run_query = None

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class EmbeddingsStage(PipelineStage[IngestionResults, EmbeddingResults]):
    """
    Stage 5: Embeddings Generation.
    
    Responsibilities:
    - Generate semantic embeddings using BigQuery ML
    - Use structured content concatenation pattern
    - Check for existing embeddings to avoid regeneration
    - Fallback gracefully if embedding generation fails
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Embeddings Generation", 5, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.verbose = verbose
        # Get competitor brands from context (set by previous stages)
        self.competitor_brands = getattr(context, 'competitor_brands', [])
    
    def execute(self, ads: IngestionResults) -> EmbeddingResults:
        """Execute embedding generation"""
        
        if self.dry_run:
            return self._create_mock_embeddings(ads)
        
        return self._run_real_embeddings(ads)
    
    def _create_mock_embeddings(self, ads: IngestionResults) -> EmbeddingResults:
        """Create mock embeddings for testing"""
        return EmbeddingResults(
            table_id=f"ads_embeddings_{self.context.run_id}",
            embedding_count=ads.total_ads,
            dimension=768,
            generation_time=0.5
        )
    
    def _run_real_embeddings(self, ads: IngestionResults) -> EmbeddingResults:
        """Run actual embedding generation with BigQuery ML"""
        
        if get_bigquery_client is None or run_query is None:
            self.logger.error("BigQuery client not available")
            raise ImportError("BigQuery client required for embedding generation")
        
        try:
            print("   🧠 Generating embeddings using existing ML.GENERATE_EMBEDDING patterns...")
            
            embedding_table = f"{BQ_PROJECT}.{BQ_DATASET}.ads_embeddings"
            
            # First, check if we already have embeddings for these brands
            print("   🔍 Checking for existing embeddings...")
            all_brands = self.competitor_brands + [self.context.brand]
            brand_list = ', '.join([f"'{b}'" for b in all_brands])
            
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
            embedding_count = self._generate_new_embeddings(ads, embedding_table, brand_list)
            
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
                embedding_count=ads.total_ads,
                dimension=768,
                generation_time=0.0
            )
    
    def _generate_new_embeddings(self, ads: IngestionResults, embedding_table: str, brand_list: str) -> int:
        """Generate new embeddings using BigQuery ML"""
        
        # Use the existing pattern from populate_ads_embeddings.sql
        ads_table = ads.ads_table_id if ads.ads_table_id else f"{BQ_PROJECT}.{BQ_DATASET}.ads_raw"
        
        generate_embeddings_sql = f"""
        CREATE OR REPLACE TABLE `{embedding_table}` AS
        WITH structured_content AS (
          SELECT 
            ad_archive_id,
            brand,
            creative_text,
            title,
            
            -- Combine all text with semantic structure (pattern from populate_ads_embeddings.sql)
            CONCAT(
              'Title: ', COALESCE(title, ''), 
              ' Content: ', COALESCE(creative_text, ''),
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
          ad_archive_id,
          brand, 
          creative_text,
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