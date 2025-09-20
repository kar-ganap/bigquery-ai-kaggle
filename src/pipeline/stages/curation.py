"""
Stage 2: AI Competitor Curation via BigQuery

Clean, focused implementation of competitor curation with hybrid consensus validation.
"""
import os
import time
import pandas as pd
from datetime import datetime
from typing import List

from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import CompetitorCandidate, ValidatedCompetitor

try:
    from src.utils.bigquery_client import get_bigquery_client, run_query, load_dataframe_to_bq
    from src.competitive_intel.curation.competitor_name_validator import CompetitorNameValidator
except ImportError:
    get_bigquery_client = None
    run_query = None
    load_dataframe_to_bq = None
    CompetitorNameValidator = None

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class CurationStage(PipelineStage[List[CompetitorCandidate], List[ValidatedCompetitor]]):
    """
    Stage 2: AI-powered competitor curation with hybrid consensus validation.
    
    Responsibilities:
    - Aggressive pre-filtering using enhanced name validation
    - Deterministic scoring to reduce expensive AI calls
    - 3-round AI consensus validation via BigQuery ML
    - Quality scoring and tier assignment
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False):
        super().__init__("AI Competitor Curation", 2, context.run_id)
        self.context = context
        self.dry_run = dry_run

        if not dry_run and CompetitorNameValidator:
            self.name_validator = CompetitorNameValidator()
        else:
            self.name_validator = None

    def ensure_gemini_model(self):
        """Ensure Gemini model is available for AI.GENERATE_TABLE"""
        model_id = f"{BQ_PROJECT}.{BQ_DATASET}.gemini_model"

        if get_bigquery_client is None:
            raise ImportError("BigQuery client required for model creation")

        client = get_bigquery_client()

        try:
            # Check if model exists
            client.get_model(model_id)
            print(f"   ✅ Gemini model exists: {model_id}")
            return model_id
        except Exception as e:
            # Create remote model connection to Gemini
            print(f"   🔧 Creating Gemini model connection: {model_id}")
            create_model_sql = f"""
            CREATE OR REPLACE MODEL `{model_id}`
            REMOTE WITH CONNECTION `{BQ_PROJECT}.us.vertex-ai`
            OPTIONS (ENDPOINT = 'gemini-2.5-flash');
            """

            try:
                client.query(create_model_sql).result()
                print(f"   ✅ Created Gemini model: {model_id}")
                return model_id
            except Exception as e:
                print(f"   ❌ Failed to create Gemini model: {e}")
                print("   💡 Please ensure you have a Vertex AI connection set up")
                raise
    
    def execute(self, candidates: List[CompetitorCandidate]) -> List[ValidatedCompetitor]:
        """Execute AI competitor curation"""
        
        if self.dry_run:
            return self._create_mock_validated_competitors(candidates)
        
        return self._run_real_curation(candidates)
    
    def _create_mock_validated_competitors(self, candidates: List[CompetitorCandidate]) -> List[ValidatedCompetitor]:
        """Create mock validated competitors for testing"""
        validated = [
            ValidatedCompetitor(
                company_name=c.company_name,
                is_competitor=True,
                tier="Direct-Rival" if i < 3 else "Adjacent",
                market_overlap_pct=80 - i*10,
                customer_substitution_ease="Easy" if i < 3 else "Medium",
                confidence=0.9 - i*0.05,
                reasoning="Mock reasoning for testing",
                evidence_sources="Mock sources for testing",
                quality_score=0.8 - i*0.05
            )
            for i, c in enumerate(candidates[:5])
        ]
        
        print(f"   📈 Mock curation generated {len(validated)} validated competitors")
        return validated
    
    def _run_real_curation(self, candidates: List[CompetitorCandidate]) -> List[ValidatedCompetitor]:
        """Run actual AI curation with hybrid consensus validation"""
        
        if not all([get_bigquery_client, load_dataframe_to_bq, run_query]):
            self.logger.error("BigQuery utilities not available")
            raise ImportError("BigQuery utilities required for curation")
        
        if not self.name_validator:
            self.logger.error("CompetitorNameValidator not available")
            raise ImportError("CompetitorNameValidator required for curation")
        
        print("   📋 Preparing candidates for AI curation...")
        
        # Convert candidates to DataFrame
        candidates_data = []
        for c in candidates:
            candidates_data.append({
                'target_brand': self.context.brand,
                'target_vertical': self.context.vertical or 'Unknown',
                'company_name': c.company_name,
                'source_url': c.source_url,
                'source_title': c.source_title,
                'query_used': c.query_used,
                'raw_score': c.raw_score,
                'found_in': c.found_in,
                'discovery_method': c.discovery_method,
                'discovered_at': datetime.now()
            })
        
        df_candidates = pd.DataFrame(candidates_data)
        
        # Apply aggressive pre-filtering to reduce AI workload
        df_validated = self._apply_name_validation_filter(df_candidates)
        
        if df_validated.empty:
            print("   ⚠️  No valid competitor names found after validation")
            return []
        
        # Load validated candidates to BigQuery
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.competitors_raw_{self.context.run_id}"
        print(f"   💾 Loading {len(df_validated)} validated candidates to BigQuery...")
        load_dataframe_to_bq(df_validated, table_id, write_disposition="WRITE_TRUNCATE")
        
        # Stage 1: Deterministic pre-filtering
        df_prefiltered = self._apply_deterministic_prefiltering(table_id)
        
        if df_prefiltered.empty:
            print("   ⚠️  No candidates passed pre-filtering")
            return []
        
        # Stage 2: AI consensus validation
        df_curated = self._run_ai_consensus_validation(df_prefiltered)
        
        if df_curated.empty:
            print("   ⚠️  No competitors validated by AI consensus")
            return []
        
        # Calculate quality scores and convert to ValidatedCompetitor objects
        validated_competitors = self._process_final_results(df_curated)
        
        print(f"   ✅ Validated {len(validated_competitors)} competitors from {len(candidates)} candidates")
        return validated_competitors
    
    def _apply_name_validation_filter(self, df_candidates: pd.DataFrame) -> pd.DataFrame:
        """Apply aggressive pre-filtering using enhanced name validator"""
        print(f"   🔍 Aggressive pre-filtering {len(df_candidates)} candidates with enhanced name validator...")
        
        candidate_names = df_candidates['company_name'].tolist()
        
        # Use high confidence threshold (0.7+) to get cleanest candidates
        high_confidence_results = self.name_validator.get_high_confidence_competitors(candidate_names, min_confidence=0.7)
        high_confidence_names = [name for name, result in high_confidence_results]
        
        # If we don't have enough high confidence names, use regular threshold but cap
        if len(high_confidence_names) < 50:
            regular_results = self.name_validator.get_clean_competitors(candidate_names, min_confidence=0.4)
            # Sort by confidence and take top candidates
            regular_results.sort(key=lambda x: x[1].confidence, reverse=True)
            valid_names = [name for name, result in regular_results[:100]]  # Cap at 100 for AI performance
            print(f"   ⚠️  Only {len(high_confidence_names)} high-confidence names, using top 100 regular confidence names")
        else:
            valid_names = high_confidence_names[:75]  # Cap high confidence at 75 for faster AI processing
            print(f"   ✅ Using {len(valid_names)} high-confidence names (capped at 75)")
        
        # Filter DataFrame to keep only selected names
        df_filtered = df_candidates[df_candidates['company_name'].isin(valid_names)]
        
        # Add validation metadata
        valid_candidates = []
        for _, row in df_filtered.iterrows():
            validation = self.name_validator.validate_name(row['company_name'])
            row_dict = row.to_dict()
            row_dict['name_validation_confidence'] = validation.confidence
            row_dict['name_validation_reasons'] = '; '.join(validation.reasons)
            valid_candidates.append(row_dict)
        
        df_validated = pd.DataFrame(valid_candidates)
        filtered_count = len(candidate_names) - len(df_validated)
        print(f"   📊 Aggressively filtered out {filtered_count} candidates ({filtered_count/len(candidate_names)*100:.1f}%)")
        print(f"   ✅ Kept {len(df_validated)} highest-quality names for AI curation")
        
        return df_validated
    
    def _apply_deterministic_prefiltering(self, table_id: str) -> pd.DataFrame:
        """Apply deterministic pre-filtering to reduce AI calls"""
        print("   📊 Stage 1: Deterministic pre-filtering...")
        
        prefilter_query = f"""
        SELECT *,
          -- Pre-filter score based on deterministic signals
          (
            (raw_score * 0.4) +
            (CASE WHEN discovery_method = 'standard' THEN 0.3 ELSE 0.15 END) +
            (CASE WHEN found_in = 'title' THEN 0.2 ELSE 0.1 END) +
            (CASE 
              WHEN LOWER(source_title) LIKE '%competitor%' OR LOWER(source_title) LIKE '%alternative%' THEN 0.1
              ELSE 0.0 END)
          ) as prefilter_score
        FROM `{table_id}`
        WHERE 
          -- Only pass high-potential candidates to expensive AI validation
          (
            (raw_score >= 2.0 AND discovery_method = 'standard') OR
            (raw_score >= 2.5 AND discovery_method != 'standard') OR
            (raw_score >= 1.5 AND (LOWER(source_title) LIKE '%competitor%' OR LOWER(source_title) LIKE '%alternative%')) OR
            (raw_score >= 1.8 AND found_in = 'title')
          )
        ORDER BY prefilter_score DESC
        LIMIT 15  -- Cap at 15 candidates for AI validation (45 total AI calls)
        """
        
        df_prefiltered = run_query(prefilter_query, BQ_PROJECT)
        prefilter_count = len(df_prefiltered)
        print(f"   ✅ Pre-filtered to {prefilter_count} high-potential candidates")
        
        return df_prefiltered
    
    def _run_ai_consensus_validation(self, df_prefiltered: pd.DataFrame) -> pd.DataFrame:
        """Run AI consensus validation with 3 rounds per candidate"""
        prefilter_count = len(df_prefiltered)
        print(f"   🧠 Stage 2: AI consensus validation for {prefilter_count} candidates...")

        # Ensure Gemini model is available
        model_id = self.ensure_gemini_model()

        # Prepare consensus validation query
        consensus_results = []
        batch_size = 5  # Process in batches to manage BigQuery limits
        
        for batch_start in range(0, prefilter_count, batch_size):
            batch_end = min(batch_start + batch_size, prefilter_count)
            batch_df = df_prefiltered.iloc[batch_start:batch_end]
            print(f"     Processing batch {batch_start//batch_size + 1} ({len(batch_df)} candidates)...")
            
            # Load batch to BigQuery for consensus validation
            batch_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.competitors_batch_{self.context.run_id}_{batch_start}"
            load_dataframe_to_bq(batch_df, batch_table_id, write_disposition="WRITE_TRUNCATE")
            
            # Run 3 AI validation rounds for this batch
            batch_consensus = []
            for round_num in range(3):
                print(f"       AI validation round {round_num + 1}/3...")
                
                ai_query = self._build_ai_validation_query(batch_table_id, round_num)
                round_result = run_query(ai_query, BQ_PROJECT)
                batch_consensus.append(round_result)
            
            consensus_results.extend(batch_consensus)
        
        print("   🗳️  Computing consensus from 3 AI validation rounds...")
        
        # Compute consensus for each candidate
        return self._compute_consensus_results(consensus_results, df_prefiltered)
    
    def _build_ai_validation_query(self, batch_table_id: str, round_num: int) -> str:
        """Build AI validation query for BigQuery ML"""
        return f"""
        WITH source_data AS (
          SELECT *,
            CONCAT(
              'Analyze if "', company_name, '" is a legitimate competitor of "', '{self.context.brand}', 
              '" in the ', '{self.context.vertical or 'Unknown'}', ' industry. ',
              
              'Context: This candidate was found through the query "', query_used, 
              '" in a web search result titled "', source_title, '". ',
              'Discovery method: ', discovery_method, '. ',
              'Search relevance score: ', CAST(raw_score as STRING), '. ',
              'Pre-filter score: ', CAST(ROUND(prefilter_score, 2) as STRING), '. ',
              
              'Instructions: Be conservative - only mark as competitor if confident they actually compete.',
              '1. company_name: Return the exact company name "', company_name, '"',
              '2. is_competitor: TRUE if this is a real company that competes with the target brand, FALSE otherwise',
              '3. tier: Categorize as "Direct-Rival", "Market-Leader", "Disruptor", "Niche-Player", or "Adjacent"',
              '4. market_overlap_pct: Estimate 0-100% how much their target markets overlap',
              '5. customer_substitution_ease: "Easy", "Medium", or "Hard"',
              '6. confidence: 0.0-1.0 confidence in your assessment',
              '7. reasoning: Brief explanation (max 200 chars)',
              '8. evidence_sources: What information you used (max 150 chars)'
            ) as analysis_prompt
          FROM `{batch_table_id}`
        ),
        ai_analysis AS (
          SELECT * FROM AI.GENERATE_TABLE(
            MODEL `{BQ_PROJECT}.{BQ_DATASET}.gemini_model`,
            (SELECT analysis_prompt as prompt FROM source_data),
            STRUCT(
              'company_name STRING, is_competitor BOOL, tier STRING, market_overlap_pct INT64, customer_substitution_ease STRING, confidence FLOAT64, reasoning STRING, evidence_sources STRING'
              AS output_schema,
              'SHARED' AS request_type
            )
          )
        )
        SELECT 
          orig.*,
          ai.is_competitor,
          ai.tier,
          ai.market_overlap_pct,
          ai.customer_substitution_ease,
          ai.confidence,
          ai.reasoning,
          ai.evidence_sources,
          {round_num} as round_num
        FROM source_data orig
        JOIN ai_analysis ai ON orig.company_name = ai.company_name
        """
    
    def _compute_consensus_results(self, consensus_results: List[pd.DataFrame], df_prefiltered: pd.DataFrame) -> pd.DataFrame:
        """Compute consensus from multiple AI validation rounds"""
        # Combine all rounds
        all_rounds_df = pd.concat(consensus_results, ignore_index=True)
        
        final_consensus = []
        for company_name in df_prefiltered['company_name'].unique():
            company_rounds = all_rounds_df[all_rounds_df['company_name'] == company_name]
            
            if len(company_rounds) < 3:
                print(f"     ⚠️  Only {len(company_rounds)} rounds for {company_name}, skipping")
                continue
            
            # Majority vote for is_competitor
            is_competitor_votes = company_rounds['is_competitor'].sum()
            consensus_is_competitor = is_competitor_votes >= 2  # 2 out of 3
            
            if not consensus_is_competitor:
                continue  # Skip non-competitors
            
            # Average numerical values
            consensus_confidence = company_rounds['confidence'].mean()
            consensus_market_overlap = int(company_rounds['market_overlap_pct'].mean())
            
            # Mode for categorical values
            consensus_tier = company_rounds['tier'].mode().iloc[0] if not company_rounds['tier'].mode().empty else 'Niche-Player'
            consensus_substitution = company_rounds['customer_substitution_ease'].mode().iloc[0] if not company_rounds['customer_substitution_ease'].mode().empty else 'Medium'
            
            # Combine reasoning from best round (highest confidence)
            best_round = company_rounds.loc[company_rounds['confidence'].idxmax()]
            
            consensus_record = best_round.copy()
            consensus_record['is_competitor'] = consensus_is_competitor
            consensus_record['confidence'] = consensus_confidence
            consensus_record['market_overlap_pct'] = consensus_market_overlap
            consensus_record['tier'] = consensus_tier
            consensus_record['customer_substitution_ease'] = consensus_substitution
            consensus_record['reasoning'] = f"Consensus ({is_competitor_votes}/3 votes): {best_round['reasoning']}"
            
            final_consensus.append(consensus_record)
        
        print(f"   ✅ Consensus validation complete: {len(final_consensus)} validated competitors")
        return pd.DataFrame(final_consensus) if final_consensus else pd.DataFrame()
    
    def _process_final_results(self, df_curated: pd.DataFrame) -> List[ValidatedCompetitor]:
        """Calculate quality scores and convert to ValidatedCompetitor objects"""
        
        # Calculate quality scores
        df_curated['quality_score'] = df_curated.apply(
            lambda row: (
                (row['confidence'] * 0.4) +
                (min(row['raw_score'] / 5.0, 1.0) * 0.3) +
                (row['market_overlap_pct'] / 100.0 * 0.2) +
                (0.1 if row['discovery_method'] == 'standard' else 0.05)
            ) if row['is_competitor'] else 0.0,
            axis=1
        )
        
        # Convert to ValidatedCompetitor objects
        validated = []
        if len(df_curated) > 0:
            for _, row in df_curated.iterrows():
                validated.append(ValidatedCompetitor(
                    company_name=row['company_name'],
                    is_competitor=row['is_competitor'],
                    tier=row['tier'],
                    market_overlap_pct=int(row['market_overlap_pct']),
                    customer_substitution_ease=row['customer_substitution_ease'],
                    confidence=float(row['confidence']),
                    reasoning=row['reasoning'],
                    evidence_sources=row['evidence_sources'],
                    quality_score=float(row['quality_score'])
                ))
        
        # Sort by quality score
        validated.sort(key=lambda x: x.quality_score, reverse=True)
        
        return validated