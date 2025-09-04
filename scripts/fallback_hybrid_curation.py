#!/usr/bin/env python3
"""
Hybrid Fallback: Python + Vertex AI for Competitor Curation
This script provides a fallback when pure BigQuery AI approach fails or is unavailable
"""

import os
import sys
import json
import time
import argparse
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import pandas as pd

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from bigquery_client import get_bigquery_client, run_query, load_dataframe_to_bq

# Vertex AI imports (optional - fallback only)
try:
    import vertexai
    from vertexai.language_models import TextGenerationModel
    VERTEX_AI_AVAILABLE = True
except ImportError:
    VERTEX_AI_AVAILABLE = False
    print("⚠️  Vertex AI not available. Install google-cloud-aiplatform for hybrid mode.")

# Configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")
VERTEX_AI_LOCATION = os.environ.get("VERTEX_AI_LOCATION", "us-central1")

@dataclass
class CurationResult:
    """AI curation result structure matching BigQuery schema"""
    is_competitor: bool
    tier: str
    market_overlap_pct: int
    customer_substitution_ease: str
    confidence: float
    reasoning: str
    evidence_sources: str

class HybridCompetitorCurator:
    """Hybrid competitor curation using Python + Vertex AI"""
    
    def __init__(self, project_id: str = None, dataset_id: str = None):
        self.project_id = project_id or BQ_PROJECT
        self.dataset_id = dataset_id or BQ_DATASET
        self.client = get_bigquery_client(project_id)
        
        # Initialize Vertex AI if available
        if VERTEX_AI_AVAILABLE:
            vertexai.init(project=self.project_id, location=VERTEX_AI_LOCATION)
            self.text_model = TextGenerationModel.from_pretrained("text-bison@002")
        else:
            self.text_model = None
    
    def get_raw_competitors(self) -> pd.DataFrame:
        """Load raw competitor candidates from BigQuery"""
        query = f"""
        SELECT 
            target_brand,
            target_vertical,
            company_name,
            source_url,
            source_title,
            query_used,
            raw_score,
            found_in,
            discovery_method,
            discovered_at
        FROM `{self.project_id}.{self.dataset_id}.competitors_raw`
        WHERE discovered_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
        ORDER BY target_brand, raw_score DESC
        """
        
        return run_query(query, self.project_id)
    
    def create_curation_prompt(self, row: pd.Series) -> str:
        """Create AI curation prompt for a competitor candidate"""
        return f"""
Analyze if "{row['company_name']}" is a legitimate competitor of "{row['target_brand']}" in the {row.get('target_vertical', 'unknown')} industry.

Context:
- Found via query: "{row['query_used']}"
- Source: "{row['source_title']}"
- Discovery method: {row['discovery_method']}
- Search relevance: {row['raw_score']:.2f}

Instructions:
Respond with a JSON object containing:
1. "is_competitor": true/false - Is this a real company that competes with the target brand?
2. "tier": One of ["Direct-Rival", "Market-Leader", "Disruptor", "Niche-Player", "Adjacent"]
3. "market_overlap_pct": 0-100 integer - How much do their target markets overlap?
4. "customer_substitution_ease": One of ["Easy", "Medium", "Hard"] - How easily can customers switch?
5. "confidence": 0.0-1.0 float - Your confidence in this assessment
6. "reasoning": Brief explanation (max 200 chars)
7. "evidence_sources": What info you used (max 150 chars)

Be conservative - only mark as competitor if confident they actually compete.
Consider: Similar customers? Similar products/services? Geographic overlap?

Example response:
{{"is_competitor": true, "tier": "Direct-Rival", "market_overlap_pct": 85, "customer_substitution_ease": "Medium", "confidence": 0.9, "reasoning": "Both athletic apparel brands target similar demographics with overlapping product lines", "evidence_sources": "Company websites, market research data"}}
"""
    
    def curate_single_candidate(self, row: pd.Series, retry_count: int = 2) -> Optional[CurationResult]:
        """Curate a single competitor candidate using AI"""
        if not self.text_model:
            print("❌ Vertex AI not available for hybrid curation")
            return None
        
        prompt = self.create_curation_prompt(row)
        
        for attempt in range(retry_count + 1):
            try:
                # Generate AI response
                response = self.text_model.predict(
                    prompt=prompt,
                    temperature=0.1,  # Low temperature for consistent results
                    max_output_tokens=500,
                    top_p=0.8,
                    top_k=40
                )
                
                # Parse JSON response
                response_text = response.text.strip()
                
                # Extract JSON from response (may have extra text)
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = response_text[json_start:json_end]
                    data = json.loads(json_str)
                    
                    # Validate required fields
                    required_fields = ['is_competitor', 'tier', 'market_overlap_pct', 
                                     'customer_substitution_ease', 'confidence', 
                                     'reasoning', 'evidence_sources']
                    
                    if all(field in data for field in required_fields):
                        return CurationResult(
                            is_competitor=bool(data['is_competitor']),
                            tier=str(data['tier']),
                            market_overlap_pct=int(data['market_overlap_pct']),
                            customer_substitution_ease=str(data['customer_substitution_ease']),
                            confidence=float(data['confidence']),
                            reasoning=str(data['reasoning'])[:200],  # Truncate
                            evidence_sources=str(data['evidence_sources'])[:150]  # Truncate
                        )
                    else:
                        print(f"⚠️  Missing fields in AI response for {row['company_name']}")
                        
            except Exception as e:
                print(f"❌ AI curation attempt {attempt + 1} failed for {row['company_name']}: {e}")
                if attempt < retry_count:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                    
        return None
    
    def curate_batch(self, candidates_df: pd.DataFrame, batch_size: int = 10) -> pd.DataFrame:
        """Curate candidates in batches with progress tracking"""
        curated_results = []
        total_candidates = len(candidates_df)
        
        print(f"🔄 Processing {total_candidates} candidates in batches of {batch_size}...")
        
        for i in range(0, total_candidates, batch_size):
            batch = candidates_df.iloc[i:i+batch_size]
            batch_results = []
            
            print(f"   Batch {i//batch_size + 1}/{(total_candidates-1)//batch_size + 1}:")
            
            for idx, row in batch.iterrows():
                print(f"     Curating {row['target_brand']} → {row['company_name']}...")
                
                curation = self.curate_single_candidate(row)
                
                if curation:
                    # Combine original data with curation results
                    result_row = row.copy()
                    result_row['is_competitor'] = curation.is_competitor
                    result_row['tier'] = curation.tier
                    result_row['market_overlap_pct'] = curation.market_overlap_pct
                    result_row['customer_substitution_ease'] = curation.customer_substitution_ease
                    result_row['confidence'] = curation.confidence
                    result_row['reasoning'] = curation.reasoning
                    result_row['evidence_sources'] = curation.evidence_sources
                    result_row['curated_at'] = pd.Timestamp.now()
                    
                    # Calculate quality score
                    quality_score = (
                        (curation.confidence * 0.4) +
                        (min(row['raw_score'] / 5.0, 1.0) * 0.3) +
                        (curation.market_overlap_pct / 100.0 * 0.2) +
                        (0.1 if row['discovery_method'] == 'standard' else 0.05)
                    ) if curation.is_competitor else 0.0
                    
                    result_row['quality_score'] = quality_score
                    
                    batch_results.append(result_row)
                    print(f"       ✅ Curated (competitor: {curation.is_competitor})")
                else:
                    print(f"       ❌ Failed to curate")
                
                # Rate limiting
                time.sleep(0.5)
            
            curated_results.extend(batch_results)
            
            # Progress update
            completed = min(i + batch_size, total_candidates)
            print(f"   Progress: {completed}/{total_candidates} ({completed/total_candidates*100:.1f}%)")
            
            # Batch pause
            if i + batch_size < total_candidates:
                time.sleep(2)
        
        return pd.DataFrame(curated_results)
    
    def run_hybrid_curation(self, max_candidates: int = 100) -> Dict:
        """Run hybrid curation pipeline"""
        print("🔄 Starting Hybrid Competitor Curation (Python + Vertex AI)")
        print("=" * 70)
        
        if not VERTEX_AI_AVAILABLE:
            return {'status': 'FAILED', 'error': 'Vertex AI not available'}
        
        pipeline_results = {
            'start_time': time.time(),
            'method': 'hybrid',
            'status': 'RUNNING'
        }
        
        try:
            # Step 1: Load raw candidates
            print("📋 Step 1: Loading raw competitor candidates...")
            raw_df = self.get_raw_competitors()
            
            if raw_df.empty:
                print("❌ No raw competitors found to curate")
                return {'status': 'FAILED', 'error': 'No raw competitors found'}
            
            # Limit candidates to avoid excessive API costs
            if len(raw_df) > max_candidates:
                print(f"⚠️  Limiting to top {max_candidates} candidates by raw_score")
                raw_df = raw_df.nlargest(max_candidates, 'raw_score')
            
            print(f"   Loaded {len(raw_df)} candidates for curation")
            
            # Step 2: Curate using Vertex AI
            print("🤖 Step 2: AI curation using Vertex AI...")
            curated_df = self.curate_batch(raw_df)
            
            if curated_df.empty:
                print("❌ No candidates were successfully curated")
                return {'status': 'FAILED', 'error': 'Curation failed for all candidates'}
            
            # Step 3: Load to BigQuery
            print("💾 Step 3: Loading curated results to BigQuery...")
            table_id = f"{self.project_id}.{self.dataset_id}.competitors_curated"
            
            load_dataframe_to_bq(curated_df, table_id, write_disposition="WRITE_TRUNCATE")
            
            # Step 4: Create validated view
            print("🔍 Step 4: Creating validated competitors view...")
            validated_view_sql = f"""
            CREATE OR REPLACE VIEW `{self.project_id}.{self.dataset_id}.competitors_validated` AS
            SELECT *
            FROM `{table_id}`
            WHERE is_competitor = TRUE
              AND confidence >= 0.6
              AND quality_score >= 0.4
            ORDER BY target_brand, quality_score DESC, market_overlap_pct DESC
            """
            
            self.client.query(validated_view_sql).result()
            
            # Success statistics
            total_curated = len(curated_df)
            validated_count = len(curated_df[
                (curated_df['is_competitor'] == True) & 
                (curated_df['confidence'] >= 0.6) & 
                (curated_df['quality_score'] >= 0.4)
            ])
            
            pipeline_results.update({
                'status': 'SUCCESS',
                'total_processed': total_curated,
                'validated_competitors': validated_count,
                'avg_confidence': curated_df['confidence'].mean(),
                'avg_quality_score': curated_df['quality_score'].mean()
            })
            
        except Exception as e:
            print(f"❌ Hybrid curation failed: {e}")
            pipeline_results['status'] = 'FAILED'
            pipeline_results['error'] = str(e)
        
        pipeline_results['end_time'] = time.time()
        pipeline_results['duration'] = pipeline_results['end_time'] - pipeline_results['start_time']
        
        # Summary
        if pipeline_results['status'] == 'SUCCESS':
            print(f"\n✅ Hybrid curation completed successfully!")
            print(f"   Duration: {pipeline_results['duration']:.1f} seconds")
            print(f"   Total processed: {pipeline_results['total_processed']}")
            print(f"   Validated competitors: {pipeline_results['validated_competitors']}")
            print(f"   Average confidence: {pipeline_results['avg_confidence']:.3f}")
        else:
            print(f"\n❌ Hybrid curation failed: {pipeline_results.get('error', 'Unknown error')}")
        
        return pipeline_results

def main():
    parser = argparse.ArgumentParser(description="Hybrid Competitor Curation using Python + Vertex AI")
    parser.add_argument("--project", default=BQ_PROJECT, help="BigQuery project ID")
    parser.add_argument("--dataset", default=BQ_DATASET, help="BigQuery dataset ID")
    parser.add_argument("--max-candidates", type=int, default=100, 
                       help="Maximum candidates to process (cost control)")
    parser.add_argument("--batch-size", type=int, default=10, 
                       help="Batch size for processing")
    
    args = parser.parse_args()
    
    if not VERTEX_AI_AVAILABLE:
        print("❌ Vertex AI not available. Install with: pip install google-cloud-aiplatform")
        sys.exit(1)
    
    # Initialize curator
    curator = HybridCompetitorCurator(args.project, args.dataset)
    
    # Run hybrid curation
    results = curator.run_hybrid_curation(args.max_candidates)
    
    # Exit with appropriate code
    if results['status'] == 'SUCCESS':
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()