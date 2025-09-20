#!/usr/bin/env python3
"""
Pure SQL BigQuery AI Competitor Curation Orchestrator
Executes the AI curation pipeline using BigQuery AI.GENERATE_TABLE
"""

import os
import sys
import time
import argparse
from typing import Dict, List, Optional
from pathlib import Path

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from bigquery_client import get_bigquery_client, run_query, create_table_from_query

# Configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")

class BigQueryAICurator:
    """Pure SQL approach using BigQuery AI for competitor curation"""
    
    def __init__(self, project_id: str = None, dataset_id: str = None):
        self.project_id = project_id or BQ_PROJECT
        self.dataset_id = dataset_id or BQ_DATASET
        self.client = get_bigquery_client(project_id)
        
    def ensure_gemini_model(self):
        """Ensure Gemini model is available for AI.GENERATE_TABLE"""
        model_id = f"{self.project_id}.{self.dataset_id}.gemini_model"
        
        try:
            # Check if model exists
            self.client.get_model(model_id)
            print(f"✅ Gemini model exists: {model_id}")
            return model_id
        except:
            # Create remote model connection to Gemini
            print(f"🔧 Creating Gemini model connection: {model_id}")
            create_model_sql = f"""
            CREATE OR REPLACE MODEL `{model_id}`
            REMOTE WITH CONNECTION `{self.project_id}.us.vertex-ai`
            OPTIONS (ENDPOINT = 'gemini-2.5-flash');
            """
            
            try:
                self.client.query(create_model_sql).result()
                print(f"✅ Created Gemini model: {model_id}")
                return model_id
            except Exception as e:
                print(f"❌ Failed to create Gemini model: {e}")
                print("💡 Please ensure you have a Vertex AI connection set up")
                print("   See: https://cloud.google.com/bigquery/docs/generate-text")
                raise
    
    def load_sql_file(self, sql_file: str) -> str:
        """Load SQL file and substitute variables"""
        sql_path = Path(__file__).parent.parent / "sql" / sql_file
        
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        
        with open(sql_path, 'r') as f:
            sql = f.read()
        
        # Substitute template variables
        sql = sql.replace('${BQ_PROJECT}', self.project_id)
        sql = sql.replace('${BQ_DATASET}', self.dataset_id)
        
        return sql
    
    def execute_sql_steps(self, sql_content: str) -> List[Dict]:
        """Execute multiple SQL statements from file content"""
        results = []
        
        # Split by semicolon and filter out empty statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements, 1):
            if not statement:
                continue
                
            print(f"🔄 Executing SQL step {i}/{len(statements)}...")
            
            # Add semicolon back for execution
            statement += ';'
            
            try:
                job = self.client.query(statement)
                result = job.result()
                
                # Capture result info
                step_result = {
                    'step': i,
                    'statement_type': statement.strip().split()[0].upper(),
                    'rows_affected': result.total_rows if hasattr(result, 'total_rows') else None,
                    'job_id': job.job_id,
                    'status': 'SUCCESS'
                }
                
                print(f"   ✅ Step {i} completed ({step_result['statement_type']})")
                
                results.append(step_result)
                
                # Brief pause between operations
                time.sleep(1)
                
            except Exception as e:
                print(f"   ❌ Step {i} failed: {e}")
                results.append({
                    'step': i,
                    'status': 'FAILED',
                    'error': str(e)
                })
                # Continue with other steps
        
        return results
    
    def run_curation_pipeline(self) -> Dict:
        """Run the complete AI curation pipeline"""
        print("🤖 Starting BigQuery AI Competitor Curation Pipeline")
        print("=" * 60)
        
        pipeline_results = {
            'start_time': time.time(),
            'steps': {}
        }
        
        # Step 1: Ensure Gemini model is available
        print("\n📋 Step 1: Setting up Gemini model...")
        try:
            model_id = self.ensure_gemini_model()
            pipeline_results['steps']['model_setup'] = {'status': 'SUCCESS', 'model_id': model_id}
        except Exception as e:
            pipeline_results['steps']['model_setup'] = {'status': 'FAILED', 'error': str(e)}
            return pipeline_results
        
        # Step 2: Execute curation SQL
        print("\n🧠 Step 2: Running AI competitor curation...")
        try:
            curation_sql = self.load_sql_file("06_curate_competitors.sql")
            curation_results = self.execute_sql_steps(curation_sql)
            
            pipeline_results['steps']['curation'] = {
                'status': 'SUCCESS',
                'sql_steps': curation_results
            }
            
            # Count curated competitors
            count_query = f"""
            SELECT COUNT(*) as total_curated,
                   COUNTIF(is_competitor = TRUE) as validated_competitors,
                   AVG(confidence) as avg_confidence
            FROM `{self.project_id}.{self.dataset_id}.competitors_curated`
            """
            
            df = run_query(count_query, self.project_id)
            if not df.empty:
                stats = df.iloc[0]
                print(f"📊 Curation Summary:")
                print(f"   Total processed: {stats['total_curated']}")
                print(f"   Validated competitors: {stats['validated_competitors']}")
                print(f"   Average confidence: {stats['avg_confidence']:.3f}")
                
                pipeline_results['steps']['curation']['stats'] = stats.to_dict()
                
        except Exception as e:
            print(f"❌ Curation failed: {e}")
            pipeline_results['steps']['curation'] = {'status': 'FAILED', 'error': str(e)}
        
        # Step 3: Run validation tests
        print("\n🧪 Step 3: Running validation tests...")
        try:
            validation_sql = self.load_sql_file("07_validation_tests.sql")
            validation_results = self.execute_sql_steps(validation_sql)
            
            pipeline_results['steps']['validation'] = {
                'status': 'SUCCESS',
                'test_results': validation_results
            }
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            pipeline_results['steps']['validation'] = {'status': 'FAILED', 'error': str(e)}
        
        # Pipeline summary
        pipeline_results['end_time'] = time.time()
        pipeline_results['duration'] = pipeline_results['end_time'] - pipeline_results['start_time']
        
        successful_steps = sum(1 for step in pipeline_results['steps'].values() 
                             if step.get('status') == 'SUCCESS')
        total_steps = len(pipeline_results['steps'])
        
        print(f"\n🎯 Pipeline Complete!")
        print(f"   Duration: {pipeline_results['duration']:.1f} seconds")
        print(f"   Success Rate: {successful_steps}/{total_steps} steps")
        
        if successful_steps == total_steps:
            print("✅ All steps completed successfully!")
            print("\n🎉 Next Steps:")
            print("   1. Review curated competitors in BigQuery console")
            print("   2. Run validation queries to check quality")
            print("   3. Proceed to business insights generation (Subgoal 4)")
        else:
            print("⚠️  Some steps failed. Check logs above.")
        
        return pipeline_results
    
    def get_sample_results(self, limit: int = 10) -> None:
        """Display sample curated results"""
        print(f"\n📋 Sample Curated Competitors (Top {limit}):")
        print("-" * 80)
        
        sample_query = f"""
        SELECT 
            target_brand,
            company_name,
            tier,
            market_overlap_pct,
            customer_substitution_ease,
            confidence,
            reasoning
        FROM `{self.project_id}.{self.dataset_id}.competitors_validated`
        ORDER BY quality_score DESC
        LIMIT {limit}
        """
        
        try:
            df = run_query(sample_query, self.project_id)
            
            for _, row in df.iterrows():
                print(f"🎯 {row['target_brand']} → {row['company_name']}")
                print(f"   Tier: {row['tier']} | Overlap: {row['market_overlap_pct']}% | Confidence: {row['confidence']:.2f}")
                print(f"   Substitution: {row['customer_substitution_ease']}")
                print(f"   Reasoning: {row['reasoning'][:100]}...")
                print()
                
        except Exception as e:
            print(f"❌ Failed to get sample results: {e}")

def main():
    parser = argparse.ArgumentParser(description="Run BigQuery AI Competitor Curation Pipeline")
    parser.add_argument("--project", default=BQ_PROJECT, help="BigQuery project ID")
    parser.add_argument("--dataset", default=BQ_DATASET, help="BigQuery dataset ID")
    parser.add_argument("--sample", action="store_true", help="Show sample results after curation")
    parser.add_argument("--sample-limit", type=int, default=10, help="Number of sample results to show")
    
    args = parser.parse_args()
    
    # Initialize curator
    curator = BigQueryAICurator(args.project, args.dataset)
    
    # Run pipeline
    results = curator.run_curation_pipeline()
    
    # Show sample results if requested
    if args.sample:
        curator.get_sample_results(args.sample_limit)
    
    # Exit with appropriate code
    successful_steps = sum(1 for step in results['steps'].values() 
                          if step.get('status') == 'SUCCESS')
    total_steps = len(results['steps'])
    
    if successful_steps == total_steps:
        print("\n🎉 Pipeline completed successfully!")
        sys.exit(0)
    else:
        print(f"\n⚠️  Pipeline completed with {total_steps - successful_steps} failed steps")
        sys.exit(1)

if __name__ == "__main__":
    main()