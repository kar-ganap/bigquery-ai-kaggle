"""
Clean Pipeline Orchestrator

A lightweight orchestrator that demonstrates the modular architecture.
This replaces the 4,000+ line monolith with clean, testable code.
"""
import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from .core.base import PipelineContext, StageError
from .core.progress import ProgressTracker
from .models.results import PipelineResults, IntelligenceOutput
from .models.candidates import CompetitorCandidate, ValidatedCompetitor

from .stages.discovery import DiscoveryStage
from .stages.curation import CurationStage
from .stages.ranking import RankingStage
from .stages.ingestion import IngestionStage
from .stages.strategic_labeling import StrategicLabelingStage
from .stages.embeddings import EmbeddingsStage
from .stages.analysis import AnalysisStage
from .stages.visual_intelligence import VisualIntelligenceStage
from .stages.enhanced_output import EnhancedOutputStage
from .stages.multidimensional_intelligence import MultiDimensionalIntelligenceStage

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class CompetitiveIntelligencePipeline:
    """
    Clean, modular pipeline orchestrator.
    
    Benefits of this approach:
    - Each stage is in its own file (~100-200 lines)
    - Stages can be unit tested independently
    - Easy to add new stages or modify existing ones
    - Clear separation of concerns
    - Maintainable and debuggable
    """
    
    def __init__(self, brand: str, vertical: str = "", dry_run: bool = False, verbose: bool = False):
        self.brand = brand
        self.vertical = vertical
        self.dry_run = dry_run
        self.verbose = verbose
        
        # Generate run ID
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_id = f"{brand.lower().replace(' ', '_')}_{timestamp}"
        
        # Create pipeline context
        self.context = PipelineContext(brand, vertical, self.run_id, verbose)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize progress tracker
        self.progress = ProgressTracker(total_stages=10)  # 10 stages total (1-10)
        
        # Stage timings
        self.stage_timings = {}
        
    def _setup_logging(self):
        """Setup pipeline logging"""
        self.logger = logging.getLogger(f"pipeline_{self.run_id}")
        self.logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # Optional file handler
        if not self.dry_run:
            log_file = f"data/output/pipeline_{self.run_id}.log"
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def execute_pipeline(self) -> PipelineResults:
        """
        Execute the full pipeline.
        
        This is where the magic happens - but now it's clean and understandable!
        """
        start_time = time.time()
        
        try:
            print("\n" + "=" * 70)
            print("🚀 COMPETITIVE INTELLIGENCE PIPELINE")
            print("=" * 70)
            print(f"Target Brand: {self.brand}")
            print(f"Vertical: {self.vertical or 'Auto-detect'}")
            print(f"Run ID: {self.run_id}")
            print(f"Output: {'dry-run' if self.dry_run else 'terminal → data/output'}")
            print("=" * 70 + "\n")
            
            # Stage 1: Discovery
            discovery_stage = DiscoveryStage(self.context, self.dry_run)
            candidates = discovery_stage.run(self.context, self.progress)
            print(f"✅ Stage 1 complete - Found {len(candidates)} candidates")
            
            # Stage 2: AI Competitor Curation
            curation_stage = CurationStage(self.context, self.dry_run)
            validated_competitors = curation_stage.run(candidates, self.progress)
            print(f"✅ Stage 2 complete - Validated {len(validated_competitors)} competitors")
            
            # Stage 3: Meta Ad Activity Ranking
            ranking_stage = RankingStage(self.context, self.dry_run, self.verbose)
            ranked_competitors = ranking_stage.run(validated_competitors, self.progress)
            print(f"✅ Stage 3 complete - Ranked {len(ranked_competitors)} Meta-active competitors")
            
            # Stage 4: Meta Ads Ingestion  
            ingestion_stage = IngestionStage(self.context, self.dry_run, self.verbose)
            ingestion_results = ingestion_stage.run(ranked_competitors, self.progress)
            print(f"✅ Stage 4 complete - Collected {ingestion_results.total_ads} ads from {len(ingestion_results.brands)} brands")
            
            # Store competitor brands in context for later stages
            self.context.competitor_brands = [comp.company_name for comp in ranked_competitors]
            
            # Stage 5: Strategic Labeling
            strategic_labeling_stage = StrategicLabelingStage(self.context, self.dry_run, self.verbose)
            strategic_results = strategic_labeling_stage.run(ingestion_results, self.progress)
            print(f"✅ Stage 5 complete - Generated strategic labels for {strategic_results.labeled_ads} ads")
            
            # Stage 6: Embeddings Generation
            embeddings_stage = EmbeddingsStage(self.context, self.dry_run, self.verbose)
            embeddings_results = embeddings_stage.run(ingestion_results, self.progress)
            print(f"✅ Stage 6 complete - Generated {embeddings_results.embedding_count} embeddings")

            # Stage 7: Visual Intelligence
            visual_intel_stage = VisualIntelligenceStage(self.context, self.dry_run)
            visual_intel_results = visual_intel_stage.run(embeddings_results, self.progress)
            print(f"✅ Stage 7 complete - Visual intelligence: {visual_intel_results.sampled_ads} ads analyzed, ${visual_intel_results.cost_estimate:.2f}")

            # Stage 8: Strategic Analysis
            analysis_stage = AnalysisStage(self.context, self.dry_run, self.verbose)
            analysis_results = analysis_stage.run(embeddings_results, self.progress)
            print(f"✅ Stage 8 complete - Strategic analysis complete")
            
            # Stage 9: Multi-Dimensional Intelligence
            multidim_intel_stage = MultiDimensionalIntelligenceStage("Multi-Dimensional Intelligence", 9, self.run_id)
            # Pass competitor brands to the stage for proper analysis
            multidim_intel_stage.competitor_brands = self.context.competitor_brands + [self.context.brand]
            # Pass Visual Intelligence results to the stage for L1-L4 integration
            multidim_intel_stage.visual_intelligence_results = visual_intel_results.__dict__ if visual_intel_results else {}
            multidim_intel_results = multidim_intel_stage.run(analysis_results, self.progress)
            print(f"✅ Stage 9 complete - Multi-dimensional intelligence analysis complete")
            
            # Stage 10: Intelligence Output
            output_stage = EnhancedOutputStage(self.context, self.dry_run, self.verbose)
            intelligence_output = output_stage.run(multidim_intel_results, self.progress)
            print(f"✅ Stage 10 complete - Intelligence output generated")
            
            # Success!
            duration = time.time() - start_time
            timings = self.progress.get_timings()
            
            return PipelineResults(
                success=True,
                brand=self.brand,
                vertical=self.vertical,
                output=intelligence_output,
                duration_seconds=duration,
                stage_timings=timings,
                run_id=self.run_id
            )
            
        except StageError as e:
            # Stage-specific error
            duration = time.time() - start_time
            self.logger.error(f"Pipeline failed: {str(e)}")
            
            return PipelineResults(
                success=False,
                brand=self.brand,
                vertical=self.vertical,
                output=None,
                duration_seconds=duration,
                stage_timings=self.progress.get_timings(),
                error=str(e),
                run_id=self.run_id
            )
            
        except Exception as e:
            # Unexpected error
            duration = time.time() - start_time
            self.logger.error(f"Pipeline failed with unexpected error: {str(e)}")
            
            return PipelineResults(
                success=False,
                brand=self.brand,
                vertical=self.vertical,
                output=None,
                duration_seconds=duration,
                stage_timings=self.progress.get_timings(),
                error=f"Unexpected error: {str(e)}",
                run_id=self.run_id
            )


def main():
    """CLI entry point - much cleaner than the original!"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Competitive Intelligence Pipeline")
    parser.add_argument("--brand", required=True, help="Target brand name")
    parser.add_argument("--vertical", help="Brand vertical (auto-detected if not provided)")
    parser.add_argument("--dry-run", action="store_true", help="Run with mock data")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Create and run pipeline
    pipeline = CompetitiveIntelligencePipeline(
        brand=args.brand,
        vertical=args.vertical or "",
        dry_run=args.dry_run,
        verbose=args.verbose
    )
    
    results = pipeline.execute_pipeline()
    
    # Print results
    if results.success:
        print(f"\n✅ Pipeline completed successfully in {results.duration_seconds:.1f}s")
        print(f"📊 Run ID: {results.run_id}")
    else:
        print(f"\n❌ Pipeline failed: {results.error}")
        sys.exit(1)


if __name__ == "__main__":
    main()