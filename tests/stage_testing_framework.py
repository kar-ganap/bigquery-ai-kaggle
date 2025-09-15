#!/usr/bin/env python3
"""
Stage Testing Framework for Independent Pipeline Validation

This framework allows testing each stage of the competitive intelligence pipeline
independently with full traceability and caching between stages.
"""
import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# Add project root to path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.pipeline.core.base import PipelineContext
from src.pipeline.core.progress import ProgressTracker
from src.pipeline.stages.discovery import DiscoveryStage
from src.pipeline.stages.curation import CurationStage
from src.pipeline.stages.ranking import RankingStage
from src.pipeline.stages.ingestion import IngestionStage
from src.pipeline.stages.strategic_labeling import StrategicLabelingStage
from src.pipeline.stages.embeddings import EmbeddingsStage
from src.pipeline.stages.analysis import AnalysisStage
from src.pipeline.stages.multidimensional_intelligence import MultiDimensionalIntelligenceStage
from src.pipeline.stages.output import OutputStage


class StageTestingFramework:
    """
    Framework for independent stage testing with caching and full traceability
    """
    
    def __init__(self, brand: str, vertical: str = "", test_id: str = None):
        self.brand = brand
        self.vertical = vertical
        self.test_id = test_id or f"test_{brand.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create test output directory
        self.test_dir = Path(f"data/output/stage_tests/{self.test_id}")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize context
        self.context = PipelineContext(brand, vertical, self.test_id, verbose=True)
        self.progress = ProgressTracker(total_stages=9)
        
        # Stage results cache
        self.stage_results = {}
        self.stage_timings = {}
        
        print("🧪 STAGE TESTING FRAMEWORK INITIALIZED")
        print("=" * 60)
        print(f"Brand: {self.brand}")
        print(f"Vertical: {self.vertical or 'Auto-detect'}")
        print(f"Test ID: {self.test_id}")
        print(f"Test Directory: {self.test_dir}")
        print("=" * 60 + "\n")
    
    def save_stage_result(self, stage_name: str, stage_num: int, input_data: Any, output_data: Any, timing: float):
        """Save stage results with full traceability"""
        stage_key = f"stage_{stage_num}_{stage_name.lower().replace(' ', '_')}"
        
        result_data = {
            "stage_info": {
                "name": stage_name,
                "number": stage_num,
                "timestamp": datetime.now().isoformat(),
                "duration_seconds": timing
            },
            "input_summary": self._summarize_data(input_data),
            "output_summary": self._summarize_data(output_data),
            "full_output": self._serialize_data(output_data)
        }
        
        # Save to cache
        self.stage_results[stage_key] = output_data
        self.stage_timings[stage_key] = timing
        
        # Save to file
        result_file = self.test_dir / f"{stage_key}_result.json"
        with open(result_file, 'w') as f:
            json.dump(result_data, f, indent=2, default=str)
        
        print(f"💾 Cached {stage_name} results: {result_file}")
        print(f"⏱️  Duration: {timing:.2f}s")
        print(f"📊 Output summary: {result_data['output_summary']}")
        print()
    
    def load_stage_result(self, stage_num: int, stage_name: str):
        """Load cached stage result"""
        stage_key = f"stage_{stage_num}_{stage_name.lower().replace(' ', '_')}"
        
        if stage_key in self.stage_results:
            print(f"📁 Loading cached result for {stage_name}")
            return self.stage_results[stage_key]
        
        # Try to load from file
        result_file = self.test_dir / f"{stage_key}_result.json"
        if result_file.exists():
            with open(result_file, 'r') as f:
                data = json.load(f)
                return data['full_output']
        
        return None
    
    def test_stage_1_discovery(self, force_run: bool = False) -> List:
        """Test Stage 1: Discovery independently"""
        print("🔍 TESTING STAGE 1: DISCOVERY")
        print("-" * 40)
        
        # Check for cached result
        if not force_run:
            cached = self.load_stage_result(1, "discovery")
            if cached:
                print("✅ Using cached Discovery results")
                return cached
        
        # Initialize and run discovery stage
        start_time = time.time()
        discovery_stage = DiscoveryStage(self.context, dry_run=False)
        
        try:
            candidates = discovery_stage.run(self.context, self.progress)
            duration = time.time() - start_time
            
            # Save results with full traceability
            self.save_stage_result("Discovery", 1, self.context, candidates, duration)
            
            print(f"✅ Stage 1 Complete - {len(candidates)} candidates discovered")
            
            # Print detailed results
            print("\n📋 DISCOVERY RESULTS:")
            for i, candidate in enumerate(candidates[:5], 1):  # Show first 5
                print(f"   {i}. {candidate.company_name}")
                print(f"      Source: {candidate.source_url}")
                print(f"      Score: {candidate.raw_score:.3f}")
                print(f"      Query: {candidate.query_used}")
                if hasattr(candidate, 'discovery_method'):
                    print(f"      Method: {candidate.discovery_method}")
                print()
            
            if len(candidates) > 5:
                print(f"   ... and {len(candidates) - 5} more candidates")
            
            return candidates
            
        except Exception as e:
            print(f"❌ Stage 1 Failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def test_stage_2_curation(self, candidates: List = None, force_run: bool = False) -> List:
        """Test Stage 2: Curation independently"""
        print("🤖 TESTING STAGE 2: AI COMPETITOR CURATION")
        print("-" * 40)
        
        # Load input data
        if candidates is None:
            candidates = self.load_stage_result(1, "discovery")
            if not candidates:
                print("❌ No candidates available. Run Stage 1 first.")
                return []
        
        # Check for cached result
        if not force_run:
            cached = self.load_stage_result(2, "curation")
            if cached:
                print("✅ Using cached Curation results")
                return cached
        
        # Initialize and run curation stage
        start_time = time.time()
        curation_stage = CurationStage(self.context, dry_run=False)
        
        try:
            validated_competitors = curation_stage.run(candidates, self.progress)
            duration = time.time() - start_time
            
            # Save results with full traceability
            self.save_stage_result("Curation", 2, candidates, validated_competitors, duration)
            
            print(f"✅ Stage 2 Complete - {len(validated_competitors)} validated competitors")
            
            # Print detailed results
            print("\n📋 CURATION RESULTS:")
            for i, competitor in enumerate(validated_competitors[:5], 1):  # Show first 5
                print(f"   {i}. {competitor.company_name}")
                print(f"      Confidence: {competitor.confidence:.3f}")
                print(f"      Quality Score: {competitor.quality_score:.3f}")
                print(f"      Market Overlap: {competitor.market_overlap_pct}%")
                print()
            
            if len(validated_competitors) > 5:
                print(f"   ... and {len(validated_competitors) - 5} more competitors")
            
            return validated_competitors
            
        except Exception as e:
            print(f"❌ Stage 2 Failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def test_stage_3_ranking(self, validated_competitors: List = None, force_run: bool = False) -> List:
        """Test Stage 3: Ranking independently"""
        print("📊 TESTING STAGE 3: META AD ACTIVITY RANKING")
        print("-" * 40)
        
        # Load input data
        if validated_competitors is None:
            validated_competitors = self.load_stage_result(2, "curation")
            if not validated_competitors:
                print("❌ No validated competitors available. Run Stage 2 first.")
                return []
        
        # Check for cached result
        if not force_run:
            cached = self.load_stage_result(3, "ranking")
            if cached:
                print("✅ Using cached Ranking results")
                return cached
        
        # Initialize and run ranking stage
        start_time = time.time()
        ranking_stage = RankingStage(self.context, dry_run=False, verbose=True)
        
        try:
            ranked_competitors = ranking_stage.run(validated_competitors, self.progress)
            duration = time.time() - start_time
            
            # Save results with full traceability
            self.save_stage_result("Ranking", 3, validated_competitors, ranked_competitors, duration)
            
            print(f"✅ Stage 3 Complete - {len(ranked_competitors)} ranked Meta-active competitors")
            
            # Print detailed results
            print("\n📋 RANKING RESULTS:")
            for i, competitor in enumerate(ranked_competitors, 1):
                print(f"   {i}. {competitor.company_name}")
                print(f"      Quality Score: {competitor.quality_score:.3f}")
                print(f"      Market Overlap: {competitor.market_overlap_pct}%")
                if hasattr(competitor, 'estimated_ad_count'):
                    print(f"      Est. Ad Count: {competitor.estimated_ad_count}")
                if hasattr(competitor, 'meta_classification'):
                    print(f"      Meta Activity: {competitor.meta_classification}")
                print()
            
            return ranked_competitors
            
        except Exception as e:
            print(f"❌ Stage 3 Failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return []
    
    def test_stage_4_ingestion(self, ranked_competitors: List = None, force_run: bool = False) -> Any:
        """Test Stage 4: Ingestion independently with FIXED data limits"""
        print("📱 TESTING STAGE 4: META ADS INGESTION (FIXED LIMITS)")
        print("-" * 50)
        print("🚨 TESTING CRITICAL FIX: max_ads=30→500, max_pages=3→10")
        print("📊 EXPECTED: 16x more ads captured per brand")
        
        # Load input data
        if ranked_competitors is None:
            ranked_competitors = self.load_stage_result(3, "ranking")
            if not ranked_competitors:
                print("❌ No ranked competitors available. Run Stage 3 first.")
                return None
        
        # Check for cached result
        if not force_run:
            cached = self.load_stage_result(4, "ingestion")
            if cached:
                print("✅ Using cached Ingestion results")
                return cached
        
        # Initialize and run ingestion stage
        start_time = time.time()
        ingestion_stage = IngestionStage(self.context, dry_run=False, verbose=True)
        
        try:
            ingestion_results = ingestion_stage.run(ranked_competitors, self.progress)
            duration = time.time() - start_time
            
            # Save results with full traceability
            self.save_stage_result("Ingestion", 4, ranked_competitors, ingestion_results, duration)
            
            print(f"✅ Stage 4 Complete - {ingestion_results.total_ads} total ads from {len(ingestion_results.brands)} brands")
            
            # Print detailed results with data coverage analysis
            print("\n📋 INGESTION RESULTS:")
            print(f"   📊 Total ads collected: {ingestion_results.total_ads}")
            print(f"   🏢 Brands with ads: {len(ingestion_results.brands)}")
            print(f"   ⏱️  Ingestion duration: {duration:.1f}s")
            
            if hasattr(ingestion_results, 'ads_table_id') and ingestion_results.ads_table_id:
                print(f"   💾 BigQuery table: {ingestion_results.ads_table_id}")
            
            print("\n🔍 DATA COVERAGE ANALYSIS:")
            if ingestion_results.total_ads > 100:
                print("   ✅ EXCELLENT: 100+ ads collected - Fix appears successful!")
                coverage_improvement = ingestion_results.total_ads / 30  # vs old limit
                print(f"   📈 Improvement: {coverage_improvement:.1f}x more ads than old limit")
            elif ingestion_results.total_ads > 50:
                print("   ✅ GOOD: 50+ ads collected - Significant improvement")
            elif ingestion_results.total_ads > 30:
                print("   ⚠️  PARTIAL: More than 30 ads but not full potential")
            else:
                print("   ❌ LIMITED: Still hitting 30 ad limit - Fix may not be applied")
            
            # Per-brand breakdown if available
            if hasattr(ingestion_results, 'ads') and ingestion_results.ads:
                brand_counts = {}
                for ad in ingestion_results.ads:
                    brand = ad.get('brand', 'Unknown')
                    brand_counts[brand] = brand_counts.get(brand, 0) + 1
                
                print("\n📊 ADS PER BRAND:")
                for brand, count in sorted(brand_counts.items(), key=lambda x: x[1], reverse=True):
                    status = "✅ EXCELLENT" if count > 100 else "✅ GOOD" if count > 50 else "⚠️ LIMITED" if count > 30 else "❌ MINIMAL"
                    print(f"   {brand}: {count} ads ({status})")
            
            return ingestion_results
            
        except Exception as e:
            print(f"❌ Stage 4 Failed: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def _summarize_data(self, data: Any) -> str:
        """Create a summary of data for logging"""
        if isinstance(data, list):
            return f"List with {len(data)} items"
        elif isinstance(data, dict):
            return f"Dict with keys: {list(data.keys())[:5]}"
        elif hasattr(data, '__dict__'):
            return f"Object: {type(data).__name__}"
        else:
            return f"Type: {type(data).__name__}"
    
    def _serialize_data(self, data: Any) -> Any:
        """Serialize data for JSON storage"""
        if hasattr(data, '__dict__'):
            # Convert objects to dictionaries
            if isinstance(data, list):
                return [self._serialize_data(item) for item in data]
            else:
                return data.__dict__
        return data
    
    def generate_test_report(self) -> str:
        """Generate a comprehensive test report"""
        report_file = self.test_dir / "test_report.md"
        
        report = f"""# Pipeline Stage Testing Report

**Test ID:** {self.test_id}
**Brand:** {self.brand}
**Vertical:** {self.vertical or 'Auto-detected'}
**Timestamp:** {datetime.now().isoformat()}

## Test Results Summary

"""
        
        for stage_key, timing in self.stage_timings.items():
            stage_name = stage_key.replace('stage_', '').replace('_', ' ').title()
            report += f"- **{stage_name}:** {timing:.2f}s\n"
        
        report += f"\n## Total Test Duration: {sum(self.stage_timings.values()):.2f}s\n"
        report += f"\n## Test Files Location: {self.test_dir}\n"
        
        with open(report_file, 'w') as f:
            f.write(report)
        
        print(f"📄 Test report generated: {report_file}")
        return str(report_file)


def clean_tables_for_fresh_start():
    """Clean up BigQuery tables for a fresh start"""
    print("🧹 CLEANING BIGQUERY TABLES FOR FRESH START")
    print("=" * 50)
    
    try:
        # Use our existing clean dataset refresh script
        from auto_clean_dataset_refresh import main as clean_main
        clean_main()
        print("✅ BigQuery tables cleaned successfully")
    except Exception as e:
        print(f"⚠️  Could not clean tables automatically: {e}")
        print("Please run: uv run python auto_clean_dataset_refresh.py")


def main():
    """Main CLI entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Stage Testing Framework")
    parser.add_argument("--brand", required=True, help="Target brand name")
    parser.add_argument("--vertical", help="Brand vertical")
    parser.add_argument("--stage", type=int, help="Test specific stage (1-9)")
    parser.add_argument("--force", action="store_true", help="Force re-run even if cached")
    parser.add_argument("--clean", action="store_true", help="Clean tables before testing")
    
    args = parser.parse_args()
    
    if args.clean:
        clean_tables_for_fresh_start()
        print()
    
    # Initialize framework
    framework = StageTestingFramework(args.brand, args.vertical or "")
    
    # Test specific stage or all stages
    if args.stage == 1 or args.stage is None:
        candidates = framework.test_stage_1_discovery(force_run=args.force)
        
        if args.stage is None and candidates:
            # Continue to stage 2 if testing all stages
            validated_competitors = framework.test_stage_2_curation(candidates, force_run=args.force)
            
            if validated_competitors:
                # Continue to stage 3 if testing all stages
                framework.test_stage_3_ranking(validated_competitors, force_run=args.force)
    
    elif args.stage == 2:
        framework.test_stage_2_curation(force_run=args.force)
    
    elif args.stage == 3:
        framework.test_stage_3_ranking(force_run=args.force)
    
    # Generate report
    framework.generate_test_report()
    print("🎯 Stage testing complete!")


if __name__ == "__main__":
    main()