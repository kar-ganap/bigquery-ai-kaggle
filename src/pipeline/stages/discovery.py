"""
Stage 1: Competitor Discovery via Google CSE

Clean, focused implementation of competitor discovery.
"""
from typing import List
from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import CompetitorCandidate

try:
    from scripts.discover_competitors_v2 import CompetitorDiscovery
except ImportError:
    CompetitorDiscovery = None


class DiscoveryStage(PipelineStage[PipelineContext, List[CompetitorCandidate]]):
    """
    Stage 1: Discover competitor candidates using Google Custom Search Engine.
    
    Responsibilities:
    - Auto-detect brand vertical if not provided
    - Execute comprehensive competitor discovery queries
    - Convert raw results to standardized format
    - Handle dry-run mode for testing
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False):
        super().__init__("Competitor Discovery", 1, context.run_id)
        self.context = context
        self.dry_run = dry_run
    
    def execute(self, input_data: PipelineContext) -> List[CompetitorCandidate]:
        """Execute competitor discovery"""
        
        if self.dry_run:
            return self._create_mock_candidates()
        
        return self._run_real_discovery()
    
    def _create_mock_candidates(self) -> List[CompetitorCandidate]:
        """Create mock candidates for testing"""
        candidates = [
            CompetitorCandidate(
                company_name=f"Competitor_{i}",
                source_url=f"https://example.com/{i}",
                source_title=f"Title {i}",
                query_used="test query",
                raw_score=0.8 - i*0.05,
                found_in="title",
                discovery_method="standard"
            )
            for i in range(1, 6)
        ]
        
        print(f"   📈 Mock discovery generated {len(candidates)} candidates")
        return candidates
    
    def _run_real_discovery(self) -> List[CompetitorCandidate]:
        """Run actual competitor discovery"""
        if CompetitorDiscovery is None:
            self.logger.error("Discovery module not available")
            raise ImportError("CompetitorDiscovery module required for non-dry-run execution")
        
        try:
            print("   📊 Initializing discovery engine...")
            discovery = CompetitorDiscovery()
            
            # Auto-detect vertical if not provided
            vertical = self._detect_vertical(discovery)
            
            # Run discovery
            print(f"   🎯 Discovering competitors for {self.context.brand}...")
            raw_candidates = discovery.discover_competitors(
                brand=self.context.brand,
                vertical=vertical,
                max_results_per_query=10
            )
            
            # Convert to pipeline format
            candidates = self._convert_candidates(raw_candidates)
            print(f"✅ Discovery complete: {len(candidates)} unique candidates found")
            
            return candidates
            
        except Exception as e:
            self.logger.error(f"Discovery failed: {str(e)}")
            raise
    
    def _detect_vertical(self, discovery) -> str:
        """Auto-detect brand vertical if not provided"""
        if self.context.vertical:
            return self.context.vertical
            
        print("   🔍 Detecting brand vertical...")
        detected = discovery.detect_brand_vertical(self.context.brand)
        
        if detected:
            print(f"   ✅ Detected vertical: {detected}")
            self.context.vertical = detected
            return detected
        else:
            print("   ⚠️  Could not detect vertical, using generic queries")
            return ""
    
    def _convert_candidates(self, raw_candidates) -> List[CompetitorCandidate]:
        """Convert raw discovery results to pipeline format"""
        candidates = []
        
        for rc in raw_candidates:
            candidates.append(CompetitorCandidate(
                company_name=rc.company_name,
                source_url=rc.source_url,
                source_title=rc.source_title,
                query_used=rc.query_used,
                raw_score=rc.raw_score,
                found_in=rc.found_in,
                discovery_method=rc.discovery_method
            ))
        
        print(f"   📈 Discovery found {len(candidates)} raw candidates")
        return candidates