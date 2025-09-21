"""
Stage 3: Meta Ad Activity Ranking & Intelligent Capping

Clean, focused implementation of competitor ranking based on Meta ads activity.
"""
import time
from typing import List

from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import ValidatedCompetitor

from src.utils.ads_fetcher import MetaAdsFetcher


class RankingStage(PipelineStage[List[ValidatedCompetitor], List[ValidatedCompetitor]]):
    """
    Stage 3: Meta Ad Activity Ranking & Intelligent Capping.
    
    Responsibilities:
    - Probe Meta ad activity for validated competitors
    - Re-rank competitors by Meta activity + AI confidence
    - Apply intelligent capping (max 10 competitors)
    - Filter out competitors with no Meta presence
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Meta Ad Activity Ranking", 3, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.verbose = verbose
    
    def execute(self, competitors: List[ValidatedCompetitor]) -> List[ValidatedCompetitor]:
        """Execute Meta ad activity ranking"""
        
        if self.dry_run:
            return self._create_mock_ranking(competitors)
        
        return self._run_real_ranking(competitors)
    
    def _create_mock_ranking(self, competitors: List[ValidatedCompetitor]) -> List[ValidatedCompetitor]:
        """Create mock ranking for testing"""
        # Mock ranking - return top 10 by quality score
        ranked = sorted(competitors, key=lambda x: x.quality_score, reverse=True)[:10]
        
        # Add mock Meta data
        for i, comp in enumerate(ranked):
            comp.meta_tier = 3 - (i // 3)  # Vary tiers: 3, 2, 1
            comp.estimated_ad_count = f"{20 - i*2}+" if comp.meta_tier == 3 else f"{15 - i}+"
            comp.meta_classification = {3: "Major Player", 2: "Moderate Player", 1: "Minor Player"}[comp.meta_tier]
        
        print(f"   🎯 [DRY RUN] Selected top {len(ranked)} competitors by quality score")
        return ranked
    
    def _run_real_ranking(self, competitors: List[ValidatedCompetitor]) -> List[ValidatedCompetitor]:
        """Run actual Meta ad activity ranking"""
        
        if MetaAdsFetcher is None:
            self.logger.error("MetaAdsFetcher not available")
            raise ImportError("MetaAdsFetcher required for Meta ad ranking")
        
        try:
            # Step 1: Probe Meta ad activity with intelligent prioritization
            print(f"   🔍 Smart probing Meta ad activity for {len(competitors)} competitors...")
            fetcher = MetaAdsFetcher()
            
            # Sort competitors by AI quality before Meta probing
            competitors_sorted = sorted(competitors, 
                                      key=lambda x: x.confidence * x.market_overlap_pct * x.quality_score, 
                                      reverse=True)
            competitor_names = [c.company_name for c in competitors_sorted]
            
            # Use target_count=10 for early exit
            ad_tiers = fetcher.get_competitor_ad_tiers(competitor_names, target_count=10)
            
            # Step 2: Re-rank competitors by Meta activity + AI confidence
            ranked_competitors = []
            for competitor in competitors:
                tier_data = ad_tiers.get(competitor.company_name, {'tier': -1, 'estimated_count': 'Error'})
                meta_tier = tier_data['tier']
                
                # Calculate new meta-aware quality score
                meta_weight = self._get_meta_weight(meta_tier)
                
                # Only keep competitors with Meta presence for a Meta ads intelligence system
                if meta_tier > 0:
                    meta_aware_score = (competitor.quality_score * 0.4) + (meta_weight * 0.6)
                    competitor.quality_score = meta_aware_score  # Update score
                    competitor.meta_tier = meta_tier
                    competitor.estimated_ad_count = tier_data['estimated_count']
                    competitor.meta_classification = tier_data['classification']
                    ranked_competitors.append(competitor)
                elif self.verbose:
                    classification = tier_data.get('classification', f'Tier {meta_tier}')
                    print(f"   ❌ Filtered out {competitor.company_name}: {classification}")
            
            # Step 3: Apply intelligent capping (max 10 competitors)
            ranked_competitors.sort(key=lambda x: x.quality_score, reverse=True)
            if len(ranked_competitors) > 10:
                print(f"   🎯 Applying intelligent capping: {len(ranked_competitors)} → 10 competitors")
                ranked = ranked_competitors[:10]
            else:
                ranked = ranked_competitors
            
            # Step 4: Display final selection
            if ranked:
                print(f"   ✅ Selected {len(ranked)} Meta-active competitors:")
                for i, comp in enumerate(ranked[:5], 1):
                    print(f"      {i}. {comp.company_name} - {comp.meta_classification} ({comp.estimated_ad_count} ads)")
                if len(ranked) > 5:
                    print(f"      ... and {len(ranked) - 5} more")
            else:
                # No competitors have Meta presence - graceful exit
                raise ValueError(
                    "❌ INSUFFICIENT META DATA: No validated competitors are actively advertising on Meta. "
                    "This Meta Ads Competitive Intelligence system requires brands with active Meta ad presence. "
                    "Consider expanding to competitors from other advertising platforms."
                )
            
            return ranked
                
        except Exception as e:
            self.logger.error(f"Meta ad ranking failed: {str(e)}")
            if self.verbose:
                import traceback
                traceback.print_exc()
            raise
    
    def _get_meta_weight(self, meta_tier: int) -> float:
        """Get meta weight based on tier"""
        meta_weights = {
            3: 1.0,   # Major Player (20+ ads) - Full weight
            2: 0.8,   # Moderate Player (11-19 ads) - High weight  
            1: 0.6,   # Minor Player (1-10 ads) - Medium weight
            0: 0.0,   # No Meta Presence - Zero weight
            -1: 0.0   # API Error - Zero weight
        }
        return meta_weights.get(meta_tier, 0.0)
    
    def get_competitor_brands(self, ranked_competitors: List[ValidatedCompetitor]) -> List[str]:
        """Extract competitor brand names for downstream stages"""
        return [comp.company_name for comp in ranked_competitors]