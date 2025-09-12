"""
Stage 4: Meta Ads Ingestion (Full Data Collection)

Clean, focused implementation of ad data collection from Meta ads.
"""
import os
import time
import pandas as pd
from datetime import datetime
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..core.base import PipelineStage, PipelineContext  
from ..models.candidates import ValidatedCompetitor, IngestionResults

try:
    from scripts.utils.ads_fetcher import MetaAdsFetcher
except ImportError:
    try:
        from scripts.ingest_fb_ads import MetaAdsFetcher
    except ImportError:
        MetaAdsFetcher = None

try:
    from scripts.utils.bigquery_client import load_dataframe_to_bq
except ImportError:
    load_dataframe_to_bq = None

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class IngestionStage(PipelineStage[List[ValidatedCompetitor], IngestionResults]):
    """
    Stage 4: Meta Ads Ingestion (Full Data Collection).
    
    Responsibilities:
    - Fetch ads for top competitors using parallel processing
    - Also fetch ads for target brand itself
    - Normalize ad data to pipeline format
    - Load ads to BigQuery for downstream processing
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Meta Ads Ingestion", 4, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.verbose = verbose
    
    def execute(self, competitors: List[ValidatedCompetitor]) -> IngestionResults:
        """Execute Meta ads ingestion"""
        
        if self.dry_run:
            return self._create_mock_ingestion(competitors)
        
        return self._run_real_ingestion(competitors)
    
    def _create_mock_ingestion(self, competitors: List[ValidatedCompetitor]) -> IngestionResults:
        """Create mock ad ingestion for testing"""
        mock_ads = []
        for comp in competitors[:3]:
            for i in range(20):
                mock_ads.append({
                    'ad_archive_id': f"{comp.company_name}_{i}",
                    'brand': comp.company_name,
                    'creative_text': f"Mock ad text for {comp.company_name} ad {i}",
                    'title': f"Mock title {i}",
                    'created_date': datetime.now().isoformat()
                })
        
        results = IngestionResults(
            ads=mock_ads,
            brands=[c.company_name for c in competitors[:3]],
            total_ads=len(mock_ads),
            ingestion_time=0.5,
            ads_table_id=None
        )
        
        print(f"   📈 [DRY RUN] Generated {results.total_ads} mock ads from {len(results.brands)} brands")
        return results
    
    def _run_real_ingestion(self, competitors: List[ValidatedCompetitor]) -> IngestionResults:
        """Run actual Meta ads ingestion with parallel processing"""
        
        if MetaAdsFetcher is None:
            self.logger.error("Meta Ads fetcher not available")
            raise ImportError("MetaAdsFetcher required for ad ingestion")
        
        try:
            print("   📱 Initializing Meta Ads fetcher...")
            fetcher = MetaAdsFetcher()
            
            # Select top competitors by quality score * market overlap
            top_competitors = sorted(
                competitors,
                key=lambda x: x.quality_score * (x.market_overlap_pct / 100.0),
                reverse=True
            )[:5]  # Top 5 competitors
            
            print(f"   🎯 Fetching ads for top {len(top_competitors)} competitors:")
            for comp in top_competitors:
                print(f"      • {comp.company_name} (confidence: {comp.confidence:.2f}, overlap: {comp.market_overlap_pct}%)")
            
            all_ads = []
            brands_with_ads = []
            
            # Parallel fetching with 3 workers to prevent timeout
            all_ads, brands_with_ads = self._fetch_competitor_ads_parallel(fetcher, top_competitors)
            
            # Also fetch ads for the target brand itself
            target_ads = self._fetch_target_brand_ads(fetcher)
            if target_ads:
                all_ads.extend(target_ads)
                brands_with_ads.append(self.context.brand)
            
            results = IngestionResults(
                ads=all_ads,
                brands=brands_with_ads,
                total_ads=len(all_ads),
                ingestion_time=0.0,  # Will be set by caller
                ads_table_id=None
            )
            
            print(f"\n   📊 Ingestion summary: {len(all_ads)} total ads from {len(results.brands)} brands")
            
            # Load ads to BigQuery for embedding generation
            if len(all_ads) > 0 and load_dataframe_to_bq:
                try:
                    ads_df = pd.DataFrame(all_ads)
                    ads_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.ads_raw_{self.context.run_id}"
                    print(f"   💾 Loading {len(ads_df)} ads to BigQuery table {ads_table_id}...")
                    load_dataframe_to_bq(ads_df, ads_table_id, write_disposition="WRITE_TRUNCATE")
                    results.ads_table_id = ads_table_id
                except Exception as load_e:
                    print(f"   ⚠️  Could not load ads to BigQuery: {load_e}")
                    results.ads_table_id = None
            
            return results
            
        except Exception as e:
            self.logger.error(f"Ingestion failed: {str(e)}")
            if self.verbose:
                import traceback
                traceback.print_exc()
            # Return empty results rather than failing completely
            return IngestionResults(ads=[], brands=[], total_ads=0, ingestion_time=0.0, ads_table_id=None)
    
    def _fetch_competitor_ads_parallel(self, fetcher, competitors: List[ValidatedCompetitor]):
        """Fetch ads for competitors using parallel processing"""
        
        def fetch_competitor_ads(comp):
            """Helper function to fetch ads for a single competitor"""
            try:
                start_time = time.time()
                print(f"   📲 Starting fetch for {comp.company_name}...")
                
                # Fetch ads for this competitor
                ads, fetch_result = fetcher.fetch_company_ads_with_metadata(
                    company_name=comp.company_name,
                    max_ads=30,
                    max_pages=3,
                    delay_between_requests=0.5
                )
                
                elapsed = time.time() - start_time
                
                if ads:
                    # Process ads to pipeline format
                    processed_ads = []
                    for ad in ads:
                        ad_data = self._normalize_ad_data(ad, comp.company_name)
                        processed_ads.append(ad_data)
                    
                    return (comp.company_name, processed_ads, elapsed, None)
                else:
                    return (comp.company_name, [], elapsed, "No ads found")
                    
            except Exception as e:
                elapsed = time.time() - start_time if 'start_time' in locals() else 0
                return (comp.company_name, [], elapsed, str(e))
        
        # Use 3 parallel workers to optimize for 5 competitors
        max_workers = min(3, len(competitors))
        print(f"\n   🚀 Parallel fetching with {max_workers} workers to prevent timeout...")
        
        all_ads = []
        brands_with_ads = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_comp = {executor.submit(fetch_competitor_ads, comp): comp 
                             for comp in competitors}
            
            # Process completed tasks as they finish
            for future in as_completed(future_to_comp):
                comp_name, processed_ads, elapsed, error = future.result()
                
                if error and error != "No ads found":
                    self.logger.warning(f"Failed to fetch ads for {comp_name}: {error}")
                    print(f"      ❌ {comp_name}: Error in {elapsed:.1f}s - {error[:100]}")
                elif processed_ads:
                    all_ads.extend(processed_ads)
                    brands_with_ads.append(comp_name)
                    print(f"      ✅ {comp_name}: Found {len(processed_ads)} ads in {elapsed:.1f}s")
                else:
                    print(f"      ⚠️  {comp_name}: No ads found in {elapsed:.1f}s")
        
        return all_ads, brands_with_ads
    
    def _fetch_target_brand_ads(self, fetcher):
        """Fetch ads for the target brand itself"""
        print(f"\n   📲 Fetching ads for target brand: {self.context.brand}...")
        try:
            target_ads, _ = fetcher.fetch_company_ads_with_metadata(
                company_name=self.context.brand,
                max_ads=30,
                max_pages=3,
                delay_between_requests=0.5
            )
            
            if target_ads:
                processed_target_ads = []
                for ad in target_ads:
                    ad_data = self._normalize_ad_data(ad, self.context.brand)
                    processed_target_ads.append(ad_data)
                print(f"      ✅ Found {len(target_ads)} ads for target brand")
                return processed_target_ads
            else:
                print(f"      ⚠️  No ads found for target brand")
                return []
                
        except Exception as e:
            self.logger.warning(f"Failed to fetch ads for target brand {self.context.brand}: {str(e)}")
            print(f"      ⚠️  Could not fetch target brand ads: {str(e)}")
            return []
    
    def _normalize_ad_data(self, ad: dict, brand_name: str) -> dict:
        """Normalize ad data to pipeline format"""
        
        # Handle different formats of ad data - either from MetaAdsFetcher or direct API
        snapshot = ad.get("snapshot", {}) or {}
        
        # Extract temporal data from different possible sources
        start_date_str = (ad.get("start_date_string") or 
                         ad.get("ad_delivery_start_time") or
                         ad.get("start_date"))
        end_date_str = (ad.get("end_date_string") or 
                       ad.get("ad_delivery_stop_time") or
                       ad.get("end_date"))
        
        # Extract page name from different sources
        page_name = ad.get('page_name') or snapshot.get('page_name') or brand_name
        
        # Extract creative content
        creative_text = (ad.get('creative_text') or 
                        snapshot.get('body', {}).get('text') if isinstance(snapshot.get('body'), dict) else 
                        snapshot.get('body') or '')
        
        title = ad.get('title') or snapshot.get('title') or ''
        
        # Extract media information 
        snapshot_url = ad.get('snapshot_url') or ad.get('url')
        publisher_platforms = ad.get('publisher_platforms') or ad.get('publisher_platform', [])
        if isinstance(publisher_platforms, list):
            publisher_platforms = ','.join(publisher_platforms)
        
        return {
            'ad_archive_id': ad.get('ad_id') or ad.get('ad_archive_id'),
            'brand': brand_name,
            'page_name': page_name,
            'creative_text': creative_text,
            'title': title,
            'cta_text': ad.get('cta_text', ''),
            'impressions_lower': ad.get('impressions', {}).get('lower_bound'),
            'impressions_upper': ad.get('impressions', {}).get('upper_bound'),
            'spend_lower': ad.get('spend', {}).get('lower_bound'),
            'spend_upper': ad.get('spend', {}).get('upper_bound'),
            'currency': ad.get('currency', 'USD'),
            
            # Capture temporal data from ScrapeCreators API
            'start_date_string': start_date_str,
            'end_date_string': end_date_str,
            
            # Additional fields for comprehensive ad data
            'snapshot_url': snapshot_url,
            'publisher_platforms': publisher_platforms,
            'display_format': snapshot.get('display_format'),
            'landing_url': snapshot.get('link_url'),
            'cta_type': snapshot.get('cta_type'),
            'media_type': ad.get('media_type'),
            'image_url': ad.get('image_url'),
            'video_url': ad.get('video_url'),
            'card_index': ad.get('card_index'),
            
            'created_date': datetime.now().isoformat()
        }