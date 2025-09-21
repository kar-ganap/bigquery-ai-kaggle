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

from src.utils.ads_fetcher import MetaAdsFetcher
from src.utils.media_storage import MediaStorageManager

try:
    from src.utils.bigquery_client import load_dataframe_to_bq, run_query
except ImportError:
    try:
        from scripts.utils.bigquery_client import load_dataframe_to_bq, run_query  # Legacy fallback
    except ImportError:
        load_dataframe_to_bq = None
        run_query = None

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

        # Load configuration from environment
        self.max_ads = int(os.getenv('MAX_ADS_PER_COMPANY', '500'))
        self.max_pages = int(os.getenv('MAX_PAGES_PER_COMPANY', '10'))
        self.delay_between_requests = float(os.getenv('DELAY_BETWEEN_REQUESTS', '0.5'))
        self.image_budget = int(os.getenv('MULTIMODAL_IMAGE_BUDGET', '60'))

        # Initialize media storage manager for classify-and-download
        try:
            self.media_manager = MediaStorageManager()
            self.media_storage_enabled = True
        except Exception as e:
            print(f"   ⚠️  Media storage disabled: {str(e)}")
            self.media_manager = None
            self.media_storage_enabled = False
    
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
            
            # Sequential fetching with delays (no parallel processing to avoid API issues)
            all_ads, brands_with_ads = self._fetch_competitor_ads_sequential(fetcher, top_competitors)
            
            # Also fetch ads for the target brand itself (with delay if we fetched competitor ads)
            if brands_with_ads:
                delay = self.delay_between_requests * 2
                print(f"   ⏱️  Waiting {delay}s before fetching target brand...")
                time.sleep(delay)
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
                    # Prepare ads data for BigQuery by handling array fields correctly
                    prepared_ads = []
                    for ad in all_ads:
                        prepared_ad = ad.copy()

                        # Convert array fields to JSON strings for BigQuery compatibility
                        if 'image_urls' in prepared_ad and isinstance(prepared_ad['image_urls'], list):
                            # Store as JSON string array for now, will convert properly in strategic labeling
                            prepared_ad['image_urls_json'] = str(prepared_ad['image_urls'])
                            # Keep first image for backward compatibility
                            prepared_ad['image_url'] = prepared_ad['image_urls'][0] if prepared_ad['image_urls'] else None
                            del prepared_ad['image_urls']

                        if 'video_urls' in prepared_ad and isinstance(prepared_ad['video_urls'], list):
                            # Store as JSON string array for now, will convert properly in strategic labeling
                            prepared_ad['video_urls_json'] = str(prepared_ad['video_urls'])
                            # Keep first video for backward compatibility
                            prepared_ad['video_url'] = prepared_ad['video_urls'][0] if prepared_ad['video_urls'] else None
                            del prepared_ad['video_urls']

                        prepared_ads.append(prepared_ad)

                    ads_df = pd.DataFrame(prepared_ads)
                    ads_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.ads_raw_{self.context.run_id}"
                    print(f"   💾 Loading {len(ads_df)} ads to BigQuery table {ads_table_id}...")
                    load_dataframe_to_bq(ads_df, ads_table_id, write_disposition="WRITE_TRUNCATE")
                    results.ads_table_id = ads_table_id

                    # Note: Deduplication is handled in Stage 5 (Strategic Labeling)
                    # where ads_raw is transformed into ads_with_dates
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
    
    def _fetch_competitor_ads_sequential(self, fetcher, competitors: List[ValidatedCompetitor]):
        """Fetch ads for competitors using sequential processing with delays to avoid API issues"""

        print(f"\n   🔄 Sequential fetching with delays between calls...")

        all_ads = []
        brands_with_ads = []

        for i, comp in enumerate(competitors):
            try:
                start_time = time.time()
                print(f"   📲 Starting fetch for {comp.company_name} ({i+1}/{len(competitors)})...")

                # Add delay between API calls (except for first call)
                if i > 0:
                    delay = self.delay_between_requests * 2  # Double the normal delay between competitors
                    print(f"   ⏱️  Waiting {delay}s before next API call...")
                    time.sleep(delay)

                # Fetch ads for this competitor
                ads, fetch_result = fetcher.fetch_company_ads_with_metadata(
                    company_name=comp.company_name,
                    max_ads=self.max_ads,
                    max_pages=self.max_pages,
                    delay_between_requests=self.delay_between_requests
                )

                elapsed = time.time() - start_time

                if ads:
                    # Process ads to pipeline format
                    processed_ads = []
                    for ad in ads:
                        ad_data = self._normalize_ad_data(ad, comp.company_name)
                        processed_ads.append(ad_data)

                    all_ads.extend(processed_ads)
                    brands_with_ads.append(comp.company_name)
                    print(f"      ✅ {comp.company_name}: Found {len(processed_ads)} ads in {elapsed:.1f}s")
                else:
                    print(f"      ⚠️  {comp.company_name}: No ads found in {elapsed:.1f}s")

            except Exception as e:
                elapsed = time.time() - start_time if 'start_time' in locals() else 0
                self.logger.warning(f"Failed to fetch ads for {comp.company_name}: {str(e)}")
                print(f"      ❌ {comp.company_name}: Error in {elapsed:.1f}s - {str(e)[:100]}")

        return all_ads, brands_with_ads

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
                    max_ads=self.max_ads,
                    max_pages=self.max_pages,
                    delay_between_requests=self.delay_between_requests
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
                max_ads=self.max_ads,
                max_pages=self.max_pages,
                delay_between_requests=self.delay_between_requests
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
        
        # Extract creative content - trust the fetcher's creative_text if available
        creative_text = ad.get('creative_text', '')

        # Fallback to extracting from snapshot if fetcher didn't provide creative_text
        if not creative_text:
            if isinstance(snapshot.get('body'), dict):
                creative_text = snapshot.get('body', {}).get('text', '')
            else:
                creative_text = snapshot.get('body', '') or ''
        
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
            'card_index': ad.get('card_index'),

            # MEDIA CLASSIFICATION AND STORAGE: Classify and download at ingestion
            **self._classify_and_store_media(ad, brand_name),

            'created_date': datetime.now().isoformat()
        }

    def _classify_and_store_media(self, ad: dict, brand_name: str) -> dict:
        """Classify media type and download/store media at ingestion time"""

        if not self.media_storage_enabled:
            # Fallback: use old media_type field if media storage is disabled
            return {
                'computed_media_type': ad.get('media_type', 'unknown'),
                'media_storage_path': None
            }

        try:
            media_type, storage_path = self.media_manager.classify_and_store_media(ad, brand_name)
            return {
                'computed_media_type': media_type,
                'media_storage_path': storage_path
            }
        except Exception as e:
            print(f"   ⚠️  Media classification failed for ad {ad.get('ad_id', 'unknown')}: {str(e)}")
            return {
                'computed_media_type': 'unknown',
                'media_storage_path': None
            }

    # Note: ads_with_dates deduplication is now handled in Stage 5 (Strategic Labeling)
    # This maintains proper separation of concerns and avoids schema mismatches