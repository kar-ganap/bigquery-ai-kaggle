"""
Stage 6: Strategic Analysis

Clean, focused implementation of competitive intelligence analysis.
"""
import os
import time
from typing import List

from ..core.base import PipelineStage, PipelineContext
from ..models.candidates import EmbeddingResults, AnalysisResults

try:
    from scripts.utils.bigquery_client import get_bigquery_client, run_query
except ImportError:
    get_bigquery_client = None
    run_query = None

try:
    from scripts.analysis.temporal_intelligence_engine import TemporalIntelligenceEngine
except ImportError:
    TemporalIntelligenceEngine = None

try:
    from scripts.analysis.enhanced_3d_whitespace_detector import Enhanced3DWhiteSpaceDetector
except ImportError:
    Enhanced3DWhiteSpaceDetector = None

# Environment configuration
BQ_PROJECT = os.environ.get("BQ_PROJECT", "bigquery-ai-kaggle-469620")
BQ_DATASET = os.environ.get("BQ_DATASET", "ads_demo")


class AnalysisStage(PipelineStage[EmbeddingResults, AnalysisResults]):
    """
    Stage 6: Strategic Analysis.
    
    Responsibilities:
    - Current state analysis using strategic labels
    - Competitive copying detection using embeddings
    - Temporal intelligence and evolution analysis
    - Wide net forecasting with business impact
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Strategic Analysis", 6, context.run_id)
        self.context = context
        self.dry_run = dry_run
        self.verbose = verbose
        # Get competitor brands from context (set by previous stages)
        self.competitor_brands = getattr(context, 'competitor_brands', [])
        self.temporal_engine = None
        self.whitespace_detector = None
    
    def execute(self, embeddings: EmbeddingResults) -> AnalysisResults:
        """Execute strategic analysis"""
        
        if self.dry_run:
            return self._create_mock_analysis()
        
        return self._run_real_analysis(embeddings)
    
    def _create_mock_analysis(self) -> AnalysisResults:
        """Create mock analysis for testing"""
        return AnalysisResults(
            status="success",
            current_state={
                'promotional_intensity': 0.67,
                'urgency_score': 0.58,
                'brand_voice_score': 0.73,
                'market_position': 'defensive',
                'promotional_volatility': 0.12
            },
            influence={
                'copying_detected': True,
                'top_copier': 'Competitor_1',
                'similarity_score': 0.84,
                'lag_days': 5
            },
            evolution={
                'momentum_status': 'ACCELERATING',
                'velocity_change_7d': 0.15,
                'velocity_change_30d': 0.08,
                'trend_direction': 'increasing'
            },
            forecasts={
                'executive_summary': 'MODERATE INCREASE: Expected 15% rise in competitive pressure',
                'business_impact_score': 3,
                'confidence': 'MEDIUM',
                'next_30_days': 'increased_competition'
            }
            # All other fields (metadata, velocity, patterns, etc.) get default values automatically
        )
    
    def _run_real_analysis(self, embeddings: EmbeddingResults) -> AnalysisResults:
        """Run actual strategic analysis"""
        
        if get_bigquery_client is None or run_query is None:
            self.logger.error("BigQuery client not available")
            raise ImportError("BigQuery client required for analysis")
        
        try:
            print("   🔍 Running enhanced strategic analysis with temporal intelligence...")
            
            # Robust polling mechanism to ensure BigQuery table consistency after Strategic Labeling
            import time
            print("   ⏰ Waiting for BigQuery strategic data availability...")
            self._wait_for_strategic_data_availability()
            
            analysis = AnalysisResults()
            analysis.status = "success"
            # Note: Using dataclass constructor ensures all fields get proper defaults
            
            # Initialize enhanced intelligence modules
            self._initialize_intelligence_engines()
            
            # Step 1: Current State Analysis
            print("   📊 Analyzing current strategic position...")
            try:
                analysis.current_state = self._analyze_current_state()
                print(f"   ✅ Current state analysis complete: PI = {analysis.current_state.get('promotional_intensity', 'MISSING')}")
            except Exception as e:
                print(f"   ❌ Current state analysis failed: {e}")
                analysis.current_state = {
                    'promotional_intensity': 0.0,
                    'urgency_score': 0.0,
                    'brand_voice_score': 0.0,
                    'market_position': 'unknown',
                    'promotional_volatility': 0.0
                }
            
            # Step 2: Competitive Copying Detection
            print("   🎯 Detecting competitive copying patterns...")
            try:
                analysis.influence = self._detect_copying_patterns(embeddings)
                print("   ✅ Copying detection complete")
            except Exception as e:
                print(f"   ❌ Copying detection failed: {e}")
                analysis.influence = {'copying_detected': False, 'similarity_score': 0.0}
            
            # Step 3: Temporal Intelligence Analysis
            print("   📈 Analyzing temporal intelligence (where did we come from)...")
            try:
                analysis.evolution = self._analyze_temporal_intelligence()
                print("   ✅ Temporal intelligence complete")
            except Exception as e:
                print(f"   ❌ Temporal intelligence failed: {e}")
                analysis.evolution = {'trend_direction': 'stable', 'momentum_status': 'STABLE', 'velocity_change_7d': 0.0, 'velocity_change_30d': 0.0}
            
            # Step 4: Wide Net Forecasting
            print("   🔮 Generating Wide Net forecasting (where are we going)...")
            try:
                analysis.forecasts = self._generate_forecasts()
                print("   ✅ Forecasting complete")
            except Exception as e:
                print(f"   ❌ Forecasting failed: {e}")
                analysis.forecasts = {'next_30_days': 'stable_market', 'confidence': 'LOW', 'business_impact_score': 2}
            
            # DEBUG: Log final analysis result before returning
            print(f"   🔍 DEBUG ANALYSIS: Final analysis.current_state = {analysis.current_state}")
            print(f"   🔍 DEBUG ANALYSIS: About to return analysis with current_state PI = {analysis.current_state.get('promotional_intensity', 'MISSING')}")
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Strategic analysis failed: {str(e)}")
            print(f"   ❌ Analysis failed, using fallback: {e}")
            
            # Fallback to basic mock results
            return self._create_fallback_analysis()
    
    def _initialize_intelligence_engines(self):
        """Initialize the temporal intelligence and whitespace detection engines"""
        if TemporalIntelligenceEngine:
            self.temporal_engine = TemporalIntelligenceEngine(
                project_id=BQ_PROJECT,
                dataset_id=BQ_DATASET,
                brand=self.context.brand,
                competitors=self.competitor_brands,
                run_id=self.context.run_id
            )
            print("   ⏰ Temporal Intelligence Engine initialized")
        
        if Enhanced3DWhiteSpaceDetector:
            self.whitespace_detector = Enhanced3DWhiteSpaceDetector(
                project_id=BQ_PROJECT,
                dataset_id=BQ_DATASET,
                brand=self.context.brand,
                competitors=self.competitor_brands
            )
            print("   🎯 Enhanced 3D White Space Detector initialized")
    
    def _analyze_current_state(self) -> dict:
        """Analyze current strategic position using strategic labels"""
        
        # Check if we have strategic labels
        strategic_labels_query = f"""
        SELECT COUNT(*) as has_strategic_data
        FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
        WHERE brand IN ({', '.join([f"'{b}'" for b in [self.context.brand] + self.competitor_brands])})
        """
        
        try:
            strategic_result = run_query(strategic_labels_query)
            has_strategic_data = strategic_result.iloc[0]['has_strategic_data'] > 0 if not strategic_result.empty else False
            strategic_count = strategic_result.iloc[0]['has_strategic_data'] if not strategic_result.empty else 0
            
            if has_strategic_data:
                print("   ✅ Using existing strategic labels for analysis")
                print(f"   🔍 DEBUG: Found {strategic_count} records with strategic data")
                
                current_state_query = f"""
                SELECT 
                    brand,
                    AVG(promotional_intensity) as avg_promotional_intensity,
                    AVG(urgency_score) as avg_urgency_score, 
                    AVG(brand_voice_score) as avg_brand_voice_score,
                    STDDEV(promotional_intensity) as promotional_volatility,
                    CASE 
                        WHEN AVG(promotional_intensity) > 0.6 THEN 'offensive'
                        WHEN AVG(promotional_intensity) > 0.4 THEN 'balanced'
                        ELSE 'defensive'
                    END as market_position
                FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
                WHERE brand = '{self.context.brand}'
                GROUP BY brand
                """
                
                current_result = run_query(current_state_query)
                if not current_result.empty:
                    row = current_result.iloc[0]
                    result = {
                        'promotional_intensity': float(row.get('avg_promotional_intensity', 0)),
                        'urgency_score': float(row.get('avg_urgency_score', 0)),
                        'brand_voice_score': float(row.get('avg_brand_voice_score', 0)),
                        'market_position': row.get('market_position', 'unknown'),
                        'promotional_volatility': float(row.get('promotional_volatility', 0))
                    }
                    print(f"   📊 Analysis metrics found: PI={result['promotional_intensity']:.3f}, US={result['urgency_score']:.3f}, BV={result['brand_voice_score']:.3f}")
                    print(f"   🔍 DEBUG ANALYSIS: Returning current_state = {result}")
                    for key, value in result.items():
                        print(f"   🔍 DEBUG ANALYSIS: result['{key}'] = {value} (type: {type(value)})")
                    return result
            else:
                print("   ⚠️  No strategic labels found, using basic analysis")
                
        except Exception as e:
            print(f"   ⚠️  Current state analysis error: {e}")
        
        # Fallback - ensure we return all fields with float default values
        print("   ⚠️  Returning fallback analysis with float default values")
        return {
            'promotional_intensity': 0.0,
            'urgency_score': 0.0,
            'brand_voice_score': 0.0,
            'market_position': 'unknown',
            'promotional_volatility': 0.0
        }
    
    def _detect_copying_patterns(self, embeddings: EmbeddingResults) -> dict:
        """Detect competitive copying patterns using embeddings joined with strategic labels for timestamps"""
        
        if embeddings.embedding_count == 0:
            return {'copying_detected': False, 'similarity_score': 0}
        
        try:
            # Join embeddings with strategic labels to get timestamps for temporal analysis
            copying_query = f"""
            WITH brand_embeddings_with_time AS (
                SELECT 
                    e.brand,
                    e.ad_archive_id,
                    e.content_embedding,
                    s.start_timestamp
                FROM `{embeddings.table_id}` e
                LEFT JOIN `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates` s
                ON e.ad_archive_id = s.ad_archive_id AND e.brand = s.brand
                WHERE e.content_embedding IS NOT NULL
                    AND s.start_timestamp IS NOT NULL
                    AND e.brand IN ({', '.join([f"'{self.context.brand}'"] + [f"'{b}'" for b in self.competitor_brands])})
            ),
            brand_similarity AS (
                SELECT 
                    a.brand as original_brand,
                    b.brand as potential_copier,
                    ML.DISTANCE(a.content_embedding, b.content_embedding, 'COSINE') as similarity_score,
                    DATE_DIFF(DATE(b.start_timestamp), DATE(a.start_timestamp), DAY) as lag_days
                FROM brand_embeddings_with_time a
                CROSS JOIN brand_embeddings_with_time b  
                WHERE a.brand = '{self.context.brand}'
                    AND b.brand != '{self.context.brand}'
                    AND b.brand IN ({', '.join([f"'{b}'" for b in self.competitor_brands])})
                    AND DATE(b.start_timestamp) >= DATE(a.start_timestamp)
                    AND ML.DISTANCE(a.content_embedding, b.content_embedding, 'COSINE') < 0.3
            )
            SELECT 
                potential_copier,
                AVG(similarity_score) as avg_similarity,
                MIN(lag_days) as min_lag_days,
                COUNT(*) as comparison_count
            FROM brand_similarity
            WHERE lag_days >= 0  -- Only consider ads that came after original
            GROUP BY potential_copier
            ORDER BY avg_similarity ASC
            LIMIT 1
            """
            
            copying_result = run_query(copying_query)
            if not copying_result.empty:
                row = copying_result.iloc[0]
                similarity = float(row.get('avg_similarity', 1.0))
                # Convert COSINE distance to similarity (1 - distance)
                similarity_pct = max(0, 1 - similarity)
                return {
                    'copying_detected': True,
                    'top_copier': row.get('potential_copier', 'Unknown'),
                    'similarity_score': similarity_pct,
                    'lag_days': int(row.get('min_lag_days', 0))
                }
                
        except Exception as e:
            print(f"   ⚠️  Copying detection error: {e}")
        
        return {'copying_detected': False, 'similarity_score': 0}
    
    def _analyze_temporal_intelligence(self) -> dict:
        """Analyze temporal intelligence using the temporal engine"""
        
        if self.temporal_engine:
            try:
                temporal_sql = self.temporal_engine.generate_temporal_analysis_sql()
                temporal_result = run_query(temporal_sql)
                if not temporal_result.empty:
                    row = temporal_result.iloc[0]
                    return {
                        'momentum_status': row.get('momentum_status', 'STABLE'),
                        'velocity_change_7d': float(row.get('velocity_change_7d', 0)),
                        'velocity_change_30d': float(row.get('velocity_change_30d', 0)),
                        'cta_intensity_shift': float(row.get('cta_intensity_shift', 0)),
                        'creative_status': row.get('creative_status', 'FRESH_CREATIVES'),
                        'avg_campaign_age': float(row.get('avg_campaign_age', 15))
                    }
                    
            except Exception as e:
                print(f"   ⚠️  Temporal analysis fallback: {e}")
        
        # Fallback to basic evolution analysis
        return {'trend_direction': 'stable', 'data_available': False}
    
    def _generate_forecasts(self) -> dict:
        """Generate Wide Net forecasting with business impact"""
        
        if self.temporal_engine:
            try:
                forecast_sql = self.temporal_engine.generate_wide_net_forecasting_sql()
                forecast_result = run_query(forecast_sql)
                if not forecast_result.empty:
                    # Get top forecast
                    top_forecast = forecast_result.iloc[0]
                    return {
                        'executive_summary': top_forecast.get('executive_summary', 'STABLE: No significant changes predicted'),
                        'business_impact_score': int(top_forecast.get('business_impact_score', 2)),
                        'confidence': 'HIGH' if top_forecast.get('business_impact_score', 2) >= 4 else 'MEDIUM',
                        'top_predictions': self._extract_top_predictions(forecast_result)
                    }
                    
            except Exception as e:
                print(f"   ⚠️  Forecasting fallback: {e}")
        
        # Fallback to basic forecasting
        return {'next_30_days': 'stable_market', 'confidence': 'LOW', 'data_available': False}
    
    def _extract_top_predictions(self, forecast_result) -> list:
        """Extract top predictions from forecast results"""
        predictions = []
        for _, row in forecast_result.head(3).iterrows():
            predictions.append({
                'brand': row.get('brand', 'Unknown'),
                'forecast': row.get('executive_summary', 'No forecast'),
                'impact_score': int(row.get('business_impact_score', 2)),
                'video_change': float(row.get('video_change_magnitude', 0)),
                'promotional_change': float(row.get('promotional_change_magnitude', 0))
            })
        return predictions
    
    def _create_fallback_analysis(self) -> AnalysisResults:
        """Create fallback analysis when real analysis fails"""
        return AnalysisResults(
            status="success",
            current_state={
                'promotional_intensity': 0.0,
                'urgency_score': 0.0,
                'brand_voice_score': 0.0,
                'market_position': 'unknown',
                'promotional_volatility': 0.0
            },
            influence={
                'copying_detected': False,
                'top_copier': 'None',
                'similarity_score': 0.0,
                'lag_days': 0
            },
            evolution={
                'trend_direction': 'stable',
                'momentum_status': 'STABLE',
                'velocity_change_7d': 0.0,
                'velocity_change_30d': 0.0
            },
            forecasts={
                'next_30_days': 'stable_market',
                'confidence': 'LOW',
                'business_impact_score': 2
            }
        )
    
    def _wait_for_strategic_data_availability(self, max_attempts: int = 10, delay_seconds: int = 3):
        """Wait for strategic data to be available in BigQuery with polling mechanism"""
        import time
        
        for attempt in range(max_attempts):
            try:
                # Check for strategic data availability
                check_query = f"""
                SELECT COUNT(*) as strategic_count
                FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
                WHERE brand = '{self.context.brand}'
                    AND promotional_intensity IS NOT NULL
                    AND promotional_intensity > 0
                """
                
                result = run_query(check_query)
                if not result.empty:
                    strategic_count = result.iloc[0]['strategic_count']
                    if strategic_count > 0:
                        print(f"   ✅ Strategic data available! Found {strategic_count} records with metrics")
                        return True
                
                print(f"   ⏳ Attempt {attempt + 1}/{max_attempts}: Strategic data not ready, waiting {delay_seconds}s...")
                time.sleep(delay_seconds)
                
            except Exception as e:
                print(f"   ⚠️  Strategic data check attempt {attempt + 1} failed: {e}")
                time.sleep(delay_seconds)
        
        print(f"   ⚠️  Strategic data not available after {max_attempts} attempts, proceeding anyway...")
        return False