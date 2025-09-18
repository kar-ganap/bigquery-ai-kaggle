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
    from src.utils.bigquery_client import get_bigquery_client, run_query
except ImportError:
    get_bigquery_client = None
    run_query = None

try:
    from src.competitive_intel.intelligence.temporal_intelligence_module import TemporalIntelligenceEngine
except ImportError:
    TemporalIntelligenceEngine = None

try:
    from src.competitive_intel.analysis.enhanced_whitespace_detection import Enhanced3DWhiteSpaceDetector
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
                'promotional_volatility': 0.12,
                'avg_cta_aggressiveness': 4.2
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
            
            # Step 1: CTA Intelligence Analysis (Required for temporal intelligence)
            print("   🎯 Executing CTA Intelligence Analysis...")
            try:
                self._execute_cta_intelligence_analysis()
                print("   ✅ CTA Intelligence analysis complete")
            except Exception as e:
                print(f"   ❌ CTA Intelligence analysis failed: {e}")

            # Step 2: Current State Analysis
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
                    'promotional_volatility': 0.0,
                    'avg_cta_aggressiveness': 0.0
                }
            
            # Step 3: Competitive Copying Detection
            print("   🎯 Detecting competitive copying patterns...")
            try:
                analysis.influence = self._detect_copying_patterns(embeddings)
                print("   ✅ Copying detection complete")
            except Exception as e:
                print(f"   ❌ Copying detection failed: {e}")
                analysis.influence = {'copying_detected': False, 'similarity_score': 0.0}

            # Step 4: Temporal Intelligence Analysis (now enhanced with CTA data)
            print("   📈 Analyzing temporal intelligence (where did we come from)...")
            try:
                analysis.evolution = self._analyze_temporal_intelligence()
                print("   ✅ Temporal intelligence complete")
            except Exception as e:
                print(f"   ❌ Temporal intelligence failed: {e}")
                analysis.evolution = {'trend_direction': 'stable', 'momentum_status': 'STABLE', 'velocity_change_7d': 0.0, 'velocity_change_30d': 0.0}

            # Step 5: Wide Net Forecasting
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

                # Query CTA aggressiveness from the CTA analysis table
                cta_aggressiveness = 0.0
                try:
                    cta_query = f"""
                    SELECT
                        -- Calculate numeric CTA aggressiveness score (0-10 scale)
                        CASE
                            WHEN cta_adoption_rate = 0 THEN 0.0
                            ELSE
                                (high_urgency_ctas * 10.0 +
                                 medium_engagement_ctas * 6.0 +
                                 consultative_ctas * 4.0 +
                                 low_pressure_ctas * 2.0) /
                                GREATEST(ads_with_cta, 1)
                        END as avg_cta_aggressiveness
                    FROM `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis`
                    WHERE brand = '{self.context.brand}'
                    """
                    cta_result = run_query(cta_query)
                    if not cta_result.empty:
                        cta_aggressiveness = float(cta_result.iloc[0].get('avg_cta_aggressiveness', 0.0))
                        print(f"   🎯 CTA aggressiveness score: {cta_aggressiveness:.2f}/10")
                    else:
                        print("   ⚠️  No CTA data found, using default value")
                except Exception as e:
                    print(f"   ⚠️  CTA aggressiveness query failed: {e}")

                if not current_result.empty:
                    row = current_result.iloc[0]
                    result = {
                        'promotional_intensity': float(row.get('avg_promotional_intensity', 0)),
                        'urgency_score': float(row.get('avg_urgency_score', 0)),
                        'brand_voice_score': float(row.get('avg_brand_voice_score', 0)),
                        'market_position': row.get('market_position', 'unknown'),
                        'promotional_volatility': float(row.get('promotional_volatility', 0)),
                        'avg_cta_aggressiveness': cta_aggressiveness
                    }
                    print(f"   📊 Analysis metrics found: PI={result['promotional_intensity']:.3f}, US={result['urgency_score']:.3f}, BV={result['brand_voice_score']:.3f}, CTA={result['avg_cta_aggressiveness']:.2f}")
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
            'promotional_volatility': 0.0,
            'avg_cta_aggressiveness': 0.0
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
                        'top_predictions': self._extract_top_predictions(forecast_result),
                        'next_7_days': self._generate_short_term_forecast(forecast_result, 7),
                        'next_14_days': self._generate_intermediate_forecast(forecast_result, 14),
                        'next_30_days': self._generate_long_term_forecast(forecast_result, 30)
                    }
                    
            except Exception as e:
                print(f"   ⚠️  Forecasting fallback: {e}")
        
        # Fallback to basic forecasting with progressive timeline
        return {
            'next_7_days': 'stable_market', 
            'next_14_days': 'stable_market', 
            'next_30_days': 'stable_market', 
            'confidence': 'LOW', 
            'data_available': False
        }
    
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
    
    def _generate_short_term_forecast(self, forecast_result, days: int) -> str:
        """Generate 7-day short-term forecast focusing on immediate tactical changes"""
        if forecast_result.empty:
            return "stable_tactical_environment"
        
        # Analyze immediate tactical patterns (urgency, promotional intensity changes)
        top_row = forecast_result.iloc[0]
        promotional_change = top_row.get('promotional_change_magnitude', 0)
        urgency_change = top_row.get('urgency_change_magnitude', 0)
        
        if abs(promotional_change) > 0.3 or abs(urgency_change) > 0.3:
            if promotional_change > 0.3:
                return "immediate_promotional_escalation_expected"
            elif promotional_change < -0.3:
                return "promotional_deescalation_likely"
            elif urgency_change > 0.3:
                return "urgency_messaging_increase_predicted"
            else:
                return "tactical_messaging_shift_anticipated"
        
        return "stable_short_term_outlook"
    
    def _generate_intermediate_forecast(self, forecast_result, days: int) -> str:
        """Generate 14-day intermediate forecast focusing on strategic positioning"""
        if forecast_result.empty:
            return "stable_competitive_positioning"
        
        # Analyze competitive positioning and strategic messaging shifts
        top_row = forecast_result.iloc[0]
        video_change = top_row.get('video_change_magnitude', 0)
        business_impact = top_row.get('business_impact_score', 2)
        
        if business_impact >= 4:
            return "significant_market_disruption_building"
        elif business_impact >= 3:
            if abs(video_change) > 0.2:
                return "creative_strategy_evolution_underway"
            else:
                return "competitive_pressure_mounting"
        elif abs(video_change) > 0.25:
            return "content_strategy_pivot_emerging"
        
        return "steady_competitive_equilibrium"
    
    def _generate_long_term_forecast(self, forecast_result, days: int) -> str:
        """Generate 30-day strategic forecast focusing on market positioning"""
        if forecast_result.empty:
            return "stable_market_outlook"
        
        # Analyze long-term strategic market positioning
        top_row = forecast_result.iloc[0]
        business_impact = top_row.get('business_impact_score', 2)
        executive_summary = top_row.get('executive_summary', '').upper()
        
        if business_impact >= 5:
            return "major_market_transformation_expected"
        elif business_impact >= 4:
            if 'AGGRESSIVE' in executive_summary or 'EXPANSION' in executive_summary:
                return "market_expansion_phase_predicted"
            else:
                return "competitive_intensity_increase_likely"
        elif business_impact >= 3:
            return "moderate_market_evolution_anticipated"
        elif 'DEFENSIVE' in executive_summary or 'MAINTAIN' in executive_summary:
            return "defensive_positioning_solidification"
        
        return "stable_market_continuation"
    
    def _create_fallback_analysis(self) -> AnalysisResults:
        """Create fallback analysis when real analysis fails"""
        return AnalysisResults(
            status="success",
            current_state={
                'promotional_intensity': 0.0,
                'urgency_score': 0.0,
                'brand_voice_score': 0.0,
                'market_position': 'unknown',
                'promotional_volatility': 0.0,
                'avg_cta_aggressiveness': 0.0
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

    def _execute_cta_intelligence_analysis(self):
        """Execute CTA Intelligence analysis to create cta_aggressiveness_analysis table for temporal intelligence"""

        # Get brands from context
        brands = [self.context.brand] + self.competitor_brands
        brands_filter = "', '".join(brands)

        # Enhanced CTA Intelligence SQL - creates the table that temporal intelligence expects
        cta_analysis_sql = f"""
        CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis` AS

        WITH cta_analysis AS (
          SELECT
            brand,
            ad_archive_id,
            cta_text,
            LENGTH(COALESCE(cta_text, '')) as cta_length,

            -- CTA Presence Analysis
            CASE
              WHEN cta_text IS NOT NULL AND LENGTH(cta_text) > 0 THEN 'HAS_CTA'
              ELSE 'NO_CTA'
            END as cta_presence,

            -- Enhanced CTA Aggressiveness Classification
            CASE
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(cta_text, '')), r'\\b(BUY NOW|ORDER NOW|SHOP NOW|LIMITED TIME|ACT FAST|HURRY)\\b') THEN 'HIGH_URGENCY'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(cta_text, '')), r'\\b(LEARN MORE|GET STARTED|DISCOVER|EXPLORE)\\b') THEN 'MEDIUM_ENGAGEMENT'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(cta_text, '')), r'\\b(BROWSE|VIEW|SEE MORE|FIND OUT)\\b') THEN 'LOW_PRESSURE'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(cta_text, '')), r'\\b(BOOK|SCHEDULE|CONSULT|TRY|EXPERIENCE)\\b') THEN 'CONSULTATIVE'
              WHEN cta_text IS NOT NULL AND LENGTH(cta_text) > 0 THEN 'OTHER'
              ELSE 'NO_CTA'
            END as cta_aggressiveness

          FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_raw_{self.context.run_id}`
          WHERE brand IN ('{brands_filter}')
        )

        SELECT
          brand,
          COUNT(*) as total_ads,
          COUNT(CASE WHEN cta_presence = 'HAS_CTA' THEN 1 END) as ads_with_cta,
          ROUND(COUNT(CASE WHEN cta_presence = 'HAS_CTA' THEN 1 END) * 100.0 / COUNT(*), 1) as cta_adoption_rate,
          ROUND(AVG(CASE WHEN cta_length > 0 THEN cta_length END), 1) as avg_cta_length,
          COUNT(CASE WHEN cta_aggressiveness = 'HIGH_URGENCY' THEN 1 END) as high_urgency_ctas,
          COUNT(CASE WHEN cta_aggressiveness = 'MEDIUM_ENGAGEMENT' THEN 1 END) as medium_engagement_ctas,
          COUNT(CASE WHEN cta_aggressiveness = 'LOW_PRESSURE' THEN 1 END) as low_pressure_ctas,
          COUNT(CASE WHEN cta_aggressiveness = 'CONSULTATIVE' THEN 1 END) as consultative_ctas,

          -- Dominant Strategy
          CASE
            WHEN COUNT(CASE WHEN cta_aggressiveness = 'HIGH_URGENCY' THEN 1 END) >= GREATEST(
              COUNT(CASE WHEN cta_aggressiveness = 'MEDIUM_ENGAGEMENT' THEN 1 END),
              COUNT(CASE WHEN cta_aggressiveness = 'LOW_PRESSURE' THEN 1 END),
              COUNT(CASE WHEN cta_aggressiveness = 'CONSULTATIVE' THEN 1 END)
            ) THEN 'HIGH_URGENCY'
            WHEN COUNT(CASE WHEN cta_aggressiveness = 'MEDIUM_ENGAGEMENT' THEN 1 END) >= GREATEST(
              COUNT(CASE WHEN cta_aggressiveness = 'LOW_PRESSURE' THEN 1 END),
              COUNT(CASE WHEN cta_aggressiveness = 'CONSULTATIVE' THEN 1 END)
            ) THEN 'MEDIUM_ENGAGEMENT'
            WHEN COUNT(CASE WHEN cta_aggressiveness = 'LOW_PRESSURE' THEN 1 END) >= COUNT(CASE WHEN cta_aggressiveness = 'CONSULTATIVE' THEN 1 END)
            THEN 'LOW_PRESSURE'
            ELSE 'CONSULTATIVE'
          END as dominant_cta_strategy,

          CURRENT_TIMESTAMP() as analysis_timestamp

        FROM cta_analysis
        GROUP BY brand
        ORDER BY cta_adoption_rate DESC;
        """

        # Execute the CTA Intelligence analysis
        run_query(cta_analysis_sql)
        print(f"   ✅ Created cta_aggressiveness_analysis table for temporal intelligence")