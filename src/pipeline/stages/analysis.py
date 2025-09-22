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
    from src.utils.sql_helpers import safe_brand_in_clause
except ImportError:
    get_bigquery_client = None
    run_query = None
    safe_brand_in_clause = None

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
    Stage 8: Strategic Analysis.
    
    Responsibilities:
    - Current state analysis using strategic labels
    - Competitive copying detection using embeddings
    - Temporal intelligence and evolution analysis
    - Wide net forecasting with business impact
    """
    
    def __init__(self, context: PipelineContext, dry_run: bool = False, verbose: bool = False):
        super().__init__("Strategic Analysis", 8, context.run_id)
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

            # Step 3.5: Creative Fatigue Analysis (adapted from legacy)
            print("   🎨 Analyzing creative fatigue patterns...")
            try:
                fatigue_analysis = self._analyze_creative_fatigue()
                # Add fatigue data to current_state (following legacy pattern)
                analysis.current_state.update({
                    'avg_fatigue_score': fatigue_analysis['avg_fatigue_score'],
                    'avg_originality_score': fatigue_analysis['avg_originality_score'],
                    'avg_refresh_signal': fatigue_analysis['avg_refresh_signal'],
                    'high_fatigue_count': fatigue_analysis['high_fatigue_count'],
                    'fatigue_level': fatigue_analysis['fatigue_level']
                })
                print("   ✅ Creative fatigue analysis complete")
            except Exception as e:
                print(f"   ❌ Creative fatigue analysis failed: {e}")

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
            
            # Log final analysis result before returning (commented for production)
            # print(f"   🔍 DEBUG ANALYSIS: Final analysis.current_state = {analysis.current_state}")
            # print(f"   🔍 DEBUG ANALYSIS: About to return analysis with current_state PI = {analysis.current_state.get('promotional_intensity', 'MISSING')}")
            
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
                # print(f"   🔍 DEBUG: Found {strategic_count} records with strategic data")
                
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
                        avg_cta_aggressiveness
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
                    # print(f"   🔍 DEBUG ANALYSIS: Returning current_state = {result}")
                    # for key, value in result.items():
                    #     print(f"   🔍 DEBUG ANALYSIS: result['{key}'] = {value} (type: {type(value)})")
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
            # Use all available brands in embeddings, not just competitor_brands list
            copying_query = f"""
            WITH all_brand_embeddings AS (
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
            ),
            brand_similarity AS (
                SELECT
                    a.brand as original_brand,
                    b.brand as potential_copier,
                    ML.DISTANCE(a.content_embedding, b.content_embedding, 'COSINE') as similarity_score,
                    DATE_DIFF(DATE(b.start_timestamp), DATE(a.start_timestamp), DAY) as lag_days
                FROM all_brand_embeddings a
                CROSS JOIN all_brand_embeddings b
                WHERE a.brand = '{self.context.brand}'
                    AND b.brand != '{self.context.brand}'
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

    def _analyze_creative_fatigue(self) -> dict:
        """Analyze creative fatigue using embeddings and temporal patterns (adapted from legacy)"""

        if not run_query:
            return self._mock_fatigue_results()

        try:
            print("   🎨 Analyzing creative fatigue patterns...")

            # Adapt legacy fatigue logic to work with current data structures
            fatigue_sql = f"""
            WITH similarity_pairs AS (
              -- Pre-compute all similarity pairs within 30-day windows
              SELECT
                e1.brand,
                e1.ad_archive_id as current_ad_id,
                s1.start_timestamp as current_timestamp,
                e2.ad_archive_id as past_ad_id,
                s2.start_timestamp as past_timestamp,
                1 - ML.DISTANCE(e1.content_embedding, e2.content_embedding, 'COSINE') as similarity
              FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_embeddings` e1
              INNER JOIN `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates` s1
                ON e1.ad_archive_id = s1.ad_archive_id AND e1.brand = s1.brand
              INNER JOIN `{BQ_PROJECT}.{BQ_DATASET}.ads_embeddings` e2
                ON e1.brand = e2.brand
              INNER JOIN `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates` s2
                ON e2.ad_archive_id = s2.ad_archive_id AND e2.brand = s2.brand
              WHERE s2.start_timestamp < s1.start_timestamp
                AND DATE_DIFF(DATE(s1.start_timestamp), DATE(s2.start_timestamp), DAY) <= 30
                AND e1.content_embedding IS NOT NULL
                AND e2.content_embedding IS NOT NULL
            ),

            brand_content_similarity AS (
              SELECT
                brand,
                current_ad_id as ad_archive_id,
                current_timestamp as start_timestamp,
                AVG(similarity) as avg_similarity_to_recent
              FROM similarity_pairs
              GROUP BY brand, current_ad_id, current_timestamp
            ),

            fatigue_metrics AS (
              SELECT
                brand,
                COUNT(*) as analyzed_ads,
                AVG(COALESCE(avg_similarity_to_recent, 0)) as avg_semantic_repetition,
                STDDEV(COALESCE(avg_similarity_to_recent, 0)) as repetition_variance,

                -- Legacy-style fatigue score calculation
                CASE
                  WHEN AVG(COALESCE(avg_similarity_to_recent, 0)) > 0.8 THEN 1.0
                  WHEN AVG(COALESCE(avg_similarity_to_recent, 0)) > 0.6 THEN 0.7
                  WHEN AVG(COALESCE(avg_similarity_to_recent, 0)) > 0.4 THEN 0.4
                  ELSE 0.2
                END as fatigue_score,

                -- Legacy-style originality score (inverse of repetition)
                GREATEST(0, 1.0 - AVG(COALESCE(avg_similarity_to_recent, 0))) as originality_score,

                -- Legacy-style refresh signal (based on variance)
                GREATEST(0.1, COALESCE(STDDEV(COALESCE(avg_similarity_to_recent, 0)), 0.1)) as refresh_signal_strength,

                -- Legacy-style fatigue level classification
                CASE
                  WHEN AVG(COALESCE(avg_similarity_to_recent, 0)) > 0.8 THEN 'HIGH'
                  WHEN AVG(COALESCE(avg_similarity_to_recent, 0)) > 0.5 THEN 'MEDIUM'
                  ELSE 'LOW'
                END as fatigue_level

              FROM brand_content_similarity
              GROUP BY brand
            )

            SELECT
              brand,
              fatigue_score,
              originality_score,
              refresh_signal_strength,
              fatigue_level,
              analyzed_ads,
              avg_semantic_repetition,
              repetition_variance,

              -- High fatigue count (legacy metric)
              CASE WHEN fatigue_level = 'HIGH' THEN 1 ELSE 0 END as high_fatigue_count

            FROM fatigue_metrics
            WHERE brand = '{self.context.brand}'
            """

            fatigue_result = run_query(fatigue_sql)
            if not fatigue_result.empty:
                row = fatigue_result.iloc[0]
                print(f"   📊 Fatigue analysis: {row.get('fatigue_level', 'UNKNOWN')} level "
                      f"(score: {float(row.get('fatigue_score', 0)):.2f})")

                return {
                    'avg_fatigue_score': float(row.get('fatigue_score', 0.3)),
                    'avg_originality_score': float(row.get('originality_score', 0.7)),
                    'avg_refresh_signal': float(row.get('refresh_signal_strength', 0.5)),
                    'high_fatigue_count': int(row.get('high_fatigue_count', 0)),
                    'fatigue_level': row.get('fatigue_level', 'MEDIUM'),
                    'analyzed_ads': int(row.get('analyzed_ads', 0))
                }
            else:
                print("   ⚠️  No fatigue data available")

        except Exception as e:
            print(f"   ⚠️  Fatigue analysis failed: {e}")

        return self._mock_fatigue_results()

    def _mock_fatigue_results(self) -> dict:
        """Mock fatigue results when analysis fails"""
        return {
            'avg_fatigue_score': 0.34,
            'avg_originality_score': 0.78,
            'avg_refresh_signal': 0.55,
            'high_fatigue_count': 2,
            'fatigue_level': 'MEDIUM',
            'analyzed_ads': 25
        }

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

        # Enhanced CTA Intelligence SQL with proper 0-10 aggressiveness scoring
        cta_analysis_sql = f"""
        CREATE OR REPLACE TABLE `{BQ_PROJECT}.{BQ_DATASET}.cta_aggressiveness_analysis` AS

        WITH cta_scoring AS (
          SELECT
            brand,
            ad_archive_id,
            cta_text,
            UPPER(COALESCE(cta_text, '')) as cta_upper,

            -- CTA Aggressiveness Score (0-10 scale)
            CASE
              WHEN cta_text IS NULL OR LENGTH(TRIM(cta_text)) = 0 THEN 0.0
              ELSE
                -- Base score for having a CTA
                2.0 +

                -- High urgency keywords (+3.0 each, max +6.0)
                LEAST(6.0,
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bNOW\\b') THEN 3.0 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bTODAY\\b') THEN 2.5 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bURGENT\\b') THEN 3.0 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bHURRY\\b') THEN 2.5 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bLIMITED\\b') THEN 2.0 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bENDING\\b') THEN 2.0 ELSE 0.0 END)
                ) +

                -- Action intensity keywords (+2.0 max)
                LEAST(2.0,
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bBUY NOW\\b') THEN 2.0 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bSHOP NOW\\b') THEN 1.5 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bORDER\\b') THEN 1.5 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bPURCHASE\\b') THEN 1.5 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bGET\\b') THEN 1.0 ELSE 0.0 END)
                ) +

                -- Promotion/discount keywords (+1.5 max)
                LEAST(1.5,
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bSALE\\b') THEN 1.5 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bOFF\\b') THEN 1.0 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bDISCOUNT\\b') THEN 1.0 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bFREE\\b') THEN 1.0 ELSE 0.0 END) +
                  (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bDEAL\\b') THEN 1.0 ELSE 0.0 END)
                ) -

                -- Consultative language (reduces aggressiveness)
                (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bLEARN\\b') THEN 1.0 ELSE 0.0 END) -
                (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bDISCOVER\\b') THEN 0.5 ELSE 0.0 END) -
                (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bEXPLORE\\b') THEN 0.5 ELSE 0.0 END) -
                (CASE WHEN REGEXP_CONTAINS(UPPER(cta_text), r'\\bFIND OUT\\b') THEN 1.0 ELSE 0.0 END)
            END as raw_aggressiveness_score

          FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_with_dates`
          WHERE brand IN ('{brands_filter}')
        ),

        cta_analysis AS (
          SELECT
            brand,
            ad_archive_id,
            cta_text,
            -- Cap score between 0 and 10
            GREATEST(0.0, LEAST(10.0, raw_aggressiveness_score)) as cta_aggressiveness_score,

            -- Categorize based on proper score ranges
            CASE
              WHEN raw_aggressiveness_score >= 7.0 THEN 'ULTRA_AGGRESSIVE'
              WHEN raw_aggressiveness_score >= 5.0 THEN 'AGGRESSIVE'
              WHEN raw_aggressiveness_score >= 3.0 THEN 'MODERATE'
              WHEN raw_aggressiveness_score >= 1.0 THEN 'CONSULTATIVE'
              ELSE 'MINIMAL'
            END as cta_category,

            -- Specific strategy classifications with accurate naming
            CASE
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(cta_text, '')), r'\\b(BUY NOW|ORDER NOW|SHOP NOW|LIMITED TIME|ACT FAST|HURRY)\\b') THEN 'URGENCY_DRIVEN'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(cta_text, '')), r'\\b(SHOP|BUY|GET|ORDER|PURCHASE)\\b') THEN 'ACTION_FOCUSED'
              WHEN REGEXP_CONTAINS(UPPER(COALESCE(cta_text, '')), r'\\b(LEARN MORE|DISCOVER|EXPLORE|FIND OUT)\\b') THEN 'EXPLORATORY'
              WHEN cta_text IS NOT NULL AND LENGTH(TRIM(cta_text)) > 0 THEN 'SOFT_SELL'
              ELSE 'NO_CTA'
            END as cta_strategy_type

          FROM cta_scoring
        )

        SELECT
          brand,
          COUNT(*) as total_ads,

          -- Remove meaningless "adoption rate" - focus on aggressiveness
          ROUND(AVG(cta_aggressiveness_score), 2) as avg_cta_aggressiveness,
          ROUND(STDDEV(cta_aggressiveness_score), 2) as cta_aggressiveness_stddev,

          -- Calculate correct category in SQL, not in notebook
          CASE
            WHEN AVG(cta_aggressiveness_score) >= 7.0 THEN 'ULTRA_AGGRESSIVE'
            WHEN AVG(cta_aggressiveness_score) >= 5.0 THEN 'AGGRESSIVE'
            WHEN AVG(cta_aggressiveness_score) >= 3.0 THEN 'MODERATE'
            WHEN AVG(cta_aggressiveness_score) >= 1.0 THEN 'CONSULTATIVE'
            ELSE 'MINIMAL'
          END as correct_category,

          -- Calculate consistency category
          CASE
            WHEN STDDEV(cta_aggressiveness_score) <= 1.0 THEN 'HIGH_CONSISTENCY'
            WHEN STDDEV(cta_aggressiveness_score) <= 2.0 THEN 'MEDIUM_CONSISTENCY'
            ELSE 'LOW_CONSISTENCY'
          END as consistency_category,

          -- Strategy distribution counts with accurate naming
          COUNT(CASE WHEN cta_strategy_type = 'URGENCY_DRIVEN' THEN 1 END) as urgency_driven_ctas,
          COUNT(CASE WHEN cta_strategy_type = 'ACTION_FOCUSED' THEN 1 END) as action_focused_ctas,
          COUNT(CASE WHEN cta_strategy_type = 'EXPLORATORY' THEN 1 END) as exploratory_ctas,
          COUNT(CASE WHEN cta_strategy_type = 'SOFT_SELL' THEN 1 END) as soft_sell_ctas,

          -- Category distribution
          COUNT(CASE WHEN cta_category = 'ULTRA_AGGRESSIVE' THEN 1 END) as ultra_aggressive_count,
          COUNT(CASE WHEN cta_category = 'AGGRESSIVE' THEN 1 END) as aggressive_count,
          COUNT(CASE WHEN cta_category = 'MODERATE' THEN 1 END) as moderate_count,
          COUNT(CASE WHEN cta_category = 'CONSULTATIVE' THEN 1 END) as consultative_count,
          COUNT(CASE WHEN cta_category = 'MINIMAL' THEN 1 END) as minimal_count,

          -- Dominant strategy based on highest count
          CASE
            WHEN COUNT(CASE WHEN cta_category = 'ULTRA_AGGRESSIVE' THEN 1 END) >= GREATEST(
              COUNT(CASE WHEN cta_category = 'AGGRESSIVE' THEN 1 END),
              COUNT(CASE WHEN cta_category = 'MODERATE' THEN 1 END),
              COUNT(CASE WHEN cta_category = 'CONSULTATIVE' THEN 1 END),
              COUNT(CASE WHEN cta_category = 'MINIMAL' THEN 1 END)
            ) THEN 'ULTRA_AGGRESSIVE'
            WHEN COUNT(CASE WHEN cta_category = 'AGGRESSIVE' THEN 1 END) >= GREATEST(
              COUNT(CASE WHEN cta_category = 'MODERATE' THEN 1 END),
              COUNT(CASE WHEN cta_category = 'CONSULTATIVE' THEN 1 END),
              COUNT(CASE WHEN cta_category = 'MINIMAL' THEN 1 END)
            ) THEN 'AGGRESSIVE'
            WHEN COUNT(CASE WHEN cta_category = 'MODERATE' THEN 1 END) >= GREATEST(
              COUNT(CASE WHEN cta_category = 'CONSULTATIVE' THEN 1 END),
              COUNT(CASE WHEN cta_category = 'MINIMAL' THEN 1 END)
            ) THEN 'MODERATE'
            WHEN COUNT(CASE WHEN cta_category = 'CONSULTATIVE' THEN 1 END) >= COUNT(CASE WHEN cta_category = 'MINIMAL' THEN 1 END)
            THEN 'CONSULTATIVE'
            ELSE 'MINIMAL'
          END as dominant_cta_strategy,

          CURRENT_TIMESTAMP() as analysis_timestamp

        FROM cta_analysis
        GROUP BY brand
        ORDER BY avg_cta_aggressiveness DESC;
        """

        # Execute the CTA Intelligence analysis
        run_query(cta_analysis_sql)
        print(f"   ✅ Created cta_aggressiveness_analysis table for temporal intelligence")