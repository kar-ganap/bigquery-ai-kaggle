#!/usr/bin/env python3
"""
SARIMA Model Evaluation for Ad Intelligence Time Series Forecasting

Evaluates the appropriateness of SARIMA (Seasonal AutoRegressive Integrated Moving Average)
models for our advertising data compared to the current BigQuery AI.FORECAST approach.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class TimeSeriesCharacteristics:
    """Characteristics of our ad intelligence time series data"""
    # Data properties
    frequency: str = "weekly"  # Weekly aggregation
    history_length_weeks: int = 52  # Typical: 1 year of history
    forecast_horizon: int = 30  # Days ahead we forecast
    
    # Series characteristics  
    n_brands: int = 5  # Warby Parker + 4 competitors
    n_metrics: int = 3  # promotional_intensity, urgency_score, video_pct
    
    # Data quality
    missing_data_pct: float = 0.15  # ~15% missing weeks (ads not always active)
    irregularity: str = "high"  # Irregular ad campaigns
    
    # Patterns
    has_trend: bool = True
    has_seasonality: bool = True  # Holiday/seasonal campaigns
    has_multiple_seasonalities: bool = True  # Weekly, monthly, quarterly patterns
    has_external_factors: bool = True  # Competitor actions, market events
    
    # Volume
    avg_weekly_observations: int = 50  # Ads per brand per week
    data_sparsity: str = "moderate"  # Some weeks with no ads

class SARIMAEvaluation:
    """Evaluate SARIMA models for ad intelligence forecasting"""
    
    def __init__(self):
        self.data_chars = TimeSeriesCharacteristics()
        
    def evaluate_sarima_pros_cons(self) -> Dict[str, List[str]]:
        """Comprehensive evaluation of SARIMA for our use case"""
        
        pros = [
            # Statistical rigor
            "✅ **Strong statistical foundation**: Well-established theory with proven track record",
            "✅ **Interpretable parameters**: p, d, q, P, D, Q, s have clear meanings",
            "✅ **Handles seasonality well**: Explicit seasonal components (weekly, monthly patterns)",
            "✅ **Confidence intervals**: Proper statistical confidence bands",
            "✅ **Lower computational cost**: Runs locally, no API calls needed",
            "✅ **Deterministic results**: Same input = same output (reproducible)",
            "✅ **No quota limits**: Can run unlimited forecasts without cost",
            "✅ **Fine control**: Can tune each component (AR, I, MA) separately",
            "✅ **Diagnostic tools**: ACF, PACF plots for model validation",
            "✅ **Works with shorter series**: Can work with 50-100 observations"
        ]
        
        cons = [
            # Data challenges
            "❌ **Irregular time series**: Ad campaigns start/stop irregularly",
            "❌ **Multiple seasonalities**: SARIMA handles only one seasonal period well",
            "❌ **Missing data handling**: Requires imputation strategies",
            "❌ **Sparse data periods**: Some brands have weeks with no ads",
            "❌ **Assumes stationarity**: Requires differencing/transformations",
            
            # Modeling limitations
            "❌ **Linear relationships only**: Can't capture non-linear patterns",
            "❌ **No cross-series learning**: Each brand modeled independently",
            "❌ **Manual parameter selection**: Requires grid search or auto-ARIMA",
            "❌ **No automatic outlier handling**: Sensitive to campaign spikes",
            "❌ **Single univariate series**: Doesn't leverage multivariate relationships",
            
            # Competitive intelligence specific
            "❌ **No competitor interaction**: Can't model competitive responses",
            "❌ **No external events**: Can't incorporate market shocks/events",
            "❌ **Static model**: Doesn't adapt to regime changes automatically",
            "❌ **No semantic understanding**: Can't use ad content/messaging"
        ]
        
        return {"pros": pros, "cons": cons}
    
    def compare_with_bigquery_ai_forecast(self) -> Dict[str, Dict]:
        """Compare SARIMA with current BigQuery AI.FORECAST (TimesFM)"""
        
        comparison = {
            "SARIMA": {
                "approach": "Classical statistical time series",
                "training_data_needed": "50-100 observations minimum",
                "handles_irregularity": "Poor - requires regular intervals",
                "seasonality": "Single seasonal period",
                "multivariate": "No - univariate only",
                "computational_cost": "Low - runs locally",
                "interpretability": "High - clear parameters",
                "confidence_intervals": "Statistical - well-calibrated",
                "zero_shot": "No - needs historical data",
                "adapts_to_changes": "No - static once fitted",
                "external_factors": "Limited - via SARIMAX only",
                "implementation_complexity": "Medium - parameter tuning needed",
                "maintenance": "High - regular retraining needed"
            },
            "BigQuery AI.FORECAST (TimesFM 2.0)": {
                "approach": "Deep learning foundation model",
                "training_data_needed": "Works with any amount",
                "handles_irregularity": "Excellent - designed for it",
                "seasonality": "Multiple seasonalities automatically",
                "multivariate": "Yes - multiple series together",
                "computational_cost": "API calls (quota-based)",
                "interpretability": "Low - black box model",
                "confidence_intervals": "ML-based prediction intervals",
                "zero_shot": "Yes - pre-trained on 100B points",
                "adapts_to_changes": "Yes - handles regime shifts",
                "external_factors": "Implicit through patterns",
                "implementation_complexity": "Low - just SQL",
                "maintenance": "Low - Google maintains model"
            }
        }
        
        return comparison
    
    def generate_recommendation(self) -> Dict[str, str]:
        """Generate recommendation for our specific use case"""
        
        return {
            "primary_recommendation": "**Continue with BigQuery AI.FORECAST (TimesFM)**",
            
            "reasoning": """
            Our advertising data has several characteristics that make TimesFM superior:
            
            1. **Irregular campaigns**: Ads start/stop unpredictably, which SARIMA handles poorly
            2. **Multiple seasonalities**: Weekly, monthly, quarterly, and holiday patterns exist simultaneously
            3. **Sparse data**: Some brands have weeks with no ads - TimesFM handles this gracefully
            4. **Competitive dynamics**: Multiple brands interact - TimesFM can model this implicitly
            5. **Zero-shot capability**: New brands/products can be forecasted immediately
            6. **Maintenance-free**: No retraining or parameter tuning needed
            """,
            
            "hybrid_approach": """
            Consider a hybrid approach for robustness:
            
            1. **Primary**: BigQuery AI.FORECAST for main predictions
            2. **Validation**: SARIMA on well-behaved metrics (e.g., total weekly ad volume)
            3. **Ensemble**: Weighted average when both models agree
            4. **Fallback**: SARIMA when API quotas exhausted
            """,
            
            "when_to_use_sarima": """
            SARIMA could be valuable in these specific scenarios:
            
            1. **Regular metrics**: Total industry ad volume (less sparse)
            2. **Established brands**: Long history with consistent advertising
            3. **Offline analysis**: When API access is limited
            4. **Interpretability required**: Regulatory or audit requirements
            5. **Baseline comparison**: Benchmark for ML model performance
            """,
            
            "implementation_if_needed": """
            If implementing SARIMA as supplement:
            
            ```python
            from statsmodels.tsa.statespace.sarimax import SARIMAX
            from pmdarima import auto_arima  # For automatic parameter selection
            
            # For promotional_intensity (most regular metric)
            model = auto_arima(
                y=weekly_promo_intensity,
                seasonal=True, 
                m=4,  # Monthly seasonality (4 weeks)
                stepwise=True,
                suppress_warnings=True,
                max_p=3, max_q=3,  # Limit complexity
                max_P=2, max_Q=2,
                trace=False
            )
            ```
            """
        }
    
    def data_preprocessing_requirements(self) -> Dict[str, List[str]]:
        """Data preprocessing needed for SARIMA"""
        
        return {
            "required_steps": [
                "1. **Regular intervals**: Aggregate to fixed weekly periods",
                "2. **Missing data imputation**: Forward-fill or interpolate gaps",
                "3. **Outlier detection**: Cap extreme promotional spikes",
                "4. **Stationarity tests**: ADF test, KPSS test",
                "5. **Differencing**: Remove trend (usually d=1)",
                "6. **Seasonal differencing**: Remove seasonal pattern (D=1)",
                "7. **Log transformation**: Stabilize variance if needed",
                "8. **Train/test split**: Hold out last 4 weeks for validation"
            ],
            
            "challenges_for_our_data": [
                "- Irregular ad campaigns make regular intervals artificial",
                "- Imputation introduces bias during inactive periods",
                "- Promotional spikes are signal, not outliers",
                "- Multiple seasonalities require careful selection",
                "- Competitive interactions ignored in preprocessing"
            ]
        }
    
    def generate_sql_comparison(self) -> str:
        """Generate SQL showing both approaches side by side"""
        
        return """
        -- Comparison: BigQuery AI.FORECAST vs SARIMA preparation
        
        -- Current approach: BigQuery AI.FORECAST (TimesFM)
        WITH ai_forecast AS (
          SELECT
            brand,
            week_start,
            promotional_intensity,
            -- Direct forecasting with TimesFM
            ML.FORECAST(
              MODEL `project.dataset.promotional_intensity_model`,
              STRUCT(30 AS horizon, 0.95 AS confidence_level)
            ) OVER (
              PARTITION BY brand 
              ORDER BY week_start
            ) AS forecast
          FROM weekly_metrics
        )
        
        -- SARIMA data preparation (would need Python/R for actual modeling)
        , sarima_prep AS (
          SELECT
            brand,
            week_start,
            promotional_intensity,
            -- Fill missing weeks with 0 (required for SARIMA)
            COALESCE(promotional_intensity, 0) as promo_filled,
            -- Calculate differences for stationarity
            promotional_intensity - LAG(promotional_intensity, 1) 
              OVER (PARTITION BY brand ORDER BY week_start) as diff_1,
            -- Seasonal difference (4 weeks)
            promotional_intensity - LAG(promotional_intensity, 4) 
              OVER (PARTITION BY brand ORDER BY week_start) as seasonal_diff,
            -- Log transformation for variance stabilization
            SAFE.LN(promotional_intensity + 1) as log_promo
          FROM (
            -- Generate complete weekly series (fill gaps)
            SELECT 
              brand,
              week_start,
              MAX(promotional_intensity) as promotional_intensity
            FROM weekly_metrics
            GROUP BY brand, week_start
          )
        )
        
        SELECT 
          'TimesFM handles irregular data directly' as advantage_1,
          'SARIMA requires extensive preprocessing' as challenge_1,
          'TimesFM includes uncertainty automatically' as advantage_2,
          'SARIMA needs separate confidence interval calculation' as challenge_2
        """

def main():
    """Run SARIMA evaluation"""
    evaluator = SARIMAEvaluation()
    
    print("=" * 80)
    print("SARIMA MODEL EVALUATION FOR AD INTELLIGENCE FORECASTING")
    print("=" * 80)
    
    # Pros and Cons
    pros_cons = evaluator.evaluate_sarima_pros_cons()
    print("\n📊 SARIMA PROS:")
    for pro in pros_cons["pros"]:
        print(f"  {pro}")
    
    print("\n📊 SARIMA CONS:")
    for con in pros_cons["cons"]:
        print(f"  {con}")
    
    # Comparison
    print("\n" + "=" * 80)
    print("COMPARISON: SARIMA vs BigQuery AI.FORECAST")
    print("=" * 80)
    
    comparison = evaluator.compare_with_bigquery_ai_forecast()
    for model, attrs in comparison.items():
        print(f"\n📈 {model}:")
        for key, value in attrs.items():
            print(f"  {key:25s}: {value}")
    
    # Recommendation
    print("\n" + "=" * 80)
    print("RECOMMENDATION")
    print("=" * 80)
    
    rec = evaluator.generate_recommendation()
    for key, value in rec.items():
        print(f"\n{key.upper()}:")
        print(value)
    
    # Data requirements
    print("\n" + "=" * 80)
    print("DATA PREPROCESSING FOR SARIMA")
    print("=" * 80)
    
    prep = evaluator.data_preprocessing_requirements()
    print("\nRequired Steps:")
    for step in prep["required_steps"]:
        print(f"  {step}")
    
    print("\nChallenges for Our Data:")
    for challenge in prep["challenges_for_our_data"]:
        print(f"  {challenge}")

if __name__ == "__main__":
    main()