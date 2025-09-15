"""
Data models for pipeline stage results and outputs.
"""
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any


@dataclass
class IngestionResults:
    """Results from ad ingestion stage"""
    ads: List[Dict]
    brands: List[str]
    total_ads: int
    ingestion_time: float
    ads_table_id: Optional[str] = None
    
    def to_dataframe(self):
        """Convert to pandas DataFrame for BigQuery loading"""
        return pd.DataFrame(self.ads)


@dataclass
class EmbeddingResults:
    """Results from embedding generation"""
    table_id: str
    embedding_count: int
    dimension: int = 768
    generation_time: float = 0.0


@dataclass
class AnalysisResults:
    """Comprehensive strategic analysis results"""
    status: str = "success"
    message: str = ""
    current_state: Dict = field(default_factory=dict)
    influence: Dict = field(default_factory=dict)
    evolution: Dict = field(default_factory=dict)
    forecasts: Dict = field(default_factory=dict)
    # Level 3 enhancements
    velocity: Dict = field(default_factory=dict)
    patterns: Dict = field(default_factory=dict)
    rhythms: Dict = field(default_factory=dict)
    momentum: Dict = field(default_factory=dict)
    white_spaces: Dict = field(default_factory=dict)
    cascades: Dict = field(default_factory=dict)
    channel_intelligence: Dict = field(default_factory=dict)  # Channel performance & optimization
    metadata: Dict = field(default_factory=dict)


@dataclass
class IntelligenceOutput:
    """4-level progressive disclosure output"""
    level_1: Dict = field(default_factory=dict)  # Executive summary
    level_2: Dict = field(default_factory=dict)  # Strategic dashboard
    level_3: Dict = field(default_factory=dict)  # Actionable interventions
    level_4: Dict = field(default_factory=dict)  # SQL dashboards


@dataclass
class PipelineResults:
    """Complete pipeline execution results"""
    success: bool
    brand: str
    vertical: Optional[str]
    output: Optional[IntelligenceOutput]
    duration_seconds: float
    stage_timings: Dict[str, float]
    error: Optional[str] = None
    run_id: str = ""