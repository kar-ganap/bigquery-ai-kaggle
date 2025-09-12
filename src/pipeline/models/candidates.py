"""
Data models for competitor candidates and validated competitors.
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict
import pandas as pd


@dataclass
class CompetitorCandidate:
    """Raw competitor candidate from discovery stage"""
    company_name: str
    source_url: str
    source_title: str
    query_used: str
    raw_score: float
    found_in: str
    discovery_method: str


@dataclass
class ValidatedCompetitor:
    """AI-validated competitor from curation stage"""
    company_name: str
    is_competitor: bool
    tier: str
    market_overlap_pct: int
    customer_substitution_ease: str
    confidence: float
    reasoning: str
    evidence_sources: str
    quality_score: float
    # Meta ad activity fields (added in Stage 3)
    meta_tier: int = 0
    estimated_ad_count: str = "0"
    meta_classification: str = "Unknown"


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
class StrategicLabelResults:
    """Results from strategic labeling stage"""
    table_id: str
    labeled_ads: int
    generation_time: float


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


@dataclass
class IntelligenceOutput:
    """4-level progressive disclosure output"""
    level_1: Dict = field(default_factory=dict)  # Executive summary
    level_2: Dict = field(default_factory=dict)  # Strategic dashboard
    level_3: Dict = field(default_factory=dict)  # Actionable interventions
    level_4: Dict = field(default_factory=dict)  # SQL dashboards