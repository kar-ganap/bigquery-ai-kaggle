"""
Base classes and interfaces for pipeline stages.
"""
from abc import ABC, abstractmethod
from typing import Any, TypeVar, Generic
from ..core.progress import ProgressTracker
import logging

# Generic type for stage inputs and outputs
T = TypeVar('T')
U = TypeVar('U')


class PipelineStage(ABC, Generic[T, U]):
    """
    Abstract base class for all pipeline stages.
    
    Provides common infrastructure for:
    - Logging
    - Error handling
    - Progress tracking
    - Input/output validation
    """
    
    def __init__(self, stage_name: str, stage_number: int, run_id: str):
        self.stage_name = stage_name
        self.stage_number = stage_number
        self.run_id = run_id
        self.logger = logging.getLogger(f"pipeline_{run_id}")
        
    @abstractmethod
    def execute(self, input_data: T) -> U:
        """
        Execute the stage logic.
        
        Args:
            input_data: The input data for this stage
            
        Returns:
            The output data from this stage
            
        Raises:
            StageError: When the stage encounters an error
        """
        pass
    
    def run(self, input_data: T, progress_tracker: ProgressTracker) -> U:
        """
        Execute the stage with error handling and progress tracking.
        
        Args:
            input_data: The input data for this stage
            progress_tracker: Progress tracker for reporting
            
        Returns:
            The output data from this stage
        """
        progress_tracker.start_stage(self.stage_number, self.stage_name)
        
        try:
            self.logger.info(f"Starting {self.stage_name}")
            result = self.execute(input_data)
            
            duration = progress_tracker.end_stage(self.stage_number)
            self.logger.info(f"Completed {self.stage_name} in {duration:.1f}s")
            
            return result
            
        except Exception as e:
            self.logger.error(f"{self.stage_name} failed: {str(e)}")
            progress_tracker.end_stage(self.stage_number)
            raise StageError(f"{self.stage_name} failed: {str(e)}") from e


class StageError(Exception):
    """Exception raised when a pipeline stage fails"""
    pass


class PipelineContext:
    """
    Shared context for pipeline execution.
    
    Contains:
    - Brand and vertical information
    - Run ID and configuration
    - Shared dependencies (BigQuery client, etc.)
    """
    
    def __init__(self, brand: str, vertical: str, run_id: str, verbose: bool = False):
        self.brand = brand
        self.vertical = vertical
        self.run_id = run_id
        self.verbose = verbose
        
        # Will be set by orchestrator
        self.discovery_engine = None
        self.name_validator = None
        self.meta_ads_fetcher = None
        self.temporal_engine = None
        self.whitespace_detector = None