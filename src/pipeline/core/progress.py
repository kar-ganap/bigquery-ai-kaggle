"""
Progress tracking utilities for pipeline stages.
"""
import time
from typing import Dict


class ProgressTracker:
    """Enhanced progress tracking with ETA calculation"""
    
    def __init__(self, total_stages: int = 7):
        self.total_stages = total_stages
        self.current_stage = 0
        self.stage_starts = {}
        self.stage_durations = {}
        self.start_time = time.time()
        
        # Historical averages for ETA calculation (in seconds)
        self.expected_durations = {
            1: 90,   # Discovery
            2: 90,   # Curation  
            3: 120,  # Meta ad ranking
            4: 180,  # Ingestion
            5: 300,  # Embeddings
            6: 240,  # Analysis
            7: 60    # Output
        }
    
    def start_stage(self, stage_num: int, stage_name: str):
        """Mark the start of a stage"""
        self.current_stage = stage_num
        self.stage_starts[stage_num] = time.time()
        
        # Calculate progress and ETA
        progress_pct = max(0, (stage_num - 1) / self.total_stages * 100)
        elapsed = time.time() - self.start_time
        
        # Estimate remaining time
        if stage_num == 1:
            eta = sum(self.expected_durations.values())
        else:
            avg_stage_time = elapsed / (stage_num - 1)
            remaining_stages = self.total_stages - (stage_num - 1)
            eta = avg_stage_time * remaining_stages
        
        eta_mins = int(eta // 60)
        eta_secs = int(eta % 60)
        elapsed_mins = int(elapsed // 60)
        elapsed_secs = int(elapsed % 60)
        
        print(f"🔄 STAGE {stage_num}/{self.total_stages}: {stage_name.upper()}")
        print(f"   Progress: {progress_pct:.0f}% | Elapsed: {elapsed_mins}:{elapsed_secs:02d} | ETA: {eta_mins}:{eta_secs:02d} remaining")
        print("=" * 70)
    
    def end_stage(self, stage_num: int):
        """Mark the end of a stage"""
        if stage_num in self.stage_starts:
            duration = time.time() - self.stage_starts[stage_num]
            self.stage_durations[stage_num] = duration
            return duration
        return 0
    
    def get_timings(self) -> Dict[str, float]:
        """Get all stage timings"""
        return self.stage_durations.copy()