"""
Pipeline Framework Module

This module contains the core pipeline infrastructure for the web crawler.
It provides the base classes and data structures used by all pipeline stages.

Components:
-----------
- PipelineManager: Orchestrates multiple pipeline stages
- PipelineStage: Abstract base class for all stages
- PipelineData: Data container that flows through the pipeline

Usage:
------
from web_crawler.pipeline import PipelineStage, PipelineData

class MyCustomStage(PipelineStage):
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        # Your processing logic
        return data
"""

from .base_pipeline import PipelineManager
from .stage import PipelineStage
from .pipeline_data import PipelineData

__all__ = [
    'PipelineManager',
    'PipelineStage',
    'PipelineData',
]