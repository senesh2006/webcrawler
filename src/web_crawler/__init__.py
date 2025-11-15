"""
Web Crawler - A modular, pipeline-based web crawler built with Python.

Features:
- Pipeline architecture with 10 stages
- Concurrent processing with multi-threading
- Configurable via YAML
- Multiple storage backends (SQLite, filesystem)
- Respects robots.txt
- Rate limiting per domain
"""

__version__ = "1.0.0"
__author__ = "Your Name"

from .core.pipeline_crawler import PipelineCrawler, CrawlerConfig
from .config.crawler_config import ConfigLoader, validate_config
from .pipeline.pipeline_data import PipelineData

__all__ = [
    'PipelineCrawler',
    'CrawlerConfig',
    'ConfigLoader',
    'validate_config',
    'PipelineData',
]