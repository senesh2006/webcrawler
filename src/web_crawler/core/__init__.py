"""
Core Module - High-level crawler orchestration.

This module contains the main crawler orchestrator that connects all
pipeline stages and provides a simple interface for starting and managing
web crawling operations.

Components:
-----------
- PipelineCrawler: Main crawler orchestrator that builds and manages the pipeline
- CrawlerConfig: Complete configuration for all pipeline stages

Usage:
------
from web_crawler.core import PipelineCrawler, CrawlerConfig
from web_crawler.config import ConfigLoader

# Load configuration
config = ConfigLoader.load_from_yaml('config/default.yaml')

# Create crawler
crawler = PipelineCrawler(config)

# Start crawling
crawler.start(['https://example.com'])

# Monitor progress
status = crawler.get_status()
print(status)

# Wait for completion
crawler.wait_for_completion()

# Stop crawler
crawler.stop()

# Get detailed statistics
stats = crawler.get_detailed_stats()
"""

from .pipeline_crawler import PipelineCrawler, CrawlerConfig

__all__ = [
    'PipelineCrawler',
    'CrawlerConfig',
]