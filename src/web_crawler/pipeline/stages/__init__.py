"""
Pipeline Stages Module

This module contains all 10 pipeline stages for the web crawler.
Each stage is responsible for a specific task in the crawling pipeline.

Pipeline Flow:
--------------
1. URLValidationStage       - Validates and normalizes URLs
2. DuplicateDetectionStage  - Prevents re-crawling same URLs
3. RobotsValidationStage    - Checks robots.txt compliance
4. RateLimitingStage        - Controls request rate per domain
5. FetchStage               - Downloads web pages (HTTP requests)
6. ParseStage               - Parses HTML with BeautifulSoup
7. LinkExtractionStage      - Extracts links from parsed HTML
8. ContentExtractionStage   - Extracts text content from pages
9. StorageStage             - Persists data to storage
10. LinkReinjectionStage    - Feeds discovered links back to pipeline

Usage:
------
from web_crawler.pipeline.stages import (
    URLValidationStage, 
    URLValidationConfig,
    FetchStage,
    FetchConfig
)

# Create stage with configuration
config = URLValidationConfig(allowed_domains=['example.com'])
stage = URLValidationStage(input_queue, output_queue, config)
stage.start()
"""

# Stage 1: URL Validation
from .url_validation_stage import URLValidationStage, URLValidationConfig

# Stage 2: Duplicate Detection
from .duplicate_detection_stage import (
    DuplicateDetectionStage, 
    DuplicateDetectionConfig,
    DetectionMethod
)

# Stage 3: Robots.txt Validation
from .robots_validation_stage import RobotsValidationStage, RobotsConfig

# Stage 4: Rate Limiting
from .rate_limiting_stage import RateLimitingStage, RateLimitConfig

# Stage 5: HTTP Fetch
from .fetch_stage import FetchStage, FetchConfig

# Stage 6: HTML Parsing
from .parse_stage import ParseStage, ParseConfig

# Stage 7: Link Extraction
from .link_extraction_stage import LinkExtractionStage, LinkExtractionConfig

# Stage 8: Content Extraction
from .content_extraction_stage import (
    ContentExtractionStage, 
    ContentExtractionConfig
)

# Stage 9: Storage
from .storage_stage import StorageStage, StorageConfig

# Stage 10: Link Re-injection
from .link_reinjection_stage import LinkReinjectionStage, LinkReinjectionConfig


__all__ = [
    # ========================================================================
    # STAGES
    # ========================================================================
    'URLValidationStage',
    'DuplicateDetectionStage',
    'RobotsValidationStage',
    'RateLimitingStage',
    'FetchStage',
    'ParseStage',
    'LinkExtractionStage',
    'ContentExtractionStage',
    'StorageStage',
    'LinkReinjectionStage',
    
    # ========================================================================
    # CONFIGURATIONS
    # ========================================================================
    'URLValidationConfig',
    'DuplicateDetectionConfig',
    'RobotsConfig',
    'RateLimitConfig',
    'FetchConfig',
    'ParseConfig',
    'LinkExtractionConfig',
    'ContentExtractionConfig',
    'StorageConfig',
    'LinkReinjectionConfig',
    
    # ========================================================================
    # ENUMS & UTILITIES
    # ========================================================================
    'DetectionMethod',
]