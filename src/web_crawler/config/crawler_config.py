"""
Crawler Configuration Management - Centralized configuration loading and validation.
Supports loading from YAML files with validation and defaults.
"""

import logging
import yaml
from typing import Dict, Any, Optional
from pathlib import Path
from dataclasses import asdict, dataclass

from ..pipeline.stages.url_validation_stage import URLValidationConfig
from ..pipeline.stages.duplicate_detection_stage import DuplicateDetectionConfig
from ..pipeline.stages.robots_validation_stage import RobotsConfig
from ..pipeline.stages.rate_limiting_stage import RateLimitConfig
# Note: FetchConfig is defined below in this file in the original structure, 
# or imported. Based on your file structure, it seems FetchConfig was imported 
# but for this update I will assume we are keeping the import structure or 
# if FetchConfig is defined elsewhere, you need to update it there.
# However, since I cannot see the definition of FetchConfig in the imports 
# of the file you sent (it imports it from ..pipeline.stages.fetch_stage),
# YOU MUST UPDATE THE CLASS DEFINITION IN fetch_stage.py OR HERE if you moved it.
#
# WAIT - In your provided file `crawler_config.py`, it imports FetchConfig:
# from ..pipeline.stages.fetch_stage import FetchConfig
#
# This means I cannot just "give you the file" without telling you that
# the actual class definition is in `src/web_crawler/pipeline/stages/fetch_stage.py`.
#
# HOWEVER, to make this cleaner and avoid circular imports if you change things,
# usually Config classes are better kept in this config file or a separate definitions file.
#
# For now, I will assume you want the code for `crawler_config.py` that LOADS these values.
# You MUST also update the dataclass definition in `fetch_stage.py` (as shown in my previous response).


# ... imports ...
from ..pipeline.stages.url_validation_stage import URLValidationConfig
from ..pipeline.stages.duplicate_detection_stage import DuplicateDetectionConfig
from ..pipeline.stages.robots_validation_stage import RobotsConfig
from ..pipeline.stages.rate_limiting_stage import RateLimitConfig
from ..pipeline.stages.fetch_stage import FetchConfig
from ..pipeline.stages.parse_stage import ParseConfig
from ..pipeline.stages.link_extraction_stage import LinkExtractionConfig
from ..pipeline.stages.content_extraction_stage import ContentExtractionConfig
from ..pipeline.stages.storage_stage import StorageConfig
from ..pipeline.stages.link_reinjection_stage import LinkReinjectionConfig
from ..core.pipeline_crawler import CrawlerConfig


class ConfigurationError(Exception):
    """Raised when configuration is invalid."""
    pass


class ConfigLoader:
    """Loads and validates crawler configuration from YAML files."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @staticmethod
    def load_from_yaml(config_path: str) -> CrawlerConfig:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            CrawlerConfig object
            
        Raises:
            ConfigurationError: If configuration is invalid
        """
        logger = logging.getLogger(__name__)
        
        # Check if file exists
        path = Path(config_path)
        if not path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
        
        # Load YAML
        try:
            with open(path, 'r') as f:
                config_dict = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {config_path}: {e}")
        except Exception as e:
            raise ConfigurationError(f"Failed to read {config_path}: {e}")
        
        if not config_dict:
            raise ConfigurationError(f"Empty configuration file: {config_path}")
        
        logger.info(f"Loaded configuration from {config_path}")
        
        # Parse configuration
        try:
            return ConfigLoader._parse_config(config_dict)
        except Exception as e:
            raise ConfigurationError(f"Failed to parse configuration: {e}")
    
    @staticmethod
    def _parse_config(config_dict: Dict[str, Any]) -> CrawlerConfig:
        """Parse configuration dictionary into CrawlerConfig object."""
        
        # Extract stage configurations
        stages = config_dict.get('stages', {})
        pipeline = config_dict.get('pipeline', {})
        
        # URL Validation
        url_val_cfg = stages.get('url_validation', {})
        url_validation = URLValidationConfig(
            allowed_domains=url_val_cfg.get('allowed_domains', []),
            blocked_domains=url_val_cfg.get('blocked_domains', []),
            max_depth=url_val_cfg.get('max_depth', 3),
            allowed_schemes=url_val_cfg.get('allowed_schemes', ['http', 'https']),
            blocked_extensions=url_val_cfg.get('blocked_extensions')
        )
        
        # Duplicate Detection
        dup_cfg = stages.get('duplicate_detection', {})
        duplicate_detection = DuplicateDetectionConfig(
            method=dup_cfg.get('method', 'set'),
            bloom_capacity=dup_cfg.get('bloom_capacity', 1_000_000),
            bloom_error_rate=dup_cfg.get('bloom_error_rate', 0.01),
            database_path=dup_cfg.get('database_path', 'data/seen_urls.db'),
            case_sensitive=dup_cfg.get('case_sensitive', False)
        )
        
        # Robots.txt Validation
        robots_cfg = stages.get('robots', {})
        robots = RobotsConfig(
            user_agent=robots_cfg.get('user_agent', 'MyWebCrawler/1.0'),
            respect_robots_txt=robots_cfg.get('respect_robots_txt', True),
            cache_ttl_hours=robots_cfg.get('cache_ttl_hours', 24),
            default_on_error=robots_cfg.get('default_on_error', 'allow'),
            timeout_seconds=robots_cfg.get('timeout_seconds', 10),
            max_retries=robots_cfg.get('max_retries', 2)
        )
        
        # Rate Limiting
        rate_cfg = stages.get('rate_limiting', {})
        rate_limiting = RateLimitConfig(
            default_delay_seconds=rate_cfg.get('default_delay_seconds', 1.0),
            max_concurrent_per_domain=rate_cfg.get('max_concurrent_per_domain', 1),
            respect_crawl_delay=rate_cfg.get('respect_crawl_delay', True),
            global_rate_limit=rate_cfg.get('global_rate_limit'),
            burst_size=rate_cfg.get('burst_size', 5),
            strategy=rate_cfg.get('strategy', 'fixed')
        )
        
        # HTTP Fetch (UPDATED FOR BROWSER SUPPORT)
        fetch_cfg = stages.get('fetch', {})
        fetch = FetchConfig(
            timeout_seconds=fetch_cfg.get('timeout_seconds', 30),
            max_retries=fetch_cfg.get('max_retries', 3),
            max_redirects=fetch_cfg.get('max_redirects', 5),
            verify_ssl=fetch_cfg.get('verify_ssl', True),
            user_agent=fetch_cfg.get('user_agent', 'MyWebCrawler/1.0'),
            max_content_size_mb=fetch_cfg.get('max_content_size_mb', 10),
            
            # --- NEW BROWSER CONFIGURATION FIELDS ---
            use_headless_browser=fetch_cfg.get('use_headless_browser', False),
            browser_type=fetch_cfg.get('browser_type', 'chromium'),
            wait_for_selector=fetch_cfg.get('wait_for_selector', None),
            page_load_delay=fetch_cfg.get('page_load_delay', 1.0),
            # ----------------------------------------

            accept_language=fetch_cfg.get('accept_language', 'en-US,en;q=0.9'),
            retry_backoff_factor=fetch_cfg.get('retry_backoff_factor', 2.0),
            pool_connections=fetch_cfg.get('pool_connections', 10),
            pool_maxsize=fetch_cfg.get('pool_maxsize', 20)
        )
        
        # HTML Parsing
        parse_cfg = stages.get('parse', {})
        parse = ParseConfig(
            parser=parse_cfg.get('parser', 'html.parser'),
            extract_metadata=parse_cfg.get('extract_metadata', True),
            extract_open_graph=parse_cfg.get('extract_open_graph', True),
            extract_twitter_card=parse_cfg.get('extract_twitter_card', True),
            extract_canonical=parse_cfg.get('extract_canonical', True),
            extract_language=parse_cfg.get('extract_language', True),
            handle_malformed_html=parse_cfg.get('handle_malformed_html', True),
            strip_javascript=parse_cfg.get('strip_javascript', True),
            strip_comments=parse_cfg.get('strip_comments', True),
            max_html_size_mb=parse_cfg.get('max_html_size_mb', 5)
        )
        
        # Link Extraction
        link_cfg = stages.get('link_extraction', {})
        link_extraction = LinkExtractionConfig(
            extract_anchor_text=link_cfg.get('extract_anchor_text', True),
            extract_images=link_cfg.get('extract_images', True),
            extract_stylesheets=link_cfg.get('extract_stylesheets', False),
            extract_scripts=link_cfg.get('extract_scripts', False),
            follow_iframes=link_cfg.get('follow_iframes', False),
            ignore_fragments=link_cfg.get('ignore_fragments', True),
            ignore_query_params=link_cfg.get('ignore_query_params', False),
            same_domain_only=link_cfg.get('same_domain_only', False),
            max_links_per_page=link_cfg.get('max_links_per_page', 1000)
        )
        
        # Content Extraction
        content_cfg = stages.get('content_extraction', {})
        content_extraction = ContentExtractionConfig(
            method=content_cfg.get('method', 'simple'),
            min_text_length=content_cfg.get('min_text_length', 100),
            remove_boilerplate=content_cfg.get('remove_boilerplate', True),
            extract_headings=content_cfg.get('extract_headings', True),
            extract_paragraphs=content_cfg.get('extract_paragraphs', True),
            extract_lists=content_cfg.get('extract_lists', True),
            extract_tables=content_cfg.get('extract_tables', False),
            normalize_whitespace=content_cfg.get('normalize_whitespace', True),
            remove_extra_newlines=content_cfg.get('remove_extra_newlines', True),
            min_paragraph_length=content_cfg.get('min_paragraph_length', 20),
            max_content_length=content_cfg.get('max_content_length', 1_000_000)
        )
        
        # Storage
        storage_cfg = stages.get('storage', {})
        storage = StorageConfig(
            storage_type=storage_cfg.get('storage_type', 'sqlite'),
            database_path=storage_cfg.get('database_path', 'data/crawler.db'),
            postgres_host=storage_cfg.get('postgres_host', 'localhost'),
            postgres_port=storage_cfg.get('postgres_port', 5432),
            postgres_database=storage_cfg.get('postgres_database', 'crawler'),
            postgres_user=storage_cfg.get('postgres_user', 'crawler'),
            postgres_password=storage_cfg.get('postgres_password', ''),
            output_directory=storage_cfg.get('output_directory', 'data/pages'),
            store_html=storage_cfg.get('store_html', True),
            store_text=storage_cfg.get('store_text', True),
            store_metadata=storage_cfg.get('store_metadata', True),
            store_links=storage_cfg.get('store_links', True),
            batch_size=storage_cfg.get('batch_size', 100),
            compress_html=storage_cfg.get('compress_html', True),
            on_conflict=storage_cfg.get('on_conflict', 'replace')
        )
        
        # Link Re-injection
        reinj_cfg = stages.get('link_reinjection', {})
        link_reinjection = LinkReinjectionConfig(
            max_depth=reinj_cfg.get('max_depth', 3),
            max_total_pages=reinj_cfg.get('max_total_pages', 10000),
            same_domain_only=reinj_cfg.get('same_domain_only', False),
            prioritize_shallow_pages=reinj_cfg.get('prioritize_shallow_pages', True),
            auto_stop=reinj_cfg.get('auto_stop', True),
            stop_on_max_pages=reinj_cfg.get('stop_on_max_pages', True)
        )
        
        # Create CrawlerConfig
        crawler_config = CrawlerConfig(
            url_validation=url_validation,
            duplicate_detection=duplicate_detection,
            robots=robots,
            rate_limiting=rate_limiting,
            fetch=fetch,
            parse=parse,
            link_extraction=link_extraction,
            content_extraction=content_extraction,
            storage=storage,
            link_reinjection=link_reinjection,
            queue_size=pipeline.get('queue_size', 1000),
            url_validation_workers=pipeline.get('url_validation_workers', 2),
            duplicate_detection_workers=pipeline.get('duplicate_detection_workers', 2),
            robots_validation_workers=pipeline.get('robots_validation_workers', 2),
            rate_limiting_workers=pipeline.get('rate_limiting_workers', 3),
            fetch_workers=pipeline.get('fetch_workers', 10),
            parse_workers=pipeline.get('parse_workers', 3),
            link_extraction_workers=pipeline.get('link_extraction_workers', 2),
            content_extraction_workers=pipeline.get('content_extraction_workers', 2),
            storage_workers=pipeline.get('storage_workers', 2),
            link_reinjection_workers=pipeline.get('link_reinjection_workers', 2)
        )
        
        return crawler_config
    
    @staticmethod
    def save_to_yaml(config: CrawlerConfig, output_path: str):
        """Save configuration to YAML file."""
        logger = logging.getLogger(__name__)
        
        config_dict = {
            'pipeline': {
                'queue_size': config.queue_size,
                'url_validation_workers': config.url_validation_workers,
                'duplicate_detection_workers': config.duplicate_detection_workers,
                'robots_validation_workers': config.robots_validation_workers,
                'rate_limiting_workers': config.rate_limiting_workers,
                'fetch_workers': config.fetch_workers,
                'parse_workers': config.parse_workers,
                'link_extraction_workers': config.link_extraction_workers,
                'content_extraction_workers': config.content_extraction_workers,
                'storage_workers': config.storage_workers,
                'link_reinjection_workers': config.link_reinjection_workers,
            },
            'stages': {
                'url_validation': asdict(config.url_validation),
                'duplicate_detection': asdict(config.duplicate_detection),
                'robots': asdict(config.robots),
                'rate_limiting': asdict(config.rate_limiting),
                'fetch': asdict(config.fetch),
                'parse': asdict(config.parse),
                'link_extraction': asdict(config.link_extraction),
                'content_extraction': asdict(config.content_extraction),
                'storage': asdict(config.storage),
                'link_reinjection': asdict(config.link_reinjection),
            }
        }
        
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(path, 'w') as f:
                yaml.dump(config_dict, f, default_flow_style=False, indent=2, sort_keys=False)
            logger.info(f"Configuration saved to {output_path}")
        except Exception as e:
            raise ConfigurationError(f"Failed to save configuration: {e}")
    
    @staticmethod
    def create_default_config() -> CrawlerConfig:
        """Create a default configuration."""
        return CrawlerConfig(
            url_validation=URLValidationConfig(),
            duplicate_detection=DuplicateDetectionConfig(),
            robots=RobotsConfig(),
            rate_limiting=RateLimitConfig(),
            fetch=FetchConfig(),
            parse=ParseConfig(),
            link_extraction=LinkExtractionConfig(),
            content_extraction=ContentExtractionConfig(),
            storage=StorageConfig(),
            link_reinjection=LinkReinjectionConfig(),
            queue_size=1000,
            url_validation_workers=2,
            duplicate_detection_workers=2,
            robots_validation_workers=2,
            rate_limiting_workers=3,
            fetch_workers=10,
            parse_workers=3,
            link_extraction_workers=2,
            content_extraction_workers=2,
            storage_workers=2,
            link_reinjection_workers=2
        )


def validate_config(config: CrawlerConfig) -> bool:
    """Validate crawler configuration."""
    logger = logging.getLogger(__name__)
    
    if config.fetch_workers < 1:
        raise ConfigurationError("fetch_workers must be at least 1")
    
    if config.queue_size < 10:
        raise ConfigurationError("queue_size must be at least 10")
    
    if config.url_validation.max_depth < 0:
        raise ConfigurationError("max_depth cannot be negative")
    
    if config.link_reinjection.max_depth < 0:
        raise ConfigurationError("link_reinjection max_depth cannot be negative")
    
    logger.info("Configuration validated successfully")
    return True