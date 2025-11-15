"""
Pipeline Crawler - Main orchestrator that connects all pipeline stages.
This is the high-level interface for running the web crawler.
"""

import logging
import threading
import time
from typing import List, Optional
from queue import Queue
from dataclasses import dataclass

from ..pipeline.stages.url_validation_stage import URLValidationStage, URLValidationConfig
from ..pipeline.stages.duplicate_detection_stage import DuplicateDetectionStage, DuplicateDetectionConfig
from ..pipeline.stages.robots_validation_stage import RobotsValidationStage, RobotsConfig
from ..pipeline.stages.rate_limiting_stage import RateLimitingStage, RateLimitConfig
from ..pipeline.stages.fetch_stage import FetchStage, FetchConfig
from ..pipeline.stages.parse_stage import ParseStage, ParseConfig
from ..pipeline.stages.link_extraction_stage import LinkExtractionStage, LinkExtractionConfig
from ..pipeline.stages.content_extraction_stage import ContentExtractionStage, ContentExtractionConfig
from ..pipeline.stages.storage_stage import StorageStage, StorageConfig
from ..pipeline.stages.link_reinjection_stage import LinkReinjectionStage, LinkReinjectionConfig
from ..pipeline.pipeline_data import PipelineData


@dataclass
class CrawlerConfig:
    """Master configuration for the entire crawler pipeline."""
    # Stage configurations
    url_validation: URLValidationConfig
    duplicate_detection: DuplicateDetectionConfig
    robots: RobotsConfig
    rate_limiting: RateLimitConfig
    fetch: FetchConfig
    parse: ParseConfig
    link_extraction: LinkExtractionConfig
    content_extraction: ContentExtractionConfig
    storage: StorageConfig
    link_reinjection: LinkReinjectionConfig
    
    # Pipeline settings
    queue_size: int = 1000
    
    # Worker counts per stage
    url_validation_workers: int = 2
    duplicate_detection_workers: int = 2
    robots_validation_workers: int = 2
    rate_limiting_workers: int = 3
    fetch_workers: int = 10
    parse_workers: int = 3
    link_extraction_workers: int = 2
    content_extraction_workers: int = 2
    storage_workers: int = 2
    link_reinjection_workers: int = 2


class PipelineCrawler:
    """
    Main crawler orchestrator.
    
    Builds and manages the complete pipeline with all 10 stages.
    Provides high-level interface for starting/stopping crawling.
    """
    
    def __init__(self, config: CrawlerConfig):
        """
        Initialize pipeline crawler.
        
        Args:
            config: Complete crawler configuration
        """
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Pipeline components
        self.stages = []
        self.queues = []
        
        # State
        self.is_running = False
        self.start_time = None
        
        # Build pipeline
        self._build_pipeline()
        
        self.logger.info("Pipeline crawler initialized")
    
    def _build_pipeline(self):
        """Build the complete pipeline with all stages and queues."""
        self.logger.info("Building pipeline...")
        
        # Create queues between stages
        q1 = Queue(maxsize=self.config.queue_size)
        q2 = Queue(maxsize=self.config.queue_size)
        q3 = Queue(maxsize=self.config.queue_size)
        q4 = Queue(maxsize=self.config.queue_size)
        q5 = Queue(maxsize=self.config.queue_size)
        q6 = Queue(maxsize=self.config.queue_size)
        q7 = Queue(maxsize=self.config.queue_size)
        q8 = Queue(maxsize=self.config.queue_size)
        q9 = Queue(maxsize=self.config.queue_size)
        q10 = Queue(maxsize=self.config.queue_size)
        
        self.queues = [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10]
        
        # Create stages - mix of positional and keyword arguments
        stage1 = URLValidationStage(
            q1, q2,
            self.config.url_validation,
            num_workers=self.config.url_validation_workers
        )
        
        stage2 = DuplicateDetectionStage(
            q2, q3,
            self.config.duplicate_detection,
            num_workers=self.config.duplicate_detection_workers
        )
        
        stage3 = RobotsValidationStage(
            q3, q4,
            self.config.robots,
            num_workers=self.config.robots_validation_workers
        )
        
        stage4 = RateLimitingStage(
            q4, q5,
            self.config.rate_limiting,
            num_workers=self.config.rate_limiting_workers
        )
        
        stage5 = FetchStage(
            q5, q6,
            self.config.fetch,
            num_workers=self.config.fetch_workers
        )
        
        stage6 = ParseStage(
            q6, q7,
            self.config.parse,
            num_workers=self.config.parse_workers
        )
        
        stage7 = LinkExtractionStage(
            q7, q8,
            self.config.link_extraction,
            num_workers=self.config.link_extraction_workers
        )
        
        stage8 = ContentExtractionStage(
            q8, q9,
            self.config.content_extraction,
            num_workers=self.config.content_extraction_workers
        )
        
        stage9 = StorageStage(
            q9, q10,
            self.config.storage,
            num_workers=self.config.storage_workers
        )
        
        stage10 = LinkReinjectionStage(
            q10, None,
            self.config.link_reinjection,
            q1,  # Reinjection queue (back to stage 1)
            num_workers=self.config.link_reinjection_workers
        )
        
        self.stages = [
            stage1, stage2, stage3, stage4, stage5,
            stage6, stage7, stage8, stage9, stage10
        ]
        
        self.logger.info(f"Pipeline built with {len(self.stages)} stages")
    
    def start(self, seed_urls: List[str]):
        """
        Start crawling from seed URLs.
        
        Args:
            seed_urls: List of starting URLs
        """
        if self.is_running:
            self.logger.warning("Crawler already running")
            return
        
        self.logger.info(f"Starting crawler with {len(seed_urls)} seed URLs")
        self.start_time = time.time()
        
        # Start all stages
        for stage in self.stages:
            stage.start()
            self.logger.debug(f"Started stage: {stage.name}")
        
        # Inject seed URLs into first queue
        for url in seed_urls:
            data = PipelineData(url=url, depth=0)
            self.queues[0].put(data)
            self.logger.info(f"Injected seed URL: {url}")
        
        self.is_running = True
        self.logger.info("Crawler started successfully")
    
    def stop(self):
        """Stop the crawler gracefully."""
        if not self.is_running:
            self.logger.warning("Crawler not running")
            return
        
        self.logger.info("Stopping crawler...")
        
        # Stop link re-injection first (prevents new URLs)
        if hasattr(self.stages[-1], 'stop_crawling'):
            self.stages[-1].stop_crawling()
            self.logger.info("Stopped link re-injection")
        
        # Wait for queues to drain
        self.logger.info("Waiting for queues to drain...")
        timeout = 60
        start_wait = time.time()
        
        while time.time() - start_wait < timeout:
            if self._all_queues_empty():
                break
            time.sleep(1)
        
        # Stop all stages
        for stage in reversed(self.stages):
            stage.stop()
            self.logger.debug(f"Stopped stage: {stage.name}")
        
        self.is_running = False
        elapsed = time.time() - self.start_time
        self.logger.info(f"Crawler stopped. Total runtime: {elapsed:.2f} seconds")
    
    def _all_queues_empty(self) -> bool:
        """Check if all queues are empty."""
        return all(q.empty() for q in self.queues)
    
    def get_status(self) -> dict:
        """
        Get current crawler status.
        
        Returns:
            dict with status information
        """
        status = {
            'is_running': self.is_running,
            'runtime_seconds': time.time() - self.start_time if self.start_time else 0,
            'stages': [],
            'queues': [],
            'overall': {}
        }
        
        # Collect stage statistics
        total_processed = 0
        total_errors = 0
        
        for stage in self.stages:
            stage_stats = stage.get_stats()
            status['stages'].append({
                'name': stage.name,
                'processed': stage_stats.get('processed', 0),
                'errors': stage_stats.get('errors', 0),
                'is_running': stage_stats.get('is_running', False),
                'workers': stage_stats.get('workers', 0)
            })
            
            total_processed += stage_stats.get('processed', 0)
            total_errors += stage_stats.get('errors', 0)
        
        # Collect queue sizes
        for i, queue in enumerate(self.queues):
            status['queues'].append({
                'stage': i + 1,
                'size': queue.qsize(),
                'maxsize': queue.maxsize
            })
        
        # Overall statistics
        reinjection_stats = self.stages[-1].get_stats().get('reinjection_stats', {})
        status['overall'] = {
            'total_items_processed': total_processed,
            'total_errors': total_errors,
            'pages_crawled': reinjection_stats.get('pages_processed', 0),
            'crawl_complete': self._is_crawl_complete()
        }
        
        return status
    
    def _is_crawl_complete(self) -> bool:
        """Check if crawling is complete."""
        if not self.is_running:
            return True
        
        # Check if link re-injection stage says we're done
        reinjection_stage = self.stages[-1]
        if hasattr(reinjection_stage, 'is_crawling_complete'):
            if reinjection_stage.is_crawling_complete():
                return True
        
        # Check if all queues are empty (no more work)
        if self._all_queues_empty():
            # Give it a moment to ensure no new items
            time.sleep(2)
            if self._all_queues_empty():
                return True
        
        return False
    
    def wait_for_completion(self, check_interval: float = 5.0, timeout: Optional[float] = None):
        """
        Wait for crawling to complete.
        
        Args:
            check_interval: Seconds between completion checks
            timeout: Maximum seconds to wait (None = no timeout)
        """
        start_time = time.time()
        
        self.logger.info("Waiting for crawl completion...")
        
        while True:
            if self._is_crawl_complete():
                self.logger.info("Crawl completed")
                break
            
            if timeout and (time.time() - start_time) > timeout:
                self.logger.warning(f"Timeout ({timeout}s) reached, stopping crawler")
                self.stop()
                break
            
            time.sleep(check_interval)
    
    def get_detailed_stats(self) -> dict:
        """
        Get detailed statistics from all stages.
        
        Returns:
            dict with detailed stats per stage
        """
        stats = {
            'crawler': {
                'is_running': self.is_running,
                'runtime': time.time() - self.start_time if self.start_time else 0,
            },
            'stages': {}
        }
        
        for stage in self.stages:
            stats['stages'][stage.name] = stage.get_stats()
        
        return stats
    
    def print_status(self):
        """Print a formatted status summary."""
        status = self.get_status()
        
        print("\n" + "="*60)
        print("CRAWLER STATUS")
        print("="*60)
        print(f"Running: {status['is_running']}")
        print(f"Runtime: {status['runtime_seconds']:.2f} seconds")
        print(f"Pages Crawled: {status['overall']['pages_crawled']}")
        print(f"Total Processed: {status['overall']['total_items_processed']}")
        print(f"Total Errors: {status['overall']['total_errors']}")
        print(f"Complete: {status['overall']['crawl_complete']}")
        
        print("\nStage Status:")
        print("-"*60)
        for stage_stat in status['stages']:
            print(f"  {stage_stat['name']:25} | "
                  f"Processed: {stage_stat['processed']:6} | "
                  f"Errors: {stage_stat['errors']:4}")
        
        print("\nQueue Status:")
        print("-"*60)
        for queue_stat in status['queues']:
            fill_pct = (queue_stat['size'] / queue_stat['maxsize'] * 100) if queue_stat['maxsize'] > 0 else 0
            print(f"  Queue {queue_stat['stage']:2} → {queue_stat['stage']+1:2} | "
                  f"Size: {queue_stat['size']:4} / {queue_stat['maxsize']:4} "
                  f"({fill_pct:.1f}%)")
        
        print("="*60 + "\n")