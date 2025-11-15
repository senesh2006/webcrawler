"""
Link Re-injection Stage - Feeds extracted links back into the pipeline.
This stage closes the crawling loop by creating new PipelineData objects
for discovered URLs and sending them back to Stage 1.
"""

import logging
import threading
from typing import Optional, Set
from dataclasses import dataclass
from queue import Queue

from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class LinkReinjectionConfig:
    """Configuration for link re-injection stage."""
    max_depth: int = 3  # Maximum crawl depth
    max_total_pages: int = 10000  # Stop after crawling this many pages
    same_domain_only: bool = False  # Only reinject same-domain links
    
    # Priority settings
    prioritize_shallow_pages: bool = True  # Crawl shallow pages first
    
    # Control
    auto_stop: bool = True  # Automatically stop when no more URLs
    stop_on_max_pages: bool = True  # Stop when max_total_pages reached


class LinkReinjectionStage(PipelineStage):
    """
    Stage 10: Link Re-injection.
    
    Responsibilities:
    - Take extracted links from processed pages
    - Create new PipelineData objects for each link
    - Increment depth for child pages
    - Feed links back to Stage 1 (URL Validation)
    - Track crawl progress and enforce limits
    - Provide termination conditions
    
    This stage CLOSES THE LOOP - it feeds discovered URLs back to
    the start of the pipeline, enabling recursive crawling.
    """
    
    def __init__(self, input_queue, output_queue, config: LinkReinjectionConfig,
                 reinjection_queue: Queue, num_workers: int = 2):
        """
        Initialize link re-injection stage.
        
        Args:
            input_queue: Queue to read processed pages from
            output_queue: Queue to write to (typically None for last stage)
            config: Configuration
            reinjection_queue: Queue to send new URLs to (Stage 1 input)
            num_workers: Number of worker threads
        """
        super().__init__(
            name="LinkReinjection",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.reinjection_queue = reinjection_queue
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Track progress
        self.urls_reinjected = 0
        self.urls_skipped = 0
        self.stats_lock = threading.Lock()
        
        # Track total pages processed (for max limit)
        self.total_pages_processed = 0
        
        # Termination flag
        self.should_stop_crawling = False
        
        # Statistics
        self.stats = {
            'pages_processed': 0,
            'links_found': 0,
            'links_reinjected': 0,
            'links_skipped_depth': 0,
            'links_skipped_domain': 0,
            'links_skipped_max_pages': 0,
        }
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        """
        Process page and reinject discovered links.
        
        Returns:
            None (this is typically the last stage)
        """
        try:
            with self.stats_lock:
                self.stats['pages_processed'] += 1
                self.total_pages_processed += 1
            
            # Check if we've hit max pages limit
            if self.config.stop_on_max_pages:
                if self.total_pages_processed >= self.config.max_total_pages:
                    if not self.should_stop_crawling:
                        self.logger.warning(
                            f"Reached max pages limit ({self.config.max_total_pages}). "
                            f"Stopping link re-injection."
                        )
                        self.should_stop_crawling = True
                    return None
            
            # Check if page has links
            if not hasattr(data, 'links') or not data.links:
                self.logger.debug(f"No links to reinject from {data.url}")
                return None
            
            links = data.links
            with self.stats_lock:
                self.stats['links_found'] += len(links)
            
            self.logger.info(f"Processing {len(links)} links from {data.url}")
            
            # Check depth limit
            child_depth = data.depth + 1
            if child_depth > self.config.max_depth:
                with self.stats_lock:
                    self.stats['links_skipped_depth'] += len(links)
                self.logger.debug(
                    f"Skipping {len(links)} links - depth {child_depth} exceeds max {self.config.max_depth}"
                )
                return None
            
            # Process each link
            links_reinjected = 0
            
            for link_url in links:
                # Check if should stop
                if self.should_stop_crawling:
                    break
                
                # Domain filtering (if enabled)
                if self.config.same_domain_only:
                    if not self._is_same_domain(link_url, data.url):
                        with self.stats_lock:
                            self.stats['links_skipped_domain'] += 1
                        continue
                
                # Create new PipelineData for discovered URL
                new_data = PipelineData(
                    url=link_url,
                    depth=child_depth,
                    parent_url=data.url
                )
                
                # Reinject into pipeline (Stage 1 queue)
                try:
                    self.reinjection_queue.put(new_data, block=False)
                    links_reinjected += 1
                    
                    with self.stats_lock:
                        self.stats['links_reinjected'] += 1
                        self.urls_reinjected += 1
                    
                except Exception as e:
                    self.logger.warning(f"Failed to reinject {link_url}: {e}")
            
            self.logger.info(
                f"Reinjected {links_reinjected}/{len(links)} links from {data.url} "
                f"at depth {child_depth}"
            )
            
            # Don't pass to output queue (this is the last stage)
            return None
            
        except Exception as e:
            self.logger.error(f"Error reinjecting links from {data.url}: {e}", 
                            exc_info=True)
            return None
    
    def _is_same_domain(self, url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain."""
        try:
            from urllib.parse import urlparse
            
            domain1 = urlparse(url1).netloc.lower()
            domain2 = urlparse(url2).netloc.lower()
            
            # Remove port if present
            if ':' in domain1:
                domain1 = domain1.split(':')[0]
            if ':' in domain2:
                domain2 = domain2.split(':')[0]
            
            return domain1 == domain2
        except Exception:
            return False
    
    def is_crawling_complete(self) -> bool:
        """
        Check if crawling is complete.
        
        Returns:
            True if crawling should stop
        """
        # Check if we've hit limits
        if self.should_stop_crawling:
            return True
        
        # Check if max pages reached
        if self.config.stop_on_max_pages:
            if self.total_pages_processed >= self.config.max_total_pages:
                return True
        
        return False
    
    def get_progress(self) -> dict:
        """
        Get crawl progress information.
        
        Returns:
            dict with progress metrics
        """
        with self.stats_lock:
            progress = {
                'pages_processed': self.total_pages_processed,
                'max_pages': self.config.max_total_pages,
                'progress_percentage': 0.0,
                'links_reinjected': self.stats['links_reinjected'],
                'is_complete': self.is_crawling_complete()
            }
            
            if self.config.max_total_pages > 0:
                progress['progress_percentage'] = (
                    self.total_pages_processed / self.config.max_total_pages * 100
                )
            
            return progress
    
    def stop_crawling(self):
        """Manually stop crawling (stops reinjecting links)."""
        self.logger.info("Manually stopping link re-injection")
        self.should_stop_crawling = True
    
    def get_stats(self) -> dict:
        """Get link re-injection statistics."""
        base_stats = super().get_stats()
        
        with self.stats_lock:
            base_stats['reinjection_stats'] = self.stats.copy()
            base_stats['reinjection_stats']['total_pages_processed'] = self.total_pages_processed
            base_stats['reinjection_stats']['crawling_stopped'] = self.should_stop_crawling
            
            # Calculate percentages
            if self.stats['links_found'] > 0:
                reinjection_rate = (
                    self.stats['links_reinjected'] / self.stats['links_found'] * 100
                )
                base_stats['reinjection_stats']['reinjection_rate'] = round(reinjection_rate, 2)
        
        return base_stats


# Example usage
if __name__ == "__main__":
    from queue import Queue
    import time
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Testing Link Re-injection Stage")
    print("=" * 60)
    
    # Create queues
    input_q = Queue()
    output_q = Queue()
    reinjection_q = Queue()  # This would be Stage 1's input queue
    
    # Configure stage
    config = LinkReinjectionConfig(
        max_depth=2,
        max_total_pages=100,
        same_domain_only=False
    )
    
    # Create and start stage
    stage = LinkReinjectionStage(
        input_q, output_q, config, 
        reinjection_queue=reinjection_q,
        num_workers=2
    )
    stage.start()
    
    # Simulate crawled pages with links
    test_pages = [
        {
            'url': 'https://example.com/',
            'depth': 0,
            'links': [
                'https://example.com/page1',
                'https://example.com/page2',
                'https://example.com/page3',
            ]
        },
        {
            'url': 'https://example.com/page1',
            'depth': 1,
            'links': [
                'https://example.com/page4',
                'https://example.com/page5',
            ]
        },
        {
            'url': 'https://example.com/page2',
            'depth': 1,
            'links': [
                'https://example.com/page6',
            ]
        },
        {
            'url': 'https://example.com/page3',
            'depth': 2,  # At max depth
            'links': [
                'https://example.com/page7',  # Should be skipped (depth 3)
            ]
        }
    ]
    
    print(f"Submitting {len(test_pages)} processed pages...")
    
    # Submit processed pages
    for page in test_pages:
        data = PipelineData(url=page['url'], depth=page['depth'])
        data.links = page['links']
        input_q.put(data)
        print(f"  → {page['url']} (depth {page['depth']}, {len(page['links'])} links)")
    
    # Wait for processing
    print("\nProcessing...")
    time.sleep(3)
    
    # Check reinjected URLs
    print(f"\n{'='*60}")
    print("Reinjected URLs (back to Stage 1):")
    print('='*60)
    
    reinjected_urls = []
    while not reinjection_q.empty():
        new_data = reinjection_q.get()
        reinjected_urls.append(new_data)
        print(f"  → {new_data.url} (depth {new_data.depth}, parent: {new_data.parent_url})")
    
    print(f"\nTotal reinjected: {len(reinjected_urls)}")
    
    # Show statistics
    print(f"\n{'='*60}")
    print("Stage Statistics:")
    print('='*60)
    stats = stage.get_stats()
    reinj_stats = stats['reinjection_stats']
    
    print(f"Pages processed: {reinj_stats['pages_processed']}")
    print(f"Links found: {reinj_stats['links_found']}")
    print(f"Links reinjected: {reinj_stats['links_reinjected']}")
    print(f"Links skipped (depth): {reinj_stats['links_skipped_depth']}")
    print(f"Links skipped (domain): {reinj_stats['links_skipped_domain']}")
    print(f"Reinjection rate: {reinj_stats.get('reinjection_rate', 0)}%")
    
    # Show progress
    print(f"\n{'='*60}")
    print("Crawl Progress:")
    print('='*60)
    progress = stage.get_progress()
    print(f"Pages crawled: {progress['pages_processed']}/{progress['max_pages']}")
    print(f"Progress: {progress['progress_percentage']:.1f}%")
    print(f"Is complete: {progress['is_complete']}")
    
    # Stop stage
    stage.stop()
    print("\nStage stopped.")
    
    print(f"\n{'='*60}")
    print("Summary:")
    print('='*60)
    print(f"This stage successfully reinjected {len(reinjected_urls)} discovered URLs")
    print(f"back to the pipeline for crawling. The pipeline continues until:")
    print(f"  1. Max depth reached ({config.max_depth})")
    print(f"  2. Max pages crawled ({config.max_total_pages})")
    print(f"  3. No more URLs to crawl")
    print(f"  4. Manual stop requested")