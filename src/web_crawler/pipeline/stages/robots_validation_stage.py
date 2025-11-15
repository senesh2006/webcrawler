"""
Robots.txt Validation Stage - Respects website crawling rules.
Checks robots.txt before crawling and extracts crawl-delay.
"""

import threading
import logging
from typing import Optional, Dict
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
import time
from datetime import datetime, timedelta

from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class RobotsConfig:
    """Configuration for robots.txt validation stage."""
    user_agent: str = "MyWebCrawler/1.0"
    respect_robots_txt: bool = True
    cache_ttl_hours: int = 24  # Re-fetch robots.txt after this time
    default_on_error: str = "allow"  # 'allow' or 'disallow' when fetch fails
    timeout_seconds: int = 10
    max_retries: int = 2


class RobotsTxtCache:
    """
    Thread-safe cache for robots.txt parsers.
    
    Caches RobotFileParser objects per domain to avoid re-fetching
    robots.txt for every URL from the same domain.
    """
    
    def __init__(self, ttl_hours: int = 24, timeout_seconds: int = 10):
        self.cache: Dict[str, dict] = {}
        self.ttl = timedelta(hours=ttl_hours)
        self.timeout = timeout_seconds
        self.lock = threading.RLock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_parser(self, domain: str, user_agent: str, 
                   default_on_error: str = "allow") -> tuple:
        """
        Get RobotFileParser for domain.
        
        Returns:
            tuple: (RobotFileParser, crawl_delay)
        """
        with self.lock:
            # Check if we have cached parser
            if domain in self.cache:
                cached = self.cache[domain]
                
                # Check if cache is still valid
                if datetime.now() - cached['timestamp'] < self.ttl:
                    self.logger.debug(f"Using cached robots.txt for {domain}")
                    return cached['parser'], cached['crawl_delay']
                else:
                    self.logger.debug(f"Cache expired for {domain}, re-fetching")
            
            # Fetch and parse robots.txt
            parser, crawl_delay = self._fetch_robots_txt(
                domain, user_agent, default_on_error
            )
            
            # Cache the result
            self.cache[domain] = {
                'parser': parser,
                'crawl_delay': crawl_delay,
                'timestamp': datetime.now()
            }
            
            return parser, crawl_delay
    
    def _fetch_robots_txt(self, domain: str, user_agent: str,
                          default_on_error: str) -> tuple:
        """
        Fetch and parse robots.txt for domain.
        
        Returns:
            tuple: (RobotFileParser, crawl_delay)
        """
        robots_url = f"https://{domain}/robots.txt"
        
        self.logger.info(f"Fetching robots.txt from {robots_url}")
        
        parser = RobotFileParser()
        parser.set_url(robots_url)
        
        try:
            # Fetch robots.txt with timeout
            import socket
            original_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(self.timeout)
            
            try:
                parser.read()
                self.logger.info(f"Successfully fetched robots.txt for {domain}")
            finally:
                socket.setdefaulttimeout(original_timeout)
            
            # Extract crawl-delay if specified
            crawl_delay = None
            try:
                # Try to get crawl-delay for our user-agent
                request_rate = parser.crawl_delay(user_agent)
                if request_rate:
                    crawl_delay = float(request_rate)
                    self.logger.info(f"Crawl-delay for {domain}: {crawl_delay}s")
            except Exception as e:
                self.logger.debug(f"No crawl-delay found for {domain}: {e}")
            
            return parser, crawl_delay
            
        except Exception as e:
            self.logger.warning(f"Failed to fetch robots.txt for {domain}: {e}")
            
            # Create permissive or restrictive parser based on config
            if default_on_error == "allow":
                self.logger.info(f"Allowing all URLs for {domain} (fetch failed)")
                # Empty parser allows everything
                empty_parser = RobotFileParser()
                empty_parser.parse([])  # Empty rules = allow all
                return empty_parser, None
            else:
                self.logger.info(f"Disallowing all URLs for {domain} (fetch failed)")
                # Restrictive parser
                restrictive_parser = RobotFileParser()
                restrictive_parser.parse([
                    "User-agent: *",
                    "Disallow: /"
                ])
                return restrictive_parser, None
    
    def clear(self):
        """Clear the cache."""
        with self.lock:
            self.cache.clear()
    
    def get_stats(self) -> dict:
        """Get cache statistics."""
        with self.lock:
            return {
                'cached_domains': len(self.cache),
                'domains': list(self.cache.keys())
            }


class RobotsValidationStage(PipelineStage):
    """
    Stage 3: Robots.txt Validation.
    
    Responsibilities:
    - Fetch and parse robots.txt for each domain
    - Check if URL is allowed for our user-agent
    - Extract crawl-delay for rate limiting
    - Cache robots.txt to avoid re-fetching
    """
    
    def __init__(self, input_queue, output_queue, config: RobotsConfig,
                 num_workers: int = 2):
        super().__init__(
            name="RobotsValidation",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize robots.txt cache
        self.robots_cache = RobotsTxtCache(
            ttl_hours=config.cache_ttl_hours,
            timeout_seconds=config.timeout_seconds
        )
        
        # Statistics
        self.stats = {
            'allowed': 0,
            'disallowed': 0,
            'fetch_errors': 0,
            'bypassed': 0  # When respect_robots_txt is False
        }
        self.stats_lock = threading.Lock()
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove port if present
            if ':' in domain:
                domain = domain.split(':')[0]
            
            return domain
        except Exception as e:
            self.logger.error(f"Failed to extract domain from {url}: {e}")
            raise
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        """
        Check if URL is allowed by robots.txt.
        
        Returns:
            PipelineData with crawl_delay metadata if allowed, None if disallowed
        """
        try:
            url = data.url
            
            # If not respecting robots.txt, pass through
            if not self.config.respect_robots_txt:
                with self.stats_lock:
                    self.stats['bypassed'] += 1
                self.logger.debug(f"Bypassing robots.txt check for {url}")
                return data
            
            # Extract domain
            domain = self._extract_domain(url)
            
            # Get robots.txt parser for domain
            parser, crawl_delay = self.robots_cache.get_parser(
                domain,
                self.config.user_agent,
                self.config.default_on_error
            )
            
            # Check if URL is allowed
            is_allowed = parser.can_fetch(self.config.user_agent, url)
            
            if is_allowed:
                with self.stats_lock:
                    self.stats['allowed'] += 1
                
                # Add crawl-delay to metadata for rate limiting stage
                if crawl_delay is not None:
                    if not hasattr(data, 'metadata'):
                        data.metadata = {}
                    data.metadata['crawl_delay'] = crawl_delay
                    self.logger.debug(f"URL allowed with crawl-delay {crawl_delay}s: {url}")
                else:
                    self.logger.debug(f"URL allowed: {url}")
                
                return data
            else:
                with self.stats_lock:
                    self.stats['disallowed'] += 1
                
                self.logger.info(f"URL disallowed by robots.txt: {url}")
                return None  # Drop disallowed URL
        
        except Exception as e:
            with self.stats_lock:
                self.stats['fetch_errors'] += 1
            
            self.logger.error(f"Error checking robots.txt for {data.url}: {e}",
                            exc_info=True)
            data.errors.append(f"RobotsValidation error: {str(e)}")
            
            # On error, follow default_on_error setting
            if self.config.default_on_error == "allow":
                self.logger.warning(f"Allowing URL on error: {data.url}")
                return data
            else:
                self.logger.warning(f"Disallowing URL on error: {data.url}")
                return None
    
    def get_stats(self) -> dict:
        """Get robots.txt validation statistics."""
        base_stats = super().get_stats()
        
        with self.stats_lock:
            base_stats['robots_stats'] = self.stats.copy()
        
        # Add cache stats
        base_stats['robots_stats']['cache'] = self.robots_cache.get_stats()
        
        return base_stats
    
    def clear_cache(self):
        """Clear robots.txt cache (useful for testing)."""
        self.logger.info("Clearing robots.txt cache")
        self.robots_cache.clear()


# Example usage and testing
if __name__ == "__main__":
    from queue import Queue
    import time
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create queues
    input_q = Queue()
    output_q = Queue()
    
    # Configure stage
    config = RobotsConfig(
        user_agent="TestCrawler/1.0",
        respect_robots_txt=True,
        default_on_error="allow",
        cache_ttl_hours=1
    )
    
    # Create and start stage
    stage = RobotsValidationStage(input_q, output_q, config, num_workers=2)
    stage.start()
    
    # Test URLs from different domains
    test_urls = [
        # Wikipedia allows most crawling
        "https://en.wikipedia.org/wiki/Web_crawler",
        "https://en.wikipedia.org/wiki/Python_(programming_language)",
        
        # Example.com (typically allows all)
        "https://example.com/page1",
        "https://example.com/page2",
        
        # Test with disallowed path (hypothetical)
        "https://example.com/private/admin",
    ]
    
    print("Testing Robots.txt Validation Stage")
    print("=" * 60)
    
    # Submit URLs
    for url in test_urls:
        data = PipelineData(url=url, depth=0)
        input_q.put(data)
        print(f"Submitted: {url}")
    
    # Wait for processing
    print("\nProcessing...")
    time.sleep(5)
    
    # Check results
    print(f"\n{'='*60}")
    print("Results:")
    print('='*60)
    
    allowed_urls = []
    while not output_q.empty():
        result = output_q.get()
        allowed_urls.append(result.url)
        
        # Check if crawl-delay was set
        crawl_delay = result.metadata.get('crawl_delay') if hasattr(result, 'metadata') else None
        if crawl_delay:
            print(f"✓ ALLOWED (delay: {crawl_delay}s): {result.url}")
        else:
            print(f"✓ ALLOWED: {result.url}")
    
    print(f"\nSummary:")
    print(f"  Submitted: {len(test_urls)}")
    print(f"  Allowed: {len(allowed_urls)}")
    print(f"  Blocked: {len(test_urls) - len(allowed_urls)}")
    
    # Show statistics
    print(f"\n{'='*60}")
    print("Stage Statistics:")
    print('='*60)
    stats = stage.get_stats()
    print(f"  Allowed: {stats['robots_stats']['allowed']}")
    print(f"  Disallowed: {stats['robots_stats']['disallowed']}")
    print(f"  Errors: {stats['robots_stats']['fetch_errors']}")
    print(f"  Cached domains: {stats['robots_stats']['cache']['cached_domains']}")
    print(f"  Domains: {stats['robots_stats']['cache']['domains']}")
    
    # Stop stage
    stage.stop()
    print("\nStage stopped.")