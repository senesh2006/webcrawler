"""
Rate Limiting Stage - Controls request rate to avoid overwhelming servers.
Implements per-domain rate limiting with configurable strategies.
"""

import threading
import logging
import time
from typing import Optional, Dict, List
from dataclasses import dataclass
from urllib.parse import urlparse
from collections import deque
from datetime import datetime

from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting stage."""
    default_delay_seconds: float = 1.0  # Default delay between requests
    max_concurrent_per_domain: int = 1  # Max concurrent requests per domain
    respect_crawl_delay: bool = True  # Use crawl-delay from robots.txt
    global_rate_limit: Optional[int] = None  # Max requests/second across all domains
    burst_size: int = 5  # Allow burst of requests before enforcing limit
    strategy: str = "fixed"  # 'fixed', 'token_bucket', or 'sliding_window'


class FixedDelayLimiter:
    """
    Simple fixed delay rate limiter.
    
    Enforces minimum time between requests to the same domain.
    """
    
    def __init__(self, default_delay: float = 1.0):
        self.default_delay = default_delay
        self.last_request_time: Dict[str, float] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def wait_if_needed(self, domain: str, crawl_delay: Optional[float] = None):
        """
        Wait if necessary before allowing request.
        
        Args:
            domain: Domain to rate limit
            crawl_delay: Optional crawl-delay from robots.txt
        """
        # Use crawl-delay if provided, otherwise default
        delay = crawl_delay if crawl_delay is not None else self.default_delay
        
        with self.lock:
            current_time = time.time()
            
            if domain in self.last_request_time:
                last_time = self.last_request_time[domain]
                elapsed = current_time - last_time
                
                if elapsed < delay:
                    sleep_time = delay - elapsed
                    self.logger.debug(f"Rate limiting {domain}: sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                    current_time = time.time()
            
            # Update last request time
            self.last_request_time[domain] = current_time
    
    def get_stats(self) -> dict:
        """Get limiter statistics."""
        with self.lock:
            return {
                'tracked_domains': len(self.last_request_time),
                'domains': list(self.last_request_time.keys())
            }


class TokenBucketLimiter:
    """
    Token bucket rate limiter.
    
    Each domain has a bucket of tokens. Requests consume tokens.
    Tokens refill at a fixed rate. Allows bursts.
    """
    
    def __init__(self, default_delay: float = 1.0, burst_size: int = 5):
        self.default_delay = default_delay
        self.burst_size = burst_size
        
        # Per-domain token buckets
        self.buckets: Dict[str, dict] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _get_or_create_bucket(self, domain: str, crawl_delay: Optional[float]) -> dict:
        """Get or create token bucket for domain."""
        if domain not in self.buckets:
            delay = crawl_delay if crawl_delay is not None else self.default_delay
            self.buckets[domain] = {
                'tokens': self.burst_size,
                'max_tokens': self.burst_size,
                'refill_rate': 1.0 / delay,  # tokens per second
                'last_refill': time.time()
            }
        return self.buckets[domain]
    
    def _refill_tokens(self, bucket: dict):
        """Refill tokens based on elapsed time."""
        current_time = time.time()
        elapsed = current_time - bucket['last_refill']
        
        # Calculate tokens to add
        tokens_to_add = elapsed * bucket['refill_rate']
        bucket['tokens'] = min(bucket['max_tokens'], bucket['tokens'] + tokens_to_add)
        bucket['last_refill'] = current_time
    
    def wait_if_needed(self, domain: str, crawl_delay: Optional[float] = None):
        """Wait until a token is available."""
        with self.lock:
            bucket = self._get_or_create_bucket(domain, crawl_delay)
            
            while True:
                self._refill_tokens(bucket)
                
                if bucket['tokens'] >= 1.0:
                    # Consume one token
                    bucket['tokens'] -= 1.0
                    self.logger.debug(f"Token consumed for {domain}, "
                                    f"remaining: {bucket['tokens']:.2f}")
                    break
                else:
                    # Wait for token to refill
                    wait_time = (1.0 - bucket['tokens']) / bucket['refill_rate']
                    self.logger.debug(f"No tokens for {domain}, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
    
    def get_stats(self) -> dict:
        """Get limiter statistics."""
        with self.lock:
            stats = {
                'tracked_domains': len(self.buckets),
                'domain_stats': {}
            }
            
            for domain, bucket in self.buckets.items():
                self._refill_tokens(bucket)
                stats['domain_stats'][domain] = {
                    'tokens': round(bucket['tokens'], 2),
                    'max_tokens': bucket['max_tokens'],
                    'refill_rate': round(bucket['refill_rate'], 3)
                }
            
            return stats


class SlidingWindowLimiter:
    """
    Sliding window rate limiter.
    
    Tracks timestamps of recent requests. Ensures no more than N requests
    in a sliding time window.
    """
    
    def __init__(self, default_delay: float = 1.0, window_size: int = 10):
        self.default_delay = default_delay
        self.window_size = window_size
        
        # Per-domain request timestamps
        self.windows: Dict[str, deque] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _get_or_create_window(self, domain: str) -> deque:
        """Get or create sliding window for domain."""
        if domain not in self.windows:
            self.windows[domain] = deque(maxlen=self.window_size)
        return self.windows[domain]
    
    def _clean_old_requests(self, window: deque, window_duration: float):
        """Remove timestamps older than window duration."""
        current_time = time.time()
        cutoff_time = current_time - window_duration
        
        while window and window[0] < cutoff_time:
            window.popleft()
    
    def wait_if_needed(self, domain: str, crawl_delay: Optional[float] = None):
        """Wait if request rate exceeds limit."""
        delay = crawl_delay if crawl_delay is not None else self.default_delay
        window_duration = delay * self.window_size
        
        with self.lock:
            window = self._get_or_create_window(domain)
            
            while True:
                current_time = time.time()
                self._clean_old_requests(window, window_duration)
                
                # Check if we can make a request
                if len(window) < self.window_size:
                    window.append(current_time)
                    self.logger.debug(f"Request allowed for {domain}, "
                                    f"window: {len(window)}/{self.window_size}")
                    break
                else:
                    # Window is full, wait until oldest request expires
                    oldest_time = window[0]
                    wait_time = (oldest_time + window_duration) - current_time
                    
                    if wait_time > 0:
                        self.logger.debug(f"Window full for {domain}, "
                                        f"waiting {wait_time:.2f}s")
                        time.sleep(wait_time)
    
    def get_stats(self) -> dict:
        """Get limiter statistics."""
        with self.lock:
            stats = {
                'tracked_domains': len(self.windows),
                'domain_stats': {}
            }
            
            for domain, window in self.windows.items():
                stats['domain_stats'][domain] = {
                    'recent_requests': len(window),
                    'window_size': self.window_size
                }
            
            return stats


class GlobalRateLimiter:
    """
    Global rate limiter across all domains.
    
    Ensures total request rate doesn't exceed limit.
    """
    
    def __init__(self, max_requests_per_second: int):
        self.max_rate = max_requests_per_second
        self.request_times: deque = deque()
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def wait_if_needed(self):
        """Wait if global rate limit would be exceeded."""
        with self.lock:
            current_time = time.time()
            cutoff_time = current_time - 1.0  # 1 second window
            
            # Remove old timestamps
            while self.request_times and self.request_times[0] < cutoff_time:
                self.request_times.popleft()
            
            # Check if we're at the limit
            if len(self.request_times) >= self.max_rate:
                # Wait until oldest request is 1 second old
                oldest_time = self.request_times[0]
                wait_time = (oldest_time + 1.0) - current_time
                
                if wait_time > 0:
                    self.logger.debug(f"Global rate limit hit, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
                    current_time = time.time()
            
            # Record this request
            self.request_times.append(current_time)
    
    def get_stats(self) -> dict:
        """Get global limiter statistics."""
        with self.lock:
            # Clean old requests
            current_time = time.time()
            cutoff_time = current_time - 1.0
            
            while self.request_times and self.request_times[0] < cutoff_time:
                self.request_times.popleft()
            
            return {
                'current_rate': len(self.request_times),
                'max_rate': self.max_rate
            }


class RateLimitingStage(PipelineStage):
    """
    Stage 4: Rate Limiting.
    
    Responsibilities:
    - Enforce per-domain rate limits
    - Respect crawl-delay from robots.txt
    - Prevent overwhelming servers
    - Support multiple rate limiting strategies
    """
    
    def __init__(self, input_queue, output_queue, config: RateLimitConfig,
                 num_workers: int = 3):
        super().__init__(
            name="RateLimiting",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize appropriate limiter based on strategy
        self.domain_limiter = self._create_limiter()
        
        # Global rate limiter (optional)
        self.global_limiter = None
        if config.global_rate_limit:
            self.global_limiter = GlobalRateLimiter(config.global_rate_limit)
            self.logger.info(f"Global rate limit: {config.global_rate_limit} req/s")
        
        # Statistics
        self.stats = {
            'requests_delayed': 0,
            'total_wait_time': 0.0
        }
        self.stats_lock = threading.Lock()
    
    def _create_limiter(self):
        """Create appropriate rate limiter based on strategy."""
        strategy = self.config.strategy.lower()
        
        if strategy == "fixed":
            self.logger.info("Using fixed delay rate limiting")
            return FixedDelayLimiter(self.config.default_delay_seconds)
        
        elif strategy == "token_bucket":
            self.logger.info("Using token bucket rate limiting")
            return TokenBucketLimiter(
                self.config.default_delay_seconds,
                self.config.burst_size
            )
        
        elif strategy == "sliding_window":
            self.logger.info("Using sliding window rate limiting")
            return SlidingWindowLimiter(
                self.config.default_delay_seconds,
                self.config.burst_size
            )
        
        else:
            raise ValueError(f"Unknown strategy: {strategy}. "
                           f"Use 'fixed', 'token_bucket', or 'sliding_window'")
    
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
        Apply rate limiting before passing URL to next stage.
        
        Returns:
            PipelineData after appropriate delay
        """
        try:
            url = data.url
            domain = self._extract_domain(url)
            
            # Get crawl-delay from metadata (set by robots.txt stage)
            crawl_delay = None
            if self.config.respect_crawl_delay and hasattr(data, 'metadata'):
                crawl_delay = data.metadata.get('crawl_delay')
            
            start_time = time.time()
            
            # Apply global rate limit first (if configured)
            if self.global_limiter:
                self.global_limiter.wait_if_needed()
            
            # Apply per-domain rate limit
            self.domain_limiter.wait_if_needed(domain, crawl_delay)
            
            wait_time = time.time() - start_time
            
            if wait_time > 0.01:  # Only count if we actually waited
                with self.stats_lock:
                    self.stats['requests_delayed'] += 1
                    self.stats['total_wait_time'] += wait_time
                
                self.logger.debug(f"Rate limited {domain}: waited {wait_time:.2f}s")
            
            return data
        
        except Exception as e:
            self.logger.error(f"Error in rate limiting for {data.url}: {e}",
                            exc_info=True)
            data.errors.append(f"RateLimiting error: {str(e)}")
            # Pass through on error - don't want to block pipeline
            return data
    
    def get_stats(self) -> dict:
        """Get rate limiting statistics."""
        base_stats = super().get_stats()
        
        with self.stats_lock:
            base_stats['rate_limit_stats'] = self.stats.copy()
            
            # Add average wait time
            if self.stats['requests_delayed'] > 0:
                avg_wait = self.stats['total_wait_time'] / self.stats['requests_delayed']
                base_stats['rate_limit_stats']['avg_wait_time'] = round(avg_wait, 3)
        
        # Add limiter-specific stats
        base_stats['rate_limit_stats']['limiter'] = self.domain_limiter.get_stats()
        
        if self.global_limiter:
            base_stats['rate_limit_stats']['global'] = self.global_limiter.get_stats()
        
        return base_stats


# Example usage
if __name__ == "__main__":
    from queue import Queue
    import time
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Testing Rate Limiting Stage")
    print("=" * 60)
    
    # Test different strategies
    strategies = ["fixed", "token_bucket", "sliding_window"]
    
    for strategy in strategies:
        print(f"\n{'='*60}")
        print(f"Testing {strategy.upper()} strategy")
        print('='*60)
        
        input_q = Queue()
        output_q = Queue()
        
        config = RateLimitConfig(
            default_delay_seconds=0.5,
            strategy=strategy,
            burst_size=3,
            global_rate_limit=10  # Max 10 requests/second globally
        )
        
        stage = RateLimitingStage(input_q, output_q, config, num_workers=2)
        stage.start()
        
        # Create test URLs (multiple requests to same domains)
        test_urls = [
            "https://example.com/page1",
            "https://example.com/page2",
            "https://example.com/page3",  # Should be rate limited
            "https://test.com/page1",
            "https://test.com/page2",
            "https://example.com/page4",  # Should be rate limited more
        ]
        
        start_time = time.time()
        
        # Submit URLs
        for url in test_urls:
            data = PipelineData(url=url, depth=0)
            # Simulate crawl-delay from robots.txt for some URLs
            if "example.com" in url:
                data.metadata = {'crawl_delay': 1.0}
            input_q.put(data)
        
        # Wait for processing
        time.sleep(len(test_urls) * 0.5 + 2)
        
        elapsed = time.time() - start_time
        
        # Check results
        processed = 0
        while not output_q.empty():
            output_q.get()
            processed += 1
        
        print(f"\nResults:")
        print(f"  URLs submitted: {len(test_urls)}")
        print(f"  URLs processed: {processed}")
        print(f"  Total time: {elapsed:.2f}s")
        print(f"  Average time per URL: {elapsed/len(test_urls):.2f}s")
        
        # Show statistics
        stats = stage.get_stats()
        print(f"\nStatistics:")
        print(f"  Requests delayed: {stats['rate_limit_stats']['requests_delayed']}")
        print(f"  Total wait time: {stats['rate_limit_stats']['total_wait_time']:.2f}s")
        if 'avg_wait_time' in stats['rate_limit_stats']:
            print(f"  Avg wait time: {stats['rate_limit_stats']['avg_wait_time']:.2f}s")
        
        stage.stop()
        time.sleep(1)