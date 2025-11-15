"""
Duplicate Detection Stage - Prevents crawling the same URL multiple times.
Supports set-based, bloom filter, and database-based duplicate detection.
"""

import threading
import logging
from typing import Optional, Set
from dataclasses import dataclass
from enum import Enum


from ..stage import PipelineStage
from ..pipeline_data import PipelineData


class DetectionMethod(Enum):
    """Methods for duplicate detection."""
    SET = "set"  # In-memory set (exact, fast, memory intensive)
    BLOOM = "bloom"  # Bloom filter (probabilistic, memory efficient)
    DATABASE = "database"  # Persistent storage


@dataclass
class DuplicateDetectionConfig:
    """Configuration for duplicate detection stage."""
    method: str = "set"  # 'set', 'bloom', or 'database'
    
    # Bloom filter settings (if method='bloom')
    bloom_capacity: int = 1_000_000  # Expected number of URLs
    bloom_error_rate: float = 0.01  # 1% false positive rate
    
    # Database settings (if method='database')
    database_path: str = "data/seen_urls.db"
    
    # General settings
    case_sensitive: bool = False  # Treat URLs as case-insensitive


class SetBasedDetector:
    """
    Simple set-based duplicate detection.
    
    Pros:
    - Fast O(1) lookups
    - Exact matching (no false positives)
    - Simple implementation
    
    Cons:
    - Memory usage grows linearly with URLs
    - Not persistent (lost on restart)
    - Not suitable for millions of URLs
    """
    
    def __init__(self, case_sensitive: bool = False):
        self.seen_urls: Set[str] = set()
        self.case_sensitive = case_sensitive
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def is_seen(self, url: str) -> bool:
        """Check if URL has been seen before."""
        check_url = url if self.case_sensitive else url.lower()
        
        with self.lock:
            return check_url in self.seen_urls
    
    def mark_seen(self, url: str) -> bool:
        """
        Mark URL as seen.
        
        Returns:
            True if URL was newly added, False if already existed
        """
        check_url = url if self.case_sensitive else url.lower()
        
        with self.lock:
            if check_url in self.seen_urls:
                return False
            self.seen_urls.add(check_url)
            return True
    
    def count(self) -> int:
        """Get count of seen URLs."""
        with self.lock:
            return len(self.seen_urls)
    
    def clear(self):
        """Clear all seen URLs."""
        with self.lock:
            self.seen_urls.clear()


class BloomFilterDetector:
    """
    Bloom filter based duplicate detection.
    
    Pros:
    - Fixed memory size regardless of URL count
    - Can handle millions/billions of URLs
    - Very fast lookups
    
    Cons:
    - Probabilistic (small chance of false positives)
    - Cannot remove items once added
    - May skip some valid URLs if false positive occurs
    
    Note: Requires 'pybloom-live' or 'pybloomfiltermmap3' package
    """
    
    def __init__(self, capacity: int = 1_000_000, error_rate: float = 0.01,
                 case_sensitive: bool = False):
        self.case_sensitive = case_sensitive
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        try:
            from pybloom_live import BloomFilter
            self.bloom = BloomFilter(capacity=capacity, error_rate=error_rate)
            self.logger.info(f"Initialized Bloom filter: capacity={capacity}, "
                           f"error_rate={error_rate}")
        except ImportError:
            self.logger.error("pybloom-live not installed. Install with: "
                            "pip install pybloom-live")
            raise
        
        self._count = 0
    
    def is_seen(self, url: str) -> bool:
        """Check if URL has been seen before (may have false positives)."""
        check_url = url if self.case_sensitive else url.lower()
        
        with self.lock:
            return check_url in self.bloom
    
    def mark_seen(self, url: str) -> bool:
        """
        Mark URL as seen.
        
        Returns:
            True if URL was newly added, False if already existed
        """
        check_url = url if self.case_sensitive else url.lower()
        
        with self.lock:
            if check_url in self.bloom:
                return False
            self.bloom.add(check_url)
            self._count += 1
            return True
    
    def count(self) -> int:
        """Get approximate count of seen URLs."""
        with self.lock:
            return self._count
    
    def clear(self):
        """Clear bloom filter (requires recreation)."""
        with self.lock:
            # Bloom filters can't be cleared, need to recreate
            capacity = self.bloom.capacity
            error_rate = self.bloom.error_rate
            from pybloom_live import BloomFilter
            self.bloom = BloomFilter(capacity=capacity, error_rate=error_rate)
            self._count = 0


class DatabaseDetector:
    """
    Database-based duplicate detection.
    
    Pros:
    - Persistent across restarts
    - Can handle unlimited URLs
    - Can query and analyze seen URLs
    
    Cons:
    - Slower than in-memory options
    - Requires database setup
    - More complex
    """
    
    def __init__(self, database_path: str = "data/seen_urls.db",
                 case_sensitive: bool = False):
        self.database_path = database_path
        self.case_sensitive = case_sensitive
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite database."""
        import sqlite3
        import os
        
        # Create directory if needed
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)
        
        with self.lock:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            # Create table with index
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS seen_urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create index for fast lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_url ON seen_urls(url)
            """)
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Database initialized at {self.database_path}")
    
    def is_seen(self, url: str) -> bool:
        """Check if URL exists in database."""
        import sqlite3
        
        check_url = url if self.case_sensitive else url.lower()
        
        with self.lock:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT 1 FROM seen_urls WHERE url = ? LIMIT 1", 
                         (check_url,))
            result = cursor.fetchone() is not None
            
            conn.close()
            return result
    
    def mark_seen(self, url: str) -> bool:
        """
        Mark URL as seen in database.
        
        Returns:
            True if URL was newly added, False if already existed
        """
        import sqlite3
        
        check_url = url if self.case_sensitive else url.lower()
        
        with self.lock:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute("INSERT INTO seen_urls (url) VALUES (?)", 
                             (check_url,))
                conn.commit()
                conn.close()
                return True
            except sqlite3.IntegrityError:
                # URL already exists
                conn.close()
                return False
    
    def count(self) -> int:
        """Get count of seen URLs."""
        import sqlite3
        
        with self.lock:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM seen_urls")
            count = cursor.fetchone()[0]
            
            conn.close()
            return count
    
    def clear(self):
        """Clear all seen URLs from database."""
        import sqlite3
        
        with self.lock:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM seen_urls")
            conn.commit()
            conn.close()


class DuplicateDetectionStage(PipelineStage):
    """
    Stage 2: Duplicate Detection.
    
    Prevents crawling the same URL multiple times by tracking seen URLs.
    Supports multiple detection methods with different trade-offs.
    """
    
    def __init__(self, input_queue, output_queue, config: DuplicateDetectionConfig, num_workers: int = 2):
        super().__init__(
            name="DuplicateDetection",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize appropriate detector
        self.detector = self._create_detector()
        
        # Statistics
        self.stats = {
            'unique_urls': 0,
            'duplicate_urls': 0,
            'total_checked': 0
        }
        self.stats_lock = threading.Lock()
    
    def _create_detector(self):
        """Create appropriate duplicate detector based on config."""
        method = self.config.method.lower()
        
        if method == "set":
            self.logger.info("Using set-based duplicate detection")
            return SetBasedDetector(case_sensitive=self.config.case_sensitive)
        
        elif method == "bloom":
            self.logger.info("Using Bloom filter duplicate detection")
            return BloomFilterDetector(
                capacity=self.config.bloom_capacity,
                error_rate=self.config.bloom_error_rate,
                case_sensitive=self.config.case_sensitive
            )
        
        elif method == "database":
            self.logger.info("Using database-based duplicate detection")
            return DatabaseDetector(
                database_path=self.config.database_path,
                case_sensitive=self.config.case_sensitive
            )
        
        else:
            raise ValueError(f"Unknown detection method: {method}. "
                           f"Use 'set', 'bloom', or 'database'")
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        """
        Check if URL is duplicate.
        
        Returns:
            PipelineData if URL is new, None if duplicate
        """
        try:
            url = data.url
            
            with self.stats_lock:
                self.stats['total_checked'] += 1
            
            # Check if URL already seen
            if self.detector.is_seen(url):
                with self.stats_lock:
                    self.stats['duplicate_urls'] += 1
                
                self.logger.debug(f"Duplicate URL filtered: {url}")
                return None  # Drop duplicate
            
            # Mark as seen (thread-safe operation)
            was_new = self.detector.mark_seen(url)
            
            if was_new:
                with self.stats_lock:
                    self.stats['unique_urls'] += 1
                
                self.logger.debug(f"New URL accepted: {url}")
                return data  # Pass to next stage
            else:
                # Race condition: another thread marked it between check and mark
                with self.stats_lock:
                    self.stats['duplicate_urls'] += 1
                
                self.logger.debug(f"Duplicate URL (race condition): {url}")
                return None
        
        except Exception as e:
            self.logger.error(f"Error in duplicate detection for {data.url}: {e}",
                            exc_info=True)
            data.errors.append(f"DuplicateDetection error: {str(e)}")
            # On error, pass through (don't want to lose URLs due to detection failure)
            return data
    
    def get_stats(self) -> dict:
        """Get duplicate detection statistics."""
        base_stats = super().get_stats()
        
        with self.stats_lock:
            base_stats['duplicate_stats'] = self.stats.copy()
            base_stats['duplicate_stats']['seen_urls_count'] = self.detector.count()
            base_stats['duplicate_stats']['detection_method'] = self.config.method
        
        return base_stats
    
    def reset(self):
        """Clear all seen URLs (useful for testing)."""
        self.logger.warning("Resetting duplicate detector - clearing all seen URLs")
        self.detector.clear()
        
        with self.stats_lock:
            self.stats = {
                'unique_urls': 0,
                'duplicate_urls': 0,
                'total_checked': 0
            }


# Example usage
if __name__ == "__main__":
    from queue import Queue
    import time
    
    logging.basicConfig(level=logging.DEBUG)
    
    # Test all three methods
    methods = ["set", "bloom", "database"]
    
    for method in methods:
        print(f"\n{'='*60}")
        print(f"Testing {method.upper()} method")
        print('='*60)
        
        input_q = Queue()
        output_q = Queue()
        
        config = DuplicateDetectionConfig(
            method=method,
            bloom_capacity=100,  # Small for testing
            database_path=f"data/test_seen_{method}.db"
        )
        
        try:
            stage = DuplicateDetectionStage(input_q, output_q, config, num_workers=2)
            stage.start()
            
            # Test URLs (with intentional duplicates)
            test_urls = [
                "https://example.com/page1",
                "https://example.com/page2",
                "https://example.com/page1",  # Duplicate
                "https://example.com/page3",
                "https://example.com/PAGE1",  # Duplicate (case-insensitive)
                "https://example.com/page4",
                "https://example.com/page2",  # Duplicate
            ]
            
            for url in test_urls:
                data = PipelineData(url=url, depth=0)
                input_q.put(data)
            
            # Wait for processing
            time.sleep(2)
            
            # Check results
            unique_count = output_q.qsize()
            print(f"\nInput URLs: {len(test_urls)}")
            print(f"Unique URLs passed: {unique_count}")
            
            print(f"\nUnique URLs:")
            while not output_q.empty():
                result = output_q.get()
                print(f"  - {result.url}")
            
            print(f"\nStage statistics:")
            stats = stage.get_stats()
            print(f"  Unique: {stats['duplicate_stats']['unique_urls']}")
            print(f"  Duplicates: {stats['duplicate_stats']['duplicate_urls']}")
            print(f"  Seen URLs: {stats['duplicate_stats']['seen_urls_count']}")
            
            stage.stop()
            
        except ImportError as e:
            if method == "bloom":
                print(f"Skipping bloom test - pybloom-live not installed")
                print(f"Install with: pip install pybloom-live")
            else:
                raise