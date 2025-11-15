"""
Storage Stage - Persists crawled data to storage.
Supports multiple storage backends: SQLite, PostgreSQL, filesystem.
"""

import logging
import threading
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
import json
import os
from datetime import datetime
import sqlite3
from pathlib import Path

from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class StorageConfig:
    """Configuration for storage stage."""
    storage_type: str = "sqlite"  # 'sqlite', 'postgresql', 'filesystem', 'json'
    
    # SQLite settings
    database_path: str = "data/crawler.db"
    
    # PostgreSQL settings
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "crawler"
    postgres_user: str = "crawler"
    postgres_password: str = ""
    
    # Filesystem settings
    output_directory: str = "data/pages"
    
    # What to store
    store_html: bool = True
    store_text: bool = True
    store_metadata: bool = True
    store_links: bool = True
    
    # Performance
    batch_size: int = 100  # Batch insert for better performance
    compress_html: bool = True  # GZIP compression for HTML
    
    # Database options
    create_indexes: bool = True
    on_conflict: str = "replace"  # 'replace', 'ignore', or 'update'


class SQLiteStorage:
    """SQLite database storage backend."""
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Thread-local connections
        self.local = threading.local()
        
        # Initialize database
        self._init_database()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self.local, 'conn'):
            self.local.conn = sqlite3.connect(
                self.config.database_path,
                check_same_thread=False
            )
            self.local.conn.row_factory = sqlite3.Row
        return self.local.conn
    
    def _init_database(self):
        """Initialize database schema."""
        # Create directory if needed
        os.makedirs(os.path.dirname(self.config.database_path), exist_ok=True)
        
        conn = sqlite3.connect(self.config.database_path)
        cursor = conn.cursor()
        
        # Pages table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                final_url TEXT,
                title TEXT,
                content_type TEXT,
                status_code INTEGER,
                depth INTEGER,
                parent_url TEXT,
                raw_html TEXT,
                text_content TEXT,
                word_count INTEGER,
                char_count INTEGER,
                crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                metadata TEXT
            )
        """)
        
        # Links table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_url TEXT NOT NULL,
                target_url TEXT NOT NULL,
                anchor_text TEXT,
                FOREIGN KEY (source_url) REFERENCES pages(url)
            )
        """)
        
        # Images table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_url TEXT NOT NULL,
                image_url TEXT NOT NULL,
                alt_text TEXT,
                FOREIGN KEY (page_url) REFERENCES pages(url)
            )
        """)
        
        # Crawl statistics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawl_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pages_crawled INTEGER,
                links_found INTEGER,
                errors_count INTEGER
            )
        """)
        
        # Create indexes
        if self.config.create_indexes:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_url ON pages(url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_final_url ON pages(final_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawled_at ON pages(crawled_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_source ON links(source_url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_links_target ON links(target_url)")
        
        conn.commit()
        conn.close()
        
        self.logger.info(f"Database initialized at {self.config.database_path}")
    
    def store_page(self, data: PipelineData) -> bool:
        """Store page data in database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Prepare metadata
            metadata_json = None
            if self.config.store_metadata and hasattr(data, 'metadata'):
                metadata_json = json.dumps(data.metadata)
            
            # Prepare HTML
            html_content = None
            if self.config.store_html and data.raw_html:
                if self.config.compress_html:
                    import gzip
                    html_content = gzip.compress(data.raw_html.encode('utf-8'))
                else:
                    html_content = data.raw_html
            
            # Prepare text content
            text_content = data.text_content if self.config.store_text else None
            
            # Get word count from metadata
            word_count = 0
            char_count = 0
            if hasattr(data, 'metadata') and 'content' in data.metadata:
                word_count = data.metadata['content'].get('word_count', 0)
                char_count = data.metadata['content'].get('char_count', 0)
            
            # Insert or replace page
            if self.config.on_conflict == "replace":
                cursor.execute("""
                    INSERT OR REPLACE INTO pages 
                    (url, final_url, title, content_type, status_code, depth, 
                     parent_url, raw_html, text_content, word_count, char_count, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data.url,
                    getattr(data, 'final_url', None),
                    data.title,
                    getattr(data, 'content_type', None),
                    getattr(data, 'status_code', None),
                    data.depth,
                    data.parent_url,
                    html_content,
                    text_content,
                    word_count,
                    char_count,
                    metadata_json
                ))
            elif self.config.on_conflict == "ignore":
                cursor.execute("""
                    INSERT OR IGNORE INTO pages 
                    (url, final_url, title, content_type, status_code, depth, 
                     parent_url, raw_html, text_content, word_count, char_count, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    data.url,
                    getattr(data, 'final_url', None),
                    data.title,
                    getattr(data, 'content_type', None),
                    getattr(data, 'status_code', None),
                    data.depth,
                    data.parent_url,
                    html_content,
                    text_content,
                    word_count,
                    char_count,
                    metadata_json
                ))
            
            # Store links
            if self.config.store_links and hasattr(data, 'links') and data.links:
                # Get anchor texts from metadata
                anchor_texts = {}
                if hasattr(data, 'metadata') and 'anchor_texts' in data.metadata:
                    anchor_texts = data.metadata['anchor_texts']
                
                for link in data.links:
                    anchor_text = anchor_texts.get(link)
                    cursor.execute("""
                        INSERT INTO links (source_url, target_url, anchor_text)
                        VALUES (?, ?, ?)
                    """, (data.url, link, anchor_text))
            
            # Store images
            if hasattr(data, 'images') and data.images:
                for image_url in data.images:
                    cursor.execute("""
                        INSERT INTO images (page_url, image_url)
                        VALUES (?, ?)
                    """, (data.url, image_url))
            
            conn.commit()
            self.logger.debug(f"Stored page: {data.url}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing page {data.url}: {e}", exc_info=True)
            return False
    
    def close(self):
        """Close database connection."""
        if hasattr(self.local, 'conn'):
            self.local.conn.close()


class FilesystemStorage:
    """Filesystem storage backend."""
    
    def __init__(self, config: StorageConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Create output directory
        os.makedirs(self.config.output_directory, exist_ok=True)
    
    def store_page(self, data: PipelineData) -> bool:
        """Store page data to filesystem."""
        try:
            # Create domain-based directory structure
            from urllib.parse import urlparse
            parsed = urlparse(data.url)
            domain = parsed.netloc.replace(':', '_')
            
            domain_dir = os.path.join(self.config.output_directory, domain)
            os.makedirs(domain_dir, exist_ok=True)
            
            # Generate filename from URL
            path = parsed.path.strip('/').replace('/', '_')
            if not path:
                path = 'index'
            
            # Limit filename length
            if len(path) > 200:
                path = path[:200]
            
            base_path = os.path.join(domain_dir, path)
            
            # Store HTML
            if self.config.store_html and data.raw_html:
                html_file = f"{base_path}.html"
                if self.config.compress_html:
                    import gzip
                    with gzip.open(f"{html_file}.gz", 'wt', encoding='utf-8') as f:
                        f.write(data.raw_html)
                else:
                    with open(html_file, 'w', encoding='utf-8') as f:
                        f.write(data.raw_html)
            
            # Store text content
            if self.config.store_text and data.text_content:
                text_file = f"{base_path}.txt"
                with open(text_file, 'w', encoding='utf-8') as f:
                    f.write(data.text_content)
            
            # Store metadata
            if self.config.store_metadata:
                metadata = {
                    'url': data.url,
                    'final_url': getattr(data, 'final_url', None),
                    'title': data.title,
                    'status_code': getattr(data, 'status_code', None),
                    'depth': data.depth,
                    'parent_url': data.parent_url,
                    'crawled_at': datetime.now().isoformat(),
                }
                
                if hasattr(data, 'links'):
                    metadata['links'] = data.links
                
                if hasattr(data, 'metadata'):
                    metadata['page_metadata'] = data.metadata
                
                meta_file = f"{base_path}.json"
                with open(meta_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
            
            self.logger.debug(f"Stored page to filesystem: {base_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing page {data.url} to filesystem: {e}", 
                            exc_info=True)
            return False
    
    def close(self):
        """No cleanup needed for filesystem storage."""
        pass


class StorageStage(PipelineStage):
    """
    Stage 9: Storage.
    
    Responsibilities:
    - Persist crawled data to storage
    - Support multiple storage backends
    - Batch processing for performance
    - Store pages, links, and metadata
    - Handle storage errors gracefully
    """
    
    def __init__(self, input_queue, output_queue, config: StorageConfig,
                 num_workers: int = 2):
        super().__init__(
            name="Storage",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize storage backend
        self.storage = self._create_storage()
        
        # Batch processing
        self.batch: List[PipelineData] = []
        self.batch_lock = threading.Lock()
        
        # Statistics
        self.stats = {
            'pages_stored': 0,
            'pages_failed': 0,
            'links_stored': 0,
            'batches_processed': 0,
        }
        self.stats_lock = threading.Lock()
    
    def _create_storage(self):
        """Create appropriate storage backend."""
        storage_type = self.config.storage_type.lower()
        
        if storage_type == "sqlite":
            self.logger.info("Using SQLite storage")
            return SQLiteStorage(self.config)
        
        elif storage_type == "filesystem":
            self.logger.info("Using filesystem storage")
            return FilesystemStorage(self.config)
        
        elif storage_type == "postgresql":
            self.logger.error("PostgreSQL storage not yet implemented")
            raise NotImplementedError("PostgreSQL storage coming soon")
        
        else:
            self.logger.warning(f"Unknown storage type '{storage_type}', using SQLite")
            return SQLiteStorage(self.config)
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        """
        Store page data.
        
        Returns:
            PipelineData (passed through for potential further stages)
        """
        try:
            # Store page
            success = self.storage.store_page(data)
            
            # Update statistics
            with self.stats_lock:
                if success:
                    self.stats['pages_stored'] += 1
                    
                    # Count links
                    if hasattr(data, 'links'):
                        self.stats['links_stored'] += len(data.links)
                else:
                    self.stats['pages_failed'] += 1
            
            if success:
                self.logger.info(f"Successfully stored: {data.url}")
            else:
                self.logger.warning(f"Failed to store: {data.url}")
                data.errors.append("Storage failed")
            
            # Pass through to next stage (if any)
            return data
            
        except Exception as e:
            with self.stats_lock:
                self.stats['pages_failed'] += 1
            
            self.logger.error(f"Error in storage stage for {data.url}: {e}", 
                            exc_info=True)
            data.errors.append(f"Storage error: {str(e)}")
            return data
    
    def stop(self):
        """Stop stage and cleanup."""
        self.logger.info("Stopping storage stage")
        super().stop()
        
        # Close storage backend
        if self.storage:
            self.storage.close()
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        base_stats = super().get_stats()
        
        with self.stats_lock:
            base_stats['storage_stats'] = self.stats.copy()
            
            # Calculate success rate
            total = self.stats['pages_stored'] + self.stats['pages_failed']
            if total > 0:
                success_rate = (self.stats['pages_stored'] / total) * 100
                base_stats['storage_stats']['success_rate'] = round(success_rate, 2)
        
        return base_stats


# Example usage
if __name__ == "__main__":
    from queue import Queue
    import time
    from bs4 import BeautifulSoup
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Testing Storage Stage")
    print("=" * 60)
    
    # Create queues
    input_q = Queue()
    output_q = Queue()
    
    # Configure stage
    config = StorageConfig(
        storage_type="sqlite",
        database_path="data/test_crawler.db",
        store_html=True,
        store_text=True,
        store_metadata=True,
        store_links=True,
        compress_html=False
    )
    
    # Create and start stage
    stage = StorageStage(input_q, output_q, config, num_workers=2)
    stage.start()
    
    # Create test data
    test_pages = [
        {
            'url': 'https://example.com/page1',
            'title': 'Test Page 1',
            'html': '<html><body><h1>Page 1</h1><p>Content 1</p></body></html>',
            'text': 'Page 1 Content 1',
            'links': ['https://example.com/page2', 'https://example.com/page3']
        },
        {
            'url': 'https://example.com/page2',
            'title': 'Test Page 2',
            'html': '<html><body><h1>Page 2</h1><p>Content 2</p></body></html>',
            'text': 'Page 2 Content 2',
            'links': ['https://example.com/page1']
        }
    ]
    
    print(f"Submitting {len(test_pages)} pages for storage...")
    
    # Submit test pages
    for page in test_pages:
        data = PipelineData(url=page['url'], depth=0)
        data.title = page['title']
        data.raw_html = page['html']
        data.text_content = page['text']
        data.links = page['links']
        data.status_code = 200
        data.content_type = 'text/html'
        data.metadata = {
            'content': {
                'word_count': len(page['text'].split()),
                'char_count': len(page['text'])
            }
        }
        
        input_q.put(data)
        print(f"  → {page['url']}")
    
    # Wait for processing
    print("\nStoring...")
    time.sleep(3)
    
    # Check results
    print(f"\n{'='*60}")
    print("Results:")
    print('='*60)
    
    processed = 0
    while not output_q.empty():
        result = output_q.get()
        processed += 1
        print(f"✓ Stored: {result.url}")
    
    print(f"\nProcessed: {processed} pages")
    
    # Show statistics
    print(f"\n{'='*60}")
    print("Stage Statistics:")
    print('='*60)
    stats = stage.get_stats()
    storage_stats = stats['storage_stats']
    
    print(f"Pages stored: {storage_stats['pages_stored']}")
    print(f"Pages failed: {storage_stats['pages_failed']}")
    print(f"Links stored: {storage_stats['links_stored']}")
    print(f"Success rate: {storage_stats.get('success_rate', 0)}%")
    
    # Verify database
    print(f"\n{'='*60}")
    print("Database Verification:")
    print('='*60)
    
    import sqlite3
    conn = sqlite3.connect(config.database_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM pages")
    page_count = cursor.fetchone()[0]
    print(f"Pages in database: {page_count}")
    
    cursor.execute("SELECT COUNT(*) FROM links")
    link_count = cursor.fetchone()[0]
    print(f"Links in database: {link_count}")
    
    cursor.execute("SELECT url, title FROM pages")
    print(f"\nStored pages:")
    for row in cursor.fetchall():
        print(f"  - {row[1]}: {row[0]}")
    
    conn.close()
    
    # Stop stage
    stage.stop()
    print("\nStage stopped.")