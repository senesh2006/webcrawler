"""
HTTP Fetch Stage - Downloads web pages from the internet.
Now supports both standard HTTP requests and Headless Browser (Playwright)
for JavaScript-heavy websites.
"""

import logging
import time
import threading
from typing import Optional, Dict, Any
from dataclasses import dataclass

# Standard HTTP Request Imports
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import (
    RequestException, Timeout, ConnectionError, 
    TooManyRedirects, HTTPError
)

# Headless Browser Imports
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class FetchConfig:
    """Configuration for HTTP fetch stage."""
    timeout_seconds: int = 30
    max_retries: int = 3
    max_redirects: int = 5
    verify_ssl: bool = True
    user_agent: str = "MyWebCrawler/1.0"
    max_content_size_mb: int = 10  # Skip pages larger than this
    
    # --- BROWSER CONFIGURATION (New Fields) ---
    use_headless_browser: bool = False  # If True, use Playwright
    browser_type: str = "chromium"      # chromium, firefox, or webkit
    wait_for_selector: Optional[str] = None # Specific CSS selector to wait for
    page_load_delay: float = 1.0        # Extra wait time for animations
    # ------------------------------------------

    # Request headers
    accept_language: str = "en-US,en;q=0.9"
    accept_encoding: str = "gzip, deflate"
    
    # Retry settings
    retry_backoff_factor: float = 2.0
    retry_on_status: list = None
    
    # Connection pooling
    pool_connections: int = 10
    pool_maxsize: int = 20
    
    def __post_init__(self):
        """Set default retry status codes."""
        if self.retry_on_status is None:
            self.retry_on_status = [429, 500, 502, 503, 504]


class BrowserManager:
    """
    Manages Playwright browser instances.
    Crucial: Playwright objects are NOT thread-safe. 
    We must maintain a separate Playwright instance/context per thread.
    """
    def __init__(self, config: FetchConfig):
        self.config = config
        self.thread_resources: Dict[int, Dict[str, Any]] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
        
        if self.config.use_headless_browser and not PLAYWRIGHT_AVAILABLE:
            self.logger.error("Playwright not installed! Run: pip install playwright && playwright install")

    def get_page(self):
        """
        Get a Playwright Page object for the current thread.
        Lazily initializes Playwright if it doesn't exist for this thread.
        """
        thread_id = threading.get_ident()
        
        with self.lock:
            if thread_id not in self.thread_resources:
                self._init_thread_resource(thread_id)
            
            # Create a new page (tab) for this specific request
            # We reuse the browser/context, but create fresh pages to avoid cache/cookie leakage between URLs
            context = self.thread_resources[thread_id]['context']
            return context.new_page()

    def _init_thread_resource(self, thread_id: int):
        """Initialize Playwright, Browser, and Context for a specific thread."""
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError("Playwright is not installed.")

        self.logger.debug(f"Initializing Playwright for thread {thread_id}")
        
        pw = sync_playwright().start()
        
        launch_args = {
            "headless": True,
            # Common args to reduce detection and improve stability in containers
            "args": ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        }

        if self.config.browser_type == 'firefox':
            browser = pw.firefox.launch(**launch_args)
        elif self.config.browser_type == 'webkit':
            browser = pw.webkit.launch(**launch_args)
        else:
            browser = pw.chromium.launch(**launch_args)

        # Create a persistent context for this thread with our config
        context = browser.new_context(
            user_agent=self.config.user_agent,
            viewport={'width': 1920, 'height': 1080},
            accept_downloads=False,
            ignore_https_errors=not self.config.verify_ssl
        )
        
        # Store everything so we can close it later
        self.thread_resources[thread_id] = {
            'playwright': pw,
            'browser': browser,
            'context': context
        }

    def close_all(self):
        """Clean up all browsers and playwright instances."""
        with self.lock:
            for thread_id, res in self.thread_resources.items():
                try:
                    res['context'].close()
                    res['browser'].close()
                    res['playwright'].stop()
                    self.logger.debug(f"Closed browser resources for thread {thread_id}")
                except Exception as e:
                    self.logger.error(f"Error closing resources for thread {thread_id}: {e}")
            self.thread_resources.clear()


class SessionManager:
    """Manages standard HTTP requests sessions (Legacy/Fast mode)."""
    
    def __init__(self, config: FetchConfig):
        self.config = config
        self.sessions: Dict[int, requests.Session] = {}
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_session(self) -> requests.Session:
        thread_id = threading.get_ident()
        with self.lock:
            if thread_id not in self.sessions:
                self.sessions[thread_id] = self._create_session()
            return self.sessions[thread_id]
    
    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff_factor,
            status_forcelist=self.config.retry_on_status,
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy,
                              pool_connections=self.config.pool_connections,
                              pool_maxsize=self.config.pool_maxsize)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            'User-Agent': self.config.user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': self.config.accept_language,
            'Accept-Encoding': self.config.accept_encoding,
            'Connection': 'keep-alive',
        })
        return session
    
    def close_all(self):
        with self.lock:
            for session in self.sessions.values():
                session.close()
            self.sessions.clear()


class HTTPFetcher:
    """Handles fetching logic, switching between Requests and Playwright."""
    
    def __init__(self, config: FetchConfig, session_manager: SessionManager, browser_manager: BrowserManager):
        self.config = config
        self.session_manager = session_manager
        self.browser_manager = browser_manager
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def fetch(self, url: str) -> Dict:
        """Dispatch to the correct fetcher based on config."""
        if self.config.use_headless_browser:
            return self._fetch_with_browser(url)
        else:
            return self._fetch_with_requests(url)

    def _fetch_with_browser(self, url: str) -> Dict:
        """Fetch using Playwright (Headless Browser)."""
        start_time = time.time()
        result = {
            'success': False, 'url': url, 'status_code': None, 'content': None,
            'headers': {}, 'final_url': url, 'error': None, 
            'response_time': 0.0, 'content_size': 0, 'fetch_method': 'browser'
        }

        page = None
        try:
            page = self.browser_manager.get_page()
            
            # Navigate
            # Goto returns a Response object, or None if there was a redirect error/interception
            response = page.goto(url, timeout=self.config.timeout_seconds * 1000, wait_until="domcontentloaded")
            
            # 1. Wait for JavaScript to settle (Network Idle)
            try:
                page.wait_for_load_state("networkidle", timeout=5000) # Wait up to 5s for network idle
            except PlaywrightTimeoutError:
                self.logger.debug(f"Network idle timeout for {url}, proceeding anyway.")

            # 2. Optional: Wait for specific element
            if self.config.wait_for_selector:
                try:
                    page.wait_for_selector(self.config.wait_for_selector, timeout=5000)
                except PlaywrightTimeoutError:
                    self.logger.warning(f"Selector {self.config.wait_for_selector} not found in {url}")

            # 3. Static Delay (if configured)
            if self.config.page_load_delay > 0:
                time.sleep(self.config.page_load_delay)

            # Get Data
            content = page.content() # The full DOM including JS modifications
            result['content'] = content.encode('utf-8', errors='replace') # Normalize to bytes
            result['content_size'] = len(result['content'])
            result['response_time'] = time.time() - start_time
            result['final_url'] = page.url
            result['success'] = True
            
            if response:
                result['status_code'] = response.status
                result['headers'] = response.all_headers()
            else:
                # Sometimes response is null for certain types of navigations, assume 200 if we got content
                result['status_code'] = 200

        except PlaywrightTimeoutError:
            result['error'] = f"Timeout after {self.config.timeout_seconds}s"
        except PlaywrightError as e:
            result['error'] = f"Browser Error: {str(e)}"
        except Exception as e:
            result['error'] = f"Unexpected Browser Error: {str(e)}"
        finally:
            # Crucial: Close the page (tab) to free memory
            if page:
                try:
                    page.close()
                except Exception:
                    pass
        
        return result

    def _fetch_with_requests(self, url: str) -> Dict:
        """Fetch using standard Requests library (Legacy)."""
        session = self.session_manager.get_session()
        start_time = time.time()
        
        result = {
            'success': False, 'url': url, 'status_code': None, 'content': None,
            'headers': {}, 'final_url': url, 'error': None, 
            'response_time': 0.0, 'content_size': 0, 'fetch_method': 'requests'
        }
        
        try:
            response = session.get(
                url, timeout=self.config.timeout_seconds, 
                allow_redirects=True, verify=self.config.verify_ssl, stream=True
            )
            
            result['response_time'] = time.time() - start_time
            result['status_code'] = response.status_code
            result['headers'] = dict(response.headers)
            result['final_url'] = response.url
            
            # Check content size
            content_length = response.headers.get('Content-Length')
            if content_length and int(content_length) / (1024 * 1024) > self.config.max_content_size_mb:
                result['error'] = f"Content too large"
                response.close()
                return result
            
            if 200 <= response.status_code < 300:
                content = b''
                for chunk in response.iter_content(chunk_size=8192):
                    content += chunk
                    if len(content) > self.config.max_content_size_mb * 1024 * 1024:
                        result['error'] = "Content exceeded size limit"
                        response.close()
                        return result
                result['content'] = content
                result['content_size'] = len(content)
                result['success'] = True
            else:
                result['error'] = f"HTTP {response.status_code}"
            
            response.close()
            
        except Exception as e:
            result['error'] = f"Request Error: {str(e)}"
            
        result['response_time'] = time.time() - start_time
        return result


class FetchStage(PipelineStage):
    """
    Stage 5: HTTP Fetch.
    Supports Standard Requests and Headless Browser (Playwright).
    """
    
    def __init__(self, input_queue, output_queue, config: FetchConfig, num_workers: int = 10):
        super().__init__("HTTPFetch", input_queue, output_queue, num_workers)
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize Managers
        self.session_manager = SessionManager(config)
        self.browser_manager = BrowserManager(config)
        
        # Initialize Fetcher with both managers
        self.fetcher = HTTPFetcher(config, self.session_manager, self.browser_manager)
        
        self.stats = {
            'total_fetched': 0, 'successful': 0, 'failed': 0,
            'timeouts': 0, 'browser_errors': 0, 'http_errors': {},
            'total_bytes': 0, 'total_response_time': 0.0,
        }
        self.stats_lock = threading.Lock()
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        try:
            url = data.url
            self.logger.debug(f"Fetching: {url} (Headless: {self.config.use_headless_browser})")
            
            # Fetch
            result = self.fetcher.fetch(url)
            
            # Update stats
            with self.stats_lock:
                self.stats['total_fetched'] += 1
                self.stats['total_response_time'] += result['response_time']
                if result['success']:
                    self.stats['successful'] += 1
                    self.stats['total_bytes'] += result['content_size']
                else:
                    self.stats['failed'] += 1
                    if 'Timeout' in str(result['error']):
                        self.stats['timeouts'] += 1
                    if result['status_code']:
                        self.stats['http_errors'][result['status_code']] = \
                            self.stats['http_errors'].get(result['status_code'], 0) + 1

            # Process Result
            data.status_code = result['status_code']
            data.final_url = result['final_url']
            
            if result['success']:
                try:
                    data.raw_html = result['content'].decode('utf-8', errors='replace')
                except Exception:
                    data.raw_html = result['content'].decode('latin-1', errors='replace')
                
                data.content_type = result['headers'].get('Content-Type', '')
                
                if not hasattr(data, 'metadata'):
                    data.metadata = {}
                data.metadata['response_time'] = result['response_time']
                data.metadata['fetch_method'] = result.get('fetch_method', 'unknown')
                data.metadata['headers'] = result['headers']
                
                self.logger.info(f"Fetched: {url} ({result['content_size']}b, {result['response_time']:.2f}s)")
                return data
            else:
                data.errors.append(f"Fetch failed: {result['error']}")
                self.logger.warning(f"Failed to fetch {url}: {result['error']}")
                
                # Basic retry/drop logic
                if result['status_code'] and 400 <= result['status_code'] < 500:
                    return None
                return None
        
        except Exception as e:
            self.logger.error(f"Error processing {data.url}: {e}", exc_info=True)
            data.errors.append(str(e))
            return None
    
    def stop(self):
        """Stop stage and cleanup all resources (Browsers and Sessions)."""
        self.logger.info("Stopping fetch stage...")
        super().stop()
        self.session_manager.close_all()
        self.browser_manager.close_all()
        self.logger.info("Fetch stage resources released.")