"""
Link Extraction Stage - Extracts all links from parsed HTML.
Converts relative URLs to absolute and filters invalid links.
"""

import logging
import threading
from typing import Optional, List, Set, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse, urlunparse
import re

from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class LinkExtractionConfig:
    """Configuration for link extraction stage."""
    extract_anchor_text: bool = True
    extract_images: bool = True
    extract_stylesheets: bool = False
    extract_scripts: bool = False
    follow_iframes: bool = False
    
    # Link filtering
    ignore_fragments: bool = True  # Ignore #anchor links
    ignore_query_params: bool = False  # Keep query parameters
    
    # Invalid link patterns to skip
    skip_schemes: List[str] = None  # e.g., ['mailto:', 'tel:', 'javascript:']
    skip_extensions: List[str] = None  # Already filtered in URL validation
    
    # Domain filtering
    same_domain_only: bool = False  # Only extract links to same domain
    max_links_per_page: int = 1000  # Limit links per page
    
    def __post_init__(self):
        """Set defaults for mutable fields."""
        if self.skip_schemes is None:
            self.skip_schemes = [
                'mailto:', 'tel:', 'javascript:', 'data:', 
                'ftp:', 'file:', 'about:'
            ]
        if self.skip_extensions is None:
            self.skip_extensions = []


class URLNormalizer:
    """Normalizes and validates extracted URLs."""
    
    def __init__(self, config: LinkExtractionConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def normalize_url(self, url: str, base_url: str) -> Optional[str]:
        """
        Convert relative URL to absolute and normalize.
        
        Args:
            url: URL to normalize (may be relative)
            base_url: Base URL of the current page
            
        Returns:
            Normalized absolute URL or None if invalid
        """
        try:
            # Strip whitespace
            url = url.strip()
            if not url:
                return None
            
            # Skip invalid schemes
            for scheme in self.config.skip_schemes:
                if url.lower().startswith(scheme):
                    return None
            
            # Convert relative to absolute
            absolute_url = urljoin(base_url, url)
            
            # Parse URL
            parsed = urlparse(absolute_url)
            
            # Must have scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return None
            
            # Only http/https
            if parsed.scheme not in ['http', 'https']:
                return None
            
            # Handle fragments
            fragment = '' if self.config.ignore_fragments else parsed.fragment
            
            # Handle query parameters
            query = '' if self.config.ignore_query_params else parsed.query
            
            # Reconstruct URL
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc.lower(),  # Lowercase domain
                parsed.path,
                parsed.params,
                query,
                fragment
            ))
            
            return normalized
            
        except Exception as e:
            self.logger.debug(f"Failed to normalize URL {url}: {e}")
            return None
    
    def is_same_domain(self, url: str, base_url: str) -> bool:
        """Check if URL is on the same domain as base URL."""
        try:
            url_domain = urlparse(url).netloc.lower()
            base_domain = urlparse(base_url).netloc.lower()
            
            # Remove port if present
            if ':' in url_domain:
                url_domain = url_domain.split(':')[0]
            if ':' in base_domain:
                base_domain = base_domain.split(':')[0]
            
            return url_domain == base_domain
        except Exception:
            return False


class LinkExtractor:
    """Extracts links from parsed HTML."""
    
    def __init__(self, config: LinkExtractionConfig):
        self.config = config
        self.url_normalizer = URLNormalizer(config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract_links(self, soup, base_url: str) -> List[Tuple[str, Optional[str]]]:
        """
        Extract all links from BeautifulSoup object.
        
        Returns:
            List of (url, anchor_text) tuples
        """
        links = []
        seen_urls = set()  # Deduplicate within page
        
        try:
            # Extract <a> tags
            anchor_links = self._extract_anchor_links(soup, base_url)
            links.extend(anchor_links)
            
            # Extract images
            if self.config.extract_images:
                image_links = self._extract_image_links(soup, base_url)
                links.extend(image_links)
            
            # Extract stylesheets
            if self.config.extract_stylesheets:
                stylesheet_links = self._extract_stylesheet_links(soup, base_url)
                links.extend(stylesheet_links)
            
            # Extract scripts
            if self.config.extract_scripts:
                script_links = self._extract_script_links(soup, base_url)
                links.extend(script_links)
            
            # Extract iframes
            if self.config.follow_iframes:
                iframe_links = self._extract_iframe_links(soup, base_url)
                links.extend(iframe_links)
            
            # Deduplicate and filter
            filtered_links = []
            for url, anchor_text in links:
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    
                    # Apply domain filter
                    if self.config.same_domain_only:
                        if not self.url_normalizer.is_same_domain(url, base_url):
                            continue
                    
                    filtered_links.append((url, anchor_text))
                    
                    # Limit number of links
                    if len(filtered_links) >= self.config.max_links_per_page:
                        self.logger.warning(f"Reached max links limit ({self.config.max_links_per_page})")
                        break
            
            self.logger.debug(f"Extracted {len(filtered_links)} unique links from {base_url}")
            return filtered_links
            
        except Exception as e:
            self.logger.error(f"Error extracting links from {base_url}: {e}", exc_info=True)
            return []
    
    def _extract_anchor_links(self, soup, base_url: str) -> List[Tuple[str, Optional[str]]]:
        """Extract links from <a> tags."""
        links = []
        
        try:
            anchor_tags = soup.find_all('a', href=True)
            
            for tag in anchor_tags:
                href = tag.get('href', '').strip()
                if not href:
                    continue
                
                # Normalize URL
                normalized_url = self.url_normalizer.normalize_url(href, base_url)
                if not normalized_url:
                    continue
                
                # Extract anchor text
                anchor_text = None
                if self.config.extract_anchor_text:
                    anchor_text = tag.get_text(strip=True)
                    if len(anchor_text) > 200:  # Limit anchor text length
                        anchor_text = anchor_text[:200] + '...'
                
                links.append((normalized_url, anchor_text))
            
            self.logger.debug(f"Extracted {len(links)} anchor links")
        except Exception as e:
            self.logger.warning(f"Error extracting anchor links: {e}")
        
        return links
    
    def _extract_image_links(self, soup, base_url: str) -> List[Tuple[str, Optional[str]]]:
        """Extract links from <img> tags."""
        links = []
        
        try:
            img_tags = soup.find_all('img', src=True)
            
            for tag in img_tags:
                src = tag.get('src', '').strip()
                if not src:
                    continue
                
                # Normalize URL
                normalized_url = self.url_normalizer.normalize_url(src, base_url)
                if not normalized_url:
                    continue
                
                # Get alt text as anchor text
                alt_text = tag.get('alt', '').strip() if self.config.extract_anchor_text else None
                
                links.append((normalized_url, alt_text))
            
            self.logger.debug(f"Extracted {len(links)} image links")
        except Exception as e:
            self.logger.warning(f"Error extracting image links: {e}")
        
        return links
    
    def _extract_stylesheet_links(self, soup, base_url: str) -> List[Tuple[str, None]]:
        """Extract links from <link rel="stylesheet"> tags."""
        links = []
        
        try:
            link_tags = soup.find_all('link', rel='stylesheet', href=True)
            
            for tag in link_tags:
                href = tag.get('href', '').strip()
                if not href:
                    continue
                
                normalized_url = self.url_normalizer.normalize_url(href, base_url)
                if normalized_url:
                    links.append((normalized_url, None))
            
            self.logger.debug(f"Extracted {len(links)} stylesheet links")
        except Exception as e:
            self.logger.warning(f"Error extracting stylesheet links: {e}")
        
        return links
    
    def _extract_script_links(self, soup, base_url: str) -> List[Tuple[str, None]]:
        """Extract links from <script src="..."> tags."""
        links = []
        
        try:
            script_tags = soup.find_all('script', src=True)
            
            for tag in script_tags:
                src = tag.get('src', '').strip()
                if not src:
                    continue
                
                normalized_url = self.url_normalizer.normalize_url(src, base_url)
                if normalized_url:
                    links.append((normalized_url, None))
            
            self.logger.debug(f"Extracted {len(links)} script links")
        except Exception as e:
            self.logger.warning(f"Error extracting script links: {e}")
        
        return links
    
    def _extract_iframe_links(self, soup, base_url: str) -> List[Tuple[str, None]]:
        """Extract links from <iframe src="..."> tags."""
        links = []
        
        try:
            iframe_tags = soup.find_all('iframe', src=True)
            
            for tag in iframe_tags:
                src = tag.get('src', '').strip()
                if not src:
                    continue
                
                normalized_url = self.url_normalizer.normalize_url(src, base_url)
                if normalized_url:
                    links.append((normalized_url, None))
            
            self.logger.debug(f"Extracted {len(links)} iframe links")
        except Exception as e:
            self.logger.warning(f"Error extracting iframe links: {e}")
        
        return links


class LinkExtractionStage(PipelineStage):
    """
    Stage 7: Link Extraction.
    
    Responsibilities:
    - Extract all <a href="..."> links
    - Convert relative URLs to absolute
    - Extract anchor text for context
    - Optionally extract images, stylesheets, scripts, iframes
    - Filter invalid links
    - Apply domain restrictions
    """
    
    def __init__(self, input_queue, output_queue, config: LinkExtractionConfig,
                 num_workers: int = 2):
        super().__init__(
            name="LinkExtraction",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize link extractor
        self.link_extractor = LinkExtractor(config)
        
        # Statistics
        self.stats = {
            'pages_processed': 0,
            'total_links_extracted': 0,
            'total_images_extracted': 0,
            'avg_links_per_page': 0.0,
            'max_links_in_page': 0,
        }
        self.stats_lock = threading.Lock()
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        """
        Extract links from parsed HTML.
        
        Returns:
            PipelineData with links populated
        """
        try:
            # Check if we have parsed HTML
            if not data.parsed_html:
                self.logger.warning(f"No parsed HTML for {data.url}")
                return data  # Pass through without links
            
            # Use final_url if available (after redirects), otherwise original url
            base_url = data.final_url if hasattr(data, 'final_url') and data.final_url else data.url
            
            # Extract links
            extracted_links = self.link_extractor.extract_links(data.parsed_html, base_url)
            
            # Separate URLs and anchor texts
            urls = []
            anchor_texts = {}
            images = []
            
            for url, anchor_text in extracted_links:
                # Check if it's an image (simple check)
                if url.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp')):
                    images.append(url)
                else:
                    urls.append(url)
                    if anchor_text:
                        anchor_texts[url] = anchor_text
            
            # Store in PipelineData
            data.links = urls
            
            if images:
                data.images = images
            
            # Store anchor texts in metadata
            if anchor_texts:
                if not hasattr(data, 'metadata'):
                    data.metadata = {}
                data.metadata['anchor_texts'] = anchor_texts
            
            # Update statistics
            with self.stats_lock:
                self.stats['pages_processed'] += 1
                self.stats['total_links_extracted'] += len(urls)
                self.stats['total_images_extracted'] += len(images)
                
                # Update max links
                if len(urls) > self.stats['max_links_in_page']:
                    self.stats['max_links_in_page'] = len(urls)
                
                # Update average
                if self.stats['pages_processed'] > 0:
                    self.stats['avg_links_per_page'] = (
                        self.stats['total_links_extracted'] / 
                        self.stats['pages_processed']
                    )
            
            self.logger.info(f"Extracted {len(urls)} links and {len(images)} images from {data.url}")
            return data
        
        except Exception as e:
            self.logger.error(f"Error extracting links from {data.url}: {e}", exc_info=True)
            data.errors.append(f"LinkExtraction error: {str(e)}")
            # Pass through even on error
            return data
    
    def get_stats(self) -> dict:
        """Get link extraction statistics."""
        base_stats = super().get_stats()
        
        with self.stats_lock:
            base_stats['link_extraction_stats'] = self.stats.copy()
            base_stats['link_extraction_stats']['avg_links_per_page'] = round(
                self.stats['avg_links_per_page'], 2
            )
        
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
    
    print("Testing Link Extraction Stage")
    print("=" * 60)
    
    # Create queues
    input_q = Queue()
    output_q = Queue()
    
    # Configure stage
    config = LinkExtractionConfig(
        extract_anchor_text=True,
        extract_images=True,
        same_domain_only=False,
        ignore_fragments=True
    )
    
    # Create and start stage
    stage = LinkExtractionStage(input_q, output_q, config, num_workers=2)
    stage.start()
    
    # Test HTML with various link types
    test_html = '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Test Page</title>
            <link rel="stylesheet" href="/styles.css">
        </head>
        <body>
            <h1>Test Links</h1>
            
            <!-- Absolute links -->
            <a href="https://example.com/page1">Page 1</a>
            <a href="https://example.com/page2">Page 2</a>
            
            <!-- Relative links -->
            <a href="/about">About Us</a>
            <a href="../contact">Contact</a>
            <a href="products/item1">Product 1</a>
            
            <!-- Links to skip -->
            <a href="mailto:test@example.com">Email</a>
            <a href="javascript:alert('hi')">Alert</a>
            <a href="tel:+1234567890">Phone</a>
            
            <!-- Fragment link -->
            <a href="#section1">Jump to Section</a>
            
            <!-- External domain -->
            <a href="https://external.com/page">External Link</a>
            
            <!-- Images -->
            <img src="https://example.com/image1.jpg" alt="Image 1">
            <img src="/images/logo.png" alt="Logo">
            
            <!-- Empty or invalid -->
            <a href="">Empty</a>
            <a href="#">Just Hash</a>
            <a>No href</a>
        </body>
        </html>
    '''
    
    # Create test data
    data = PipelineData(url='https://example.com/test', depth=0)
    data.parsed_html = BeautifulSoup(test_html, 'html.parser')
    data.final_url = 'https://example.com/test'
    
    print("Submitting test page for link extraction...")
    input_q.put(data)
    
    # Wait for processing
    time.sleep(2)
    
    # Check results
    print(f"\n{'='*60}")
    print("Results:")
    print('='*60)
    
    if not output_q.empty():
        result = output_q.get()
        
        print(f"\nPage: {result.url}")
        print(f"\nExtracted {len(result.links)} links:")
        for i, link in enumerate(result.links, 1):
            print(f"  {i}. {link}")
            # Show anchor text if available
            if hasattr(result, 'metadata') and 'anchor_texts' in result.metadata:
                anchor = result.metadata['anchor_texts'].get(link)
                if anchor:
                    print(f"     Anchor: \"{anchor}\"")
        
        if hasattr(result, 'images') and result.images:
            print(f"\nExtracted {len(result.images)} images:")
            for i, img in enumerate(result.images, 1):
                print(f"  {i}. {img}")
    
    # Show statistics
    print(f"\n{'='*60}")
    print("Stage Statistics:")
    print('='*60)
    stats = stage.get_stats()
    link_stats = stats['link_extraction_stats']
    
    print(f"Pages processed: {link_stats['pages_processed']}")
    print(f"Total links: {link_stats['total_links_extracted']}")
    print(f"Total images: {link_stats['total_images_extracted']}")
    print(f"Avg links per page: {link_stats['avg_links_per_page']}")
    print(f"Max links in a page: {link_stats['max_links_in_page']}")
    
    # Stop stage
    stage.stop()
    print("\nStage stopped.")