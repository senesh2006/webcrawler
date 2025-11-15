"""
HTML Parsing Stage - Parses HTML into structured format.
Extracts metadata like title, description, and creates DOM structure.
"""

import logging
import threading
from typing import Optional, Dict, List
from dataclasses import dataclass
from bs4 import BeautifulSoup, FeatureNotFound
import re

from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class ParseConfig:
    """Configuration for HTML parsing stage."""
    parser: str = "html.parser"  # 'html.parser', 'lxml', or 'html5lib'
    extract_metadata: bool = True
    extract_open_graph: bool = True  # Open Graph meta tags
    extract_twitter_card: bool = True  # Twitter Card meta tags
    extract_canonical: bool = True  # Canonical URL
    extract_language: bool = True  # Page language
    
    # Parser features
    handle_malformed_html: bool = True
    strip_javascript: bool = True
    strip_comments: bool = True
    
    # Performance
    max_html_size_mb: int = 5  # Skip parsing if HTML larger than this


class MetadataExtractor:
    """Extracts various metadata from parsed HTML."""
    
    def __init__(self, config: ParseConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract page title."""
        try:
            # Try <title> tag first
            title_tag = soup.find('title')
            if title_tag and title_tag.string:
                return title_tag.string.strip()
            
            # Try Open Graph title
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                return og_title['content'].strip()
            
            # Try <h1> as fallback
            h1_tag = soup.find('h1')
            if h1_tag:
                return h1_tag.get_text(strip=True)
            
            return None
        except Exception as e:
            self.logger.warning(f"Error extracting title: {e}")
            return None
    
    def extract_meta_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract meta description."""
        try:
            # Standard meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                return meta_desc['content'].strip()
            
            # Open Graph description
            og_desc = soup.find('meta', property='og:description')
            if og_desc and og_desc.get('content'):
                return og_desc['content'].strip()
            
            return None
        except Exception as e:
            self.logger.warning(f"Error extracting description: {e}")
            return None
    
    def extract_meta_keywords(self, soup: BeautifulSoup) -> Optional[List[str]]:
        """Extract meta keywords."""
        try:
            meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
            if meta_keywords and meta_keywords.get('content'):
                keywords = meta_keywords['content'].strip()
                # Split by comma and clean
                return [k.strip() for k in keywords.split(',') if k.strip()]
            return None
        except Exception as e:
            self.logger.warning(f"Error extracting keywords: {e}")
            return None
    
    def extract_open_graph(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract Open Graph metadata."""
        og_data = {}
        
        if not self.config.extract_open_graph:
            return og_data
        
        try:
            # Find all Open Graph meta tags
            og_tags = soup.find_all('meta', property=re.compile(r'^og:'))
            
            for tag in og_tags:
                property_name = tag.get('property', '').replace('og:', '')
                content = tag.get('content', '').strip()
                if property_name and content:
                    og_data[property_name] = content
            
            self.logger.debug(f"Extracted {len(og_data)} Open Graph tags")
        except Exception as e:
            self.logger.warning(f"Error extracting Open Graph data: {e}")
        
        return og_data
    
    def extract_twitter_card(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract Twitter Card metadata."""
        twitter_data = {}
        
        if not self.config.extract_twitter_card:
            return twitter_data
        
        try:
            # Find all Twitter Card meta tags
            twitter_tags = soup.find_all('meta', attrs={'name': re.compile(r'^twitter:')})
            
            for tag in twitter_tags:
                name = tag.get('name', '').replace('twitter:', '')
                content = tag.get('content', '').strip()
                if name and content:
                    twitter_data[name] = content
            
            self.logger.debug(f"Extracted {len(twitter_data)} Twitter Card tags")
        except Exception as e:
            self.logger.warning(f"Error extracting Twitter Card data: {e}")
        
        return twitter_data
    
    def extract_canonical_url(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract canonical URL."""
        if not self.config.extract_canonical:
            return None
        
        try:
            canonical = soup.find('link', rel='canonical')
            if canonical and canonical.get('href'):
                return canonical['href'].strip()
            return None
        except Exception as e:
            self.logger.warning(f"Error extracting canonical URL: {e}")
            return None
    
    def extract_language(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract page language."""
        if not self.config.extract_language:
            return None
        
        try:
            # Try <html lang="...">
            html_tag = soup.find('html')
            if html_tag and html_tag.get('lang'):
                return html_tag['lang'].strip()
            
            # Try <meta http-equiv="content-language">
            meta_lang = soup.find('meta', attrs={'http-equiv': 'content-language'})
            if meta_lang and meta_lang.get('content'):
                return meta_lang['content'].strip()
            
            # Try Open Graph locale
            og_locale = soup.find('meta', property='og:locale')
            if og_locale and og_locale.get('content'):
                return og_locale['content'].strip()
            
            return None
        except Exception as e:
            self.logger.warning(f"Error extracting language: {e}")
            return None
    
    def extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract page author."""
        try:
            # Try meta author tag
            meta_author = soup.find('meta', attrs={'name': 'author'})
            if meta_author and meta_author.get('content'):
                return meta_author['content'].strip()
            
            # Try article:author (Open Graph)
            og_author = soup.find('meta', property='article:author')
            if og_author and og_author.get('content'):
                return og_author['content'].strip()
            
            return None
        except Exception as e:
            self.logger.warning(f"Error extracting author: {e}")
            return None
    
    def extract_published_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract publication date."""
        try:
            # Try article:published_time
            og_published = soup.find('meta', property='article:published_time')
            if og_published and og_published.get('content'):
                return og_published['content'].strip()
            
            # Try various date meta tags
            date_names = ['publish-date', 'publication-date', 'date', 'DC.date']
            for name in date_names:
                meta_date = soup.find('meta', attrs={'name': name})
                if meta_date and meta_date.get('content'):
                    return meta_date['content'].strip()
            
            return None
        except Exception as e:
            self.logger.warning(f"Error extracting published date: {e}")
            return None
    
    def extract_all_metadata(self, soup: BeautifulSoup) -> Dict:
        """Extract all metadata from page."""
        metadata = {}
        
        if not self.config.extract_metadata:
            return metadata
        
        # Basic metadata
        metadata['title'] = self.extract_title(soup)
        metadata['description'] = self.extract_meta_description(soup)
        metadata['keywords'] = self.extract_meta_keywords(soup)
        metadata['author'] = self.extract_author(soup)
        metadata['published_date'] = self.extract_published_date(soup)
        
        # Structural metadata
        metadata['canonical_url'] = self.extract_canonical_url(soup)
        metadata['language'] = self.extract_language(soup)
        
        # Social metadata
        metadata['open_graph'] = self.extract_open_graph(soup)
        metadata['twitter_card'] = self.extract_twitter_card(soup)
        
        # Remove None values
        metadata = {k: v for k, v in metadata.items() if v is not None}
        
        return metadata


class HTMLParser:
    """Handles HTML parsing with BeautifulSoup."""
    
    def __init__(self, config: ParseConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Verify parser is available
        self._verify_parser()
    
    def _verify_parser(self):
        """Verify the configured parser is available."""
        try:
            # Test parse with the configured parser
            BeautifulSoup("<html></html>", self.config.parser)
            self.logger.info(f"Using parser: {self.config.parser}")
        except FeatureNotFound:
            self.logger.warning(f"Parser '{self.config.parser}' not available, "
                              f"falling back to 'html.parser'")
            self.config.parser = "html.parser"
    
    def parse(self, html: str) -> Optional[BeautifulSoup]:
        """
        Parse HTML string into BeautifulSoup object.
        
        Returns:
            BeautifulSoup object or None if parsing failed
        """
        try:
            # Check HTML size
            html_size_mb = len(html.encode('utf-8')) / (1024 * 1024)
            if html_size_mb > self.config.max_html_size_mb:
                self.logger.warning(f"HTML too large to parse: {html_size_mb:.2f} MB")
                return None
            
            # Parse HTML
            soup = BeautifulSoup(html, self.config.parser)
            
            # Optional cleanup
            if self.config.strip_javascript:
                self._remove_scripts(soup)
            
            if self.config.strip_comments:
                self._remove_comments(soup)
            
            self.logger.debug(f"Successfully parsed HTML ({html_size_mb:.2f} MB)")
            return soup
            
        except Exception as e:
            self.logger.error(f"Failed to parse HTML: {e}", exc_info=True)
            return None
    
    def _remove_scripts(self, soup: BeautifulSoup):
        """Remove <script> and <style> tags."""
        try:
            for tag in soup(['script', 'style']):
                tag.decompose()
        except Exception as e:
            self.logger.warning(f"Error removing scripts: {e}")
    
    def _remove_comments(self, soup: BeautifulSoup):
        """Remove HTML comments."""
        try:
            from bs4 import Comment
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()
        except Exception as e:
            self.logger.warning(f"Error removing comments: {e}")


class ParseStage(PipelineStage):
    """
    Stage 6: HTML Parsing.
    
    Responsibilities:
    - Parse raw HTML into BeautifulSoup object
    - Extract page metadata (title, description, etc.)
    - Extract Open Graph and Twitter Card data
    - Handle malformed HTML gracefully
    - Store parsed DOM for next stages
    """
    
    def __init__(self, input_queue, output_queue, config: ParseConfig,
                 num_workers: int = 3):
        super().__init__(
            name="HTMLParsing",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize parser and metadata extractor
        self.html_parser = HTMLParser(config)
        self.metadata_extractor = MetadataExtractor(config)
        
        # Statistics
        self.stats = {
            'total_parsed': 0,
            'successful': 0,
            'failed': 0,
            'skipped_too_large': 0,
            'skipped_non_html': 0,
        }
        self.stats_lock = threading.Lock()
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        """
        Parse HTML and extract metadata.
        
        Returns:
            PipelineData with parsed HTML and metadata, or None if parsing failed
        """
        try:
            # Check if we have HTML content
            if not data.raw_html:
                self.logger.warning(f"No HTML content to parse for {data.url}")
                with self.stats_lock:
                    self.stats['failed'] += 1
                return None
            
            # Check content type
            if data.content_type:
                if 'text/html' not in data.content_type.lower():
                    self.logger.debug(f"Skipping non-HTML content: {data.content_type}")
                    with self.stats_lock:
                        self.stats['skipped_non_html'] += 1
                    return None
            
            with self.stats_lock:
                self.stats['total_parsed'] += 1
            
            # Parse HTML
            soup = self.html_parser.parse(data.raw_html)
            
            if soup is None:
                with self.stats_lock:
                    self.stats['failed'] += 1
                data.errors.append("HTML parsing failed")
                return None
            
            # Store parsed HTML
            data.parsed_html = soup
            
            # Extract metadata
            if self.config.extract_metadata:
                metadata = self.metadata_extractor.extract_all_metadata(soup)
                
                # Store basic metadata in PipelineData fields
                if 'title' in metadata:
                    data.title = metadata['title']
                
                # Store all metadata
                if not hasattr(data, 'metadata'):
                    data.metadata = {}
                data.metadata['page_metadata'] = metadata
                
                self.logger.debug(f"Extracted {len(metadata)} metadata fields from {data.url}")
            
            with self.stats_lock:
                self.stats['successful'] += 1
            
            self.logger.info(f"Successfully parsed HTML for {data.url}")
            return data
        
        except Exception as e:
            with self.stats_lock:
                self.stats['failed'] += 1
            
            self.logger.error(f"Error parsing {data.url}: {e}", exc_info=True)
            data.errors.append(f"Parse error: {str(e)}")
            return None
    
    def get_stats(self) -> dict:
        """Get parsing statistics."""
        base_stats = super().get_stats()
        
        with self.stats_lock:
            base_stats['parse_stats'] = self.stats.copy()
            
            # Calculate success rate
            if self.stats['total_parsed'] > 0:
                success_rate = (self.stats['successful'] / 
                              self.stats['total_parsed'] * 100)
                base_stats['parse_stats']['success_rate'] = round(success_rate, 2)
        
        return base_stats


# Example usage
if __name__ == "__main__":
    from queue import Queue
    import time
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Testing HTML Parsing Stage")
    print("=" * 60)
    
    # Create queues
    input_q = Queue()
    output_q = Queue()
    
    # Configure stage
    config = ParseConfig(
        parser="html.parser",
        extract_metadata=True,
        extract_open_graph=True,
        extract_twitter_card=True,
        strip_javascript=True
    )
    
    # Create and start stage
    stage = ParseStage(input_q, output_q, config, num_workers=2)
    stage.start()
    
    # Test HTML samples
    test_cases = [
        {
            'url': 'https://example.com/page1',
            'html': '''
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <title>Test Page 1</title>
                    <meta name="description" content="This is a test page">
                    <meta name="keywords" content="test, example, html">
                    <meta property="og:title" content="OG Test Page">
                    <meta property="og:description" content="Open Graph description">
                    <meta property="og:image" content="https://example.com/image.jpg">
                    <link rel="canonical" href="https://example.com/page1">
                </head>
                <body>
                    <h1>Welcome to Test Page</h1>
                    <p>This is some content.</p>
                    <script>console.log('This should be removed');</script>
                </body>
                </html>
            '''
        },
        {
            'url': 'https://example.com/page2',
            'html': '''
                <html>
                <head><title>Minimal Page</title></head>
                <body><p>Minimal content</p></body>
                </html>
            '''
        },
        {
            'url': 'https://example.com/malformed',
            'html': '<html><head><title>Malformed</title><body><p>Missing closing tags'
        }
    ]
    
    print(f"Submitting {len(test_cases)} HTML documents for parsing...")
    
    # Submit test cases
    for test in test_cases:
        data = PipelineData(url=test['url'], depth=0)
        data.raw_html = test['html']
        data.content_type = 'text/html'
        input_q.put(data)
        print(f"  → {test['url']}")
    
    # Wait for processing
    print("\nParsing...")
    time.sleep(2)
    
    # Check results
    print(f"\n{'='*60}")
    print("Results:")
    print('='*60)
    
    while not output_q.empty():
        result = output_q.get()
        print(f"\n✓ Parsed: {result.url}")
        print(f"  Title: {result.title}")
        
        if hasattr(result, 'metadata') and 'page_metadata' in result.metadata:
            metadata = result.metadata['page_metadata']
            
            if 'description' in metadata:
                print(f"  Description: {metadata['description']}")
            
            if 'keywords' in metadata:
                print(f"  Keywords: {', '.join(metadata['keywords'])}")
            
            if 'language' in metadata:
                print(f"  Language: {metadata['language']}")
            
            if 'canonical_url' in metadata:
                print(f"  Canonical: {metadata['canonical_url']}")
            
            if metadata.get('open_graph'):
                print(f"  Open Graph tags: {len(metadata['open_graph'])}")
                for key, value in metadata['open_graph'].items():
                    print(f"    og:{key} = {value[:50]}...")
        
        if result.parsed_html:
            # Count some elements
            paragraphs = len(result.parsed_html.find_all('p'))
            links = len(result.parsed_html.find_all('a'))
            print(f"  Elements: {paragraphs} paragraphs, {links} links")
    
    # Show statistics
    print(f"\n{'='*60}")
    print("Stage Statistics:")
    print('='*60)
    stats = stage.get_stats()
    parse_stats = stats['parse_stats']
    
    print(f"Total parsed: {parse_stats['total_parsed']}")
    print(f"Successful: {parse_stats['successful']}")
    print(f"Failed: {parse_stats['failed']}")
    print(f"Success rate: {parse_stats.get('success_rate', 0)}%")
    print(f"Skipped (non-HTML): {parse_stats['skipped_non_html']}")
    print(f"Skipped (too large): {parse_stats['skipped_too_large']}")
    
    # Stop stage
    stage.stop()
    print("\nStage stopped.")