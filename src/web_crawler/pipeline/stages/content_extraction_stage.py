"""
Content Extraction Stage - Extracts meaningful text content from HTML.
Removes boilerplate and focuses on main article/page content.
"""

import logging
import threading
from typing import Optional, List, Dict
from dataclasses import dataclass
import re

from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class ContentExtractionConfig:
    """Configuration for content extraction stage."""
    method: str = "simple"
    min_text_length: int = 100
    remove_boilerplate: bool = True
    extract_headings: bool = True
    extract_paragraphs: bool = True
    extract_lists: bool = True
    extract_tables: bool = False
    normalize_whitespace: bool = True
    remove_extra_newlines: bool = True
    decode_html_entities: bool = True
    min_paragraph_length: int = 20
    max_content_length: int = 1_000_000


class SimpleContentExtractor:
    """Simple content extractor using BeautifulSoup."""
    
    def __init__(self, config: ContentExtractionConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.noise_tags = [
            'script', 'style', 'nav', 'header', 'footer', 
            'aside', 'iframe', 'noscript', 'button'
        ]
        
        self.content_tags = ['article', 'main', 'div', 'section']
    
    def extract(self, soup) -> Dict:
        """Extract content from BeautifulSoup object."""
        result = {
            'text_content': '',
            'headings': [],
            'paragraphs': [],
            'lists': [],
            'tables': [],
            'word_count': 0,
            'char_count': 0
        }
        
        try:
            soup_copy = soup.__copy__()
            
            if self.config.remove_boilerplate:
                self._remove_noise(soup_copy)
            
            if self.config.extract_headings:
                result['headings'] = self._extract_headings(soup_copy)
            
            if self.config.extract_paragraphs:
                result['paragraphs'] = self._extract_paragraphs(soup_copy)
            
            if self.config.extract_lists:
                result['lists'] = self._extract_lists(soup_copy)
            
            if self.config.extract_tables:
                result['tables'] = self._extract_tables(soup_copy)
            
            text = self._extract_text(soup_copy)
            text = self._clean_text(text)
            
            if len(text) < self.config.min_text_length:
                self.logger.debug(f"Content too short: {len(text)} chars")
                return result
            
            if len(text) > self.config.max_content_length:
                text = text[:self.config.max_content_length]
                self.logger.warning(f"Content truncated to {self.config.max_content_length} chars")
            
            result['text_content'] = text
            result['char_count'] = len(text)
            result['word_count'] = len(text.split())
            
            self.logger.debug(f"Extracted {result['word_count']} words, {result['char_count']} chars")
            
        except Exception as e:
            self.logger.error(f"Error extracting content: {e}", exc_info=True)
        
        return result
    
    def _remove_noise(self, soup):
        """Remove noise tags from soup."""
        try:
            for tag_name in self.noise_tags:
                for tag in soup.find_all(tag_name):
                    tag.decompose()
            
            from bs4 import Comment
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()
        except Exception as e:
            self.logger.warning(f"Error removing noise: {e}")
    
    def _extract_text(self, soup) -> str:
        """Extract all text from soup."""
        try:
            main_content = None
            for tag_name in self.content_tags:
                main_content = soup.find(tag_name)
                if main_content:
                    break
            
            if not main_content:
                main_content = soup.find('body') or soup
            
            text = main_content.get_text(separator=' ', strip=True)
            return text
            
        except Exception as e:
            self.logger.warning(f"Error extracting text: {e}")
            return ""
    
    def _extract_headings(self, soup) -> List[Dict]:
        """Extract all headings with hierarchy."""
        headings = []
        
        try:
            for level in range(1, 7):
                for tag in soup.find_all(f'h{level}'):
                    text = tag.get_text(strip=True)
                    if text:
                        headings.append({
                            'level': level,
                            'text': text
                        })
        except Exception as e:
            self.logger.warning(f"Error extracting headings: {e}")
        
        return headings
    
    def _extract_paragraphs(self, soup) -> List[str]:
        """Extract all paragraphs."""
        paragraphs = []
        
        try:
            for tag in soup.find_all('p'):
                text = tag.get_text(strip=True)
                if len(text) >= self.config.min_paragraph_length:
                    paragraphs.append(text)
        except Exception as e:
            self.logger.warning(f"Error extracting paragraphs: {e}")
        
        return paragraphs
    
    def _extract_lists(self, soup) -> List[Dict]:
        """Extract all lists."""
        lists = []
        
        try:
            for list_tag in soup.find_all(['ul', 'ol']):
                list_type = 'ordered' if list_tag.name == 'ol' else 'unordered'
                items = []
                
                for li in list_tag.find_all('li', recursive=False):
                    text = li.get_text(strip=True)
                    if text:
                        items.append(text)
                
                if items:
                    lists.append({
                        'type': list_type,
                        'items': items
                    })
        except Exception as e:
            self.logger.warning(f"Error extracting lists: {e}")
        
        return lists
    
    def _extract_tables(self, soup) -> List[Dict]:
        """Extract tables with headers and rows."""
        tables = []
        
        try:
            for table in soup.find_all('table'):
                table_data = {
                    'headers': [],
                    'rows': []
                }
                
                thead = table.find('thead')
                if thead:
                    for th in thead.find_all('th'):
                        table_data['headers'].append(th.get_text(strip=True))
                
                tbody = table.find('tbody') or table
                for tr in tbody.find_all('tr'):
                    row = []
                    for td in tr.find_all(['td', 'th']):
                        row.append(td.get_text(strip=True))
                    if row:
                        table_data['rows'].append(row)
                
                if table_data['headers'] or table_data['rows']:
                    tables.append(table_data)
        except Exception as e:
            self.logger.warning(f"Error extracting tables: {e}")
        
        return tables
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        
        try:
            if self.config.decode_html_entities:
                import html
                text = html.unescape(text)
            
            if self.config.normalize_whitespace:
                text = re.sub(r'[ \t]+', ' ', text)
            
            if self.config.remove_extra_newlines:
                text = re.sub(r'\n{3,}', '\n\n', text)
            
            text = text.strip()
            
        except Exception as e:
            self.logger.warning(f"Error cleaning text: {e}")
        
        return text


class ContentExtractionStage(PipelineStage):
    """
    Stage 8: Content Extraction.
    
    Extracts main text content from HTML, removing boilerplate.
    """
    
    def __init__(self, input_queue, output_queue, config: ContentExtractionConfig, num_workers: int = 2):
        super().__init__(
            name="ContentExtraction",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize extractor
        self.extractor = self._create_extractor()
        
        # Statistics
        self.stats = {
            'pages_processed': 0,
            'successful': 0,
            'failed': 0,
            'too_short': 0,
            'total_words': 0,
            'total_chars': 0,
            'avg_words_per_page': 0.0,
        }
        self.stats_lock = threading.Lock()
    
    def _create_extractor(self):
        """Create appropriate content extractor."""
        method = self.config.method.lower()
        
        if method == "simple":
            self.logger.info("Using simple content extractor")
            return SimpleContentExtractor(self.config)
        else:
            self.logger.warning(f"Unknown method '{method}', using simple extractor")
            return SimpleContentExtractor(self.config)
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        """Extract content from page."""
        try:
            with self.stats_lock:
                self.stats['pages_processed'] += 1
            
            if not data.parsed_html and not data.raw_html:
                self.logger.warning(f"No HTML content for {data.url}")
                with self.stats_lock:
                    self.stats['failed'] += 1
                return data
            
            if not data.parsed_html:
                self.logger.warning(f"No parsed HTML for {data.url}")
                with self.stats_lock:
                    self.stats['failed'] += 1
                return data
            
            content = self.extractor.extract(data.parsed_html)
            
            if not content['text_content']:
                with self.stats_lock:
                    self.stats['too_short'] += 1
                self.logger.debug(f"No content extracted from {data.url}")
                return data
            
            data.text_content = content['text_content']
            
            if not hasattr(data, 'metadata'):
                data.metadata = {}
            
            data.metadata['content'] = {
                'word_count': content['word_count'],
                'char_count': content['char_count'],
                'headings': content.get('headings', []),
                'paragraphs': content.get('paragraphs', []),
                'lists': content.get('lists', []),
                'tables': content.get('tables', [])
            }
            
            with self.stats_lock:
                self.stats['successful'] += 1
                self.stats['total_words'] += content['word_count']
                self.stats['total_chars'] += content['char_count']
                
                if self.stats['successful'] > 0:
                    self.stats['avg_words_per_page'] = (
                        self.stats['total_words'] / self.stats['successful']
                    )
            
            self.logger.info(f"Extracted {content['word_count']} words from {data.url}")
            return data
        
        except Exception as e:
            with self.stats_lock:
                self.stats['failed'] += 1
            
            self.logger.error(f"Error extracting content from {data.url}: {e}", exc_info=True)
            return data
    
    def get_stats(self) -> dict:
        """Get content extraction statistics."""
        base_stats = super().get_stats()
        
        with self.stats_lock:
            base_stats['content_extraction_stats'] = self.stats.copy()
            base_stats['content_extraction_stats']['avg_words_per_page'] = round(
                self.stats['avg_words_per_page'], 2
            )
        
        return base_stats