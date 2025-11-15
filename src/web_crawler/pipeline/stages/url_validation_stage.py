"""
URL Validation Stage - Validates and normalizes URLs
File: src/web_crawler/pipeline/stages/url_validation_stage.py
"""
from typing import Optional, List, Set
from dataclasses import dataclass
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
import re
import logging
from ..stage import PipelineStage
from ..pipeline_data import PipelineData


@dataclass
class URLValidationConfig:
    """Configuration for URL validation stage"""
    allowed_domains: List[str] = None
    blocked_domains: List[str] = None
    max_depth: int = 3
    allowed_schemes: List[str] = None
    blocked_extensions: List[str] = None
    normalize_urls: bool = True
    
    def __post_init__(self):
        if self.allowed_schemes is None:
            self.allowed_schemes = ['http', 'https']
        if self.blocked_extensions is None:
            self.blocked_extensions = [
                '.pdf', '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg',
                '.zip', '.tar', '.gz', '.rar', '.7z',
                '.mp3', '.mp4', '.avi', '.mov', '.wmv',
                '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                '.exe', '.dmg', '.pkg', '.deb', '.rpm'
            ]
        if self.blocked_domains is None:
            self.blocked_domains = []
        if self.allowed_domains is None:
            self.allowed_domains = []


class URLValidationStage(PipelineStage):
    """
    First pipeline stage - validates and normalizes URLs
    
    Responsibilities:
    - Validate URL format
    - Normalize URL (lowercase domain, sort params, remove fragments)
    - Check domain whitelist/blacklist
    - Check depth limits
    - Filter by file extension
    """
    
    def __init__(self, input_queue, output_queue, config: URLValidationConfig, num_workers: int = 2):
        super().__init__(
            name="URL_Validation",
            input_queue=input_queue,
            output_queue=output_queue,
            num_workers=num_workers
        )
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Convert to sets for O(1) lookup
        self.allowed_domains_set: Optional[Set[str]] = (
            set(config.allowed_domains) if config.allowed_domains else None
        )
        self.blocked_domains_set: Set[str] = set(config.blocked_domains)
        self.blocked_extensions_set: Set[str] = set(config.blocked_extensions)
        
        # URL validation regex
        self.url_pattern = re.compile(
            r'^https?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
            r'localhost|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?::\d+)?'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE
        )
    
    def process(self, data: PipelineData) -> Optional[PipelineData]:
        """Validate and normalize URL"""
        try:
            # Step 1: Basic format validation
            if not self._is_valid_format(data.url):
                self.logger.debug(f"Invalid URL format: {data.url}")
                return None
            
            # Step 2: Normalize URL
            normalized_url = self._normalize_url(data.url)
            if not normalized_url:
                return None
            data.url = normalized_url
            
            # Step 3: Parse URL for further checks
            parsed = urlparse(data.url)
            
            # Step 4: Check scheme
            if parsed.scheme not in self.config.allowed_schemes:
                self.logger.debug(f"Blocked scheme '{parsed.scheme}': {data.url}")
                return None
            
            # Step 5: Check domain whitelist/blacklist
            domain = parsed.netloc.lower()
            if not self._is_domain_allowed(domain):
                self.logger.debug(f"Domain not allowed: {domain}")
                return None
            
            # Step 6: Check depth limit
            if data.depth > self.config.max_depth:
                self.logger.debug(f"Max depth exceeded ({data.depth}): {data.url}")
                return None
            
            # Step 7: Check file extension
            if self._has_blocked_extension(parsed.path):
                self.logger.debug(f"Blocked extension: {data.url}")
                return None
            
            # All checks passed
            return data
            
        except Exception as e:
            self.logger.error(f"Error validating URL {data.url}: {str(e)}")
            return None
    
    def _is_valid_format(self, url: str) -> bool:
        """Check if URL has valid format"""
        if not url or not isinstance(url, str):
            return False
        
        # Check length
        if len(url) > 2048:
            return False
        
        # Check with regex
        return bool(self.url_pattern.match(url))
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """
        Normalize URL to canonical form
        - Convert domain to lowercase
        - Remove default ports (80, 443)
        - Remove fragments (#section)
        - Sort query parameters
        - Remove trailing slash (with exceptions)
        """
        if not self.config.normalize_urls:
            return url
        
        try:
            parsed = urlparse(url.strip())
            
            # Lowercase scheme and domain
            scheme = parsed.scheme.lower()
            netloc = parsed.netloc.lower()
            
            # Remove default ports
            if ':80' in netloc and scheme == 'http':
                netloc = netloc.replace(':80', '')
            elif ':443' in netloc and scheme == 'https':
                netloc = netloc.replace(':443', '')
            
            # Get path (remove trailing slash unless it's the root)
            path = parsed.path
            if path.endswith('/') and len(path) > 1:
                path = path.rstrip('/')
            elif not path:
                path = '/'
            
            # Sort query parameters alphabetically for consistency
            query = parsed.query
            if query:
                params = parse_qs(query, keep_blank_values=True)
                sorted_params = sorted(params.items())
                query = urlencode(sorted_params, doseq=True)
            
            # Remove fragment
            fragment = ''
            
            # Reconstruct URL
            normalized = urlunparse((scheme, netloc, path, parsed.params, query, fragment))
            return normalized
            
        except Exception as e:
            self.logger.error(f"Error normalizing URL {url}: {str(e)}")
            return None
    
    def _is_domain_allowed(self, domain: str) -> bool:
        """Check if domain is allowed based on whitelist/blacklist"""
        # Remove port if present
        domain_without_port = domain.split(':')[0]
        
        # Check blacklist first
        if domain_without_port in self.blocked_domains_set:
            return False
        
        # Check against any blocked domain patterns (subdomains)
        for blocked in self.blocked_domains_set:
            if domain_without_port.endswith('.' + blocked) or domain_without_port == blocked:
                return False
        
        # If whitelist is specified, domain must be in it
        if self.allowed_domains_set:
            # Check exact match or subdomain
            for allowed in self.allowed_domains_set:
                if domain_without_port.endswith('.' + allowed) or domain_without_port == allowed:
                    return True
            return False
        
        # No whitelist means all domains allowed (except blacklisted)
        return True
    
    def _has_blocked_extension(self, path: str) -> bool:
        """Check if URL path has a blocked file extension"""
        path_lower = path.lower()
        return any(path_lower.endswith(ext) for ext in self.blocked_extensions_set)