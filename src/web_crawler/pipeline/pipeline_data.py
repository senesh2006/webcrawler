"""
Pipeline Data Model - Data container that flows through the pipeline
File: src/web_crawler/pipeline/pipeline_data.py
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
from bs4 import BeautifulSoup


@dataclass
class PipelineData:
    """
    Data container that flows through the entire pipeline.
    Each stage adds or modifies data as it processes.
    """
    # Core fields - required
    url: str
    depth: int
    parent_url: Optional[str] = None
    
    # Content fields - populated by stages
    raw_html: Optional[str] = None
    parsed_html: Optional[BeautifulSoup] = None
    
    # Extracted data
    title: Optional[str] = None
    text_content: Optional[str] = None
    meta_description: Optional[str] = None
    meta_keywords: Optional[str] = None
    links: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    
    # HTTP response metadata
    status_code: Optional[int] = None
    content_type: Optional[str] = None
    content_length: Optional[int] = None
    response_headers: Dict[str, str] = field(default_factory=dict)
    final_url: Optional[str] = None  # After redirects
    
    # Timing and performance
    timestamp: datetime = field(default_factory=datetime.now)
    stage_timings: Dict[str, float] = field(default_factory=dict)
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    retry_count: int = 0
    
    # Custom metadata - extensible for custom stages
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_error(self, error: str, stage: str = "unknown") -> None:
        """Add an error message with stage information"""
        self.errors.append(f"[{stage}] {error}")
    
    def add_timing(self, stage: str, duration: float) -> None:
        """Record processing time for a stage"""
        self.stage_timings[stage] = duration
    
    def get_total_processing_time(self) -> float:
        """Calculate total processing time across all stages"""
        return sum(self.stage_timings.values())
    
    def has_errors(self) -> bool:
        """Check if any errors occurred during processing"""
        return len(self.errors) > 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage/serialization"""
        return {
            'url': self.url,
            'depth': self.depth,
            'parent_url': self.parent_url,
            'title': self.title,
            'text_content': self.text_content,
            'meta_description': self.meta_description,
            'meta_keywords': self.meta_keywords,
            'links': self.links,
            'images': self.images,
            'status_code': self.status_code,
            'content_type': self.content_type,
            'content_length': self.content_length,
            'final_url': self.final_url,
            'timestamp': self.timestamp.isoformat(),
            'stage_timings': self.stage_timings,
            'errors': self.errors,
            'retry_count': self.retry_count,
            'metadata': self.metadata
        }
    
    def __repr__(self) -> str:
        return f"PipelineData(url='{self.url}', depth={self.depth}, status={self.status_code})"