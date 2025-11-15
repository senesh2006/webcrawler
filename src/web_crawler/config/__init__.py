"""
Configuration Module - Configuration management and loading.

This module handles loading, saving, and validating crawler configurations.
It supports YAML-based configuration files with comprehensive validation.

Components:
-----------
- ConfigLoader: Loads and saves configurations from/to YAML files
- validate_config: Validates configuration objects
- ConfigurationError: Exception raised for invalid configurations

Usage:
------
from web_crawler.config import ConfigLoader, validate_config

# Load from YAML file
config = ConfigLoader.load_from_yaml('config/default.yaml')

# Validate configuration
validate_config(config)

# Create default configuration
config = ConfigLoader.create_default_config()

# Modify configuration
config.url_validation.allowed_domains = ['example.com']
config.fetch_workers = 20

# Save to YAML file
ConfigLoader.save_to_yaml(config, 'config/my_config.yaml')

Configuration File Format:
-------------------------
The YAML configuration file should have two main sections:

pipeline:
  queue_size: 1000
  fetch_workers: 10
  # ... other worker counts

stages:
  url_validation:
    allowed_domains:
      - example.com
    max_depth: 3
  fetch:
    timeout_seconds: 30
    max_retries: 3
  # ... other stage configurations
"""

from .crawler_config import (
    ConfigLoader,
    validate_config,
    ConfigurationError
)

__all__ = [
    'ConfigLoader',
    'validate_config',
    'ConfigurationError',
]