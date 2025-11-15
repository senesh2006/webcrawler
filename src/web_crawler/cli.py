"""
Command Line Interface for the Web Crawler.

This module provides a command-line interface for running the web crawler,
managing configurations, and monitoring crawl progress.

Usage Examples:
--------------

# Basic crawl
python -m web_crawler.cli crawl https://example.com

# Crawl with custom configuration
python -m web_crawler.cli crawl https://example.com -c config/production.yaml

# Crawl with depth and page limits
python -m web_crawler.cli crawl https://example.com -d 2 -p 1000

# Crawl specific domains only
python -m web_crawler.cli crawl https://example.com --allowed-domains example.com test.com

# Crawl with timeout
python -m web_crawler.cli crawl https://example.com -t 3600

# Create default configuration
python -m web_crawler.cli config --create-default -o config/default.yaml

# Validate configuration
python -m web_crawler.cli config --validate config/my_config.yaml

# Verbose logging
python -m web_crawler.cli -v crawl https://example.com
"""

import argparse
import logging
import sys
import os
from pathlib import Path
import time

from .core.pipeline_crawler import PipelineCrawler
from .config.crawler_config import ConfigLoader, validate_config, ConfigurationError


def setup_logging(verbose: bool = False):
    """
    Setup logging configuration.
    
    Args:
        verbose: Enable debug-level logging if True
    """
    level = logging.DEBUG if verbose else logging.INFO
    
    # Create logs directory if it doesn't exist
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_dir / 'crawler.log')
        ]
    )
    
    # Set third-party loggers to WARNING to reduce noise
    if not verbose:
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)


def crawl_command(args):
    """
    Execute the crawl command.
    
    Args:
        args: Parsed command-line arguments
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        if args.config:
            logger.info(f"Loading configuration from {args.config}")
            config = ConfigLoader.load_from_yaml(args.config)
        else:
            logger.info("Using default configuration")
            config = ConfigLoader.create_default_config()
        
        # Override with command line arguments
        if args.max_depth is not None:
            config.url_validation.max_depth = args.max_depth
            config.link_reinjection.max_depth = args.max_depth
            logger.info(f"Set max_depth to {args.max_depth}")
        
        if args.max_pages is not None:
            config.link_reinjection.max_total_pages = args.max_pages
            logger.info(f"Set max_pages to {args.max_pages}")
        
        if args.allowed_domains:
            config.url_validation.allowed_domains = args.allowed_domains
            logger.info(f"Set allowed_domains to {args.allowed_domains}")
        
        if args.user_agent:
            config.robots.user_agent = args.user_agent
            config.fetch.user_agent = args.user_agent
            logger.info(f"Set user_agent to {args.user_agent}")
        
        if args.delay is not None:
            config.rate_limiting.default_delay_seconds = args.delay
            logger.info(f"Set delay to {args.delay} seconds")
        
        # Validate configuration
        try:
            validate_config(config)
            logger.info("Configuration validated successfully")
        except ConfigurationError as e:
            logger.error(f"Configuration validation failed: {e}")
            sys.exit(1)
        
        # Create crawler
        logger.info("Creating crawler...")
        crawler = PipelineCrawler(config)
        
        # Prepare seed URLs
        seed_urls = args.urls if isinstance(args.urls, list) else [args.urls]
        logger.info(f"Starting crawl with {len(seed_urls)} seed URL(s):")
        for url in seed_urls:
            logger.info(f"  - {url}")
        
        # Start crawling
        print("\n" + "="*60)
        print("STARTING WEB CRAWLER")
        print("="*60)
        crawler.start(seed_urls)
        
        # Monitor progress
        try:
            start_time = time.time()
            check_count = 0
            
            while True:
                time.sleep(5)  # Check every 5 seconds
                check_count += 1
                
                # Print status every 5 checks (25 seconds)
                if check_count % 5 == 0:
                    crawler.print_status()
                
                # Check for completion
                status = crawler.get_status()
                if status['overall']['crawl_complete']:
                    logger.info("Crawl completed!")
                    break
                
                # Check timeout
                if args.timeout:
                    elapsed = time.time() - start_time
                    if elapsed > args.timeout:
                        logger.warning(f"Timeout ({args.timeout}s) reached")
                        break
        
        except KeyboardInterrupt:
            logger.info("\nCrawl interrupted by user")
        
        finally:
            # Stop crawler
            print("\n" + "="*60)
            print("STOPPING CRAWLER")
            print("="*60)
            crawler.stop()
        
        # Print final statistics
        print("\n" + "="*60)
        print("FINAL STATISTICS")
        print("="*60)
        crawler.print_status()
        
        detailed_stats = crawler.get_detailed_stats()
        runtime = detailed_stats['crawler']['runtime']
        print(f"\nTotal Runtime: {runtime:.2f} seconds")
        
        logger.info("Crawl finished successfully")
    
    except Exception as e:
        logger.error(f"Crawl failed: {e}", exc_info=True)
        sys.exit(1)


def config_command(args):
    """
    Execute the config command.
    
    Args:
        args: Parsed command-line arguments
    """
    logger = logging.getLogger(__name__)
    
    try:
        if args.create_default:
            # Create default configuration
            config = ConfigLoader.create_default_config()
            output_path = args.output or 'config/default.yaml'
            
            # Create directory if needed
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            ConfigLoader.save_to_yaml(config, output_path)
            print(f"✓ Default configuration created at: {output_path}")
            logger.info(f"Default configuration created at {output_path}")
        
        elif args.validate:
            # Validate configuration file
            print(f"Validating configuration: {args.validate}")
            config = ConfigLoader.load_from_yaml(args.validate)
            validate_config(config)
            print(f"✓ Configuration is valid: {args.validate}")
            logger.info(f"Configuration {args.validate} is valid")
        
        else:
            print("Error: Please specify --create-default or --validate")
            sys.exit(1)
    
    except ConfigurationError as e:
        print(f"✗ Configuration error: {e}")
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    
    except Exception as e:
        print(f"✗ Error: {e}")
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Web Crawler - A modular pipeline-based web crawler',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s crawl https://example.com
  %(prog)s crawl https://example.com -d 2 -p 1000
  %(prog)s crawl https://example.com -c config/production.yaml
  %(prog)s config --create-default -o config/default.yaml
  %(prog)s config --validate config/my_config.yaml
        """
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose (debug) logging'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # ========================================================================
    # CRAWL COMMAND
    # ========================================================================
    crawl_parser = subparsers.add_parser(
        'crawl',
        help='Start crawling websites',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='Start crawling from one or more seed URLs'
    )
    
    crawl_parser.add_argument(
        'urls',
        nargs='+',
        help='Seed URL(s) to start crawling from'
    )
    
    crawl_parser.add_argument(
        '-c', '--config',
        help='Path to YAML configuration file (default: use built-in defaults)'
    )
    
    crawl_parser.add_argument(
        '-d', '--max-depth',
        type=int,
        metavar='N',
        help='Maximum crawl depth (0 = seed URLs only)'
    )
    
    crawl_parser.add_argument(
        '-p', '--max-pages',
        type=int,
        metavar='N',
        help='Maximum number of pages to crawl'
    )
    
    crawl_parser.add_argument(
        '--allowed-domains',
        nargs='+',
        metavar='DOMAIN',
        help='Only crawl URLs from these domains (whitelist)'
    )
    
    crawl_parser.add_argument(
        '-t', '--timeout',
        type=float,
        metavar='SECONDS',
        help='Maximum crawl time in seconds'
    )
    
    crawl_parser.add_argument(
        '--user-agent',
        metavar='STRING',
        help='Custom User-Agent string'
    )
    
    crawl_parser.add_argument(
        '--delay',
        type=float,
        metavar='SECONDS',
        help='Delay between requests to same domain (seconds)'
    )
    
    crawl_parser.set_defaults(func=crawl_command)
    
    # ========================================================================
    # CONFIG COMMAND
    # ========================================================================
    config_parser = subparsers.add_parser(
        'config',
        help='Configuration management',
        description='Create or validate configuration files'
    )
    
    config_parser.add_argument(
        '--create-default',
        action='store_true',
        help='Create a default configuration file'
    )
    
    config_parser.add_argument(
        '--validate',
        metavar='FILE',
        help='Validate a configuration file'
    )
    
    config_parser.add_argument(
        '-o', '--output',
        metavar='FILE',
        help='Output path for created configuration (default: config/default.yaml)'
    )
    
    config_parser.set_defaults(func=config_command)
    
    # ========================================================================
    # PARSE AND EXECUTE
    # ========================================================================
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Execute command
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == '__main__':
    main()