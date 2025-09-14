#!/usr/bin/env python3
"""
Main entry point for the Dialogs RAG System v2.0
Advanced RAG system for dialog analysis and entity extraction
"""

import os
import sys
import argparse
from pathlib import Path
from loguru import logger
import pandas as pd

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from src.config import ConfigManager
from src.pipeline import MainPipeline


def setup_logging(config_manager: ConfigManager) -> None:
    """Setup logging configuration
    
    Args:
        config_manager: Configuration manager instance
    """
    logging_config = config_manager.get_logging_config()
    
    # Remove default logger
    logger.remove()
    
    # Add console logger
    logger.add(
        sys.stderr,
        level=logging_config.level,
        format=logging_config.format,
        colorize=True
    )
    
    # Add file logger
    log_file = Path(logging_config.file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    
    logger.add(
        log_file,
        level=logging_config.level,
        format=logging_config.format,
        rotation=logging_config.max_size,
        retention=logging_config.retention
    )


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Dialogs RAG System v2.0 - Advanced dialog analysis and entity extraction"
    )
    
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Input file path (Excel, CSV, or JSON) or directory containing dialog files"
    )
    
    parser.add_argument(
        "--config",
        "-c",
        default="config.yaml",
        help="Path to configuration file (default: config.yaml)"
    )
    
    parser.add_argument(
        "--output",
        "-o",
        help="Output directory (default: from config)"
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform a dry run without processing data"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize configuration
        config_manager = ConfigManager(args.config)
        
        # Override output directory if specified
        if args.output:
            config_manager.config.paths.output_dir = args.output
        
        # Override log level if verbose
        if args.verbose:
            config_manager.config.logging.level = "DEBUG"
        
        # Setup logging
        setup_logging(config_manager)
        
        # Create necessary directories
        config_manager.create_directories()
        
        logger.info("Starting Dialogs RAG System v2.0")
        logger.info(f"Configuration loaded from: {args.config}")
        logger.info(f"Input: {args.input}")
        logger.info(f"Output: {config_manager.get_paths_config().output_dir}")
        
        if args.dry_run:
            logger.info("Dry run mode - no data will be processed")
            return
        
        # Initialize main pipeline
        pipeline = MainPipeline(config_manager)
        
        # Check pipeline status
        status = pipeline.get_pipeline_status()
        logger.info(f"Pipeline status: {status}")
        
        # Process data
        logger.info("Starting data processing...")
        results = pipeline.process(args.input)
        
        # Log results
        logger.info("Processing completed successfully!")
        logger.info(f"Results summary: {results.get('final_results', {})}")
        
        # Print statistics
        stats = results.get('statistics', {})
        logger.info(f"Pipeline duration: {stats.get('pipeline_duration', 'Unknown')}")
        logger.info(f"Stages completed: {stats.get('stages_completed', 0)}")
        logger.info(f"Stages failed: {stats.get('stages_failed', 0)}")
        
        if results.get('errors'):
            logger.warning(f"Errors encountered: {len(results['errors'])}")
            for error in results['errors']:
                logger.warning(f"  - {error}")
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
