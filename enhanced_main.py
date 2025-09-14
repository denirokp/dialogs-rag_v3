#!/usr/bin/env python3
"""
Enhanced main entry point for the Dialogs RAG System v2.0
Advanced RAG system with improved entity extraction quality
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
from src.pipeline.enhanced_main_pipeline import EnhancedMainPipeline


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
        description="Enhanced Dialogs RAG System v2.0 - Advanced dialog analysis with improved quality"
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
    
    parser.add_argument(
        "--quality",
        choices=["standard", "enhanced"],
        default="enhanced",
        help="Entity extraction quality: standard (fast) or enhanced (high quality, default)"
    )
    
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare standard vs enhanced extraction quality"
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
        
        logger.info("Starting Enhanced Dialogs RAG System v2.0")
        logger.info(f"Configuration loaded from: {args.config}")
        logger.info(f"Input: {args.input}")
        logger.info(f"Output: {config_manager.get_paths_config().output_dir}")
        logger.info(f"Quality mode: {args.quality}")
        
        if args.dry_run:
            logger.info("Dry run mode - no data will be processed")
            return
        
        # Choose pipeline based on quality setting
        if args.quality == "enhanced":
            pipeline = EnhancedMainPipeline(config_manager, use_enhanced_extractor=True)
        else:
            pipeline = EnhancedMainPipeline(config_manager, use_enhanced_extractor=False)
        
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
        
        # Print quality metrics
        if 'quality_improvements' in results.get('final_results', {}):
            quality = results['final_results']['quality_improvements']
            logger.info(f"Quality metrics:")
            logger.info(f"  Extractor type: {quality.get('extractor_type', 'unknown')}")
            logger.info(f"  Structured entities: {quality.get('structured_entities', 0)}")
            logger.info(f"  Quality score: {quality.get('quality_score', 0):.1f}%")
            logger.info(f"  Business relevance: {quality.get('business_relevance', 'unknown')}")
        
        if results.get('errors'):
            logger.warning(f"Errors encountered: {len(results['errors'])}")
            for error in results['errors']:
                logger.warning(f"  - {error}")
        
        # Compare quality if requested
        if args.compare:
            logger.info("Running quality comparison...")
            compare_quality(args.input, config_manager)
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


def compare_quality(input_path: str, config_manager: ConfigManager):
    """Compare standard vs enhanced extraction quality
    
    Args:
        input_path: Path to input data
        config_manager: Configuration manager instance
    """
    try:
        from src.pipeline import DialogProcessor
        
        # Load and process data
        dialog_processor = DialogProcessor(config_manager)
        data = dialog_processor.process(input_path)
        
        # Test with small sample
        sample_data = data.head(3)  # Test with first 3 dialogs
        
        logger.info("Comparing extraction quality...")
        
        # Test standard extractor
        from src.pipeline import EntityExtractor
        standard_extractor = EntityExtractor(config_manager)
        
        # Test enhanced extractor
        from src.pipeline.enhanced_entity_extractor import EnhancedEntityExtractor
        enhanced_extractor = EnhancedEntityExtractor(config_manager)
        
        # Process sample with both extractors
        import asyncio
        
        async def run_comparison():
            standard_results = []
            enhanced_results = []
            
            for _, row in sample_data.iterrows():
                # Standard extraction
                standard_result = await standard_extractor._extract_entities_from_dialog(row['text'], row['dialog_id'])
                standard_results.append(standard_result)
                
                # Enhanced extraction
                enhanced_result = await enhanced_extractor._extract_entities_enhanced(row['text'], row['dialog_id'])
                enhanced_results.append(enhanced_result)
            
            return standard_results, enhanced_results
        
        # Run comparison
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        standard_results, enhanced_results = loop.run_until_complete(run_comparison())
        loop.close()
        
        # Analyze results
        standard_success = sum(1 for r in standard_results if r['extraction_success'])
        enhanced_success = sum(1 for r in enhanced_results if r['extraction_success'])
        
        standard_entities = sum(len(r['entities']) for r in standard_results if r['extraction_success'])
        enhanced_entities = sum(len(r['entities']) for r in enhanced_results if r['extraction_success'])
        
        logger.info("Quality Comparison Results:")
        logger.info(f"  Standard extractor: {standard_success}/{len(sample_data)} successful, {standard_entities} entities")
        logger.info(f"  Enhanced extractor: {enhanced_success}/{len(sample_data)} successful, {enhanced_entities} entities")
        logger.info(f"  Quality improvement: {enhanced_entities - standard_entities} more entities")
        
    except Exception as e:
        logger.error(f"Quality comparison failed: {e}")


if __name__ == "__main__":
    main()
