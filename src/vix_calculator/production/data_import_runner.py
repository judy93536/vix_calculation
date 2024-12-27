#!/usr/bin/env python3
from pathlib import Path
import logging
from datetime import datetime
import argparse

from ..data.importers.cboe_options_importer import CboeOptionsImporter
from ..data.importers.treasury_rates_importer import TreasuryRatesImporter
from ..data.importers.market_data_importer import MarketDataImporter
from ..data.processors.cboe_processor import CboeDataProcessor  # Add this line


def setup_logging():
    """Setup logging for the import process"""
    log_dir = Path('results/logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'data_import_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Also print to console
        ]
    )
    return logging.getLogger(__name__)
    
       
def run_cboe_import(config_path: Path, logger: logging.Logger) -> bool:
    """Run CBOE options data import and processing"""
    try:
        logger.info("Starting CBOE options data import")
        importer = CboeOptionsImporter(str(config_path))
        
        # Download new files
        downloaded, failed = importer.download_new_files()
        logger.info(f"Download complete: {downloaded} files downloaded, {failed} files failed")
        
        if downloaded > 0:
            logger.info("Starting data processing")
            processor = CboeDataProcessor(str(config_path))
            import_dir = Path(importer.paths['import'])
            
            processed, failed_processing = processor.process_directory(import_dir)
            logger.info(f"Processing complete: {processed} files processed, {failed_processing} files failed")
            
            return processed > 0 and failed_processing == 0
            
        return downloaded > 0 and failed == 0
        
    except Exception as e:
        logger.error(f"CBOE import/processing failed: {str(e)}")
        return False  

def run_treasury_import(config_path: Path, logger: logging.Logger) -> bool:
    """Run Treasury rates import"""
    try:
        logger.info("Starting Treasury rates import")
        importer = TreasuryRatesImporter(str(config_path))
        
        success = importer.import_rates()
        logger.info(f"Treasury rates import {'successful' if success else 'failed'}")
        return success
        
    except Exception as e:
        logger.error(f"Treasury rates import failed: {str(e)}")
        return False

def run_market_import(config_path: Path, logger: logging.Logger) -> bool:
    """Run market data import"""
    try:
        logger.info("Starting market data import")
        importer = MarketDataImporter(str(config_path))
        
        results = importer.import_all()
        success = all(results.values())
        
        for symbol, result in results.items():
            logger.info(f"{symbol} import {'successful' if result else 'failed'}")
            
        return success
        
    except Exception as e:
        logger.error(f"Market data import failed: {str(e)}")
        return False

def run_imports(config_path: Path, logger: logging.Logger) -> bool:
    """Run all data imports in sequence"""
    success = True
    
    # Run CBOE import first
    if run_cboe_import(config_path, logger):
        logger.info("CBOE import successful")
    else:
        logger.error("CBOE import failed")
        success = False

    # Run Treasury rates import
    if run_treasury_import(config_path, logger):
        logger.info("Treasury rates import successful")
    else:
        logger.error("Treasury rates import failed")
        success = False

    # Run market data import
    if run_market_import(config_path, logger):
        logger.info("Market data import successful")
    else:
        logger.error("Market data import failed")
        success = False
    
    return success

def main():
    parser = argparse.ArgumentParser(description='Run VIX data imports')
    parser.add_argument('--config', type=str, default='config/config.yaml',
                      help='Path to config file')
    parser.add_argument('--cboe-only', action='store_true',
                      help='Only run CBOE options import')
    parser.add_argument('--skip-market', action='store_true',
                      help='Skip market data import')
    parser.add_argument('--skip-treasury', action='store_true',
                      help='Skip treasury rates import')
    args = parser.parse_args()
    
    logger = setup_logging()
    config_path = Path(args.config)
    
    if not config_path.exists():
        logger.error(f"Config file not found: {config_path}")
        return

    if args.cboe_only:
        success = run_cboe_import(config_path, logger)
    else:
        success = run_imports(config_path, logger)
    
    if success:
        logger.info("All requested imports completed successfully")
        # Here we could add the call to the VIX calculator
    else:
        logger.error("One or more imports failed")

if __name__ == "__main__":
    main()
