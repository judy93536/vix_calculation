from abc import ABC, abstractmethod
import logging
from pathlib import Path
import yaml
from typing import Dict, Any

class BaseImporter(ABC):
    """Base class for all data importers"""
    
    def __init__(self, config_path: str):
        """Initialize base importer with configuration"""
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = self._setup_logging()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from yaml file"""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise Exception(f"Failed to load config from {self.config_path}: {str(e)}")

    def _setup_logging(self) -> logging.Logger:
        """Setup logging with importer-specific configuration"""
        logger = logging.getLogger(self.__class__.__name__)
        
        # If logger already has handlers, don't add more
        if not logger.handlers:
            # Get log level from config or default to INFO
            log_level = self.config.get('logging', {}).get('level', 'INFO')
            logger.setLevel(log_level)
        
        return logger

    @abstractmethod
    def disconnect(self):
        """Cleanup method to be implemented by child classes"""
        pass

















# from abc import ABC, abstractmethod
# import logging
# import os
# import yaml
# from datetime import datetime
# from typing import Dict, Any, Optional
# import pandas as pd
# from pathlib import Path

# class BaseImporter(ABC):
#     """Base class for all data importers"""
    
#     def __init__(self, config_path: str):
#         """
#         Initialize base importer with configuration
        
#         Args:
#             config_path: Path to the config.yaml file
#         """
#         self.config = self._load_config(config_path)
#         self.logger = self._setup_logging()
    
#     def _setup_logging(self) -> logging.Logger:
#         """Setup logging with importer-specific configuration"""
#         # Get importer name from class (e.g., 'CboeOptionsImporter', 'TreasuryRatesImporter')
#         importer_name = self.__class__.__name__.lower()
        
#         # Create logs directory in results/logs if it doesn't exist
#         log_dir = Path('results/logs')
#         log_dir.mkdir(parents=True, exist_ok=True)
        
#         # Create timestamp for log filename
#         timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
#         # Create log filename with importer name
#         log_file = f"{importer_name}_{timestamp}.log"
#         log_path = log_dir / log_file
        
#         # Setup logger
#         logger = logging.getLogger(importer_name)
        
#         # Remove any existing handlers to avoid duplicates
#         if logger.hasHandlers():
#             logger.handlers.clear()
        
#         # Create file handler
#         file_handler = logging.FileHandler(log_path)
        
#         # Create console handler
#         console_handler = logging.StreamHandler()
        
#         # Create formatter
#         formatter = logging.Formatter(
#             '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#         )
        
#         # Set formatter for both handlers
#         file_handler.setFormatter(formatter)
#         console_handler.setFormatter(formatter)
        
#         # Add handlers to logger
#         logger.addHandler(file_handler)
#         logger.addHandler(console_handler)
        
#         # Set level from config, default to INFO if not specified
#         log_level = self.config.get('logging', {}).get('level', 'INFO')
#         logger.setLevel(log_level)
        
#         # Log initialization
#         logger.info(f"Initialized {importer_name}")
#         logger.info(f"Log file created at: {log_path}")
        
#         return logger


