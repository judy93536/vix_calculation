import os
import shutil
import zipfile
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
import logging
import yaml
from typing import Optional, Dict, Tuple

class CboeDataProcessor:
    """Processes CBOE options data and inserts into database"""
    
    def __init__(self, config_path: str):
        """Initialize processor with configuration"""
        self.config_path = config_path
        self.config = self._load_config()
        self.engine = self._create_db_engine()
        self.logger = logging.getLogger(__name__)
        
        # Set up paths
        base_path = Path(self.config['paths']['spx']['base'])
        self.paths = {
            'import': base_path / 'import',
            'import_csv': base_path / 'import_csv',
            'zip': base_path / 'zip',
            'csv': base_path / 'csv'
        }
        
        # Ensure directories exist
        for path in self.paths.values():
            path.mkdir(parents=True, exist_ok=True)

    def _load_config(self) -> Dict:
        """Load configuration from yaml file"""
        try:
            with open(self.config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise Exception(f"Failed to load config from {self.config_path}: {str(e)}")

    
    def _create_db_engine(self) -> create_engine:
        """Create SQLAlchemy engine from config with timezone settings"""
        db_config = self.config['database']['postgres']
        db_url = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            f"?options=-c%20timezone=America/New_York"
        )
        return create_engine(db_url)

    def unzip_files(self) -> Tuple[int, int]:
        """
        Unzip files from import directory to import_csv directory
        Returns tuple of (success_count, failure_count)
        """
        success = 0
        failed = 0
        
        try:
            # Get all zip files in import directory
            zip_files = list(self.paths['import'].glob('*.zip'))
            self.logger.info(f"Found {len(zip_files)} zip files to extract")
            
            for zip_path in zip_files:
                try:
                    self.logger.info(f"Extracting {zip_path.name}")
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(self.paths['import_csv'])
                    
                    # Move zip file to archive directory
                    archive_path = self.paths['zip'] / zip_path.name
                    zip_path.rename(archive_path)
                    
                    self.logger.info(f"Successfully extracted {zip_path.name}")
                    success += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to extract {zip_path.name}: {str(e)}")
                    failed += 1
            
            return success, failed
            
        except Exception as e:
            self.logger.error(f"Unzip process failed: {str(e)}")
            return success, failed

    

    def process_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Process a single options file"""
        try:
            self.logger.info(f"Processing file: {file_path}")
            
            # Read CSV file
            df2 = pd.read_csv(file_path, encoding='utf-8')
            self.logger.info(f"Original quote_date from CSV: {df2['quote_date'].iloc[0]}")                 
            
            # Split into calls and puts
            CALLS = df2[df2.option_type == "C"]
            PUTS = df2[df2.option_type == "P"]
            
            # Remove option_type and delivery_code
            del CALLS['option_type']
            del CALLS['delivery_code']
            del PUTS['option_type']
            del PUTS['delivery_code']
            
            # Merge calls and puts
            df_pc = pd.merge(CALLS, PUTS, 
                            on=['underlying_symbol', 'quote_date', 'root', 'expiration', 'strike'])
            
            # Define column order
            cols = ['symbol', 'quote_date', 'root', 'expiry', 'strike',
                    'open_c', 'high_c', 'low_c', 'close_c',
                    'trade_volume_c', 'bid_size_1545_c', 'bid_1545_c', 'ask_size_1545_c',
                    'ask_1545_c', 'underlying_bid_1545_c', 'underlying_ask_1545_c',
                    'implied_underlying_price_1545_c', 'active_underlying_price_1545_c',
                    'implied_volatility_1545_c', 'delta_1545_c', 'gamma_1545_c',
                    'theta_1545_c', 'vega_1545_c', 'rho_1545_c', 'bid_size_eod_c',
                    'bid_eod_c', 'ask_size_eod_c', 'ask_eod_c', 'underlying_bid_eod_c',
                    'underlying_ask_eod_c', 'vwap_c', 'open_interest_c',
                    'open_p', 'high_p', 'low_p', 'close_p',
                    'trade_volume_p', 'bid_size_1545_p', 'bid_1545_p', 'ask_size_1545_p',
                    'ask_1545_p', 'underlying_bid_1545_p', 'underlying_ask_1545_p',
                    'implied_underlying_price_1545_p', 'active_underlying_price_1545_p',
                    'implied_volatility_1545_p', 'delta_1545_p', 'gamma_1545_p',
                    'theta_1545_p', 'vega_1545_p', 'rho_1545_p', 'bid_size_eod_p',
                    'bid_eod_p', 'ask_size_eod_p', 'ask_eod_p', 'underlying_bid_eod_p',
                    'underlying_ask_eod_p', 'vwap_p', 'open_interest_p']
            
            # Rename columns
            df_pc.columns = cols
            
            self.logger.info(f"Processing file: {file_path}")
        
    
            # Handle dates with explicit timezone
            df_pc['quote_date'] = pd.to_datetime(df_pc.quote_date).dt.tz_localize('America/New_York')
            df_pc['expiry'] = pd.to_datetime(df_pc.expiry).dt.tz_localize('America/New_York')
            
            # Add market close time (16:00)
            df_pc['quote_date'] = df_pc['quote_date'].apply(lambda x: x.replace(hour=16))
            df_pc['expiry'] = df_pc['expiry'].apply(lambda x: x.replace(hour=16))

            # Add debug logs
            self.logger.info(f"Final timestamps (PT):")
            self.logger.info(f"quote_date: {df_pc['quote_date'].iloc[0]}")
            self.logger.info(f"expiry: {df_pc['expiry'].iloc[0]}")
            

            
            # Add ddate
            ddate = df_pc.quote_date.dt.strftime('%Y%m%d')
            df_pc.insert(2, 'ddate', np.array(ddate))
            df_pc['ddate'] = df_pc['ddate'].astype(int)
            
            # Add dte
            df_pc.insert(5, 'dte', (df_pc.expiry-df_pc.quote_date).dt.days)
            
            # Calculate mid prices and differences
            df_pc.insert(61, 'mid_eod_c', ((df_pc.bid_eod_c + df_pc.ask_eod_c)/2).round(5))
            df_pc.insert(62, 'mid_eod_p', ((df_pc.bid_eod_p + df_pc.ask_eod_p)/2).round(5))
            df_pc.insert(63, 'mid_diff_eod', np.round(np.abs(df_pc.mid_eod_c - df_pc.mid_eod_p),5))
            
            df_pc.insert(64, 'mid_1545_c', ((df_pc.bid_1545_c + df_pc.ask_1545_c)/2).round(5))
            df_pc.insert(65, 'mid_1545_p', ((df_pc.bid_1545_p + df_pc.ask_1545_p)/2).round(5))
            df_pc.insert(66, 'mid_diff_1545', np.round(np.abs(df_pc.mid_1545_c - df_pc.mid_1545_p),5))
            
            self.logger.info(f"Successfully processed {file_path}")
            
            
            self.logger.info(f"Final quote_date before insert: {df_pc['quote_date'].iloc[0]}")
            
            return df_pc
            
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
        
    
    def insert_to_db(self, df: pd.DataFrame) -> bool:
        """Insert processed DataFrame into database"""
        try:
            # Update table name
            table_name = 'spx_eod_daily_options'  # Changed from spx_1545_eod_new
            with self.engine.begin() as conn:
                df.to_sql(table_name, conn, if_exists='append', index=False)
            self.logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Database insertion failed: {str(e)}")
            return False


    def process_directory(self, dir_path: Path) -> Tuple[int, int]:
        """Temporary process_directory to handle batch CSV import"""
        try:
            
            # First unzip files if any exist
            unzipped, failed_unzip = self.unzip_files()
            if failed_unzip > 0:
                self.logger.error(f"Some files failed to unzip: {failed_unzip}")
                return 0, failed_unzip
            
            # Override dir_path to use import_csv
            import_csv_dir = Path('/raid/Python/CBOE_VIX/SPX/spx_eod_1545/import_csv')
            self.logger.info(f"Checking directory: {import_csv_dir}")
            
            csv_files = sorted(import_csv_dir.glob('*.csv'))
            total_files = len(list(csv_files))
            self.logger.info(f"Found {total_files} CSV files to process")
            
            # Print first few files for verification
            for file in list(csv_files)[:5]:
                self.logger.info(f"Found file: {file}")
                
            processed = 0
            failed = 0
            
            for file_path in csv_files:
                self.logger.info(f"Processing file: {file_path}")
                df = self.process_file(file_path)
                if df is not None:
                    if self.insert_to_db(df):
                        processed += 1
                        # Move processed CSV to final csv directory
                        final_path = self.paths['csv'] / file_path.name
                        file_path.rename(final_path)
                    else:
                        failed += 1
                else:
                    failed += 1
                        
            self.logger.info(f"Batch processing complete: {processed} successful, {failed} failed")
            return processed, failed
                
        except Exception as e:
            self.logger.error(f"Directory processing failed: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            return 0, 1

    