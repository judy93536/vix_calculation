from pathlib import Path
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from .base_importer import BaseImporter
from typing import Dict, List, Optional

class MarketDataImporter(BaseImporter):
    """Imports market data from Yahoo Finance"""
    
    def __init__(self, config_path: str):
        """Initialize with configuration"""
        super().__init__(config_path)
        self.engine = self._create_db_engine()
        
        # Get symbols from config
        self.symbols = self.config['sources']['market_data']['yahoo']['symbols']
        self.tables = {
            '^VIX': 'vix_data',
            '^SPX': 'spx_data'
        }

    def _create_db_engine(self):
        """Create SQLAlchemy engine from config"""
        db_config = self.config['database']['postgres']
        db_url = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        return create_engine(db_url)

    def download_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Download data from Yahoo Finance
        
        Args:
            symbol: Stock symbol (e.g., "^VIX", "^SPX")
            
        Returns:
            DataFrame or None if download fails
        """
        try:
            self.logger.info(f"Downloading data for {symbol}...")
            data = yf.download(symbol, period="max")
            
            if data.empty:
                self.logger.warning(f"No data found for {symbol}")
                return None
                
            # Flatten MultiIndex columns if present
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            
            # Reset index and format columns
            data.reset_index(inplace=True)
            data.rename(columns={'Date': 'date'}, inplace=True)
            data.columns = [col.lower().replace(" ", "") for col in data.columns]
            
            # Add ddate column
            data['ddate'] = pd.to_datetime(data['date']).dt.strftime('%Y%m%d').astype(int)
            
            self.logger.info(f"Successfully downloaded {len(data)} rows for {symbol}")
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to download data for {symbol}: {str(e)}")
            return None

    def store_data(self, data: pd.DataFrame, table_name: str) -> bool:
        """
        Store data in PostgreSQL
        
        Args:
            data: DataFrame to store
            table_name: Target table name
            
        Returns:
            bool: True if successful
        """
        try:
            self.logger.info(f"Storing {len(data)} rows in {table_name}...")
            
            with self.engine.begin() as conn:
                data.to_sql(table_name, conn, if_exists='replace', index=False)
                
            self.logger.info(f"Successfully stored data in {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store data in {table_name}: {str(e)}")
            return False

    def import_all(self) -> Dict[str, bool]:
        """
        Import all configured market data
        
        Returns:
            Dict mapping symbols to success status
        """
        results = {}
        
        for symbol in self.symbols:
            if symbol not in self.tables:
                self.logger.warning(f"No table mapping for symbol {symbol}")
                continue
                
            data = self.download_data(symbol)
            if data is not None:
                results[symbol] = self.store_data(data, self.tables[symbol])
            else:
                results[symbol] = False
        
        return results

    def disconnect(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()

def main():
    """Run market data import"""
    config_path = 'config/config.yaml'
    importer = MarketDataImporter(config_path)
    
    try:
        results = importer.import_all()
        for symbol, success in results.items():
            print(f"{symbol}: {'Success' if success else 'Failed'}")
    finally:
        importer.disconnect()

if __name__ == "__main__":
    main()


