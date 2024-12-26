import pandas as pd
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from sqlalchemy import create_engine, text
from .base_importer import BaseImporter
from typing import Optional, Tuple
import time

class TreasuryRatesImporter(BaseImporter):
    """Imports Treasury rates data from treasury.gov"""
    
    def __init__(self, config_path: str):
        """Initialize with configuration"""
        super().__init__(config_path)
        self.engine = self._create_db_engine()
        self.ns = {
            "atom": "http://www.w3.org/2005/Atom",
            "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
            "d": "http://schemas.microsoft.com/ado/2007/08/dataservices"
        }

    def _create_db_engine(self):
        """Create SQLAlchemy engine from config"""
        db_config = self.config['database']['postgres']
        db_url = (
            f"postgresql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        return create_engine(db_url)

    def get_last_update(self) -> datetime:
        """Get the latest date in the database"""
        try:
            with self.engine.connect() as conn:
                last_update = conn.execute(text(
                    "SELECT MAX(date) FROM daily_treasury_par_yield;"
                )).scalar()
                
            if last_update is None:
                self.logger.info("No existing data found, starting from 2017-01-01")
                return datetime(2017, 1, 1)
            
            self.logger.info(f"Last update in database: {last_update}")
            return last_update
            
        except Exception as e:
            self.logger.error(f"Error getting last update date: {str(e)}")
            return datetime(2017, 1, 1)

    def fetch_year_data(self, year: int) -> Optional[pd.DataFrame]:
        """
        Fetch Treasury rate data for a specific year
        
        Args:
            year: Year to fetch
            
        Returns:
            DataFrame or None if fetch fails
        """
        url = (f"https://home.treasury.gov/resource-center/data-chart-center/"
               f"interest-rates/pages/xmlview?data=daily_treasury_yield_curve&"
               f"field_tdr_date_value={year}")
        
        try:
            self.logger.info(f"Fetching data for {year}...")
            opener = urllib.request.build_opener()
            tree = ET.parse(opener.open(url))
            root = tree.getroot()
            
            data = []
            for entry in root.findall("atom:entry", self.ns):
                for content in entry.findall("atom:content", self.ns):
                    for prop in content.findall("m:properties", self.ns):
                        row = {
                            "date": prop.find("d:NEW_DATE", self.ns).text[:10] if prop.find("d:NEW_DATE", self.ns) is not None else None,
                            "1mo": prop.find("d:BC_1MONTH", self.ns).text if prop.find("d:BC_1MONTH", self.ns) is not None else None,
                            "2mo": prop.find("d:BC_2MONTH", self.ns).text if prop.find("d:BC_2MONTH", self.ns) is not None else None,
                            "3mo": prop.find("d:BC_3MONTH", self.ns).text if prop.find("d:BC_3MONTH", self.ns) is not None else None,
                            "6mo": prop.find("d:BC_6MONTH", self.ns).text if prop.find("d:BC_6MONTH", self.ns) is not None else None,
                            "1yr": prop.find("d:BC_1YEAR", self.ns).text if prop.find("d:BC_1YEAR", self.ns) is not None else None,
                            "2yr": prop.find("d:BC_2YEAR", self.ns).text if prop.find("d:BC_2YEAR", self.ns) is not None else None,
                            "3yr": prop.find("d:BC_3YEAR", self.ns).text if prop.find("d:BC_3YEAR", self.ns) is not None else None,
                            "5yr": prop.find("d:BC_5YEAR", self.ns).text if prop.find("d:BC_5YEAR", self.ns) is not None else None,
                            "7yr": prop.find("d:BC_7YEAR", self.ns).text if prop.find("d:BC_7YEAR", self.ns) is not None else None,
                            "10yr": prop.find("d:BC_10YEAR", self.ns).text if prop.find("d:BC_10YEAR", self.ns) is not None else None,
                            "20yr": prop.find("d:BC_20YEAR", self.ns).text if prop.find("d:BC_20YEAR", self.ns) is not None else None,
                            "30yr": prop.find("d:BC_30YEAR", self.ns).text if prop.find("d:BC_30YEAR", self.ns) is not None else None,
                        }
                        data.append(row)
            
            if not data:
                self.logger.warning(f"No data found for {year}")
                return None
                
            df = pd.DataFrame(data)
            df["date"] = pd.to_datetime(df["date"])
            df.set_index("date", inplace=True)
            df = df.astype(float)
            
            self.logger.info(f"Successfully fetched {len(df)} rows for {year}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching data for {year}: {str(e)}")
            return None

    def store_new_data(self, df: pd.DataFrame) -> bool:
        """
        Store new data in database
        
        Args:
            df: DataFrame to store
            
        Returns:
            bool: True if successful
        """
        try:
            self.logger.info(f"Storing {len(df)} rows in database...")
            
            with self.engine.begin() as conn:
                df.to_sql(
                    "daily_treasury_par_yield",
                    con=conn,
                    if_exists="append",
                    index=True
                )
                
            self.logger.info("Successfully stored new data")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store data: {str(e)}")
            return False


    def import_rates(self, year: Optional[int] = None) -> bool:
        """
        Import Treasury rates for specified year or current year
        """
        try:
            # Get current year if not specified
            year = year or datetime.now().year
            
            # Get last update and convert to pandas Timestamp
            last_update = pd.Timestamp(self.get_last_update())  # Convert to pandas Timestamp
            
            # Fetch data
            df = self.fetch_year_data(year)
            if df is None:
                return False
                
            # Filter new data
            df = df[df.index > last_update]  # Now comparing same types
            
            if df.empty:
                self.logger.info("No new data to import")
                return True
                
            # Store new data
            return self.store_new_data(df)
            
        except Exception as e:
            self.logger.error(f"Import failed: {str(e)}")
            return False
        

    def disconnect(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()

def main():
    """Run Treasury rates import"""
    config_path = 'config/config.yaml'
    importer = TreasuryRatesImporter(config_path)
    
    try:
        success = importer.import_rates()
        print(f"Import {'successful' if success else 'failed'}")
    finally:
        importer.disconnect()

if __name__ == "__main__":
    main()


