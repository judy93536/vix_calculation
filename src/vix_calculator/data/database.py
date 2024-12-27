import os
from typing import Optional
from sqlalchemy import create_engine, Engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from dotenv import load_dotenv

class DatabaseConnection:
    """
    Manages database connections and queries for the VIX calculator.
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database connection.
        
        Args:
            connection_string: Optional SQLAlchemy connection string.
                             If not provided, will use environment variables.
        """
        self.engine = self._create_engine(connection_string)
        
    def _create_engine(self, connection_string: Optional[str] = None) -> Engine:
        """
        Create SQLAlchemy engine from connection string or environment variables.
        """
        if connection_string is None:
            load_dotenv()
            connection_string = (
                f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
                f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
            )
        
        try:
            engine = create_engine(connection_string)
            engine.connect()
            return engine
        except SQLAlchemyError as e:
            raise ConnectionError(f"Failed to connect to database: {e}")
            
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute("SELECT 1")
            return True
        except SQLAlchemyError:
            return False
            
    def get_engine(self) -> Engine:
        """Get SQLAlchemy engine."""
        return self.engine
        
    def close(self):
        """Close database connection."""
        if self.engine:
            self.engine.dispose()

class OptionDataRepository:
    """
    Repository for accessing option data from the database.
    """
    
    def __init__(self, engine: Engine):
        """
        Initialize repository with database engine.
        """
        self.engine = engine
        
    def get_spx_options(self, date: int, min_dte: int = 22, max_dte: int = 38) -> pd.DataFrame:
        """
        Get SPX option data for a specific date within DTE range.
        
        Args:
            date: Trading date in YYYYMMDD format
            min_dte: Minimum days to expiration
            max_dte: Maximum days to expiration
            
        Returns:
            DataFrame containing option data
        """
        query = """
        SELECT quote_date, ddate, symbol, root, expiry, dte, strike,
               bid_eod_c, mid_eod_c, ask_eod_c, bid_eod_p, mid_eod_p, ask_eod_p,
               mid_diff_eod, open_interest_c, open_interest_p, trade_volume_c, trade_volume_p,
               implied_volatility_1545_c, implied_volatility_1545_p,
               active_underlying_price_1545_c, active_underlying_price_1545_p
        FROM spx_eod_daily_options
        WHERE ddate = %(date)s
        AND dte > %(min_dte)s AND dte < %(max_dte)s
        AND bid_eod_c != 0 AND bid_eod_p != 0
        """
        try:
            with self.engine.connect() as conn:
                return pd.read_sql_query(query, conn, params={
                    'date': date,
                    'min_dte': min_dte,
                    'max_dte': max_dte
                })
        except SQLAlchemyError as e:
            raise RuntimeError(f"Failed to fetch option data: {e}")
            
    def get_trade_dates(self, start_date: int, end_date: int) -> pd.DataFrame:
        """
        Get all trading dates between start_date and end_date.
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            DataFrame containing trading dates
        """
        query = """
        SELECT DISTINCT ddate
        FROM spx_eod_daily_options
        WHERE ddate BETWEEN %(start_date)s AND %(end_date)s
        ORDER BY ddate
        """
        with self.engine.connect() as conn:
            return pd.read_sql_query(query, conn, params={
                'start_date': start_date,
                'end_date': end_date
            })