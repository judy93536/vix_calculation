from typing import Optional, Dict
import pandas as pd
from datetime import datetime, date
from sqlalchemy import Engine, text

class MarketDataProvider:
    """
    Provides market data from database for validation and comparison.
    """
    
    def __init__(self, engine: Engine):
        """
        Initialize with database connection.
        
        Args:
            engine: SQLAlchemy database engine
        """
        self.engine = engine
        self._vix_cache = None
        self._spx_cache = None
        self._initialize_caches()
    
    def _initialize_caches(self):
        """Initialize data caches from database"""
        self.load_vix_data()
        self.load_spx_data()
        
    def load_vix_data(self):
        """Load VIX data from database into cache"""
        query = """
        SELECT CAST(date as DATE) as date, close 
        FROM vix_data 
        ORDER BY date
        """
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
                # Convert to datetime and then to date
                df['date'] = pd.to_datetime(df['date']).dt.date
                self._vix_cache = df.set_index('date')
                print(f"Loaded {len(self._vix_cache)} VIX records")
        except Exception as e:
            print(f"Error loading VIX data: {e}")
            self._vix_cache = pd.DataFrame()
    
    def get_vix_value(self, query_date: date) -> Optional[float]:
        """
        Get VIX closing value for specific date.
        
        Args:
            query_date: Date to get VIX value for
            
        Returns:
            VIX closing value or None if not available
        """
        if self._vix_cache is None or self._vix_cache.empty:
            self.load_vix_data()
            
        try:
            # Convert datetime to date if needed
            if isinstance(query_date, datetime):
                query_date = query_date.date()
                
            # Get scalar value first, then convert to float
            scalar_value = self._vix_cache.loc[query_date, 'close']
            return float(scalar_value)
        except KeyError:
            # Only print for dates within our expected range (e.g., after 2018)
            if query_date.year >= 2018:
                print(f"No VIX data for {query_date}")
            return None
        except Exception as e:
            print(f"Error getting VIX data for {query_date}: {e}")
            return None   
        
    def load_spx_data(self):
        """Load SPX data from database into cache"""
        query = """
        SELECT CAST(date as DATE) as date, close 
        FROM spx_data 
        ORDER BY date
        """
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_query(query, conn)
                df['date'] = pd.to_datetime(df['date']).dt.date
                self._spx_cache = df.set_index('date')
                print(f"Loaded {len(self._spx_cache)} SPX records")
        except Exception as e:
            print(f"Error loading SPX data: {e}")
            self._spx_cache = pd.DataFrame()
    
    def get_spx_value(self, query_date: date) -> Optional[float]:
        """
        Get SPX closing value for specific date.
        
        Args:
            query_date: Date to get SPX value for
            
        Returns:
            SPX closing value or None if not available
        """
        if self._spx_cache is None or self._spx_cache.empty:
            self.load_spx_data()
            
        try:
            if isinstance(query_date, datetime):
                query_date = query_date.date()
                
            scalar_value = self._spx_cache.loc[query_date, 'close']
            return float(scalar_value)
        except KeyError:
            if query_date.year >= 2018:
                print(f"No SPX data for {query_date}")
            return None
        except Exception as e:
            print(f"Error getting SPX data for {query_date}: {e}")
            return None
        
    

def calculate_option_metrics(options_data: pd.DataFrame) -> Dict[str, float]:
    """
    Calculate additional metrics from option chain data
    
    Args:
        options_data: DataFrame containing option chain data
    
    Returns:
        Dictionary of calculated metrics
    """
    metrics = {}
    
    try:
        # Volume metrics
        metrics['call_volume'] = int(options_data['trade_volume_c'].sum())
        metrics['put_volume'] = int(options_data['trade_volume_p'].sum())
        metrics['put_call_volume_ratio'] = (
            float(metrics['put_volume'] / metrics['call_volume'])
            if metrics['call_volume'] > 0 else 0.0
        )
        
        # Open Interest metrics
        metrics['call_oi'] = int(options_data['open_interest_c'].sum())
        metrics['put_oi'] = int(options_data['open_interest_p'].sum())
        metrics['put_call_oi_ratio'] = (
            float(metrics['put_oi'] / metrics['call_oi'])
            if metrics['call_oi'] > 0 else 0.0
        )
        
        # Implied Volatility metrics
        call_iv = options_data['implied_volatility_1545_c'].dropna()
        put_iv = options_data['implied_volatility_1545_p'].dropna()
        
        metrics['avg_call_iv'] = float(call_iv.mean())
        metrics['avg_put_iv'] = float(put_iv.mean())
        metrics['put_call_iv_ratio'] = (
            float(metrics['avg_put_iv'] / metrics['avg_call_iv'])
            if metrics['avg_call_iv'] > 0 else 0.0
        )
        
        # IV skew metrics (OTM puts vs ATM)
        atm_strike = float(options_data['active_underlying_price_1545_c'].iloc[0])
        otm_puts = options_data[options_data['strike'] < atm_strike * 0.95]  # 5% OTM
        
        if not otm_puts.empty:
            metrics['otm_put_iv_skew'] = float(
                otm_puts['implied_volatility_1545_p'].mean() / 
                options_data['implied_volatility_1545_p'].mean()
            )
        else:
            metrics['otm_put_iv_skew'] = 1.0
            
    except Exception as e:
        print(f"Error calculating option metrics: {e}")
        # Provide default values if calculation fails
        metrics.update({
            'call_volume': 0,
            'put_volume': 0,
            'put_call_volume_ratio': 0.0,
            'call_oi': 0,
            'put_oi': 0,
            'put_call_oi_ratio': 0.0,
            'avg_call_iv': 0.0,
            'avg_put_iv': 0.0,
            'put_call_iv_ratio': 0.0,
            'otm_put_iv_skew': 1.0
        })
    
    return metrics






