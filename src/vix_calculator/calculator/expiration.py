from typing import Tuple, List
import pandas as pd
import numpy as np
from sqlalchemy import text
from datetime import datetime

def generate_fridays(start_year: int, end_year: int) -> List[str]:
    """
    Generate a list of Friday dates between start_year and end_year.
    
    Args:
        start_year: Starting year
        end_year: Ending year (inclusive)
    
    Returns:
        List of Friday dates in 'YYYY-MM-DD' format
    """
    fridays = []
    for year in range(start_year, end_year + 1):
        year_fridays = pd.date_range(start=str(year), end=str(year+1),
                                   freq='W-FRI').strftime('%Y-%m-%d').tolist()[:52]
        fridays.extend(year_fridays)
    return fridays

def get_option_data(engine, quote_date: int, dte_min: int = 22, dte_max: int = 38) -> pd.DataFrame:
    """
    Fetch SPX option data from database within DTE range.
    
    Args:
        engine: SQLAlchemy database engine
        quote_date: Trading date in YYYYMMDD format
        dte_min: Minimum days to expiration
        dte_max: Maximum days to expiration
    
    Returns:
        DataFrame containing option data
    """
    query = """
    SELECT quote_date, ddate, symbol, root, expiry, dte, strike,
           bid_eod_c, mid_eod_c, ask_eod_c, bid_eod_p, mid_eod_p, ask_eod_p, mid_diff_eod,
           open_interest_c, open_interest_p, trade_volume_c, trade_volume_p,
           implied_volatility_1545_c, implied_volatility_1545_p,
           active_underlying_price_1545_c, active_underlying_price_1545_p
    FROM spx_1545_eod
    WHERE ddate = :quote_date
    AND dte > :dte_min AND dte < :dte_max
    AND bid_eod_c != 0 AND bid_eod_p != 0
    """
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params={
            'quote_date': quote_date,
            'dte_min': dte_min,
            'dte_max': dte_max
        })

def select_expiration_dates(options_data: pd.DataFrame, fridays: List[str]) -> Tuple[float, float]:
    """
    Select appropriate near-term and next-term expiration dates.
    
    Args:
        options_data: DataFrame containing option data
        fridays: List of valid Friday expiration dates
    
    Returns:
        Tuple of (dte1, dte2) representing near and next-term DTEs
    """
    valid_dtes = []
    for expiry in options_data.expiry.unique():
        expiry_date_str = str(pd.Timestamp(expiry).date())
        if expiry_date_str in fridays:
            # Get actual DTE for this expiry
            dte = int(options_data[options_data.expiry == expiry].dte.iloc[0])
            valid_dtes.append(dte)
    
    if len(valid_dtes) >= 2:
        valid_dtes.sort()
        return valid_dtes[-2], valid_dtes[-1]
    return None, None

def validate_expirations(dte1: float, dte2: float, options_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Validate selected expiration dates and return relevant option chains.
    
    Args:
        dte1: Near-term DTE
        dte2: Next-term DTE
        options_data: DataFrame containing all option data
    
    Returns:
        Tuple of (near_calls, near_puts, next_calls, next_puts) DataFrames
    """
    if dte1 is None or dte2 is None:
        return None, None, None, None
    
    # Split data into calls and puts
    calls_cols = {
        'quote_date': 'timestamp', 'ddate': 'ddate', 'symbol': 'symbol',
        'root': 'root', 'expiry': 'expiry', 'dte': 'dte', 'strike': 'strike',
        'bid_eod_c': 'option_bid', 'mid_eod_c': 'option_mid',
        'open_interest_c': 'open_interest', 'trade_volume_c': 'option_volume',
        'implied_volatility_1545_c': 'mid_iv', 'mid_diff_eod': 'mid_diff',
        'active_underlying_price_1545_c': 'underlying_close'
    }
    
    puts_cols = {
        'quote_date': 'timestamp', 'ddate': 'ddate', 'symbol': 'symbol',
        'root': 'root', 'expiry': 'expiry', 'dte': 'dte', 'strike': 'strike',
        'bid_eod_p': 'option_bid', 'mid_eod_p': 'option_mid',
        'open_interest_p': 'open_interest', 'trade_volume_p': 'option_volume',
        'implied_volatility_1545_p': 'mid_iv', 'mid_diff_eod': 'mid_diff',
        'active_underlying_price_1545_p': 'underlying_close'
    }
    
    calls = options_data[list(calls_cols.keys())].rename(columns=calls_cols)
    puts = options_data[list(puts_cols.keys())].rename(columns=puts_cols)
    
    # Get option chains for each expiration
    near_calls = calls[calls.dte == dte1].sort_values('strike').reset_index(drop=True)
    near_puts = puts[puts.dte == dte1].sort_values('strike').reset_index(drop=True)
    next_calls = calls[calls.dte == dte2].sort_values('strike').reset_index(drop=True)
    next_puts = puts[puts.dte == dte2].sort_values('strike').reset_index(drop=True)
    
    return near_calls, near_puts, next_calls, next_puts

def select_root_symbols(near_calls: pd.DataFrame, next_calls: pd.DataFrame) -> Tuple[str, str]:
    """
    Select appropriate root symbols (SPX vs SPXW) for near and next term.
    
    Args:
        near_calls: Near-term call options
        next_calls: Next-term call options
    
    Returns:
        Tuple of (root1, root2) representing near and next-term root symbols
    """
    root1_options = near_calls.root.unique()
    root2_options = next_calls.root.unique()
    
    root1 = root1_options[0] if len(root1_options) == 1 else None
    root2 = root2_options[0] if len(root2_options) == 1 else None
    
    # Logic to handle when both SPX and SPXW exist
    if root1 is None:
        root1 = 'SPXW' if root2 == 'SPX' else 'SPX'
    if root2 is None:
        root2 = 'SPXW' if root1 == 'SPX' else 'SPX'
    
    return root1, root2


