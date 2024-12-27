from typing import Tuple, List
import pandas as pd
import numpy as np
from sqlalchemy import text
from datetime import datetime

# Generate all Fridays once at module load
def _generate_all_fridays() -> List[str]:
    """Generate all Fridays for years 2018-2024."""
    all_fridays = []
    for year in range(2018, 2026):  # 2018 to 2024 inclusive
        year_fridays = pd.date_range(start=str(year), end=str(year+1),
                                   freq='W-FRI').strftime('%Y-%m-%d').tolist()[:52]
        all_fridays.extend(year_fridays)
    return all_fridays

# Module-level variable containing all Fridays
VALID_FRIDAYS = set(_generate_all_fridays())  # Using a set for faster lookups

def get_option_data(engine, quote_date: int, 
                   initial_dte_min: int = 22, 
                   initial_dte_max: int = 38,
                   max_expansions: int = 12) -> pd.DataFrame:
    """
    Fetch SPX option data with DTE range expansion until finding valid Friday expirations.
    Following CBOE VIX methodology, only uses options expiring on Fridays.
    """
    dte_min = initial_dte_min
    dte_max = initial_dte_max
    expansion_count = 0
    
    while expansion_count < max_expansions:
        query = """
        SELECT quote_date, ddate, symbol, root, expiry, dte, strike,
               bid_eod_c, mid_eod_c, ask_eod_c, bid_eod_p, mid_eod_p, ask_eod_p, mid_diff_eod,
               open_interest_c, open_interest_p, trade_volume_c, trade_volume_p,
               implied_volatility_1545_c, implied_volatility_1545_p,
               active_underlying_price_1545_c, active_underlying_price_1545_p
        FROM spx_eod_daily_options
        WHERE ddate = :quote_date
        AND dte > :dte_min AND dte < :dte_max
        AND bid_eod_c != 0 AND bid_eod_p != 0
        ORDER BY dte
        """
        
        with engine.connect() as conn:
            data = pd.read_sql(text(query), conn, params={
                'quote_date': quote_date,
                'dte_min': dte_min,
                'dte_max': dte_max
            })
            
        # print(f"\nExpansion {expansion_count}: DTE range {dte_min}-{dte_max}, data shape: {data.shape}")
        
        if not data.empty:
            # Reset arrays for each query
            exp_dates = []
            dte_values = []
            
            # First get unique DTE and expiration combinations
            unique_exps = data[['dte', 'expiry']].drop_duplicates().sort_values('dte')
            print(f"Found unique DTEs and expirations:")
            for _, row in unique_exps.iterrows():
                expiry_date = pd.Timestamp(row['expiry']).date()
                expiry_str = str(expiry_date)
                #print(f"DTE: {row['dte']}, Date: {expiry_date}, Is Friday: {expiry_str in VALID_FRIDAYS}")
                
                # Strict Friday validation using module-level VALID_FRIDAYS
                if expiry_str in VALID_FRIDAYS:
                    exp_dates.append(expiry_date)
                    dte_values.append(float(row['dte']))
            
            # print(f"\nValid Friday expirations:")
            # for d, dte in zip(exp_dates, dte_values):
            #     print(f"Date: {d}, DTE: {dte}")
            
            if len(dte_values) >= 2:
                # Sort DTEs to ensure correct order
                valid_indices = np.argsort(dte_values)
                sorted_dates = [exp_dates[i] for i in valid_indices]
                sorted_dtes = [dte_values[i] for i in valid_indices]
                
                # Use the last two (largest) DTEs
                final_dates = sorted_dates[-2:]
                final_dtes = sorted_dtes[-2:]
                
                # print(f"\nCandidate expirations:")
                # print(f"Near-term: Date={final_dates[0]}, DTE={final_dtes[0]}")
                # print(f"Next-term: Date={final_dates[1]}, DTE={final_dtes[1]}")
                
                # Check if these DTEs satisfy our requirements
                # Near-term DTE should be >= 22 (initial_dte_min)
                if final_dtes[0] >= initial_dte_min:
                    print(f"Found valid pair of expirations with DTEs: {final_dtes}")
                    return data
                else:
                    print(f"Near-term DTE {final_dtes[0]} is less than minimum {initial_dte_min}, continuing search")
                
        # If we get here, expand the range and try again
        #dte_min -= 1
        dte_max += 1
        expansion_count += 1
        
        print(f"Expanding range to {dte_min}-{dte_max}")
    
    print(f"Failed to find valid expirations after {max_expansions} expansions")
    return pd.DataFrame()

def select_expiration_dates(options_data: pd.DataFrame, fridays: List[str] = None) -> Tuple[float, float]:
    """
    Select appropriate near-term and next-term expiration dates.
    Now uses module-level VALID_FRIDAYS for validation.
    """
    if options_data.empty:
        return None, None
        
    exp_dates = []
    dte_values = []
    
    # Get unique DTEs and expirations
    unique_exps = options_data[['dte', 'expiry']].drop_duplicates().sort_values('dte')
    
    for _, row in unique_exps.iterrows():
        expiry_date = pd.Timestamp(row['expiry']).date()
        if str(expiry_date) in VALID_FRIDAYS:
            exp_dates.append(expiry_date)
            dte_values.append(float(row['dte']))
    
    if len(dte_values) >= 2:
        # Sort both lists based on DTE values
        sorted_indices = np.argsort(dte_values)
        sorted_dte_values = [dte_values[i] for i in sorted_indices]
        return sorted_dte_values[-2], sorted_dte_values[-1]
    
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