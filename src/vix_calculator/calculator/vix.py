from dataclasses import dataclass
from typing import Tuple, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime, date

from .expiration import (
    #generate_fridays,
    #_generate_all_fridays,
    get_option_data,
    select_expiration_dates,
    validate_expirations
)
from .forward_price import prepare_strike_ranges, calculate_sigma
from ..data.interest_rates import get_interest_rates


# Constants
minsDay = 1440  # Minutes in a day
minsYear = 525600  # Minutes in a year

def calculate_minutes_to_expiry(option_data, is_standard_spx: bool):
    """Calculate minutes to expiry based on option type"""
    # Current minutes from midnight
    current_mins = minsDay - option_data.timestamp.hour * 60 - option_data.timestamp.minute
    
    # Settlement minutes (9:30 AM for standard SPX, 4:00 PM for weekly)
    settlement_mins = 570 if is_standard_spx else 960
    
    # Other minutes (days to expiry * minutes per day)
    other_mins = option_data.dte * minsDay
    
    return current_mins, settlement_mins, other_mins

def calculate_time_to_expiry(current_mins, settlement_mins, other_mins):
    """Calculate time to expiry in years"""
    return (current_mins + settlement_mins + other_mins) / minsYear



@dataclass
class VixComponents:
    """Container for intermediate VIX calculation values"""
    dte1: float
    dte2: float
    T1: float
    T2: float
    R1: float
    R2: float
    F1: float
    F2: float
    K0_1: float
    K0_2: float
    sigma1: float
    sigma2: float
    final_vix: float

class VixCalculator:
    """
    VIX Index Calculator following CBOE methodology.
    
    This implementation incorporates specific handling for:
    - SPX/SPXW option expiration dates
    - Market holidays and irregular trading hours
    - Interest rate interpolation
    - Strike price selection
    """
    
    def __init__(self, db_connection, rate_provider=None, market_data=None):
        """
        Initialize VIX calculator with data sources.
        
        Args:
            db_connection: SQLAlchemy database connection
            rate_provider: Optional custom interest rate provider
            market_data: Optional market data provider for validation
        """
        self.db_connection = db_connection
        self.rate_provider = rate_provider
        self.market_data = market_data
        self.options_data = None  # Add this to store the data
        self.minsDay = 1440
        self.minsYear = 525600
        
    def calculate(self, calculation_date: date) -> VixComponents:
        """
        Calculate VIX index value for a specific date.
        
        Args:
            calculation_date: Date to calculate VIX for
            
        Returns:
            VixComponents containing all calculation components
            
        Raises:
            ValueError: If required data is missing
            RuntimeError: If calculation fails
        """
        # Convert date to integer format YYYYMMDD
        date_int = int(calculation_date.strftime('%Y%m%d'))
        
        # Get option data
        self.options_data = get_option_data(
            engine=self.db_connection, 
            quote_date=date_int,
            initial_dte_min=22,
            initial_dte_max=38
        )
        
        # Select expiration dates (no need to pass fridays anymore)
        dte1, dte2 = select_expiration_dates(self.options_data)
        if dte1 is None or dte2 is None:
            raise ValueError(f"Could not find valid expiration dates for {calculation_date}")
            
        # Get option chains
        near_calls, near_puts, next_calls, next_puts = validate_expirations(
            dte1, dte2, self.options_data
        )

        # Calculate time components
        M_current_1, M_settlement_1, M_other_1 = calculate_minutes_to_expiry(
            near_calls.iloc[0],
            near_calls.iloc[0].root == 'SPX'
        )
        T1 = calculate_time_to_expiry(M_current_1, M_settlement_1, M_other_1)
        
        M_current_2, M_settlement_2, M_other_2 = calculate_minutes_to_expiry(
            next_calls.iloc[0],
            next_calls.iloc[0].root == 'SPX'
        )
        T2 = calculate_time_to_expiry(M_current_2, M_settlement_2, M_other_2)
        
        # Calculate interest rates
        R1, R2 = get_interest_rates(
            near_calls.iloc[0].timestamp,
            near_calls.iloc[0].dte,
            next_calls.iloc[0].dte,
            self.rate_provider
        )
        
        # Calculate forward prices
        near_min_idx = np.nanargmin(np.array(near_calls.mid_diff))
        next_min_idx = np.nanargmin(np.array(next_calls.mid_diff))
        
        F1 = near_calls.iloc[near_min_idx].strike + np.exp(R1 * T1) * (
            near_calls.iloc[near_min_idx].option_mid - near_puts.iloc[near_min_idx].option_mid
        )
        
        F2 = next_calls.iloc[next_min_idx].strike + np.exp(R2 * T2) * (
            next_calls.iloc[next_min_idx].option_mid - next_puts.iloc[next_min_idx].option_mid
        )
        
        # Now that we have F1 and F2, prepare strike ranges
        near0, next0, near_diff, next_diff, K0_1, K0_2 = prepare_strike_ranges(
            near_calls, near_puts, next_calls, next_puts, F1, F2
        )
        
        # Calculate sigmas
        sigma1, sigma2 = calculate_sigma(
            near0, next0, near_diff, next_diff,
            F1, F2, K0_1, K0_2, T1, T2, R1, R2
        )
        
        # Calculate final VIX
        N_T1 = M_other_1
        N_T2 = M_other_2
        N_30 = self.minsDay * 30
        N_365 = self.minsYear
        
        weighted_variance = (
            T1 * sigma1 * (N_T2 - N_30) / (N_T2 - N_T1) +
            T2 * sigma2 * (N_30 - N_T1) / (N_T2 - N_T1)
        ) * N_365 / N_30
        
        vix = 100 * np.sqrt(abs(weighted_variance))
        
        return VixComponents(
            dte1=dte1,
            dte2=dte2,
            T1=T1,
            T2=T2,
            R1=R1,
            R2=R2,
            F1=F1,
            F2=F2,
            K0_1=K0_1,
            K0_2=K0_2,
            sigma1=sigma1,
            sigma2=sigma2,
            final_vix=vix
        )    
        
    def get_current_options_data(self):
        """Return the options data used in the most recent calculation"""
        if self.options_data is None:
            print("Warning: No options data currently stored in calculator")
        return self.options_data
    
    
    def validate_calculation(self, components: VixComponents, actual_vix: float) -> bool:
        """
        Validate calculation against actual VIX value.
        """
        return abs(components.final_vix - actual_vix) < 0.001


