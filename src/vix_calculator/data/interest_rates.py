from typing import Tuple, Dict, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text, Engine

class InterestRateProvider:
    """
    Handles retrieval and interpolation of interest rates for VIX calculation.
    """
    
    def __init__(self, engine: Engine):
        """
        Initialize with database connection.
        
        Args:
            engine: SQLAlchemy database engine
        """
        self.engine = engine
        
    def get_rates(self, quote_date: datetime) -> pd.DataFrame:
        """
        Get interest rates from database for given date, with interpolation for missing dates.
        
        Args:
            quote_date: Date to get rates for
        
        Returns:
            DataFrame containing yield curve data
        """
        # Try exact date first
        query = """
        SELECT date, "1mo", "2mo", "3mo", "6mo", "1yr", "2yr", "3yr", 
               "5yr", "7yr", "10yr", "20yr", "30yr"
        FROM daily_treasury_par_yield
        WHERE date = %(date)s
        """
        with self.engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={'date': quote_date.date()})
            
            if not df.empty:
                return df
                
            # If no data for exact date, get surrounding dates
            query_surrounding = """
            SELECT date, "1mo", "2mo", "3mo", "6mo", "1yr", "2yr", "3yr", 
                   "5yr", "7yr", "10yr", "20yr", "30yr"
            FROM daily_treasury_par_yield
            WHERE date BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY date
            """
            # Get 5 days before and after
            start_date = quote_date.date() - pd.Timedelta(days=5)
            end_date = quote_date.date() + pd.Timedelta(days=5)
            
            df_surrounding = pd.read_sql_query(
                query_surrounding, 
                conn, 
                params={'start_date': start_date, 'end_date': end_date}
            )
            
            if df_surrounding.empty:
                raise ValueError(f"No interest rate data found near {quote_date}")
                
            # Convert dates for comparison
            df_surrounding['date'] = pd.to_datetime(df_surrounding['date'])
            quote_date_pd = pd.to_datetime(quote_date.date())
            
            # Find closest dates before and after
            df_before = df_surrounding[df_surrounding['date'] < quote_date_pd]
            df_after = df_surrounding[df_surrounding['date'] > quote_date_pd]
            
            if df_before.empty or df_after.empty:
                raise ValueError(f"Cannot interpolate rates for {quote_date}")
            
            before_date = df_before['date'].max()
            after_date = df_after['date'].min()
            
            before_rates = df_surrounding[df_surrounding['date'] == before_date].iloc[0]
            after_rates = df_surrounding[df_surrounding['date'] == after_date].iloc[0]
            
            # Calculate weights for interpolation
            total_days = (after_date - before_date).total_seconds() / (24 * 60 * 60)
            days_from_before = (quote_date_pd - before_date).total_seconds() / (24 * 60 * 60)
            weight_after = days_from_before / total_days
            weight_before = 1 - weight_after
            
            print(f"Interpolating rates between:")
            print(f"  {before_date.date()} (weight: {weight_before:.3f})")
            print(f"  {after_date.date()} (weight: {weight_after:.3f})")
            
            # Create interpolated DataFrame
            interpolated_data = {}
            for col in df_surrounding.columns:
                if col != 'date':
                    interpolated_data[col] = before_rates[col] * weight_before + after_rates[col] * weight_after
            
            interpolated_data['date'] = quote_date.date()
            return pd.DataFrame([interpolated_data]) 
    
def get_cmt_tenors() -> Dict[str, float]:
    """
    Get CMT tenors in months for available rates.
    """
    return {
        '1mo': 1.0,
        '2mo': 2.0,
        '3mo': 3.0,
        '6mo': 6.0,
        '1yr': 12.0,
        '2yr': 24.0,
        '3yr': 36.0,
        '5yr': 60.0,
        '7yr': 84.0,
        '10yr': 120.0,
        '20yr': 240.0,
        '30yr': 360.0
    }

def get_closest_cmt_tenors(months_to_expiry: float) -> Tuple[str, str]:
    """
    Find the two closest CMT tenors for interpolation.
    """
    tenors = get_cmt_tenors()
    tenor_months = list(tenors.values())
    tenor_names = list(tenors.keys())
    
    # Find the first tenor longer than our target
    for i, months in enumerate(tenor_months):
        if months > months_to_expiry:
            if i == 0:
                # If shorter than 1mo, use 1mo rate
                return tenor_names[0], tenor_names[0]
            return tenor_names[i-1], tenor_names[i]
    
    # If longer than longest tenor, use longest tenor
    return tenor_names[-1], tenor_names[-1]


def interpolate_cmt_rate(shorter_rate: float, longer_rate: float,
                        shorter_months: float, longer_months: float,
                        target_months: float) -> float:
    """
    Interpolate between two CMT rates, handling missing values.
    """
    # Handle missing rates by using the available rate
    if shorter_rate is None and longer_rate is None:
        return 0.01  # Default rate if both are missing
    if shorter_rate is None:
        return longer_rate
    if longer_rate is None:
        return shorter_rate
        
    if shorter_months == longer_months:
        return shorter_rate
        
    # Linear interpolation
    weight = (target_months - shorter_months) / (longer_months - shorter_months)
    return shorter_rate + (longer_rate - shorter_rate) * weight



def convert_to_continuous_rate(rate: float) -> float:
    """
    Convert CMT rate to continuous compounding.
    
    Args:
        rate: CMT rate in percentage form (e.g., 0.01 for 0.01%)
        
    Returns:
        Continuous compounding rate in decimal form
    """
    # Input rate is in percentage form, convert to decimal
    rate_decimal = rate / 100.0
    
    # Convert semi-annual bond equivalent yield to annual compounding
    # (1 + r/2)^2 = 1 + R where R is the annual rate
    annual_rate = (1 + rate_decimal/2)**2 - 1
    
    # Convert to continuous compounding
    # e^r = 1 + R where r is the continuous rate
    if annual_rate > -1:  # Protect against invalid log input
        continuous_rate = np.log1p(annual_rate)
    else:
        continuous_rate = 0.0001  # Smaller minimum for extreme low rates
        
    print(f"Rate conversion: {rate}% -> {continuous_rate:.8f}")
    return continuous_rate


def get_rates_for_date(quote_date: datetime, df_rates: pd.DataFrame) -> Dict[str, float]:
    """
    Get or interpolate rates for a specific date.
    """
    # Try exact date first
    df_exact = df_rates[df_rates['date'] == quote_date.date()]
    if not df_exact.empty:
        print(f"Using exact rates for {quote_date.date()}")
        return df_exact.iloc[0].to_dict()
    
    # Find surrounding dates
    df_rates['date'] = pd.to_datetime(df_rates['date'])
    quote_date_pd = pd.to_datetime(quote_date.date())
    
    df_before = df_rates[df_rates['date'] < quote_date_pd]
    df_after = df_rates[df_rates['date'] > quote_date_pd]
    
    if df_before.empty or df_after.empty:
        raise ValueError(f"Cannot interpolate rates for {quote_date}")
    
    before_date = df_before['date'].max()
    after_date = df_after['date'].min()
    
    before_rates = df_rates[df_rates['date'] == before_date].iloc[0]
    after_rates = df_rates[df_rates['date'] == after_date].iloc[0]
    
    # Calculate weights for interpolation
    total_days = (after_date - before_date).total_seconds() / (24 * 60 * 60)
    days_from_before = (quote_date_pd - before_date).total_seconds() / (24 * 60 * 60)
    weight_after = days_from_before / total_days
    weight_before = 1 - weight_after
    
    print(f"Interpolating rates for {quote_date.date()} using:")
    print(f"  Before: {before_date.date()} (weight: {weight_before:.3f})")
    print(f"  After:  {after_date.date()} (weight: {weight_after:.3f})")
    
    # Interpolate all rate columns
    rate_columns = [col for col in df_rates.columns if col != 'date']
    return {
        col: before_rates[col] * weight_before + after_rates[col] * weight_after
        for col in rate_columns
    }


def calculate_rate_for_expiry(dte: float, rates: Dict[str, float], quote_date: Optional[str] = None) -> float:
    """
    Calculate the appropriate interest rate for a given expiry.
    """
    date_str = f" ({quote_date})" if quote_date else ""
    
    # Convert DTE to months
    months_to_expiry = dte / 30.0  # Approximate
    
    # Get closest CMT tenors
    shorter_tenor, longer_tenor = get_closest_cmt_tenors(months_to_expiry)
    
    print(f"\nCalculating rate for {dte} days ({months_to_expiry:.2f} months){date_str}:")
    print(f"  Using tenors: {shorter_tenor} and {longer_tenor}")
    
    # Get rates and tenors in months
    tenors = get_cmt_tenors()
    shorter_months = tenors[shorter_tenor]
    longer_months = tenors[longer_tenor]
    shorter_rate = rates.get(shorter_tenor)
    longer_rate = rates.get(longer_tenor)
    
    # Handle missing rates
    if shorter_rate is None and longer_rate is None:
        print(f"  Warning: Both {shorter_tenor} and {longer_tenor} rates missing, using default")
        return 0.001
        
    # Use next available rate if one is missing
    if shorter_rate is None:
        print(f"  Warning: {shorter_tenor} rate missing, using {longer_tenor} rate")
        shorter_rate = longer_rate
    if longer_rate is None:
        print(f"  Warning: {longer_tenor} rate missing, using {shorter_tenor} rate")
        longer_rate = shorter_rate
    
    print(f"  {shorter_tenor}: {shorter_rate}%")
    print(f"  {longer_tenor}: {longer_rate}%")
    
    # Interpolate between CMT rates
    interpolated_rate = interpolate_cmt_rate(
        shorter_rate, longer_rate,
        shorter_months, longer_months,
        months_to_expiry
    )
    
    print(f"  Interpolated rate: {interpolated_rate:.4f}%")
    
    # Convert to continuous rate
    continuous_rate = convert_to_continuous_rate(interpolated_rate)
    print(f"  Final continuous rate: {continuous_rate:.8f}")
    
    return continuous_rate




def get_interest_rates(quote_date: datetime, dte1: float, dte2: float,
                      rate_provider) -> Tuple[float, float]:
    """
    Get interpolated, continuous-compounding interest rates for VIX calculation.
    """
    try:
        # Get rates data
        df_rates = rate_provider.get_rates(quote_date)
        if df_rates.empty:
            raise ValueError(f"No rate data available for {quote_date}")
            
        # Get or interpolate rates for the date
        rates = get_rates_for_date(quote_date, df_rates)
        
        # # Calculate rates for each expiry
        # R1 = calculate_rate_for_expiry(dte1, rates)
        # R2 = calculate_rate_for_expiry(dte2, rates)
        
        # Calculate rates for each expiry
        R1 = calculate_rate_for_expiry(dte1, rates, quote_date.strftime('%Y-%m-%d'))
        R2 = calculate_rate_for_expiry(dte2, rates, quote_date.strftime('%Y-%m-%d'))
        
        print(f"\nRate calculation for {quote_date.strftime('%Y-%m-%d')}:")
        print(f"  DTE1: {dte1:.1f} days -> R1: {R1:.8f}")
        print(f"  DTE2: {dte2:.1f} days -> R2: {R2:.8f}")
        
        return R1, R2
        
    except Exception as e:
        print(f"Error calculating interest rates for {quote_date.strftime('%Y-%m-%d')}: {e}")
        return 0.001, 0.001
    
