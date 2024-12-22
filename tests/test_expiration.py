import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine

from vix_calculator.calculator.expiration import (
    generate_fridays,
    get_option_data,
    select_expiration_dates,
    validate_expirations
)

# Test data for known good case (March 24, 2020)
KNOWN_GOOD_DATE = 20200324
EXPECTED_DTE1 = 24
EXPECTED_DTE2 = 31
EXPECTED_NEAR_STRIKES = 366
EXPECTED_NEXT_STRIKES = 326

@pytest.fixture
def db_engine():
    """Create database connection for tests"""
    return create_engine('postgresql://options:n841CM12@127.0.0.1:5432/cboe')

@pytest.fixture
def friday_dates():
    """Generate Friday dates for testing"""
    return generate_fridays(2020, 2020)

def test_generate_fridays():
    """Test Friday date generation"""
    fridays_2020 = generate_fridays(2020, 2020)
    assert len(fridays_2020) == 52  # 52 weeks in a year
    assert all(pd.to_datetime(d).weekday() == 4 for d in fridays_2020)  # All Fridays

def test_get_option_data(db_engine):
    """Test option data retrieval for known date"""
    options_data = get_option_data(db_engine, KNOWN_GOOD_DATE)
    
    assert not options_data.empty
    assert 'dte' in options_data.columns
    assert 'strike' in options_data.columns
    assert 'root' in options_data.columns
    
    # Verify we have both SPX and SPXW options
    roots = options_data.root.unique()
    assert 'SPX' in roots
    assert 'SPXW' in roots

def test_expiration_selection_known_date(db_engine, friday_dates):
    """Test expiration date selection for known good case"""
    options_data = get_option_data(db_engine, KNOWN_GOOD_DATE)
    dte1, dte2 = select_expiration_dates(options_data, friday_dates)
    
    assert dte1 == EXPECTED_DTE1
    assert dte2 == EXPECTED_DTE2

def test_validate_expirations_known_date(db_engine, friday_dates):
    """Test option chain validation for known good case"""
    options_data = get_option_data(db_engine, KNOWN_GOOD_DATE)
    dte1, dte2 = select_expiration_dates(options_data, friday_dates)
    
    near_calls, near_puts, next_calls, next_puts = validate_expirations(
        dte1, dte2, options_data
    )
    
    # Verify option chain sizes
    assert len(near_calls) == EXPECTED_NEAR_STRIKES
    assert len(near_puts) == EXPECTED_NEAR_STRIKES
    assert len(next_calls) == EXPECTED_NEXT_STRIKES
    assert len(next_puts) == EXPECTED_NEXT_STRIKES
    
    # Verify we have the expected option chains
    assert near_calls is not None
    assert near_puts is not None
    assert next_calls is not None
    assert next_puts is not None
    
    # Verify basic structure of option chains
    assert all(col in near_calls.columns for col in required_columns)
    assert all(col in near_puts.columns for col in required_columns)
    assert all(col in next_calls.columns for col in required_columns)
    assert all(col in next_puts.columns for col in required_columns)
    
    
    # Verify option chain structure
    required_columns = [
        'timestamp', 'ddate', 'symbol', 'root', 'expiry', 'dte', 'strike',
        'option_bid', 'option_mid', 'open_interest', 'option_volume',
        'mid_iv', 'mid_diff', 'underlying_close'
    ]
    
    for chain in [near_calls, near_puts, next_calls, next_puts]:
        assert all(col in chain.columns for col in required_columns)
        assert chain.strike.is_monotonic_increasing  # Strikes are ordered

def test_expiration_selection_edge_cases(db_engine, friday_dates):
    """Test expiration date selection for edge cases"""
    # Test with holiday date (if available)
    # Test with early close date (if available)
    # Test with date having missing data
    pass

def test_validate_expirations_error_cases():
    """Test error handling in expiration validation"""
    # Test with None DTEs
    result = validate_expirations(None, None, pd.DataFrame())
    assert all(r is None for r in result)
    
    # Test with empty DataFrame
    result = validate_expirations(24, 31, pd.DataFrame())
    assert all(r is None for r in result)

if __name__ == "__main__":
    pytest.main([__file__])

