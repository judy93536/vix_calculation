import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine

from vix_calculator.calculator.forward_price import (
    prepare_strike_ranges,
    calculate_sigma
)

# Known good values from March 24, 2020
KNOWN_VALUES = {
    'F1': 2443.999994480429,
    'F2': 2443.2999847857004,
    'K0_1': 2445.0,
    'K0_2': 2445.0,
    'sigma1': 0.37857464998706275,
    'sigma2': 0.3612918131851498,
    'tmp11': 0.3785770542250984,
    'tmp22': 0.3612972149532122
}

@pytest.fixture
def option_chains(db_engine):
    """Get option chains for known test date"""
    from vix_calculator.calculator.expiration import (
        get_option_data, select_expiration_dates, validate_expirations
    )
    
    test_date = 20200324
    fridays = pd.date_range(start='2020', end='2021',
                          freq='W-FRI').strftime('%Y-%m-%d').tolist()[:52]
    
    options_data = get_option_data(db_engine, test_date)
    dte1, dte2 = select_expiration_dates(options_data, fridays)
    return validate_expirations(dte1, dte2, options_data)

def test_prepare_strike_ranges(option_chains):
    """Test strike range preparation"""
    near_calls, near_puts, next_calls, next_puts = option_chains
    
    near0, next0, near_diff, next_diff, K0_1, K0_2 = prepare_strike_ranges(
        near_calls, near_puts, next_calls, next_puts,
        KNOWN_VALUES['F1'], KNOWN_VALUES['F2']
    )
    
    # Verify K0 values match known good values
    assert K0_1 == KNOWN_VALUES['K0_1']
    assert K0_2 == KNOWN_VALUES['K0_2']
    
    # Verify strike ranges
    assert len(near0) == 375  # Known size from original implementation
    assert len(next0) == 333  # Known size from original implementation
    
    # Verify strike differences
    assert near_diff.iloc[0] == near_diff.iloc[1]  # First difference matches second
    assert next_diff.iloc[0] == next_diff.iloc[1]  # First difference matches second
    
    # Verify strike ordering
    assert near0.strike.is_monotonic_increasing
    assert next0.strike.is_monotonic_increasing

def test_calculate_sigma(option_chains):
    """Test sigma calculation"""
    near_calls, near_puts, next_calls, next_puts = option_chains
    
    # First prepare strike ranges
    near0, next0, near_diff, next_diff, K0_1, K0_2 = prepare_strike_ranges(
        near_calls, near_puts, next_calls, next_puts,
        KNOWN_VALUES['F1'], KNOWN_VALUES['F2']
    )
    
    # Calculate test values
    T1 = 0.069578
    T2 = 0.089498
    R1 = 7.932946117119025e-05
    R2 = 9.999750008352602e-05
    
    sigma1, sigma2 = calculate_sigma(
        near0, next0, near_diff, next_diff,
        KNOWN_VALUES['F1'], KNOWN_VALUES['F2'],
        K0_1, K0_2, T1, T2, R1, R2
    )
    
    # Verify against known good values
    np.testing.assert_almost_equal(sigma1, KNOWN_VALUES['sigma1'], decimal=6)
    np.testing.assert_almost_equal(sigma2, KNOWN_VALUES['sigma2'], decimal=6)

def test_sigma_calculation_components(option_chains):
    """Test individual components of sigma calculation"""
    near_calls, near_puts, next_calls, next_puts = option_chains
    
    near0, next0, near_diff, next_diff, K0_1, K0_2 = prepare_strike_ranges(
        near_calls, near_puts, next_calls, next_puts,
        KNOWN_VALUES['F1'], KNOWN_VALUES['F2']
    )
    
    T1 = 0.069578
    T2 = 0.089498
    R1 = 7.932946117119025e-05
    R2 = 9.999750008352602e-05
    
    # Test tmp11 calculation
    tmp1 = near_diff * (
        np.exp(R1 * T1) * near0.option_mid
    ) / (near0.strike * near0.strike)
    tmp11 = (tmp1.sum() * 2.0) / T1
    
    np.testing.assert_almost_equal(tmp11, KNOWN_VALUES['tmp11'], decimal=6)
    
    # Test tmp22 calculation
    tmp2 = next_diff * (
        np.exp(R2 * T2) * next0.option_mid
    ) / (next0.strike * next0.strike)
    tmp22 = (tmp2.sum() * 2.0) / T2
    
    np.testing.assert_almost_equal(tmp22, KNOWN_VALUES['tmp22'], decimal=6)

if __name__ == "__main__":
    pytest.main([__file__])

