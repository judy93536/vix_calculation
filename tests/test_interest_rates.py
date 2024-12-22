import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine

from vix_calculator.data.interest_rates import (
    InterestRateProvider,
    get_interest_rates,
    interpolate_rate
)

# Known good values from March 24, 2020
KNOWN_VALUES = {
    'date': datetime(2020, 3, 24),
    'dte1': 24,
    'dte2': 31,
    'R1': 7.932946117119025e-05,
    'R2': 9.999750008352602e-05
}

@pytest.fixture
def rate_provider():
    """Create InterestRateProvider with test database connection"""
    engine = create_engine('postgresql://options:n841CM12@127.0.0.1:5432/cboe')
    return InterestRateProvider(engine)

def test_rate_provider_get_rates(rate_provider):
    """Test basic rate retrieval"""
    rates_df = rate_provider.get_rates(KNOWN_VALUES['date'])
    
    assert not rates_df.empty
    assert 'date' in rates_df.columns
    assert '1mo' in rates_df.columns
    assert '2mo' in rates_df.columns
    assert '3mo' in rates_df.columns

def test_get_interest_rates_known_date(rate_provider):
    """Test interest rate calculation for known date"""
    R1, R2 = get_interest_rates(
        KNOWN_VALUES['date'],
        KNOWN_VALUES['dte1'],
        KNOWN_VALUES['dte2'],
        rate_provider
    )
    
    np.testing.assert_almost_equal(R1, KNOWN_VALUES['R1'], decimal=8)
    np.testing.assert_almost_equal(R2, KNOWN_VALUES['R2'], decimal=8)

def test_get_interest_rates_negative_handling(rate_provider):
    """Test handling of negative interest rates"""
    # Create mock provider that returns negative rates
    class MockRateProvider:
        def get_rates(self, date):
            return pd.DataFrame({
                'date': [date],
                '1mo': [-0.01],
                '2mo': [-0.015],
                '3mo': [-0.02]
            })
    
    R1, R2 = get_interest_rates(
        KNOWN_VALUES['date'],
        KNOWN_VALUES['dte1'],
        KNOWN_VALUES['dte2'],
        MockRateProvider()
    )
    
    # Should return minimum rate of 0.001
    assert R1 == 0.001
    assert R2 == 0.001

def test_interpolate_rate():
    """Test rate interpolation function"""
    # Test exact boundaries
    assert interpolate_rate(0.01, 0.02, 23, 23, 30) == 0.01
    assert interpolate_rate(0.01, 0.02, 30, 23, 30) == 0.02
    
    # Test midpoint
    midpoint = interpolate_rate(0.01, 0.02, 26.5, 23, 30)
    np.testing.assert_almost_equal(midpoint, 0.015)
    
    # Test proportional interpolation
    rate = interpolate_rate(0.01, 0.02, 25, 23, 30)
    expected = 0.01 + (0.02 - 0.01) * (25 - 23) / (30 - 23)
    np.testing.assert_almost_equal(rate, expected)

def test_rate_provider_error_handling():
    """Test error handling in rate provider"""
    # Create provider with invalid connection
    bad_provider = InterestRateProvider(create_engine('postgresql://invalid'))
    
    # Should handle database errors gracefully
    with pytest.raises(Exception):
        bad_provider.get_rates(KNOWN_VALUES['date'])

def test_interest_rates_missing_data(rate_provider):
    """Test handling of missing rate data"""
    future_date = datetime(2025, 1, 1)  # Date we know has no data
    R1, R2 = get_interest_rates(future_date, 24, 31, rate_provider)
    
    # Should return default values
    assert R1 == 0.001
    assert R2 == 0.001

if __name__ == "__main__":
    pytest.main([__file__])


