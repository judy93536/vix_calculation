import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import create_engine

from vix_calculator.calculator.vix import VixCalculator, VixComponents
from vix_calculator.data.market_data import MarketDataProvider

# Known good test cases
TEST_CASES = [
    {
        'date': date(2020, 3, 24),
        'expected_vix': 61.885999,
        'expected_components': {
            'dte1': 24.0,
            'dte2': 31.0,
            'F1': 2443.999994480429,
            'F2': 2443.2999847857004,
            'K0_1': 2445.0,
            'K0_2': 2445.0,
            'sigma1': 0.37857464998706275,
            'sigma2': 0.3612918131851498
        }
    },
    {
        'date': date(2020, 11, 24),
        'expected_vix': 21.933917,
        'expected_components': {
            'dte1': 17.0,
            'dte2': 24.0,
            'F1': 3634.49998871356,
            'F2': 3633.7499304343505,
            'K0_1': 3635.0,
            'K0_2': 3635.0,
            'sigma1': 0.03757517495563252,
            'sigma2': 0.04334887427077329
        }
    }
]

@pytest.fixture
def calculator():
    """Create VixCalculator instance"""
    engine = create_engine('postgresql://options:n841CM12@127.0.0.1:5432/cboe')
    market_data = MarketDataProvider()
    return VixCalculator(engine, market_data=market_data)

@pytest.mark.parametrize("test_case", TEST_CASES)
def test_vix_calculation_known_dates(calculator, test_case):
    """Test VIX calculation for known dates"""
    components = calculator.calculate(test_case['date'])
    
    # Verify final VIX value
    np.testing.assert_almost_equal(
        components.final_vix,
        test_case['expected_vix'],
        decimal=6
    )
    
    # Verify individual components
    expected = test_case['expected_components']
    np.testing.assert_almost_equal(components.dte1, expected['dte1'])
    np.testing.assert_almost_equal(components.dte2, expected['dte2'])
    np.testing.assert_almost_equal(components.F1, expected['F1'])
    np.testing.assert_almost_equal(components.F2, expected['F2'])
    np.testing.assert_almost_equal(components.K0_1, expected['K0_1'])
    np.testing.assert_almost_equal(components.K0_2, expected['K0_2'])
    np.testing.assert_almost_equal(components.sigma1, expected['sigma1'])
    np.testing.assert_almost_equal(components.sigma2, expected['sigma2'])

def test_vix_calculation_error_handling(calculator):
    """Test error handling in VIX calculation"""
    # Test with date having no data
    future_date = date(2025, 1, 1)
    with pytest.raises(ValueError):
        calculator.calculate(future_date)
    
    # Test with holiday date (if available)
    # Test with early close date (if available)

def test_vix_validation(calculator):
    """Test VIX validation against market data"""
    # Calculate for known date
    test_case = TEST_CASES[0]
    components = calculator.calculate(test_case['date'])
    
    # Get market VIX value
    market_vix = calculator.market_data.get_vix_value(test_case['date'])
    
    # Validate calculation
    is_valid = calculator.validate_calculation(components, market_vix)
    assert is_valid

def test_vix_components_dataclass():
    """Test VixComponents dataclass"""
    # Test creation with valid data
    components = VixComponents(
        dte1=24.0,
        dte2=31.0,
        T1=0.069578,
        T2=0.089498,
        R1=7.932946117119025e-05,
        R2=9.999750008352602e-05,
        F1=2443.999994480429,
        F2=2443.2999847857004,
        K0_1=2445.0,
        K0_2=2445.0,
        sigma1=0.37857464998706275,
        sigma2=0.3612918131851498,
        final_vix=61.885999
    )
    
    assert isinstance(components, VixComponents)
    assert components.final_vix == 61.885999

if __name__ == "__main__":
    pytest.main([__file__])


