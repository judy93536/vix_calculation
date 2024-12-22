import pytest
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from vix_calculator.data.database import DatabaseConnection, OptionDataRepository

@pytest.fixture
def db_connection():
    """Create test database connection"""
    return DatabaseConnection()

@pytest.fixture
def option_repository(db_connection):
    """Create test option data repository"""
    return OptionDataRepository(db_connection.get_engine())

def test_database_connection(db_connection):
    """Test basic database connectivity"""
    assert db_connection.test_connection()
    
    # Test engine creation
    engine = db_connection.get_engine()
    assert engine is not None
    
    # Test basic query
    with engine.connect() as conn:
        result = conn.execute("SELECT 1").scalar()
        assert result == 1

def test_database_connection_error():
    """Test database connection error handling"""
    # Try with invalid credentials
    with pytest.raises(ConnectionError):
        DatabaseConnection('postgresql://invalid:invalid@localhost:5432/invalid')

def test_option_data_retrieval(option_repository):
    """Test option data retrieval"""
    # Test with known good date
    test_date = 20200324
    options_data = option_repository.get_spx_options(test_date)
    
    assert not options_data.empty
    assert len(options_data) > 0
    
    # Verify required columns
    required_columns = [
        'quote_date', 'ddate', 'symbol', 'root', 'expiry', 'dte', 'strike',
        'bid_eod_c', 'mid_eod_c', 'ask_eod_c', 'bid_eod_p', 'mid_eod_p', 'ask_eod_p'
    ]
    for col in required_columns:
        assert col in options_data.columns

def test_option_data_filtering(option_repository):
    """Test option data filtering"""
    test_date = 20200324
    
    # Test DTE filtering
    min_dte = 22
    max_dte = 38
    options_data = option_repository.get_spx_options(test_date, min_dte, max_dte)
    
    assert all(options_data.dte > min_dte)
    assert all(options_data.dte < max_dte)
    
    # Verify no zero bids
    assert all(options_data.bid_eod_c != 0)
    assert all(options_data.bid_eod_p != 0)

def test_trade_dates_retrieval(option_repository):
    """Test trade dates retrieval"""
    start_date = 20200101
    end_date = 20201231
    
    trade_dates = option_repository.get_trade_dates(start_date, end_date)
    
    assert not trade_dates.empty
    assert all(start_date <= date <= end_date for date in trade_dates.ddate)
    assert len(trade_dates) > 200  # Rough estimate of trading days in a year

def test_data_consistency(option_repository):
    """Test data consistency checks"""
    test_date = 20200324
    options_data = option_repository.get_spx_options(test_date)
    
    # Check for missing values
    assert not options_data.isnull().any().any()
    
    # Check strike price ordering
    for root in options_data.root.unique():
        root_data = options_data[options_data.root == root]
        assert root_data.groupby('expiry')['strike'].is_monotonic_increasing.all()
    
    # Check expiry dates are valid
    assert all(options_data.expiry > options_data.quote_date)

def test_database_connection_cleanup(db_connection):
    """Test database connection cleanup"""
    engine = db_connection.get_engine()
    
    # Verify connection works
    assert db_connection.test_connection()
    
    # Close connection
    db_connection.close()
    
    # Verify connection is closed
    with pytest.raises(Exception):
        engine.connect().execute("SELECT 1")

if __name__ == "__main__":
    pytest.main([__file__])
