from datetime import datetime, date
import pandas as pd
from sqlalchemy import create_engine
from vix_calculator.calculator.vix import VixCalculator
from vix_calculator.data.market_data import MarketDataProvider
from vix_calculator.data.database import DatabaseConnection
from vix_calculator.data.interest_rates import InterestRateProvider  # Added this import

def main():
    """
    Example script demonstrating VIX calculator usage.
    """
    print("VIX Calculator Example")
    print("=====================")
    
    # Initialize database connection
    try:
        db_conn = DatabaseConnection()
        print("\n✓ Database connected successfully")
    except Exception as e:
        print(f"\n✗ Database connection failed: {e}")
        return
    
    # Initialize market data provider
    market_data = MarketDataProvider()
    rate_provider = InterestRateProvider(db_conn.get_engine())
    
    # Create calculator instance
    calculator = VixCalculator(
        db_conn.get_engine(),
        rate_provider=rate_provider,
        market_data=market_data
    )
    #calculator = VixCalculator(db_conn.get_engine(), market_data=market_data)
    
    
    # Example 1: Calculate VIX for a single date
    test_date = date(2020, 3, 24)
    print(f"\nCalculating VIX for {test_date}")
    print("-" * 40)
    
    try:
        components = calculator.calculate(test_date)
        market_vix = market_data.get_vix_value(test_date)
        market_vix_value = market_vix.iloc[0]
        # print("\nResults:")
        # print(market_vix.iloc[0])
        
        print(f"Calculated VIX: {components.final_vix:.6f}")
        #print(f"Market VIX:     {market_vix:.6f}")
        print(f"Market VIX:     {market_vix_value:.6f}")
        print(f"Difference:     {abs(components.final_vix - market_vix_value):.6f}")
        
        print("\nCalculation Components:")
        print(f"Near-term DTE:  {components.dte1}")
        print(f"Next-term DTE:  {components.dte2}")
        print(f"Forward Price 1: {components.F1:.4f}")
        print(f"Forward Price 2: {components.F2:.4f}")
        print(f"K0_1:           {components.K0_1}")
        print(f"K0_2:           {components.K0_2}")
        print(f"Sigma 1:        {components.sigma1:.8f}")
        print(f"Sigma 2:        {components.sigma2:.8f}")
        
    except Exception as e:
        print(f"Calculation failed: {e}")
    
    # Example 2: Calculate VIX for a date range
    start_date = date(2020, 11, 10)
    end_date = date(2020, 11, 12)
    
    print(f"\nCalculating VIX for range: {start_date} to {end_date}")
    print("-" * 40)
    
    results = []
    current_date = start_date
    while current_date <= end_date:
        try:
            components = calculator.calculate(current_date)
            market_vix = market_data.get_vix_value(current_date)
            
            results.append({
                'date': current_date,
                'calculated_vix': components.final_vix,
                'market_vix': market_vix.iloc[0],
                'difference': abs(components.final_vix - market_vix.iloc[0])
            })
            
        except Exception as e:
            print(f"Failed for {current_date}: {e}")
            
        current_date = pd.to_datetime(current_date) + pd.Timedelta(days=1)
        current_date = current_date.date()
    
    # Display results
    if results:
        df_results = pd.DataFrame(results)
        print("\nResults Summary:")
        print(df_results.to_string(index=False))
        
        print("\nAccuracy Statistics:")
        print(f"Mean Difference:   {df_results['difference'].mean():.6f}")
        print(f"Max Difference:    {df_results['difference'].max():.6f}")
        print(f"Within 0.01:       {(df_results['difference'] < 0.01).mean()*100:.1f}%")

def example_custom_calculation():
    """
    Example of customized VIX calculation with specific parameters.
    """
    db_conn = DatabaseConnection()
    calculator = VixCalculator(
        db_conn.get_engine(),
        rate_provider=None,  # Could provide custom rate source
        market_data=MarketDataProvider()
    )
    
    # Calculate with specific parameters
    try:
        components = calculator.calculate(date(2020, 3, 24))
        
        # Access individual components for custom analysis
        T1 = components.T1
        T2 = components.T2
        sigma1 = components.sigma1
        sigma2 = components.sigma2
        
        # Example custom calculation
        # (This is just for demonstration - actual custom calculations would depend on your needs)
        custom_weighting = (T2 * sigma1 + T1 * sigma2) / (T1 + T2)
        print(f"\nCustom Analysis Example:")
        print(f"Custom Weighted Sigma: {custom_weighting:.6f}")
        
    except Exception as e:
        print(f"Custom calculation failed: {e}")

if __name__ == "__main__":
    main()
    # Uncomment to run custom example
    # example_custom_calculation()


