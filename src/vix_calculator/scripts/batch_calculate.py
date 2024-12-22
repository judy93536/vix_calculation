#%%
import pandas as pd
import numpy as np
import time
from datetime import datetime
from sqlalchemy import text
from tqdm import tqdm
import matplotlib.pyplot as plt

from vix_calculator.calculator.vix import VixCalculator
from vix_calculator.data.database import DatabaseConnection
from vix_calculator.data.market_data import MarketDataProvider, calculate_option_metrics
from vix_calculator.data.interest_rates import InterestRateProvider

    
def get_available_dates(db_conn):
    """Get all available dates from option chain data"""
    query = """
    SELECT DISTINCT ddate 
    FROM spx_1545_eod 
    ORDER BY ddate
    """
    with db_conn.get_engine().connect() as conn:
        dates = pd.read_sql_query(query, conn)['ddate'].tolist()
        # Convert integer dates to datetime objects
        return [pd.to_datetime(str(date), format='%Y%m%d').date() for date in dates] 
     


def store_results(engine, results_df):
    """Store calculation results in database"""
    results_df.to_sql('calculated_vix', engine, 
                      if_exists='replace', index=False)
 


def plot_results(results_df, title="VIX Calculation Results"):
    """Plot calculated vs market VIX"""
    plt.figure(figsize=(15, 8))
    plt.plot(results_df['ddate'], results_df['calculated_vix'], 
             label='Calculated VIX', alpha=0.8)
    plt.plot(results_df['ddate'], results_df['market_vix'], 
             label='Market VIX', alpha=0.8)
    
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('VIX')
    plt.legend()
    plt.grid(True)
    
    # Add difference plot
    plt.figure(figsize=(15, 4))
    diff = results_df['calculated_vix'] - results_df['market_vix']
    plt.plot(results_df['ddate'], diff, label='Difference', color='red')
    plt.axhline(y=0, color='black', linestyle='--')
    plt.title('Calculation Difference (Calculated - Market)')
    plt.xlabel('Date')
    plt.ylabel('Difference')
    plt.grid(True)
    
    plt.tight_layout()
    plt.show()

def main():
    # Initialize connections
    db_conn = DatabaseConnection()
    engine = db_conn.get_engine()
    
     # Initialize providers with engine
    market_data = MarketDataProvider(engine)
    rate_provider = InterestRateProvider(engine)
    
    # Initialize calculator with all providers
    calculator = VixCalculator(engine, 
                             rate_provider=rate_provider,
                             market_data=market_data)
    
    # Create results table
    # create_results_table(engine)
    
    
    # Get all available dates
    dates = get_available_dates(db_conn)
    print(f"Found {len(dates)} dates to process")
    
    # Process all dates
    results = []
    for date in tqdm(dates):
        try:
            start_time = time.time()  # Add this line
            
            # Calculate VIX
            components = calculator.calculate(date)
            
            # Get market VIX and ensure it's a float
            market_vix = market_data.get_vix_value(date)
            if market_vix is None:
                print(f"No market VIX data for {date}, skipping")
                continue
            
            # # Get the options data used in the calculation
            # query = """
            # SELECT * FROM spx_1545_eod 
            # WHERE ddate = :date
            # """
            # with engine.connect() as conn:
            #     options_data = pd.read_sql_query(text(query), conn, params={'date': int(date.strftime('%Y%m%d'))})
            
            
            
            # Use the options data from the calculator
            options_data = calculator.get_current_options_data()
            
            if options_data is None:
                print(f"No options data available for {date}, skipping")
                continue
                
            print(f"Options data shape: {options_data.shape}")  # Debug print

            # Calculate option metrics
            option_metrics = calculate_option_metrics(options_data)
            
           
            # Store results with all metrics
            results.append({
                'ddate': date,
                'timestamp': datetime.now(),
                'calculated_vix': float(components.final_vix),
                'market_vix': market_vix,
                'dte1': int(components.dte1),
                'dte2': int(components.dte2),
                'f1': float(components.F1),
                'f2': float(components.F2),
                'k0_1': float(components.K0_1),
                'k0_2': float(components.K0_2),
                'sigma1': float(components.sigma1),
                'sigma2': float(components.sigma2),
                'r1': float(components.R1),
                'r2': float(components.R2),
                # Option metrics
                'call_volume': option_metrics['call_volume'],
                'put_volume': option_metrics['put_volume'],
                'put_call_volume_ratio': option_metrics['put_call_volume_ratio'],
                'call_oi': option_metrics['call_oi'],
                'put_oi': option_metrics['put_oi'],
                'put_call_oi_ratio': option_metrics['put_call_oi_ratio'],
                'avg_call_iv': option_metrics['avg_call_iv'],
                'avg_put_iv': option_metrics['avg_put_iv'],
                'put_call_iv_ratio': option_metrics['put_call_iv_ratio'],
                'otm_put_iv_skew': option_metrics['otm_put_iv_skew'],
                # Calculation metrics
                'vix_diff': abs(float(components.final_vix) - market_vix) if market_vix else None,
                'calc_time': time.time() - start_time
            })
            
            
            
            # # Store in chunks of 50 instead of 100 to reduce batch size
            # if len(results) >= 50:
            #     df = pd.DataFrame(results)
            
            #     try:
            #         store_results(engine, df)
            #         results = []
            #     except Exception as e:
            #         print(f"Error storing results batch: {e}")
            #         results = []  # Clear results even on error to avoid growing too large
                    
        except Exception as e:
            print(f"Error processing date {date}: {str(e)}")
            print(f"Error type: {type(e)}")
            import traceback
            print(traceback.format_exc())
            
    
    
            
    
    
    # Store any remaining results
    if results:
        df = pd.DataFrame(results)
        store_results(engine, df)
    
    # Load all results for plotting
    query = "SELECT * FROM calculated_vix ORDER BY ddate"
    with engine.connect() as conn:
        results_df = pd.read_sql_query(query, conn)
    
    # Plot results
    plot_results(results_df)
    
    # Print statistics
    diff = abs(results_df['calculated_vix'] - results_df['market_vix'])
    print("\nCalculation Statistics:")
    print(f"Mean Difference: {diff.mean():.6f}")
    print(f"Max Difference: {diff.max():.6f}")
    print(f"Within 0.01: {(diff < 0.01).mean()*100:.1f}%")
    print(f"Within 0.1: {(diff < 0.1).mean()*100:.1f}%")

#%%
if __name__ == "__main__":
    main()


# %%
