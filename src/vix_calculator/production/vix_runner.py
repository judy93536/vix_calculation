#!/usr/bin/env python3
"""
VIX Calculator Production Runner
Handles daily calculations, updates, and monitoring
"""
import logging
from pathlib import Path
from datetime import datetime, date
import pandas as pd
from sqlalchemy import text
import time
from tqdm import tqdm

from vix_calculator.calculator.vix import VixCalculator
from vix_calculator.data.database import DatabaseConnection
from vix_calculator.data.market_data import MarketDataProvider, calculate_option_metrics
from vix_calculator.data.interest_rates import InterestRateProvider


class VixRunner:
    """Production runner for VIX calculations"""
    
    def __init__(self):
        self.db_conn = DatabaseConnection()
        self.engine = self.db_conn.get_engine()
        self.market_data = MarketDataProvider(self.engine)
        self.rate_provider = InterestRateProvider(self.engine)
        self.calculator = VixCalculator(
            self.engine,
            rate_provider=self.rate_provider,
            market_data=self.market_data
        )
        
        # Setup directories
        self.project_root = Path(__file__).parent.parent.parent.parent
        self.results_dir = self.project_root / 'results'
        self.csv_dir = self.results_dir / 'csv'
        self.logs_dir = self.results_dir / 'logs'
        
        # Create directories if they don't exist
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        
        # Setup logging
        self.logger = self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = self.logs_dir / f'vix_calculator_{timestamp}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    
    def get_all_dates(self) -> list:
        """Get all available dates from SPX option data"""
        query = """
        SELECT DISTINCT ddate
        FROM spx_1545_eod
        WHERE ddate <= 20181231
        ORDER BY ddate
        """
        with self.engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
            # Convert YYYYMMDD integer to datetime
            df['date'] = pd.to_datetime(df['ddate'].astype(str), format='%Y%m%d')
            return df['date'].dt.date.tolist()
        
        
    def process_all_dates(self):
        """Process all available dates and store results"""
        
        self.logger.info("Starting to process all dates...")
        
        dates = self.get_all_dates()
        #logger.info(f"Processing {len(dates)} dates")
        
        results = []
        for calc_date in tqdm(dates):
            try:
                start_time = time.time()
                
                # Calculate VIX
                components = self.calculator.calculate(calc_date)
                
                # Get market VIX value
                market_vix = self.market_data.get_vix_value(calc_date)
                if market_vix is None:
                    self.logger.warning(f"No market VIX data for {calc_date}, skipping")
                    continue
                
                # Get option metrics
                options_data = self.calculator.get_current_options_data()
                if options_data is None:
                    self.logger.warning(f"No options data for {calc_date}, skipping")
                    continue
                    
                # Calculate option metrics
                option_metrics = calculate_option_metrics(options_data)
                
                # Store results
                results.append({
                    'ddate': calc_date,
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
                    
            except Exception as e:
                self.logger.error(f"Error processing {calc_date}: {str(e)}", exc_info=True)
        
        # Store all results at once
        if results:
            self.logger.info(f"Storing {len(results)} results")
            self.store_results(results)
            
        self.print_summary()
    
    def store_results(self, results: list):
        """Store calculation results, replacing existing data"""
        try:
            df = pd.DataFrame(results)
            
            # Store in database
            df.to_sql('calculated_vix', self.engine, if_exists='replace', index=False)
            self.logger.info(f"Stored {len(results)} results in database")
            
            # Store to CSV with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_filename = f"vix_results_{timestamp}.csv"
            csv_path = self.csv_dir / csv_filename
            df.to_csv(csv_path, index=False)
            self.logger.info(f"Stored results to {csv_path}")
            
        except Exception as e:
            self.logger.error(f"Error storing results: {str(e)}", exc_info=True)
            
    def print_summary(self):
        """Print calculation summary statistics"""
        query = """
        WITH max_diff_record AS (
            SELECT ddate, vix_diff
            FROM calculated_vix
            ORDER BY vix_diff DESC
            LIMIT 1
        )
        SELECT 
            COUNT(*) AS total_records,
            AVG(vix_diff) AS mean_diff,
            MAX(vix_diff) AS max_diff,
            (SELECT ddate FROM max_diff_record) AS max_diff_ddate,
            SUM(CASE WHEN vix_diff < 0.01 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 AS within_001,
            SUM(CASE WHEN vix_diff < 0.1 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 AS within_01
        FROM calculated_vix
        """
        with self.engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
    
        # Log the summary
        self.logger.info("\nCalculation Summary:")
        self.logger.info(f"Total records: {df['total_records'].iloc[0]}")
        self.logger.info(f"Mean difference: {df['mean_diff'].iloc[0]:.6f}")
        self.logger.info(f"Max difference: {df['max_diff'].iloc[0]:.6f} (Date: {df['max_diff_ddate'].iloc[0]})")
        self.logger.info(f"Within 0.01: {df['within_001'].iloc[0]:.1f}%")
        self.logger.info(f"Within 0.1: {df['within_01'].iloc[0]:.1f}%")       
    

def main():
    # Create VixRunner instance (which creates directories)
    runner = VixRunner()
    
    # Setup logging
    runner.logger.info("Starting VIX calculations")
    
    # Process dates
    runner.process_all_dates()
    runner.logger.info("VIX calculations complete")

if __name__ == "__main__":
    main()