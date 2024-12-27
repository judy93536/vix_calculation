# src/vix_calculator/analysis/vix_calculation_analysis.py

import argparse
from datetime import datetime, date, timedelta
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from ..calculator.vix import VixCalculator
from ..production.vix_runner import VixRunner
from tqdm import tqdm
from collections import defaultdict

class VixCalculationAnalyzer:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.engine = self._create_db_engine()
        self.logger = self._setup_logging()
        
        # self.calculator = VixCalculator(self.engine)
        self.runner = VixRunner()
        self.runner.config = self.config  # Share our config with VixRunner
        
        # Create output directories
        self.output_dir = Path('results/analysis')
        self.plots_dir = self.output_dir / 'plots'
        self.csv_dir = self.output_dir / 'csv'
        self.error_dir = self.output_dir / 'errors'
        
        for dir_path in [self.output_dir, self.plots_dir, self.csv_dir, self.error_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)


    def _setup_logging(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        
        log_dir = Path('results/analysis/logs')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(
            log_dir / f'vix_calculation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
        file_handler.setLevel(logging.DEBUG)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _load_config(self, config_path):
        import yaml
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _create_db_engine(self):
        db_config = self.config['database']['postgres']
        db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        return create_engine(db_url)
    
    
    def get_available_dates(self, start_date: date, end_date: date) -> list:
        """Get available dates that have options data within the range"""
        query = """
        SELECT DISTINCT ddate
        FROM spx_eod_daily_options
        WHERE ddate BETWEEN %s AND %s
        ORDER BY ddate
        """
        start_int = int(start_date.strftime('%Y%m%d'))
        end_int = int(end_date.strftime('%Y%m%d'))
        
        with self.engine.connect() as conn:
            df = pd.read_sql_query(
                query, 
                conn, 
                params=(start_int, end_int)
            )
            return pd.to_datetime(df['ddate'].astype(str), format='%Y%m%d').dt.date.tolist() 
        

    def calculate_and_analyze(self, start_date: date, end_date: date):
        results = []
        failures = defaultdict(list)
        
        available_dates = self.get_available_dates(start_date, end_date)
        self.logger.info(f"Found {len(available_dates)} dates with data between {start_date} and {end_date}")
        
        for calc_date in tqdm(available_dates, desc="Processing dates"):
            try:
                # Calculate VIX using runner's calculator
                components = self.runner.calculator.calculate(calc_date)
                
                # Debug: print components structure
                #self.logger.info(f"Components for {calc_date}: {components}")
                #self.logger.info(f"Components dir: {dir(components)}")
                
                # Get market VIX value
                market_vix = self.runner.market_data.get_vix_value(calc_date)
                if market_vix is None:
                    self.logger.warning(f"No market VIX data for {calc_date}, skipping")
                    failures['no_market_data'].append(calc_date)
                    continue
                    
                result = {
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
                    'r2': float(components.R2)
                }
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Calculation failed for {calc_date}: {str(e)}", exc_info=True)
                self._save_failed_options(calc_date)
                failures['calculation_error'].append((calc_date, str(e)))

        if failures:
            self._save_failure_summary(failures, len(available_dates), len(results))
            self._save_failed_options(failed_date=calc_date)

        return pd.DataFrame(results) if results else pd.DataFrame()
        
    def analyze_differences(self, df: pd.DataFrame) -> dict:
        df['diff'] = abs(df['calculated_vix'] - df['market_vix'])
        
        analysis = {
            'mean_diff': df['diff'].mean(),
            'max_diff': df['diff'].max(),
            'max_diff_date': df.loc[df['diff'].idxmax(), 'ddate'],
            'within_01': (df['diff'] <= 0.1).mean() * 100,  # percentage
            'within_001': (df['diff'] <= 0.01).mean() * 100  # percentage
        }
        
        # Log analysis results
        self.logger.info("\nAnalysis Results:")
        self.logger.info(f"Mean difference: {analysis['mean_diff']:.6f}")
        self.logger.info(f"Max difference: {analysis['max_diff']:.6f} (Date: {analysis['max_diff_date']})")
        self.logger.info(f"Within 0.01: {analysis['within_001']:.1f}%")
        self.logger.info(f"Within 0.1: {analysis['within_01']:.1f}%")
        
        return analysis
    
    def _save_failed_options(self, failed_date: date):
        """Save options data for failed calculations"""
        try:
            query = text("""
                SELECT * FROM spx_eod_daily_options 
                WHERE quote_date = :calc_date
            """)  # Use :parameter for placeholders
            
            with self.engine.connect() as conn:
                failed_data = pd.read_sql(query, conn, params={'calc_date': failed_date})
                
            self.logger.info(f"failed_data date: {failed_date}")  
              
            if not failed_data.empty:
                filename = f'failed_options_{failed_date:%Y%m%d}_{datetime.now():%H%M%S}.csv'
                failed_data.to_csv(self.error_dir / filename, index=False)
                self.logger.info(f"Saved failed options data to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save options data: {str(e)}")


    def _save_failure_summary(self, failures: dict, total_dates: int, successful_calcs: int):
        """Save a summary of failures to a text file"""
        available_dates = failures['calculation_error'] + failures['no_market_data']
        results = total_dates - len(available_dates)
        
    
def main():
    parser = argparse.ArgumentParser(description='VIX Calculation and Analysis Tool')
    parser.add_argument('--config', type=str, default='config/config.yaml', help='Path to config file')
    parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    
    analyzer = VixCalculationAnalyzer(args.config)
    results_df = analyzer.calculate_and_analyze(start_date, end_date)
    analyzer.analyze_differences(results_df)
    

if __name__ == "__main__":
    main()


