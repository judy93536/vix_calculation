# src/vix_calculator/analysis/vix_analysis.py

import argparse
from datetime import datetime, date
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine, text
import logging

class VixAnalyzer:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.engine = self._create_db_engine()
        self.logger = self._setup_logging()
        
        # Create output directories
        self.output_dir = Path('results/analysis')
        self.plots_dir = self.output_dir / 'plots'
        self.csv_dir = self.output_dir / 'csv'
        
        for dir_path in [self.output_dir, self.plots_dir, self.csv_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

    def _load_config(self, config_path):
        import yaml
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _create_db_engine(self):
        db_config = self.config['database']['postgres']
        db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        return create_engine(db_url)


    def _setup_logging(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)  # Set to lowest level to capture everything
        
        # File handler for exceptions and debug info
        log_dir = Path('results/analysis/logs')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(
            log_dir / f'vix_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
        file_handler.setLevel(logging.DEBUG)  # Capture all levels in file
        
        # Console handler for info and above
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def get_vix_data(self, start_date: date, end_date: date) -> pd.DataFrame:
        query = text("""
            SELECT ddate, timestamp, calculated_vix, market_vix,
                    dte1, dte2, sigma1, sigma2
            FROM calculated_vix
            WHERE ddate BETWEEN :start_date AND :end_date
            ORDER BY ddate
        """)
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                'start_date': start_date,
                'end_date': end_date
            })
        return df

    
    def plot_vix_comparison(self, df: pd.DataFrame, analysis: dict) -> Path:
            plt.figure(figsize=(12, 6))
            
            # Plot VIX lines
            plt.plot(df['ddate'], df['calculated_vix'], 'g-', label='Calculated VIX')
            plt.plot(df['ddate'], df['market_vix'], 'y-', label='Market VIX')
            
            # Add metrics text box
            metrics_text = (
                f"Mean diff: {analysis['mean_diff']:.3f}\n"
                f"Max diff: {analysis['max_diff']:.3f}\n"
                f"Within 0.01: {analysis['within_001']:.1f}%\n"
                f"Within 0.1: {analysis['within_01']:.1f}%"
            )
            
            # Get y-axis range for positioning
            y_min, y_max = plt.ylim()
            y_mid = (y_max + y_min) / 2
            
            # Add text box
            plt.text(0.02, 0.7, metrics_text, 
                    transform=plt.gca().transAxes,
                    bbox=dict(facecolor='white', alpha=0.8),
                    fontsize=10,
                    verticalalignment='center')
            
            plt.title('VIX Comparison')
            plt.xlabel('Date')
            plt.ylabel('VIX Value')
            plt.grid(True)
            plt.legend()
            
            # Save plot
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            plot_path = self.plots_dir / f'vix_comparison_{timestamp}.png'
            plt.savefig(plot_path)
            plt.close()
            
            return plot_path


    def analyze_differences(self, df: pd.DataFrame) -> dict:
        df['diff'] = abs(df['calculated_vix'] - df['market_vix'])
        
        analysis = {
            'mean_diff': df['diff'].mean(),
            'max_diff': df['diff'].max(),
            'max_diff_date': df.loc[df['diff'].idxmax(), 'ddate'],
            'within_01': (df['diff'] <= 0.1).mean() * 100,  # percentage
            'within_001': (df['diff'] <= 0.01).mean() * 100  # percentage
        }
        
        return analysis
    
    def run_analysis(self, start_date: date, end_date: date):
        self.logger.info(f"Running analysis from {start_date} to {end_date}")
        
        try:
            # Get data
            df = self.get_vix_data(start_date, end_date)
            if df.empty:
                self.logger.warning("No data found for specified date range")
                return
                
            # Get analysis first
            analysis = self.analyze_differences(df)
            
            # Create plot with analysis
            plot_path = self.plot_vix_comparison(df, analysis)
            self.logger.info(f"Plot saved to {plot_path}")
            
            # Log analysis results
            self.logger.info("\nAnalysis Results:")
            self.logger.info(f"Mean difference: {analysis['mean_diff']:.6f}")
            self.logger.info(f"Max difference: {analysis['max_diff']:.6f} (Date: {analysis['max_diff_date']})")
            self.logger.info(f"Within 0.01: {analysis['within_001']:.1f}%")
            self.logger.info(f"Within 0.1: {analysis['within_01']:.1f}%")
            
            # Save results
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_path = self.csv_dir / f'vix_analysis_{timestamp}.csv'
            df.to_csv(csv_path, index=False)
            self.logger.info(f"Results saved to {csv_path}")
            
        except Exception as e:
            self.logger.error(f"Analysis failed: {str(e)}", exc_info=True)
            raise

def main():
   parser = argparse.ArgumentParser(description='VIX Analysis Tool')
   parser.add_argument('--config', type=str, default='config/config.yaml', help='Path to config file')
   parser.add_argument('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
   parser.add_argument('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
   args = parser.parse_args()
   
   start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
   end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
   
   analyzer = VixAnalyzer(args.config)
   analyzer.run_analysis(start_date, end_date)

if __name__ == "__main__":
   main()
