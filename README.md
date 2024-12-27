# VIX Calculator Implementation

A robust implementation of the CBOE VIX calculation methodology with comprehensive data management capabilities.

## Overview

This project provides:

- Complete VIX calculation following CBOE methodology
- Automated data management and processing
- Integration with CBOE data sources
- Treasury rate handling and interpolation
- Production-ready scheduling and monitoring

## Key Features

### VIX Calculation

- Accurate implementation of CBOE VIX methodology
- SPX/SPXW options handling
- CMT rate interpolation
- Market holiday support
- Result storage in PostgreSQL and CSV formats

### Data Management

- Automated SFTP downloads from CBOE
- Treasury rate updates and interpolation
- Market data synchronization
- Date-partitioned database storage
- Comprehensive data validation

## Installation

1. Clone the repository and create virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create and configure `config/config.yaml`:

```yaml
database:
  postgres:
    host: "127.0.0.1"
    port: 5432
    database: "cboe"
    user: "options"
    password: "your_password"

sftp:
  cboe:
    hostname: "sftp.datashop.livevol.com"
    username: "your_username"
    password: "your_password"
    remote_path: "/your/path/"
```

4. Set up database schema:

```bash
psql -d cboe -f sql/schema/create_tables.sql
psql -d cboe -f sql/schema/partitions.sql
```

## Usage

### Manual Operation

1. Import latest market data:

```bash
python -m src.vix_calculator.production.data_import_runner
```

2. Calculate VIX:

```bash
python -m vix_calculator.production.vix_runner
```

### Automated Operation

The project includes automated scheduling via cron jobs for production environments.

1. Set up the alert handler for notifications:

```bash
# Install required packages
pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client

# Initialize Gmail authentication (one-time setup)
python alert_handler.py --subject "Test" --body "Testing alerts"
```

2. Configure cron job:

```bash
# Edit crontab
crontab -e

# Add entry to run at 5:00 PM (17:00) Monday through Friday
0 17 * * 1-5 /raid/vscode_projects/vix_calculator/vix_calculator_cron.sh
```

The cron job will:

- Download and process new market data
- Calculate VIX values
- Store results in database and CSV files
- Send email notifications for success/failure

### Analysis Tools

Generate VIX analysis for a date range:

```bash
python -m vix_calculator.analysis.vix_analysis --start-date 2022-07-07 --end-date 2024-07-07
```

This creates:

- Plots in `results/analysis/plots`
- CSV files in `results/analysis/csv`

## Project Structure

```
vix_calculator/
├── src/                          # Source code
│   └── vix_calculator/
│       ├── analysis/            # Analysis tools
│       ├── calculator/          # VIX calculation
│       ├── data/               # Data management
│       └── production/         # Production scripts
├── sql/                        # Database management
├── tests/                      # Test suite
├── results/                    # Output files
├── config/                     # Configuration
└── requirements.txt           # Dependencies
```

## Output and Logging

Results are stored in:

1. Database: `calculated_vix` table
2. CSV files: `results/csv/vix_results_YYYYMMDD_HHMMSS.csv`
3. Logs: `results/logs/vix_calculator_YYYYMMDD_HHMMSS.log`

## Database Schema

Required tables:

- `spx_eod_daily_options`: Option chain data
- `daily_treasury_par_yield`: Treasury rates
- `calculated_vix`: VIX calculation results

See SQL schema files for detailed structure.

## Maintenance

### Database

- Monitor partition growth
- Regular integrity checks
- Backup procedures

### Data Updates

- Daily CBOE data imports
- Treasury rate updates
- Market data synchronization

## Error Handling

Comprehensive error handling for:

- SFTP connections
- Data validation
- Calculation anomalies
- Database operations
- File system operations

## Python Usage Examples

```python
from vix_calculator import VixCalculator
from vix_calculator.data import DatabaseConnection

# Initialize calculator
db_conn = DatabaseConnection()
calculator = VixCalculator(db_conn.get_engine())

# Calculate for a single date
result = calculator.calculate(date(2020, 3, 24))
```

## Support and Documentation

For more information:

- Check CBOE VIX white paper
- Review code documentation
- See test cases for examples
- Contact repository maintainers
