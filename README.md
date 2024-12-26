# VIX Calculator Implementation

This project implements the CBOE VIX calculation methodology and data management with specific enhancements for handling:

- SPX option expiration dates
- Market holidays and irregular trading hours
- CMT rate interpolation
- EOD option chains
- SFTP data imports from CBOE
- Treasury rate imports
- Market data imports

## Project Structure

```python
vix_calculator/
│
├── src/
│   └── vix_calculator/
│       ├── calculator/
│       │   ├── __init__.py
│       │   └── vix.py              # Core VIX calculation logic
│       ├── data/
│       │   ├── __init__.py
│       │   ├── database.py         # Database connection handling
│       │   ├── interest_rates.py   # Interest rate calculations
│       │   ├── market_data.py      # Market data and metrics
│       │   ├── importers/          # Data importers
│       │   │   ├── base_importer.py
│       │   │   ├── cboe_options_importer.py
│       │   │   ├── treasury_rates_importer.py
│       │   │   └── market_data_importer.py
│       │   └── processors/         # Data processors
│       │       └── cboe_processor.py
│       └── production/
│           ├── __init__.py
│           ├── vix_runner.py       # VIX calculation script
│           └── data_import_runner.py # Data import script
│
├── sql/                           # Database management
│   ├── schema/                    # Table definitions
│   │   ├── create_tables.sql
│   │   └── partitions.sql
│   └── maintenance/               # Maintenance procedures
│       ├── maintenance_procedures.sql
│       └── backup_procedures.sql
│
├── tests/
│   ├── __init__.py
│   ├── test_vix.py
│   └── test_market_data.py
│
├── results/
│   ├── csv/                        # CSV output files
│   │   └── vix_results_YYYYMMDD_HHMMSS.csv
│   └── logs/                       # Log files
│       └── vix_calculator_YYYYMMDD_HHMMSS.log
│
├── config/                        # Configuration
│   ├── config.yaml               # Main configuration
│   └── config.yaml.example       # Example configuration
├── requirements.txt
├── setup.py
└── README.md
```

## Features

### VIX Calculation

- Accurate VIX calculation matching CBOE methodology
- Proper handling of SPX/SPXW options
- Sophisticated interest rate interpolation using CMT rates
- Support for market holidays
- Integration with PostgreSQL for data storage
- Result storage in both database and CSV formats
- Comprehensive logging and error handling

### Data Management

- Automated SFTP downloads from CBOE
- Partitioned database storage by date
- Data validation and integrity checks
- Treasury rate updates and interpolation
- Market data synchronization
- Backup and maintenance procedures

## Technical Details

### Interest Rate Handling

- Uses CMT (Constant Maturity Treasury) rates
- Interpolates between appropriate tenors based on DTE
- Converts to continuous compounding
- Handles market holidays through date-based interpolation

### Option Chain Processing

- Uses EOD option chains
- Handles both SPX and SPXW options
- Selects appropriate strikes around forward price
- Processes near-term and next-term expirations

### Data Import and Storage

- Daily SFTP downloads of CBOE data
- Automatic file processing and validation
- Partitioned storage by date range
- Data integrity checks and constraints

### Data Requirements

- PostgreSQL database with:
  - SPX option chains (table: spx_1545_eod)
  - Treasury rates (table: daily_treasury_par_yield)
  - VIX results (table: calculated_vix)

## Installation and Setup

1. Clone the repository and create virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Create and configure config/config.yaml:

```
database:
  postgres:
    host: '127.0.0.1'
    port: 5432
    database: 'cboe'
    user: 'options'
    password: 'your_password'

sftp:
  cboe:
    hostname: 'sftp.datashop.livevol.com'
    username: 'your_username'
    password: 'your_password'
    remote_path: '/your/path/'
```

4. Set up database schema:

```
psql -d cboe -f sql/schema/create_tables.sql
psql -d cboe -f sql/schema/partitions.sql
```

### Daily Operations

1. Import latest market data:

```
python -m src.vix_calculator.production.data_import_runner
```

This will:

- Download new CBOE option chains
- Update Treasury rates
- Update market data
- Process and validate all data
- Store in partitioned tables

2. Calculate VIX:

```
python -m src.vix_calculator.production.vix_runner
```

This will:

- Process all available dates
- Store results in database
- Generate CSV output files
- Create detailed logs

## Database Schema

### Main Tables:

Required tables:

```sql
-- Option chain data
CREATE TABLE spx_eod_daily_options (
    symbol text NOT NULL,
    quote_date timestamp with time zone NOT NULL,
    ddate bigint NOT NULL,
    root text NOT NULL,
    expiry timestamp with time zone NOT NULL,
    dte bigint NOT NULL,
    strike double precision NOT NULL,
    -- [Call and Put option fields]
    CONSTRAINT spx_eod_daily_options_pkey PRIMARY KEY (symbol, quote_date, root, expiry, strike)
) PARTITION BY RANGE (quote_date);

-- Treasury yield data
CREATE TABLE daily_treasury_par_yield (
    date DATE PRIMARY KEY,
    "1mo" FLOAT,
    "2mo" FLOAT,
    "3mo" FLOAT,
    -- ... other tenors
);

-- Results table
CREATE TABLE calculated_vix (
    ddate DATE PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE,
    calculated_vix DOUBLE PRECISION,
    market_vix DOUBLE PRECISION,
    -- [Calculation fields]
    -- Volume metrics
    call_volume BIGINT,
    put_volume BIGINT,
    -- [Additional metrics]
);
```

## Output Files

Results are stored in:

1. Database table: calculated_vix
2. CSV files: results/csv/vix_results_YYYYMMDD_HHMMSS.csv
3. Logs: results/logs/vix_calculator_YYYYMMDD_HHMMSS.log

## Maintenance Procedures

### Database Maintenance

- Monitor partition sizes and growth
- Regular integrity checks
- Data validation procedures
- Backup and recovery procedures

### Data Updates

- Daily CBOE data imports
- Treasury rate updates
- Market data synchronization
- Results verification

### Visualization

- Direct plotting using matplotlib
- Database storage for Grafana visualization
- Comparison with actual VIX index
- CSV exports for external analysis tools

## Error Handling

The system includes comprehensive error handling for:

- SFTP connection issues
- Data validation failures
- Calculation anomalies
- Database constraints
- File system operations

### Logging

Detailed logging is provided for:

- Data imports and processing
- Calculation steps and results
- Database operations
- Error conditions and resolutions

## Python Usage Examples

### Basic VIX Calculation

```python
from vix_calculator import VixCalculator
from vix_calculator.data import DatabaseConnection

# Initialize calculator
db_conn = DatabaseConnection()
calculator = VixCalculator(db_conn.get_engine())

# Calculate for a single date
result = calculator.calculate(date(2020, 3, 24))
```

### Production Runner

To run full calculations with logging and storage:

```bash
python -m vix_calculator.production.vix_runner
```

This will:

1. Process all available dates
2. Store results in database
3. Generate CSV output files
4. Create detailed logs

### Data Import

```
from vix_calculator.data.importers import CboeOptionsImporter

# Initialize importer
importer = CboeOptionsImporter('config/config.yaml')

# Run import
importer.import_data()
```
