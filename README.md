# VIX Calculator Implementation

This project implements the CBOE VIX calculation methodology with specific enhancements for handling:

- SPX option expiration dates
- Market holidays and irregular trading hours
- CMT rate interpolation
- 15:45 EOD option chains

## Project Structure

```
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
│       │   └── market_data.py      # Market data and metrics
│       └── production/
│           ├── __init__.py
│           └── vix_runner.py       # Production calculation script
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
├── requirements.txt
├── setup.py
├── README.md
└── .env                           # Database configuration
```

## Features

- Accurate VIX calculation matching CBOE methodology
- Proper handling of SPX/SPXW options
- Sophisticated interest rate interpolation using CMT rates
- Support for market holidays
- Integration with PostgreSQL for data storage
- Result storage in both database and CSV formats
- Comprehensive logging and error handling

## Technical Details

### Interest Rate Handling

- Uses CMT (Constant Maturity Treasury) rates
- Interpolates between appropriate tenors based on DTE
- Converts to continuous compounding
- Handles market holidays through date-based interpolation

### Option Chain Processing

- Uses 15:45 EOD option chains
- Handles both SPX and SPXW options
- Selects appropriate strikes around forward price
- Processes near-term and next-term expirations

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

3. Configure database in .env:

```
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=cboe
DB_USER=options
DB_PASSWORD=your_password
```

## Usage

### Basic Usage

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

## Database Schema

Required tables:

```sql
-- Option chain data
CREATE TABLE spx_1545_eod (
    -- ... (existing schema)
);

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
    dte1 INTEGER,
    dte2 INTEGER,
    f1 DOUBLE PRECISION,
    f2 DOUBLE PRECISION,
    k0_1 DOUBLE PRECISION,
    k0_2 DOUBLE PRECISION,
    sigma1 DOUBLE PRECISION,
    sigma2 DOUBLE PRECISION,
    r1 DOUBLE PRECISION,
    r2 DOUBLE PRECISION,
    -- Volume metrics
    call_volume BIGINT,
    put_volume BIGINT,
    put_call_volume_ratio DOUBLE PRECISION,
    -- Open Interest metrics
    call_oi BIGINT,
    put_oi BIGINT,
    put_call_oi_ratio DOUBLE PRECISION,
    -- Implied Volatility metrics
    avg_call_iv DOUBLE PRECISION,
    avg_put_iv DOUBLE PRECISION,
    put_call_iv_ratio DOUBLE PRECISION,
    otm_put_iv_skew DOUBLE PRECISION,
    -- Calculation metrics
    vix_diff DOUBLE PRECISION,
    calc_time DOUBLE PRECISION
);
```

## Output Files

Results are stored in:

1. Database table: calculated_vix
2. CSV files: results/csv/vix_results_YYYYMMDD_HHMMSS.csv
3. Logs: results/logs/vix_calculator_YYYYMMDD_HHMMSS.log

## Visualization

- Direct plotting using matplotlib
- Database storage for Grafana visualization
- Comparison with actual VIX index
- CSV exports for external analysis tools
