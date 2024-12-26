-- Create partition management log table
CREATE TABLE IF NOT EXISTS partition_management_log (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    partition_name TEXT,
    date_range_start TIMESTAMP,
    date_range_end TIMESTAMP,
    operation TEXT,
    success BOOLEAN,
    details TEXT
);

-- Create main partitioned options table
CREATE TABLE spx_1545_eod_new (
    -- Primary key columns
    symbol text NOT NULL,
    quote_date timestamp with time zone NOT NULL,
    ddate bigint NOT NULL,
    root text NOT NULL,
    expiry timestamp with time zone NOT NULL,
    dte bigint NOT NULL,
    strike double precision NOT NULL,
    
    -- Call option data
    open_c double precision,
    high_c double precision,
    low_c double precision,
    close_c double precision,
    trade_volume_c bigint,
    bid_size_1545_c bigint,
    bid_1545_c double precision,
    ask_size_1545_c bigint,
    ask_1545_c double precision,
    underlying_bid_1545_c double precision,
    underlying_ask_1545_c double precision,
    implied_underlying_price_1545_c double precision,
    active_underlying_price_1545_c double precision,
    implied_volatility_1545_c double precision,
    delta_1545_c double precision,
    gamma_1545_c double precision,
    theta_1545_c double precision,
    vega_1545_c double precision,
    rho_1545_c double precision,
    bid_size_eod_c bigint,
    bid_eod_c double precision,
    ask_size_eod_c bigint,
    ask_eod_c double precision,
    underlying_bid_eod_c double precision,
    underlying_ask_eod_c double precision,
    vwap_c double precision,
    open_interest_c bigint,
    
    -- Put option data
    open_p double precision,
    high_p double precision,
    low_p double precision,
    close_p double precision,
    trade_volume_p bigint,
    bid_size_1545_p bigint,
    bid_1545_p double precision,
    ask_size_1545_p bigint,
    ask_1545_p double precision,
    underlying_bid_1545_p double precision,
    underlying_ask_1545_p double precision,
    implied_underlying_price_1545_p double precision,
    active_underlying_price_1545_p double precision,
    implied_volatility_1545_p double precision,
    delta_1545_p double precision,
    gamma_1545_p double precision,
    theta_1545_p double precision,
    vega_1545_p double precision,
    rho_1545_p double precision,
    bid_size_eod_p bigint,
    bid_eod_p double precision,
    ask_size_eod_p bigint,
    ask_eod_p double precision,
    underlying_bid_eod_p double precision,
    underlying_ask_eod_p double precision,
    vwap_p double precision,
    open_interest_p bigint,
    
    -- Calculated fields
    mid_eod_c double precision,
    mid_eod_p double precision,
    mid_diff_eod double precision,
    mid_1545_c double precision,
    mid_1545_p double precision,
    mid_diff_1545 double precision,
    
    -- Constraints
    CONSTRAINT spx_1545_eod_pkey PRIMARY KEY (symbol, quote_date, expiry, strike),
    CONSTRAINT valid_strike CHECK (strike > 0),
    CONSTRAINT valid_dates CHECK (expiry > quote_date),
    CONSTRAINT valid_dte CHECK (dte >= 0)
) PARTITION BY RANGE (quote_date);

-- Create ML/Grafana optimized index
CREATE INDEX idx_spx_1545_eod_new_ml ON spx_1545_eod_new(quote_date, strike, mid_1545_c, mid_1545_p);

-- Create index on ddate for quick lookups
CREATE INDEX idx_spx_1545_eod_new_ddate ON spx_1545_eod_new(ddate);

-- Add comments
COMMENT ON TABLE spx_1545_eod_new IS 'SPX options data partitioned by quote_date with intraday (15:45) and EOD data';
COMMENT ON TABLE partition_management_log IS 'Tracks partition creation and management operations';

