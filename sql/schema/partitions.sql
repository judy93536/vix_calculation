-- Set timezone for session
SET timezone = 'America/New_York';

-- Create yearly partitions (2018-2023)
DO $$
BEGIN
    FOR yr IN 2018..2023 LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = 'spx_eod_daily_options_y' || yr
        ) THEN
            EXECUTE format(
                'CREATE TABLE spx_eod_daily_options_y%s PARTITION OF spx_eod_daily_options
                 FOR VALUES FROM (%L::timestamptz) TO (%L::timestamptz)',
                yr,
                make_timestamptz(yr, 1, 1, 0, 0, 0, 'America/New_York'),
                make_timestamptz(yr + 1, 1, 1, 0, 0, 0, 'America/New_York')
            );
            
            -- Log partition creation
            INSERT INTO partition_management_log 
                (partition_name, date_range_start, date_range_end, operation, success, details)
            VALUES 
                ('spx_eod_daily_options_y' || yr, 
                 make_timestamptz(yr, 1, 1, 0, 0, 0, 'America/New_York'),
                 make_timestamptz(yr + 1, 1, 1, 0, 0, 0, 'America/New_York'),
                 'CREATE', true, 'Yearly partition created successfully');
        END IF;
    END LOOP;
END $$;

-- Create monthly partitions for current year (2024)
DO $$
DECLARE
    start_date timestamptz;
    partition_name text;
BEGIN
    FOR month IN 1..12 LOOP
        start_date := make_timestamptz(2024, month, 1, 0, 0, 0, 'America/New_York');
        partition_name := 'spx_eod_daily_options_y2024m' || 
                         LPAD(month::text, 2, '0');
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF spx_eod_daily_options
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                start_date,
                start_date + interval '1 month'
            );
            
            INSERT INTO partition_management_log 
                (partition_name, date_range_start, date_range_end, operation, success, details)
            VALUES 
                (partition_name, start_date, start_date + interval '1 month',
                 'CREATE', true, 'Monthly partition created successfully');
        END IF;
    END LOOP;
END $$;

-- Create monthly partitions for next year (2025)
DO $$
DECLARE
    start_date timestamptz;
    partition_name text;
BEGIN
    FOR month IN 1..12 LOOP
        start_date := make_timestamptz(2025, month, 1, 0, 0, 0, 'America/New_York');
        partition_name := 'spx_eod_daily_options_y2025m' || 
                         LPAD(month::text, 2, '0');
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF spx_eod_daily_options
                 FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                start_date,
                start_date + interval '1 month'
            );
            
            INSERT INTO partition_management_log 
                (partition_name, date_range_start, date_range_end, operation, success, details)
            VALUES 
                (partition_name, start_date, start_date + interval '1 month',
                 'CREATE', true, 'Monthly partition created successfully');
        END IF;
    END LOOP;
END $$;

-- Add comments documenting partition strategy
DO $$
BEGIN
    FOR yr IN 2018..2023 LOOP
        EXECUTE format(
            'COMMENT ON TABLE spx_eod_daily_options_y%s IS $c$SPX options data for year %s (America/New_York timezone)$c$',
            yr, yr
        );
    END LOOP;
END $$;