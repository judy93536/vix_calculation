-- Create yearly partitions (2018-2023)
DO $$
BEGIN
    FOR yr IN 2018..2023 LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = 'spx_1545_eod_y' || yr
        ) THEN
            EXECUTE format(
                'CREATE TABLE spx_1545_eod_y%s PARTITION OF spx_1545_eod_new
                 FOR VALUES FROM (%L) TO (%L)',
                yr,
                make_date(yr, 1, 1),
                make_date(yr + 1, 1, 1)
            );
            
            INSERT INTO partition_management_log 
                (partition_name, date_range_start, date_range_end, operation, success, details)
            VALUES 
                ('spx_1545_eod_y' || yr, 
                 make_date(yr, 1, 1),
                 make_date(yr + 1, 1, 1),
                 'CREATE', true, 'Yearly partition created successfully');
        END IF;
    END LOOP;
END $$;

-- Create monthly partitions for 2024
DO $$
DECLARE
    start_date date;
    partition_name text;
BEGIN
    FOR month IN 1..12 LOOP
        start_date := date_trunc('month', make_date(2024, month, 1));
        partition_name := 'spx_1545_eod_y2024m' || to_char(start_date, 'MM');
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF spx_1545_eod_new
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

-- Create monthly partitions for 2025
DO $$
DECLARE
    start_date date;
    partition_name text;
BEGIN
    FOR month IN 1..12 LOOP
        start_date := date_trunc('month', make_date(2025, month, 1));
        partition_name := 'spx_1545_eod_y2025m' || to_char(start_date, 'MM');
        
        IF NOT EXISTS (
            SELECT 1 FROM pg_class WHERE relname = partition_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF spx_1545_eod_new
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
COMMENT ON TABLE spx_1545_eod_y2018 IS 'SPX options data for year 2018';
COMMENT ON TABLE spx_1545_eod_y2019 IS 'SPX options data for year 2019';
COMMENT ON TABLE spx_1545_eod_y2020 IS 'SPX options data for year 2020';
COMMENT ON TABLE spx_1545_eod_y2021 IS 'SPX options data for year 2021';
COMMENT ON TABLE spx_1545_eod_y2022 IS 'SPX options data for year 2022';
COMMENT ON TABLE spx_1545_eod_y2023 IS 'SPX options data for year 2023';


