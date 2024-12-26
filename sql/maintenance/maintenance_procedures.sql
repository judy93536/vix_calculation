-- Function to create next month's partition
CREATE OR REPLACE FUNCTION create_next_month_partition()
RETURNS void AS $$
DECLARE
    next_month date;
    partition_name text;
BEGIN
    -- Calculate next month
    next_month := date_trunc('month', current_date + interval '1 month');
    
    -- Create partition name
    partition_name := 'spx_1545_eod_y' || 
                     to_char(next_month, 'YYYY') || 
                     'm' || 
                     to_char(next_month, 'MM');
    
    -- Check if partition already exists
    IF NOT EXISTS (
        SELECT 1 FROM pg_class WHERE relname = partition_name
    ) THEN
        -- Create the partition
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF spx_1545_eod_new
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            next_month,
            next_month + interval '1 month'
        );
        
        -- Log the creation
        INSERT INTO partition_management_log 
            (partition_name, date_range_start, date_range_end, operation, success, details)
        VALUES 
            (partition_name, next_month, next_month + interval '1 month', 
             'CREATE', true, 'Monthly partition created by maintenance procedure');
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to perform data integrity checks
CREATE OR REPLACE FUNCTION check_data_integrity()
RETURNS TABLE (
    check_name text,
    issue_count bigint,
    details text
) AS $$
BEGIN
    -- Check for duplicate keys
    RETURN QUERY
    SELECT 
        'Duplicate Keys'::text as check_name,
        COUNT(*)::bigint as issue_count,
        'Duplicate symbol, quote_date, expiry, strike combinations'::text as details
    FROM (
        SELECT symbol, quote_date, expiry, strike, COUNT(*)
        FROM spx_1545_eod_new
        GROUP BY symbol, quote_date, expiry, strike
        HAVING COUNT(*) > 1
    ) dupes;

    -- Check for invalid dates
    RETURN QUERY
    SELECT 
        'Invalid Dates'::text,
        COUNT(*)::bigint,
        'Records where expiry <= quote_date'::text
    FROM spx_1545_eod_new
    WHERE expiry <= quote_date;

    -- Check for unusual record counts
    RETURN QUERY
    SELECT 
        'Unusual Daily Record Count'::text,
        COUNT(*)::bigint,
        'Days with more than 50000 records'::text
    FROM (
        SELECT quote_date, COUNT(*)
        FROM spx_1545_eod_new
        GROUP BY quote_date
        HAVING COUNT(*) > 50000
    ) high_counts;
END;
$$ LANGUAGE plpgsql;

-- Function to monitor partition sizes
CREATE OR REPLACE FUNCTION monitor_partition_sizes()
RETURNS TABLE (
    partition_name text,
    total_size text,
    table_size text,
    index_size text,
    row_count bigint
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        relname::text,
        pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname))::text,
        pg_size_pretty(pg_relation_size(schemaname || '.' || relname))::text,
        pg_size_pretty(pg_indexes_size(schemaname || '.' || relname))::text,
        n_live_tup::bigint
    FROM pg_stat_user_tables
    WHERE relname LIKE 'spx_1545_eod%'
    ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to check partition boundaries
CREATE OR REPLACE FUNCTION check_partition_coverage()
RETURNS TABLE (
    start_date timestamp with time zone,
    end_date timestamp with time zone,
    partition_name text,
    has_gap boolean
) AS $$
DECLARE
    last_end_date timestamp with time zone;
BEGIN
    -- Create temporary table to store boundaries
    CREATE TEMPORARY TABLE partition_bounds (
        partition_name text,
        start_date timestamp with time zone,
        end_date timestamp with time zone
    );
    
    -- Get partition boundaries
    FOR partition_name, start_date, end_date IN
        SELECT 
            relname::text,
            CASE 
                WHEN relname ~ '^spx_1545_eod_y\d{4}$' THEN 
                    make_timestamptz(substring(relname from 'y(\d{4})')::integer, 1, 1, 0, 0, 0)
                ELSE
                    make_timestamptz(
                        substring(relname from 'y(\d{4})')::integer,
                        substring(relname from 'm(\d{2})')::integer,
                        1, 0, 0, 0
                    )
            END,
            CASE 
                WHEN relname ~ '^spx_1545_eod_y\d{4}$' THEN 
                    make_timestamptz(substring(relname from 'y(\d{4})')::integer + 1, 1, 1, 0, 0, 0)
                ELSE
                    make_timestamptz(
                        substring(relname from 'y(\d{4})')::integer,
                        substring(relname from 'm(\d{2})')::integer,
                        1, 0, 0, 0
                    ) + interval '1 month'
            END
        FROM pg_class
        WHERE relname LIKE 'spx_1545_eod_y%'
        AND relkind = 'r'
    LOOP
        INSERT INTO partition_bounds VALUES (partition_name, start_date, end_date);
    END LOOP;
    
    -- Check for gaps
    RETURN QUERY
    SELECT 
        pb.start_date,
        pb.end_date,
        pb.partition_name,
        CASE 
            WHEN lag(pb.end_date) OVER (ORDER BY pb.start_date) IS NULL THEN false
            WHEN pb.start_date > lag(pb.end_date) OVER (ORDER BY pb.start_date) THEN true
            ELSE false
        END as has_gap
    FROM partition_bounds pb
    ORDER BY pb.start_date;
    
    -- Clean up
    DROP TABLE partition_bounds;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
COMMENT ON FUNCTION create_next_month_partition() IS 'Creates partition for next month if it doesn''t exist';
COMMENT ON FUNCTION check_data_integrity() IS 'Performs various data integrity checks';
COMMENT ON FUNCTION monitor_partition_sizes() IS 'Reports size and row count for all partitions';
COMMENT ON FUNCTION check_partition_coverage() IS 'Checks for gaps in partition date ranges';

-- Example maintenance queries:
/*
-- Create next month's partition
SELECT create_next_month_partition();

-- Check data integrity
SELECT * FROM check_data_integrity();

-- Monitor partition sizes
SELECT * FROM monitor_partition_sizes();

-- Check partition coverage
SELECT * FROM check_partition_coverage();
*/


