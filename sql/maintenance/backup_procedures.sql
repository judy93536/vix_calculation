-- Create backup logging table
CREATE TABLE IF NOT EXISTS backup_log (
    log_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    backup_type TEXT, -- 'FULL', 'PARTITION', 'S3'
    target_location TEXT,
    success BOOLEAN,
    file_size BIGINT,
    details TEXT
);

CREATE OR REPLACE FUNCTION generate_backup_filename(backup_type TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN 'spx_eod_daily_options_' || 
           backup_type || '_' || 
           to_char(current_timestamp, 'YYYYMMDD_HH24MISS') || 
           '.backup';
END;
$$ LANGUAGE plpgsql;

-- Function to backup specific partition
CREATE OR REPLACE FUNCTION backup_partition(partition_name TEXT, backup_dir TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    backup_file TEXT;
    backup_command TEXT;
    success BOOLEAN;
BEGIN
    -- Generate backup filename
    backup_file := backup_dir || '/' || generate_backup_filename('PARTITION_' || partition_name);
    
    -- Create pg_dump command
    backup_command := format(
        'pg_dump --table=%I --format=custom --file=%L --verbose cboe',
        partition_name,
        backup_file
    );
    
    -- Execute backup
    BEGIN
        EXECUTE backup_command;
        success := true;
    EXCEPTION WHEN OTHERS THEN
        success := false;
    END;
    
    -- Log backup attempt
    INSERT INTO backup_log (
        backup_type, 
        target_location, 
        success, 
        file_size,
        details
    )
    SELECT 
        'PARTITION',
        backup_file,
        success,
        pg_size_pretty(pg_relation_size(partition_name::regclass))::TEXT,
        CASE 
            WHEN success THEN 'Partition backup completed successfully'
            ELSE 'Backup failed: ' || SQLERRM
        END;
    
    RETURN success;
END;
$$ LANGUAGE plpgsql;

-- Function for full database backup
CREATE OR REPLACE FUNCTION backup_full_database(backup_dir TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    backup_file TEXT;
    backup_command TEXT;
    success BOOLEAN;
BEGIN
    -- Generate backup filename
    backup_file := backup_dir || '/' || generate_backup_filename('FULL');
    
    -- Create pg_dump command
    backup_command := format(
        'pg_dump --format=custom --file=%L --verbose cboe',
        backup_file
    );
    
    -- Execute backup
    BEGIN
        EXECUTE backup_command;
        success := true;
    EXCEPTION WHEN OTHERS THEN
        success := false;
    END;
    
    -- Log backup attempt
    INSERT INTO backup_log (
        backup_type, 
        target_location, 
        success, 
        file_size,
        details
    )
    VALUES (
        'FULL',
        backup_file,
        success,
        (SELECT pg_database_size('cboe')),
        CASE 
            WHEN success THEN 'Full database backup completed successfully'
            ELSE 'Backup failed: ' || SQLERRM
        END
    );
    
    RETURN success;
END;
$$ LANGUAGE plpgsql;

-- Function to clean up old backups
CREATE OR REPLACE FUNCTION cleanup_old_backups(backup_dir TEXT, days_to_keep INTEGER)
RETURNS void AS $$
DECLARE
    cleanup_command TEXT;
BEGIN
    cleanup_command := format(
        'find %s -name "spx_eod_daily_options_*.backup" -type f -mtime +%s -delete',
        backup_dir,
        days_to_keep
    );
    
    EXECUTE cleanup_command;
END;
$$ LANGUAGE plpgsql;


-- Example maintenance schedule using pg_cron
-- Make sure pg_cron extension is installed: CREATE EXTENSION pg_cron;

-- Schedule full backup every Sunday at 1 AM
SELECT cron.schedule('full_backup', '0 1 * * 0', 
    $$SELECT backup_full_database('/path/to/backup/dir')$$);

-- Schedule partition backup for current month every day at 2 AM
SELECT cron.schedule('partition_backup', '0 2 * * *', 
    $$SELECT backup_partition(
        'spx_eod_daily_options_y' || 
        to_char(current_date, 'YYYY') || 
        'm' || 
        to_char(current_date, 'MM'),
        '/path/to/backup/dir'
    )$$);

-- Clean up backups older than 30 days every Monday at 3 AM
SELECT cron.schedule('backup_cleanup', '0 3 * * 1', 
    $$SELECT cleanup_old_backups('/path/to/backup/dir', 30)$$);

-- Schedule partition maintenance checks every day at 4 AM
SELECT cron.schedule('partition_maintenance', '0 4 * * *', $$
    -- Create next month's partition if needed
    SELECT create_next_month_partition();
    
    -- Run integrity checks
    INSERT INTO partition_management_log (operation, success, details)
    SELECT 
        'INTEGRITY_CHECK',
        NOT EXISTS (
            SELECT 1 FROM check_data_integrity() WHERE issue_count > 0
        ),
        CASE 
            WHEN NOT EXISTS (SELECT 1 FROM check_data_integrity() WHERE issue_count > 0)
            THEN 'All integrity checks passed'
            ELSE 'Issues found - check check_data_integrity() output'
        END;
$$);

COMMENT ON FUNCTION backup_partition(TEXT, TEXT) IS 'Creates backup of specified partition';
COMMENT ON FUNCTION backup_full_database(TEXT) IS 'Creates full database backup';
COMMENT ON FUNCTION cleanup_old_backups(TEXT, INTEGER) IS 'Removes backup files older than specified days';

-- To monitor backups:
/*
-- View recent backups
SELECT * FROM backup_log 
ORDER BY timestamp DESC 
LIMIT 10;

-- Check backup sizes over time
SELECT 
    backup_type,
    date_trunc('day', timestamp) as backup_date,
    COUNT(*) as backup_count,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_backups
FROM backup_log
GROUP BY backup_type, date_trunc('day', timestamp)
ORDER BY backup_date DESC;
*/

