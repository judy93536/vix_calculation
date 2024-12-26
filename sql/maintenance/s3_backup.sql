-- Add S3 upload tracking to backup_log
ALTER TABLE backup_log 
    ADD COLUMN IF NOT EXISTS s3_upload_status TEXT,
    ADD COLUMN IF NOT EXISTS s3_path TEXT;

-- Function to sync backup to S3
CREATE OR REPLACE FUNCTION sync_backup_to_s3(
    backup_file TEXT,
    s3_bucket TEXT DEFAULT 'judy-glacier-bucket'
)
RETURNS BOOLEAN AS $$
DECLARE
    aws_command TEXT;
    success BOOLEAN;
BEGIN
    -- Construct AWS CLI command with new path
    aws_command := format(
        'aws s3 cp %s s3://%s/spx_eod_daily_options_backups/',
        backup_file,
        s3_bucket
    );
    
    -- Execute AWS CLI command
    BEGIN
        EXECUTE aws_command;
        success := true;
    EXCEPTION WHEN OTHERS THEN
        success := false;
    END;
    
    -- Update backup log with S3 status
    UPDATE backup_log 
    SET 
        s3_upload_status = CASE 
            WHEN success THEN 'UPLOADED'
            ELSE 'FAILED'
        END,
        s3_path = CASE 
            WHEN success THEN 's3://' || s3_bucket || '/spx_options_backups/' || 
                           substring(backup_file from '[^/]+$')
            ELSE NULL
        END
    WHERE target_location = backup_file
    AND s3_upload_status IS NULL;
    
    RETURN success;
END;
$$ LANGUAGE plpgsql;

-- Modify backup functions to include S3 sync
CREATE OR REPLACE FUNCTION backup_full_database(
    backup_dir TEXT,
    sync_to_s3 BOOLEAN DEFAULT true
)
RETURNS BOOLEAN AS $$
DECLARE
    backup_file TEXT;
    backup_success BOOLEAN;
    s3_success BOOLEAN;
BEGIN
    -- Perform local backup
    backup_success := backup_full_database(backup_dir);
    
    -- If backup successful and S3 sync requested
    IF backup_success AND sync_to_s3 THEN
        backup_file := backup_dir || '/' || 
                      (SELECT target_location 
                       FROM backup_log 
                       ORDER BY timestamp DESC 
                       LIMIT 1);
        
        -- Sync to S3
        s3_success := sync_backup_to_s3(backup_file);
        
        -- Log any S3 failures
        IF NOT s3_success THEN
            INSERT INTO backup_log (
                backup_type,
                target_location,
                success,
                details
            ) VALUES (
                'S3_SYNC',
                backup_file,
                false,
                'Failed to sync backup to S3'
            );
        END IF;
    END IF;
    
    RETURN backup_success;
END;
$$ LANGUAGE plpgsql;

-- Example monitoring queries
/*
-- Check S3 sync status
SELECT 
    timestamp,
    backup_type,
    target_location,
    success as local_backup_success,
    s3_upload_status,
    s3_path
FROM backup_log
ORDER BY timestamp DESC
LIMIT 10;

-- Check for failed S3 uploads
SELECT *
FROM backup_log
WHERE success = true 
AND (s3_upload_status = 'FAILED' OR s3_upload_status IS NULL)
AND timestamp > current_timestamp - interval '24 hours';
*/


