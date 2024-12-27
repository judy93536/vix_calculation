# Backup and Maintenance Procedures

## Overview
This document describes the backup and maintenance procedures for the SPX options database.

## Backup Types
1. Full Database Backup
   - Weekly full backup (Sundays at 1 AM)
   - Includes all tables and schemas
   - Stored in custom format for efficient storage

2. Partition Backup
   - Daily backup of current month's partition (2 AM)
   - Efficient for incremental backups
   - Allows for granular recovery

3. AWS S3 Integration
   - Backup files automatically synced to S3
   - Uses StandardAI storage class for cost efficiency
   - Retains 30 days of backups

## Maintenance Schedule
| Task                    | Frequency        | Time  | Description                           |
|------------------------|------------------|-------|---------------------------------------|
| Full Backup            | Weekly (Sunday)  | 1 AM  | Complete database backup              |
| Partition Backup       | Daily           | 2 AM  | Current month partition backup        |
| Backup Cleanup         | Weekly (Monday)  | 3 AM  | Remove backups older than 30 days    |
| Partition Maintenance  | Daily           | 4 AM  | Create partitions, check integrity    |

## Monitoring and Verification
1. Backup Logging
   - All backup operations logged in `backup_log` table
   - Includes timestamps, sizes, and success status
   - Regular monitoring via provided queries

2. Integrity Checks
   - Daily automated checks
   - Verifies data consistency
   - Checks partition boundaries

## Recovery Procedures
1. Full Database Recovery
   ```bash
   pg_restore -d cboe /path/to/backup/spx_options_FULL_YYYYMMDD_HHMMSS.backup
   ```

2. Single Partition Recovery
   ```bash
   pg_restore -d cboe -t partition_name /path/to/backup/spx_options_PARTITION_*.backup
   ```

## Storage Management
- Local backups retained for 30 days
- S3 backups follow same retention policy
- Automatic cleanup of old backups

## Emergency Procedures
1. Manual Backup
   ```sql
   SELECT backup_full_database('/path/to/backup/dir');
   ```

2. Integrity Check
   ```sql
   SELECT * FROM check_data_integrity();
   ```

3. Partition Verification
   ```sql
   SELECT * FROM check_partition_coverage();
   ```

## Contact Information
- Database Administrator: [Name]
- Emergency Contact: [Phone/Email]
- AWS Account ID: [ID]

## Related Documents
- Database Schema Documentation
- AWS S3 Configuration
- Monitoring Dashboard URLs


