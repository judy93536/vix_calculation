#!/bin/bash

# vix_calculator_cron.sh
# Set project directory and virtual environment
PROJECT_DIR="/raid/vscode_projects/vix_calculator"
VENV_DIR="${PROJECT_DIR}/venv"
LOG_DIR="${PROJECT_DIR}/logs"
DATE=$(date '+%Y-%m-%d')
LOGFILE="${LOG_DIR}/vix_calculation_${DATE}.log"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Function for logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOGFILE"
}

# Function to send alert
send_alert() {
    local subject="$1"
    local body="$2"
    python "${PROJECT_DIR}/alert_handler.py" --subject "$subject" --body "$body"
}

# Function to check if market is open
is_market_open() {
    # Check if it's a weekday (1-5, where 1 is Monday)
    local day_of_week=$(date '+%u')
    if [ "$day_of_week" -gt 5 ]; then
        return 1  # Weekend
    fi
    return 0
}

# Function to run a command with timeout and logging
run_with_timeout() {
    local cmd="$1"
    local timeout_duration="$2"
    local description="$3"
    
    log "Starting $description..."
    
    if timeout "$timeout_duration" $cmd >> "$LOGFILE" 2>&1; then
        log "$description completed successfully"
        return 0
    else
        local exit_code=$?
        if [ $exit_code -eq 124 ]; then
            log "ERROR: $description timed out after $timeout_duration"
        else
            log "ERROR: $description failed with exit code $exit_code"
        fi
        return 1
    fi
}

# Main execution
log "Starting VIX calculation process"

# Change to project directory
cd "$PROJECT_DIR" || {
    log "ERROR: Could not change to project directory"
    exit 1
}

# Activate virtual environment
source "${VENV_DIR}/bin/activate" || {
    log "ERROR: Could not activate virtual environment"
    exit 1
}

# Check if market is open
if ! is_market_open; then
    log "Market is closed today. Exiting."
    exit 0
fi

# Run data import with 30 minute timeout
if ! run_with_timeout "python -m src.vix_calculator.production.data_import_runner" "30m" "Data import"; then
    # Send alert about import failure
    send_alert "VIX Import Alert" "VIX data import failed on ${DATE}. Check logs at ${LOGFILE}"
    exit 1
fi

# Wait a bit to ensure all data is properly saved
sleep 60

# Run VIX calculation with 30 minute timeout
if ! run_with_timeout "python -m vix_calculator.production.vix_runner" "30m" "VIX calculation"; then
    # Send alert about calculation failure
    send_alert "VIX Calculation Alert" "VIX calculation failed on ${DATE}. Check logs at ${LOGFILE}"
    exit 1
fi

log "VIX calculation process completed successfully"

# Optional: Send success notification
send_alert "VIX Calculation Success" "VIX calculation completed successfully for ${DATE}"

# Deactivate virtual environment
deactivate

# Archive logs older than 30 days
find "$LOG_DIR" -name "vix_calculation_*.log" -mtime +30 -exec gzip {} \;

