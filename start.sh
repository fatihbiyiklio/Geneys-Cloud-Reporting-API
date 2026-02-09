#!/bin/bash
# Auto-restart wrapper for Genesys Reporting App
# This script automatically restarts the app if it exits with code 42 (memory restart)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_FILE="logs/app_restarts.log"
mkdir -p logs

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
    echo "$1"
}

restart_count=0
max_restarts_per_hour=10
last_hour_start=$(date +%s)

while true; do
    current_time=$(date +%s)
    
    # Reset restart count every hour
    if [ $((current_time - last_hour_start)) -ge 3600 ]; then
        restart_count=0
        last_hour_start=$current_time
    fi
    
    # Check if too many restarts
    if [ $restart_count -ge $max_restarts_per_hour ]; then
        log_message "ERROR: Too many restarts ($restart_count) in the last hour. Waiting 10 minutes..."
        sleep 600
        restart_count=0
        last_hour_start=$(date +%s)
    fi
    
    log_message "Starting Streamlit app (restart count: $restart_count)"
    
    # Run the app
    streamlit run app.py --server.port=8501 --server.address=0.0.0.0
    exit_code=$?
    
    log_message "App exited with code: $exit_code"
    
    if [ $exit_code -eq 42 ]; then
        # Memory restart requested
        log_message "Memory restart requested (exit code 42). Restarting in 3 seconds..."
        restart_count=$((restart_count + 1))
        sleep 3
    elif [ $exit_code -eq 0 ]; then
        # Normal exit
        log_message "App exited normally. Stopping."
        break
    else
        # Unexpected exit
        log_message "Unexpected exit. Restarting in 10 seconds..."
        restart_count=$((restart_count + 1))
        sleep 10
    fi
done
