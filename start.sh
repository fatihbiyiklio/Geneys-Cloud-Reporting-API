#!/bin/bash
# Startup script for Genesys Reporting App

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p logs

RESTART_EXIT_CODE="${GENESYS_RESTART_EXIT_CODE:-42}"

echo "[START] Starting Genesys Reporting App..."
while true; do
    streamlit run app.py --server.port=8501 --server.address=0.0.0.0
    EXIT_CODE=$?
    if [ "$EXIT_CODE" -eq "$RESTART_EXIT_CODE" ]; then
        echo "[REBOOT] Application requested restart (exit code $RESTART_EXIT_CODE). Restarting..."
        sleep 1
        continue
    fi
    echo "[STOP] Application stopped (Exit Code $EXIT_CODE)."
    break
done
