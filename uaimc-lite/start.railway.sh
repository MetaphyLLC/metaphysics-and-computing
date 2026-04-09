#!/bin/bash
# UAIMC-lite Railway entrypoint
# Downloads the SQLite DB from R2 on first boot, then starts the service.

set -e

DB_PATH="/data/uaimc.db"
R2_URL="${UAIMC_DB_URL:-}"

# Download DB if not present on volume
if [ ! -f "$DB_PATH" ]; then
  if [ -z "$R2_URL" ]; then
    echo "ERROR: No database found at $DB_PATH and UAIMC_DB_URL not set."
    echo "Set UAIMC_DB_URL to a presigned R2/S3 URL for the initial download."
    exit 1
  fi
  echo "Downloading UAIMC database from cloud storage..."
  curl -fSL -o "$DB_PATH" "$R2_URL"
  echo "Download complete: $(du -h $DB_PATH)"
else
  echo "Database found at $DB_PATH ($(du -h $DB_PATH | cut -f1))"
fi

# Start UAIMC-lite
exec python uaimc_service.py
