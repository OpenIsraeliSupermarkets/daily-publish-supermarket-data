#!/usr/bin/env python
"""
Health check script for the data processor service.

This script validates the heartbeat status file to determine if the
data processing operations are running successfully. It's designed to be
used with Docker's HEALTHCHECK directive.

Exit codes:
    0: Service is healthy
    1: Service is unhealthy
"""

import sys
import os
from utils.heartbeat import HeartbeatManager


def main():
    """
    Main health check function.

    Reads the heartbeat file and validates:
    1. File exists and is recent
    2. No operations have failed
    3. Process is making progress: the heartbeat, or the `_metadata.last_modified`
       of a scraping / converting status file or the push cache, is recent
    """
    # Get heartbeat path from environment or use default
    app_data_path = os.environ.get("APP_DATA_PATH", "app_data")
    heartbeat_path = os.path.join(app_data_path, "heartbeat.json")

    # Maximum time without progress (1 hour by default, longer than the 30 minute
    # wait between DAG runs)
    max_age_seconds = int(os.environ.get("HEALTHCHECK_MAX_AGE_SECONDS", 3600))

    # Status files the operations update as they go
    status_paths = [
        os.path.join(app_data_path, "scraping_status"),
        os.path.join(app_data_path, "converting_status"),
        os.path.join(app_data_path, ".push_cache"),
    ]

    # Check health
    is_healthy, message = HeartbeatManager.check_health(
        heartbeat_path, max_age_seconds, status_paths
    )

    if is_healthy:
        print(f"✓ {message}")
        sys.exit(0)
    else:
        print(f"✗ {message}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
