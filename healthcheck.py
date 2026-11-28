#!/usr/bin/env python
"""
Health check script for the data processor service.

The fetcher's health monitor thread (utils/heartbeat.py, #192) runs the
health check every minute and writes the result to `health_status.json`.
This script only reads that file. It's designed to be used with Docker's
HEALTHCHECK directive.

Exit codes:
    0: Service is healthy
    1: Service is unhealthy
"""

import sys
import os

from utils.heartbeat import HEALTH_STATUS_MAX_AGE_SECONDS, HealthMonitor


def main():
    """
    Report the health monitor's last result: unhealthy when it says so, or
    when its file is missing or has not been written for 5 minutes.
    """
    app_data_path = os.environ.get("APP_DATA_PATH", "app_data")

    # Step 1: read the monitor's last result.
    is_healthy, message = HealthMonitor.read_status(
        app_data_path, HEALTH_STATUS_MAX_AGE_SECONDS
    )

    # Step 2: report it to Docker.
    if is_healthy:
        print(f"✓ {message}")
        sys.exit(0)
    else:
        print(f"✗ {message}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
