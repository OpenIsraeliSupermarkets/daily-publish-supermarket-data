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
    3. Process is actively updating the heartbeat
    """
    # Get heartbeat path from environment or use default
    app_data_path = os.environ.get('APP_DATA_PATH', 'app_data')
    heartbeat_path = os.path.join(app_data_path, 'heartbeat.json')
    
    # Maximum age for heartbeat (5 minutes by default)
    max_age_seconds = int(os.environ.get('HEALTHCHECK_MAX_AGE_SECONDS', 300))
    
    # Check health
    is_healthy, message = HeartbeatManager.check_health(heartbeat_path, max_age_seconds)
    
    if is_healthy:
        print(f"✓ {message}")
        sys.exit(0)
    else:
        print(f"✗ {message}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
