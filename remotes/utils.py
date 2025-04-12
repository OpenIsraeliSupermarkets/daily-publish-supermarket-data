"""Utility functions for remote database operations.

This module provides common utility functions used across different
database implementations.
"""

import os
from datetime import datetime, timedelta


def get_latest_file_modification_time(directory_path: str) -> datetime | None:
    """Get the most recent modification time of any file in a directory.

    This function scans all files in a directory and returns the timestamp
    of the most recently modified file.

    Args:
        directory_path (str): Path to the directory to scan

    Returns:
        datetime | None: Timestamp of most recently modified file,
                        or None if no files found
    """
    if not os.path.exists(directory_path):
        return None

    last_modified = None

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if os.path.isfile(file_path):
            mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            if last_modified is None or mtime > last_modified:
                last_modified = mtime

    return last_modified


def was_updated_within_seconds(directory_path: str, seconds: int = 24*60*60) -> bool:
    """Check if any file in a directory was updated within specified seconds.

    Args:
        directory_path (str): Path to the directory to check
        hours (int, optional): Number of hours to look back. Defaults to 24.

    Returns:
        bool: True if any file was updated within the specified hours,
              False otherwise
    """
    last_modified = get_latest_file_modification_time(directory_path)
    if last_modified is None:
        return False

    return (datetime.now() - last_modified) < timedelta(seconds=seconds)
