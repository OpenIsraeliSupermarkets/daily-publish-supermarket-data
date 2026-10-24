import os
import json
from datetime import datetime
from filelock import FileLock, Timeout


class CacheState:
    """
    Cache state for the push cache.

    This class represents the state of cached push operations, providing methods
    to manage and query the cache data for individual files.
    """

    def __init__(self, data, on_progress=None):
        """
        Initialize the CacheState with cache data.

        Args:
            data (dict): Dictionary containing the cache data
            on_progress (callable): Called after each recorded row, to save it
        """
        self._data = data
        self._on_progress = on_progress

    def get_pushed_timestamps(self, file_name):
        """
        Get the timestamps of pushed data for a specific file.

        Args:
            file_name (str): Name of the file to get timestamps for

        Returns:
            list: List of timestamps for the specified file
        """
        return self._data.get(file_name, {}).get("timestamps", [])

    def is_empty(self):
        """
        Check if the cache is empty.

        Returns:
            bool: True if the cache is empty, False otherwise
        """
        return not any(key != "_metadata" for key in self._data)

    def update_pushed_timestamps(self, file_name, pushed_timestamps):
        """
        Update the timestamps for a specific file in the cache.

        Args:
            file_name (str): Name of the file to update
            pushed_timestamps (list): List of timestamps to store
        """
        file_name = self._get_file_name(file_name)
        if file_name not in self._data:
            self._data[file_name] = {}
        self._data[file_name]["timestamps"] = pushed_timestamps

    def get_last_processed_row(self, file_name, default=-1):
        """
        Get the last processed row number for a specific file.

        Args:
            file_name (str): Name of the file to check
            default (int): Default value to return if no row is found

        Returns:
            int: The last processed row number or default value
        """
        return self._data.get("last_pushed", {}).get(
            self._get_file_name(file_name), default
        )

    def _get_file_name(self, file_name):
        return file_name.split(os.sep)[-1]

    def update_last_processed_row(self, file_name, last_processed_row):
        """
        Update the last processed row number for a specific file.

        Args:
            file_name (str): Name of the file to update
            last_processed_row (int): The row number to store
        """
        if "last_pushed" not in self._data:
            self._data["last_pushed"] = {}
        self._data["last_pushed"][self._get_file_name(file_name)] = last_processed_row
        if self._on_progress is not None:
            self._on_progress()

    def get_streamed_files(self):
        """
        Get the source files the streaming push already delivered in full.

        Returns:
            list: Source file names whose file_complete message was acked
        """
        return self._data.get("streamed_files", [])

    def mark_file_streamed(self, file_name):
        """
        Record that the streaming push delivered every row of a source file,
        so the CSV push in api_update does not send it again.

        Args:
            file_name (str): Source file name, as in the rows' file_name field
        """
        streamed = self._data.setdefault("streamed_files", [])
        if file_name not in streamed:
            streamed.append(file_name)

    def get_next_streamed_row_index(self, table_name):
        """
        Get the row_index after the last row the streaming push sent to a table.

        Args:
            table_name (str): Short-term database table name

        Returns:
            int: The next row_index, or 0 if nothing was streamed to the table
        """
        return self._data.get("streamed_row_index", {}).get(table_name, 0)

    def set_next_streamed_row_index(self, table_name, row_index):
        """
        Record the row_index the next row streamed to a table gets, so a later
        cycle does not reuse one even if its CSV push did not finish.

        Args:
            table_name (str): Short-term database table name
            row_index (int): The next row_index
        """
        self._data.setdefault("streamed_row_index", {})[table_name] = row_index

    def clear(self):
        """
        Clear the cache data.
        """
        self._data.clear()


class CacheManager:
    """
    Cache manager for the push cache.

    This class manages the caching of push operations, storing information about
    which files have been processed and their timestamps. It provides methods
    to load and save cache data to a file.
    """

    def __init__(self, app_folder):
        """
        Initialize the CacheManager with the application folder path.

        Args:
            app_folder (str): Path to the application folder where cache file will be stored
        """
        self.cache_file = os.path.join(app_folder, ".push_cache")
        self._data = None
        self._lock = FileLock(f"{self.cache_file}.lock", timeout=1)

    def __enter__(self):
        """
        Enter the context manager, loading the cache data.

        Returns:
            CacheState: A CacheState object containing the loaded cache data
        """
        try:
            self._lock.acquire()
            self._data = {}
            if os.path.exists(self.cache_file):
                try:
                    with open(self.cache_file, "r") as file:
                        self._data = json.load(file)
                except json.JSONDecodeError:
                    # If the file is corrupted, start with empty data
                    self._data = {}
            return CacheState(self._data, on_progress=self._save)
        except Timeout:
            raise RuntimeError("Could not acquire lock on cache file")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager, saving any changes to the cache data.

        Args:
            exc_type: Type of exception if any occurred
            exc_val: Exception value if any occurred
            exc_tb: Exception traceback if any occurred
        """
        try:
            self._save()
        finally:
            self._lock.release()

    def _save(self):
        """
        Write the cache data to disk, stamped with `_metadata.last_modified` (the
        scraper / parser status files' convention) so the health check sees
        api_update progress. Called on exit and after each recorded row, so a
        killed process also resumes from its last completed file.
        """
        if self._data is None:
            return
        # a cleared cache stays empty ({}), as clean_all_source_data expects
        if not CacheState(self._data).is_empty():
            self._data.setdefault("_metadata", {})["last_modified"] = (
                datetime.now().astimezone().isoformat()
            )
        # Write to a temporary file first
        temp_file = f"{self.cache_file}.tmp"
        with open(temp_file, "w") as file:
            json.dump(self._data, file)
        # Atomic rename
        os.replace(temp_file, self.cache_file)
