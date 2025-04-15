import os
import json


class CacheState:
    """
    Cache state for the push cache.
    
    This class represents the state of cached push operations, providing methods
    to manage and query the cache data for individual files.
    """
    def __init__(self, data):
        """
        Initialize the CacheState with cache data.
        
        Args:
            data (dict): Dictionary containing the cache data
        """
        self._data = data

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
        return not bool(self._data)

    def update_pushed_timestamps(self, file_name, pushed_timestamps):
        """
        Update the timestamps for a specific file in the cache.
        
        Args:
            file_name (str): Name of the file to update
            pushed_timestamps (list): List of timestamps to store
        """
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
        return self._data.get("last_processed_row", {}).get(file_name, default)

    def update_last_processed_row(self, file_name, last_processed_row):
        """
        Update the last processed row number for a specific file.
        
        Args:
            file_name (str): Name of the file to update
            last_processed_row (int): The row number to store
        """
        if "last_pushed" not in self._data:
            self._data["last_pushed"] = {}
        self._data["last_pushed"][file_name] = last_processed_row


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

    def __enter__(self):
        """
        Enter the context manager, loading the cache data.
        
        Returns:
            CacheState: A CacheState object containing the loaded cache data
        """
        self._data = {}
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "r") as file:
                self._data = json.load(file)
        return CacheState(self._data)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager, saving any changes to the cache data.
        
        Args:
            exc_type: Type of exception if any occurred
            exc_val: Exception value if any occurred
            exc_tb: Exception traceback if any occurred
        """
        if self._data is not None:
            with open(self.cache_file, "w") as file:
                json.dump(self._data, file)


