import os
import json


class CacheState:
    """
    Cache state for the push cache.
    """
    def __init__(self, data):
        self._data = data

    def get_pushed_timestamps(self, file_name):
        return self._data.get(file_name, {}).get("timestamps", [])

    def is_empty(self):
        return not bool(self._data)

    def update_pushed_timestamps(self, file_name, pushed_timestamps):
        if file_name not in self._data:
            self._data[file_name] = {}
        self._data[file_name]["timestamps"] = pushed_timestamps

    def get_last_processed_row(self, file_name, default=-1):
        return self._data.get("last_processed_row", {}).get(file_name, default)

    def update_last_processed_row(self, file_name, last_processed_row):
        if "last_pushed" not in self._data:
            self._data["last_pushed"] = {}
        self._data["last_pushed"][file_name] = last_processed_row


class CacheManager:
    """
    Cache manager for the push cache.
    """
    def __init__(self, app_folder):
        self.cache_file = os.path.join(app_folder, ".push_cache")
        self._data = None

    def __enter__(self):
        self._data = self.load()
        return CacheState(self._data)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._data is not None:
            self.save(self._data)

    def load(self):
        last_pushed = {}
        if os.path.exists(self.cache_file):
            with open(self.cache_file, "r") as file:
                last_pushed = json.load(file)
        return last_pushed

    def save(self, new_content):
        with open(self.cache_file, "w") as file:
            json.dump(new_content, file)



