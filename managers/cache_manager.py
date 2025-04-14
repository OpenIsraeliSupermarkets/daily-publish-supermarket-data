import os
import json


class CacheManager:
    def __init__(self, app_folder):
        self.cache_file = os.path.join(app_folder, ".push_cache")
        self._data = None

    def __enter__(self):
        self._data = self.load()
        return self._data

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

