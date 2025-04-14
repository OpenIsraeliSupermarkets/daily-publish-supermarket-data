import os
import json
import tempfile
import pytest
from managers.cache_manager import CacheManager

def test_cache_manager_initialization():
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_manager = CacheManager(temp_dir)
        assert cache_manager.cache_file == os.path.join(temp_dir, ".push_cache")
        assert cache_manager._data is None

def test_cache_manager_load_empty():
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_manager = CacheManager(temp_dir)
        data = cache_manager.load()
        assert data == {}

def test_cache_manager_save_and_load():
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_manager = CacheManager(temp_dir)
        test_data = {"key1": "value1", "key2": "value2"}
        
        # Save data
        cache_manager.save(test_data)
        
        # Load data
        loaded_data = cache_manager.load()
        assert loaded_data == test_data

def test_cache_manager_context_manager():
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data = {"key1": "value1"}
        
        # Use context manager
        with CacheManager(temp_dir) as data:
            data.update(test_data)
        
        # Verify data was saved
        with open(os.path.join(temp_dir, ".push_cache"), "r") as f:
            saved_data = json.load(f)
        assert saved_data == test_data

def test_cache_manager_context_manager_exception():
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data = {"key1": "value1"}
        
        try:
            with CacheManager(temp_dir) as data:
                data.update(test_data)
                raise Exception("Test exception")
        except Exception:
            pass
        
        # Verify data was still saved despite exception
        with open(os.path.join(temp_dir, ".push_cache"), "r") as f:
            saved_data = json.load(f)
        assert saved_data == test_data 