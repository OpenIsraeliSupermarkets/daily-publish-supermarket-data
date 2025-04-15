import os
import json
import pytest
import tempfile
import shutil
from unittest.mock import patch, mock_open
from filelock import Timeout
from managers.cache_manager import CacheManager, CacheState


class TestCacheState:
    def test_initialization(self):
        """Test initialization of CacheState with data."""
        test_data = {"file1": {"timestamps": [1, 2, 3]}}
        cache_state = CacheState(test_data)
        assert cache_state._data == test_data

    def test_get_pushed_timestamps(self):
        """Test retrieving pushed timestamps for a file."""
        test_data = {"file1": {"timestamps": [1, 2, 3]}}
        cache_state = CacheState(test_data)
        assert cache_state.get_pushed_timestamps("file1") == [1, 2, 3]
        assert cache_state.get_pushed_timestamps("non_existent_file") == []

    def test_is_empty(self):
        """Test checking if cache is empty."""
        assert CacheState({}).is_empty() is True
        assert CacheState({"file1": {}}).is_empty() is False

    def test_update_pushed_timestamps(self):
        """Test updating pushed timestamps for a file."""
        cache_state = CacheState({})
        cache_state.update_pushed_timestamps("file1", [1, 2, 3])
        assert cache_state._data == {"file1": {"timestamps": [1, 2, 3]}}
        
        # Update existing file
        cache_state.update_pushed_timestamps("file1", [4, 5, 6])
        assert cache_state._data == {"file1": {"timestamps": [4, 5, 6]}}

    def test_get_last_processed_row(self):
        """Test retrieving last processed row for a file."""
        test_data = {"last_pushed": {"file1": 10}}
        cache_state = CacheState(test_data)
        assert cache_state.get_last_processed_row("file1") == 10
        assert cache_state.get_last_processed_row("non_existent_file") == -1
        assert cache_state.get_last_processed_row("non_existent_file", 0) == 0

    def test_update_last_processed_row(self):
        """Test updating last processed row for a file."""
        cache_state = CacheState({})
        cache_state.update_last_processed_row("file1", 10)
        assert cache_state._data == {"last_pushed": {"file1": 10}}
        
        # Update existing file
        cache_state.update_last_processed_row("file1", 20)
        assert cache_state._data == {"last_pushed": {"file1": 20}}


class TestCacheManager:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_initialization(self, temp_dir):
        """Test initialization of CacheManager."""
        cache_manager = CacheManager(temp_dir)
        assert cache_manager.cache_file == os.path.join(temp_dir, ".push_cache")
        assert cache_manager._data is None

    def test_context_manager_new_cache(self, temp_dir):
        """Test context manager with a new cache file."""
        cache_manager = CacheManager(temp_dir)
        with cache_manager as cache_state:
            assert isinstance(cache_state, CacheState)
            assert cache_state.is_empty() is True
            
            # Update cache state
            cache_state.update_pushed_timestamps("file1", [1, 2, 3])
        
        # Verify file was created
        assert os.path.exists(cache_manager.cache_file)
        
        # Verify file content
        with open(cache_manager.cache_file, 'r') as f:
            data = json.load(f)
            assert data == {"file1": {"timestamps": [1, 2, 3]}}

    def test_context_manager_existing_cache(self, temp_dir):
        """Test context manager with an existing cache file."""
        # Create initial cache file
        cache_file = os.path.join(temp_dir, ".push_cache")
        initial_data = {"file1": {"timestamps": [1, 2, 3]}}
        with open(cache_file, 'w') as f:
            json.dump(initial_data, f)
        
        # Test loading existing data
        cache_manager = CacheManager(temp_dir)
        with cache_manager as cache_state:
            assert cache_state.get_pushed_timestamps("file1") == [1, 2, 3]
            
            # Update cache state
            cache_state.update_pushed_timestamps("file2", [4, 5, 6])
        
        # Verify file was updated
        with open(cache_file, 'r') as f:
            data = json.load(f)
            assert data == {
                "file1": {"timestamps": [1, 2, 3]},
                "file2": {"timestamps": [4, 5, 6]}
            }

    def test_corrupted_cache_file(self, temp_dir):
        """Test handling of corrupted cache file."""
        # Create corrupted cache file
        cache_file = os.path.join(temp_dir, ".push_cache")
        with open(cache_file, 'w') as f:
            f.write("This is not valid JSON")
        
        # Test loading corrupted data
        cache_manager = CacheManager(temp_dir)
        with cache_manager as cache_state:
            assert cache_state.is_empty() is True
            
            # Update cache state
            cache_state.update_pushed_timestamps("file1", [1, 2, 3])
        
        # Verify file was fixed
        with open(cache_file, 'r') as f:
            data = json.load(f)
            assert data == {"file1": {"timestamps": [1, 2, 3]}}

    @patch('filelock.FileLock.acquire')
    def test_lock_timeout(self, mock_acquire, temp_dir):
        """Test handling of lock timeout."""
        mock_acquire.side_effect = Timeout("Could not acquire lock")
        
        cache_manager = CacheManager(temp_dir)
        with pytest.raises(RuntimeError, match="Could not acquire lock on cache file"):
            with cache_manager:
                pass

    def test_atomic_file_write(self, temp_dir):
        """Test atomic file writing with temporary file."""
        cache_file = os.path.join(temp_dir, ".push_cache")
        temp_file = f"{cache_file}.tmp"
        
        # Create initial cache file
        initial_data = {"file1": {"timestamps": [1, 2, 3]}}
        with open(cache_file, 'w') as f:
            json.dump(initial_data, f)
        
        # Test that temp file is created during update
        with patch('os.replace') as mock_replace:
            cache_manager = CacheManager(temp_dir)
            with cache_manager as cache_state:
                cache_state.update_pushed_timestamps("file2", [4, 5, 6])
            
            # Verify temp file was created
            assert os.path.exists(temp_file)
            mock_replace.assert_called_once_with(temp_file, cache_file)