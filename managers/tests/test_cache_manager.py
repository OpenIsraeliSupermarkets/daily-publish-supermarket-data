import unittest
from managers.cache_manager import CacheState

class TestCacheState(unittest.TestCase):
    def setUp(self):
        """Set up test data before each test."""
        self.empty_data = {}
        self.sample_data = {
            "file1.csv": {
                "timestamps": ["2024-01-01", "2024-01-02"]
            },
            "last_pushed": {
                "file1.csv": 100
            }
        }

    def test_initialization_with_empty_data(self):
        """Test initialization with empty data."""
        cache = CacheState(self.empty_data)
        self.assertEqual(cache._data, {})

    def test_initialization_with_pre_populated_data(self):
        """Test initialization with pre-populated data."""
        cache = CacheState(self.sample_data)
        self.assertEqual(cache._data, self.sample_data)

    def test_get_pushed_timestamps_existing_file(self):
        """Test getting timestamps for an existing file."""
        cache = CacheState(self.sample_data)
        timestamps = cache.get_pushed_timestamps("file1.csv")
        self.assertEqual(timestamps, ["2024-01-01", "2024-01-02"])

    def test_get_pushed_timestamps_non_existing_file(self):
        """Test getting timestamps for a non-existing file."""
        cache = CacheState(self.sample_data)
        timestamps = cache.get_pushed_timestamps("nonexistent.csv")
        self.assertEqual(timestamps, [])

    def test_is_empty_with_empty_data(self):
        """Test is_empty with empty data."""
        cache = CacheState(self.empty_data)
        self.assertTrue(cache.is_empty())

    def test_is_empty_with_non_empty_data(self):
        """Test is_empty with non-empty data."""
        cache = CacheState(self.sample_data)
        self.assertFalse(cache.is_empty())

    def test_update_pushed_timestamps_new_file(self):
        """Test updating timestamps for a new file."""
        cache = CacheState(self.empty_data)
        cache.update_pushed_timestamps("newfile.csv", ["2024-01-03"])
        self.assertEqual(cache.get_pushed_timestamps("newfile.csv"), ["2024-01-03"])

    def test_update_pushed_timestamps_existing_file(self):
        """Test updating timestamps for an existing file."""
        cache = CacheState(self.sample_data)
        cache.update_pushed_timestamps("file1.csv", ["2024-01-03"])
        self.assertEqual(cache.get_pushed_timestamps("file1.csv"), ["2024-01-03"])

    def test_get_last_processed_row_with_default(self):
        """Test getting last processed row with default value."""
        cache = CacheState(self.empty_data)
        row = cache.get_last_processed_row("nonexistent.csv")
        self.assertEqual(row, -1)

    def test_update_last_processed_row(self):
        """Test updating last processed row."""
        cache = CacheState(self.empty_data)
        cache.update_last_processed_row("file1.csv", 150)
        self.assertEqual(cache.get_last_processed_row("file1.csv"), 150)

if __name__ == '__main__':
    unittest.main()
