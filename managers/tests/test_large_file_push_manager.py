import os
import pytest
import tempfile
import shutil
import pandas as pd
from unittest.mock import MagicMock, patch, mock_open, ANY
from managers.large_file_push_manager import LargeFilePushManager
from managers.cache_manager import CacheState
from data_models.raw import DataTable
from remotes import ShortTermDatabaseUploader


class TestLargeFilePushManager:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def mock_database_manager(self):
        """Create a mock database manager."""
        mock_db_manager = MagicMock(spec=ShortTermDatabaseUploader)
        mock_db_manager._insert_to_database = MagicMock()
        return mock_db_manager
    
    @pytest.fixture
    def test_csv_content(self):
        """Create test CSV content."""
        return "col1,col2,col3\nval1,val2,val3\nval4,val5,val6\nval7,val8,val9"
    
    @pytest.fixture
    def cache_state(self):
        """Create a cache state object."""
        return CacheState({})
    
    def test_initialization(self, temp_dir, mock_database_manager):
        """Test initialization of LargeFilePushManager."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)
        
        assert manager.outputs_folder == temp_dir
        assert manager.database_manager == mock_database_manager
        assert manager.chunk_size == 10000
    
    def test_get_header(self, temp_dir, mock_database_manager, test_csv_content):
        """Test retrieving header from a CSV file."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)
        
        # Create a test CSV file
        test_file = "test_file.csv"
        file_path = os.path.join(temp_dir, test_file)
        with open(file_path, "w") as f:
            f.write(test_csv_content)
        
        # Get header
        header = manager._get_header(test_file)
        
        # Verify
        assert list(header) == ["col1", "col2", "col3"]
    
    @patch("pandas.read_csv")
    def test_process_file_empty_chunk(self, mock_read_csv, temp_dir, mock_database_manager, cache_state):
        """Test process_file with empty chunk."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)
        
        # Mock empty chunk
        empty_chunk = pd.DataFrame()
        mock_read_csv.return_value = iter([empty_chunk])
        
        # Setup header mock
        manager._get_header = MagicMock(return_value=["col1", "col2", "col3"])
        
        # Process the file
        manager.process_file("test_file.csv", cache_state)
        
        # Verify
        assert cache_state.get_last_processed_row("test_file.csv") == -1
        mock_database_manager._insert_to_database.assert_not_called()
    
    @patch("pandas.read_csv")
    @patch("managers.large_file_push_manager.DataTable")
    def test_process_file_with_data(self, mock_data_table, mock_read_csv, temp_dir, mock_database_manager, cache_state):
        """Test process_file with data."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)
        
        # Create test data
        test_data = pd.DataFrame({
            "col1": ["val1", "val4", "val7"],
            "col2": ["val2", "val5", "val8"],
            "col3": ["val3", "val6", "val9"]
        })
        
        # Set up mock to return our test data as a chunk
        mock_read_csv.return_value = iter([test_data])
        
        # Setup header mock
        manager._get_header = MagicMock(return_value=["col1", "col2", "col3"])
        
        # Mock DataTable instance and to_dict method
        mock_instance = MagicMock()
        mock_instance.to_dict.return_value = {'mocked': 'data'}
        mock_data_table.return_value = mock_instance
        
        # Process the file
        manager.process_file("test_file.csv", cache_state)
        
        # Verify database insertion was called
        mock_database_manager._insert_to_database.assert_called_once()
        
        # Verify cache was updated
        assert cache_state.get_last_processed_row("test_file.csv") == 3
    
    @patch("pandas.read_csv")
    @patch("managers.large_file_push_manager.DataTable")
    def test_process_file_with_last_row(self, mock_data_table, mock_read_csv, temp_dir, mock_database_manager, cache_state):
        """Test process_file with a previously processed row."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)
        
        # Set up cache with a last processed row
        cache_state.update_last_processed_row("test_file.csv", 2)
        
        # Create test data
        test_data = pd.DataFrame({
            "col1": ["val7"],
            "col2": ["val8"],
            "col3": ["val9"]
        })
        
        # Set up mock to return our test data as a chunk
        mock_read_csv.return_value = iter([test_data])
        
        # Setup header mock
        manager._get_header = MagicMock(return_value=["col1", "col2", "col3"])
        
        # Mock DataTable instance and to_dict method
        mock_instance = MagicMock()
        mock_instance.to_dict.return_value = {'mocked': 'data'}
        mock_data_table.return_value = mock_instance
        
        # Process the file
        manager.process_file("test_file.csv", cache_state)
        
        # Verify read_csv was called, but don't check the skiprows parameter as it's a lambda
        mock_read_csv.assert_called_with(
            os.path.join(temp_dir, "test_file.csv"),
            skiprows=ANY,
            names=["col1", "col2", "col3"],
            chunksize=10000,
        )
        
        # Verify database insertion was called
        mock_database_manager._insert_to_database.assert_called_once()
        
        # Verify cache was updated with the last processed row
        # Starting from row 2, adding 1 row makes it 4 (0-based is 3)
        assert cache_state.get_last_processed_row("test_file.csv") == 4
    
    @patch("pandas.read_csv")
    @patch("managers.large_file_push_manager.DataTable")
    def test_process_file_multiple_chunks(self, mock_data_table, mock_read_csv, temp_dir, mock_database_manager, cache_state):
        """Test processing a file with multiple chunks."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)
        
        # Create test data for two chunks
        chunk1 = pd.DataFrame({
            "col1": ["val1", "val4"],
            "col2": ["val2", "val5"],
            "col3": ["val3", "val6"]
        })
        
        chunk2 = pd.DataFrame({
            "col1": ["val7"],
            "col2": ["val8"],
            "col3": ["val9"]
        })
        
        # Set up mock to return our test chunks
        mock_read_csv.return_value = iter([chunk1, chunk2])
        
        # Setup header mock
        manager._get_header = MagicMock(return_value=["col1", "col2", "col3"])
        
        # Mock DataTable instance and to_dict method
        mock_instance = MagicMock()
        mock_instance.to_dict.return_value = {'mocked': 'data'}
        mock_data_table.return_value = mock_instance
        
        # Process the file
        manager.process_file("test_file.csv", cache_state)
        
        # Verify database insertion was called twice (once for each chunk)
        assert mock_database_manager._insert_to_database.call_count == 2
        
        # Verify cache was updated with the final position
        # The final position should be the sum of the lengths of all chunks
        # 2 rows in chunk1 + 1 row in chunk2 + 1 (starting position) = 4
        assert cache_state.get_last_processed_row("test_file.csv") == 4 