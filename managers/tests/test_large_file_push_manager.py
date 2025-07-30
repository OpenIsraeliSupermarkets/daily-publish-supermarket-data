import os
import pytest
import tempfile
import shutil
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch, mock_open, ANY
from managers.large_file_push_manager import LargeFilePushManager
from managers.cache_manager import CacheState
from remotes import ShortTermDatabaseUploader
from typing import List, Dict


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
        mock_db_manager._insert_to_destinations = MagicMock()
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
    def test_process_file_empty_chunk(
        self, mock_read_csv, temp_dir, mock_database_manager, cache_state
    ):
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
        mock_database_manager._insert_to_destinations.assert_not_called()

    @patch("pandas.read_csv")
    @patch("managers.large_file_push_manager.DataTable")
    def test_process_file_with_data(
        self,
        mock_data_table,
        mock_read_csv,
        temp_dir,
        mock_database_manager,
        cache_state,
    ):
        """Test process_file with data."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)

        # Create test data with proper structure
        test_data = pd.DataFrame(
            {
                "found_folder": ["test_folder_1", "test_folder_2", "test_folder_3"],
                "file_name": ["test_file_1.csv", "test_file_2.csv", "test_file_3.csv"],
            }
        )

        # Set up mock to return our test data as a chunk
        mock_read_csv.return_value = iter([test_data])

        # Setup header mock
        manager._get_header = MagicMock(return_value=["found_folder", "file_name"])

        # Mock DataTable instance and to_dict method
        mock_instance = MagicMock()
        mock_instance.to_dict.return_value = {"mocked": "data"}
        mock_data_table.return_value = mock_instance

        # Process the file
        manager.process_file("test_file.csv", cache_state)

        # Verify database insertion was called
        mock_database_manager._insert_to_destinations.assert_called_once()

        # Verify cache was updated
        assert cache_state.get_last_processed_row("test_file.csv") == 2

    @patch("pandas.read_csv")
    @patch("managers.large_file_push_manager.DataTable")
    def test_process_file_with_last_row(
        self,
        mock_data_table,
        mock_read_csv,
        temp_dir,
        mock_database_manager,
        cache_state,
    ):
        """Test process_file with a previously processed row."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)

        # Set up cache with a last processed row
        cache_state.update_last_processed_row("test_file.csv", 2)

        # Create test data with proper structure
        test_data = pd.DataFrame(
            {
                "found_folder": ["test_folder_3"],
                "file_name": ["test_file_3.csv"],
            }
        )

        # Set up mock to return our test data as a chunk
        mock_read_csv.return_value = iter([test_data])

        # Setup header mock
        manager._get_header = MagicMock(return_value=["found_folder", "file_name"])

        # Mock DataTable instance and to_dict method
        mock_instance = MagicMock()
        mock_instance.to_dict.return_value = {"mocked": "data"}
        mock_data_table.return_value = mock_instance

        # Process the file
        manager.process_file("test_file.csv", cache_state)

        # Verify read_csv was called, but don't check the skiprows parameter as it's a lambda
        mock_read_csv.assert_called_with(
            os.path.join(temp_dir, "test_file.csv"),
            skiprows=ANY,
            names=["found_folder", "file_name"],
            chunksize=10000,
        )

        # Verify database insertion was called
        mock_database_manager._insert_to_destinations.assert_called_once()

        # Verify cache was updated with the last processed row
        # Starting from row 2, adding 1 row makes it 4 (0-based is 3)
        assert cache_state.get_last_processed_row("test_file.csv") == 3

    @patch("pandas.read_csv")
    @patch("managers.large_file_push_manager.DataTable")
    def test_process_file_multiple_chunks(
        self,
        mock_data_table,
        mock_read_csv,
        temp_dir,
        mock_database_manager,
        cache_state,
    ):
        """Test processing a file with multiple chunks."""
        manager = LargeFilePushManager(temp_dir, mock_database_manager)

        # Create test data for two chunks with proper structure
        chunk1 = pd.DataFrame(
            {
                "found_folder": ["test_folder_1", "test_folder_2"],
                "file_name": ["test_file_1.csv", "test_file_2.csv"],
            }
        )

        chunk2 = pd.DataFrame(
            {
                "found_folder": ["test_folder_3"],
                "file_name": ["test_file_3.csv"],
            }
        )

        # Set up mock to return our test chunks
        mock_read_csv.return_value = iter([chunk1, chunk2])

        # Setup header mock
        manager._get_header = MagicMock(return_value=["found_folder", "file_name"])

        # Mock DataTable instance and to_dict method
        mock_instance = MagicMock()
        mock_instance.to_dict.return_value = {"mocked": "data"}
        mock_data_table.return_value = mock_instance

        # Process the file
        manager.process_file("test_file.csv", cache_state)

        # Verify database insertion was called twice (once for each chunk)
        assert mock_database_manager._insert_to_destinations.call_count == 2

        # Verify cache was updated with the final position
        # The final position should be the sum of the lengths of all chunks
        # 2 rows in chunk1 + 1 row in chunk2 + 1 (starting position) = 4
        assert cache_state.get_last_processed_row("test_file.csv") == 2

    def test_process_real_csv_with_custom_chunks(
        self, temp_dir, mock_database_manager, cache_state
    ):
        """Test processing an actual CSV file with 10 rows using chunks of 3."""
        # Create a test file with 10 rows
        test_file = "test_file.csv"
        file_path = os.path.join(temp_dir, test_file)

        # Create test data with 10 rows using proper structure
        test_data = pd.DataFrame(
            {
                "found_folder": ["test_folder_" + str(i) for i in range(10)],
                "file_name": ["test_file_" + str(i) + ".csv" for i in range(10)],
                "more_data": ["test_data_" + str(i) for i in range(10)],
                "some_more_data": ["test_data_" + str(i) for i in range(10)],
            }
        )

        # Write the DataFrame to a CSV file
        test_data.to_csv(file_path, index=False)

        # Create manager with chunk size of 3
        manager = LargeFilePushManager(temp_dir, mock_database_manager, 3)

        # Process the file
        manager.process_file(test_file, cache_state)

        # Verify database insertion was called 4 times (for chunks of size 3+3+3+1)
        assert mock_database_manager._insert_to_destinations.call_count == 4

        # Verify cache was updated with the final row count
        assert cache_state.get_last_processed_row(test_file) == 9

    def test_last_row_saw_no_nulls(self, temp_dir, mock_database_manager, cache_state):
        """Test that last_row_saw doesn't contain null values."""

        test_file = "test_file.csv"
        file_path = os.path.join(temp_dir, test_file)
        # Create test data with one chunk containing null values and another with valid data
        test_data = pd.DataFrame(
            {
                "found_folder": ["test_folder"] + [np.nan for i in range(9)],
                "file_name": ["test_file_"] + [np.nan for i in range(9)],
                "more_data": ["test_data_"] + [np.nan for i in range(9)],
            }
        )
        test_data.to_csv(file_path, index=False)

        manager = LargeFilePushManager(temp_dir, mock_database_manager, 3)

        def mock_insert(table_name, items: List[Dict]):
            # Accessing private attribute for testing purposes
            # Assert last_row_saw does not contain nulls after ffill operation
            for item in items:
                for key, value in item.items():
                    assert value is not None and not pd.isna(
                        value
                    ), f"Key {key} contains NaN value"

        # Replace the method with our mock
        manager.database_manager._insert_to_destinations = mock_insert

        # Process the file
        manager.process_file(test_file, cache_state)
