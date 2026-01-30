import os
import pytest
import tempfile
import shutil
import pandas as pd
import numpy as np
from unittest.mock import MagicMock, patch, mock_open, ANY, call
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

        eof_to_send = [{"file_complete": "true",
                            "file_name": "test_file_1.csv",
                            "total_expected_records": 1
                        },{"file_complete": "true",
                            "file_name": "test_file_2.csv",
                            "total_expected_records": 1
                        }]
        last_eof_to_send = [{"file_complete": "true",
                            "file_name": "test_file_3.csv",
                            "total_expected_records": 1
                        }]
        assert mock_database_manager._insert_to_destinations.call_args_list == [call('test_file', [{"mocked": "data"},{"mocked": "data"},{"mocked": "data"}]), call('test_file', eof_to_send), call('test_file', last_eof_to_send)]

        # Verify cache was updated
        assert cache_state.get_last_processed_row("test_file.csv") == 2 # 

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
        assert mock_database_manager._insert_to_destinations.call_args_list == [call('test_file', [{"mocked": "data"}]), call('test_file', [{"file_complete": "true",
                            "file_name": "test_file_3.csv",
                            "total_expected_records": 1
                        }])]

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
        assert mock_database_manager._insert_to_destinations.call_args_list == [
            call('test_file', [{"mocked": "data"},{"mocked": "data"}]),
            call('test_file', [{"file_complete": "true",
                            "file_name": "test_file_1.csv",
                            "total_expected_records": 1
                        }]),
            call('test_file', [{"mocked": "data"}]), 
            call('test_file', [{"file_complete": "true",
                "file_name": "test_file_2.csv",
                "total_expected_records": 1
            }]),
            call('test_file', [{"file_complete": "true",
                "file_name": "test_file_3.csv",
                "total_expected_records": 1
            }])]

        # Verify cache was updated with the final position
        # The final position should be the sum of the lengths of all chunks
        # 2 rows in chunk1 (loc 0,1) + 1 row in chunk2  (loc 2)= 2
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

        # Verify database insertion calls
        # With chunk_size=3 and 10 rows, we get chunks: [0-2], [3-5], [6-8], [9]
        # Each row has a different file_name, so each triggers EOF for previous file
        
        # Chunk 1: rows 0, 1, 2
        # - Row 0: file_name="test_file_0.csv" (opens it)
        # - Row 1: file_name="test_file_1.csv" (EOF for test_file_0.csv)
        # - Row 2: file_name="test_file_2.csv" (EOF for test_file_1.csv)
        # - Send all items together: [row0, row1, row2]
        # - Send EOFs together: [test_file_0.csv, test_file_1.csv]
        
        # Chunk 2: rows 3, 4, 5 (with last_row_saw=row 2)
        # - Process row 2 (from last_row_saw), row 3, row 4, row 5
        # - Row 3: file_name="test_file_3.csv" (EOF for test_file_2.csv)
        # - Row 4: file_name="test_file_4.csv" (EOF for test_file_3.csv)
        # - Row 5: file_name="test_file_5.csv" (EOF for test_file_4.csv)
        # - Remove first item (row2): [row3, row4, row5]
        # - Send items together: [row3, row4, row5]
        # - Send EOFs together: [test_file_2.csv, test_file_3.csv, test_file_4.csv]
        
        # Chunk 3: rows 6, 7, 8 (with last_row_saw=row 5)
        # - Process row 5 (from last_row_saw), row 6, row 7, row 8
        # - Row 6: file_name="test_file_6.csv" (EOF for test_file_5.csv)
        # - Row 7: file_name="test_file_7.csv" (EOF for test_file_6.csv)
        # - Row 8: file_name="test_file_8.csv" (EOF for test_file_7.csv)
        # - Remove first item (row5): [row6, row7, row8]
        # - Send items together: [row6, row7, row8]
        # - Send EOFs together: [test_file_5.csv, test_file_6.csv, test_file_7.csv]
        
        # Chunk 4: row 9 (with last_row_saw=row 8)
        # - Process row 8 (from last_row_saw), row 9
        # - Row 9: file_name="test_file_9.csv" (EOF for test_file_8.csv)
        # - Remove first item (row8): [row9]
        # - Send items together: [row9]
        # - Send EOFs together: [test_file_8.csv]
        
        # Final EOF for the last file: [test_file_9.csv]
        
        res = []
        
        # Chunk 1: rows 0, 1, 2 - all items sent together
        res.append(call('test_file', [
            {
                "row_index": 0,
                "found_folder": "test_folder_0",
                "file_name": "test_file_0.csv",
                "content": {"more_data": "test_data_0", "some_more_data": "test_data_0"}
            },
            {
                "row_index": 1,
                "found_folder": "test_folder_1",
                "file_name": "test_file_1.csv",
                "content": {"more_data": "test_data_1", "some_more_data": "test_data_1"}
            },
            {
                "row_index": 2,
                "found_folder": "test_folder_2",
                "file_name": "test_file_2.csv",
                "content": {"more_data": "test_data_2", "some_more_data": "test_data_2"}
            }
        ]))
        res.append(call('test_file', [
            {"file_complete": "true", "file_name": "test_file_0.csv", "total_expected_records": 1},
            {"file_complete": "true", "file_name": "test_file_1.csv", "total_expected_records": 1}
        ]))
        
        # Chunk 2: rows 3, 4, 5 (row 2 removed)
        res.append(call('test_file', [
            {
                "row_index": 3,
                "found_folder": "test_folder_3",
                "file_name": "test_file_3.csv",
                "content": {"more_data": "test_data_3", "some_more_data": "test_data_3"}
            },
            {
                "row_index": 4,
                "found_folder": "test_folder_4",
                "file_name": "test_file_4.csv",
                "content": {"more_data": "test_data_4", "some_more_data": "test_data_4"}
            },
            {
                "row_index": 5,
                "found_folder": "test_folder_5",
                "file_name": "test_file_5.csv",
                "content": {"more_data": "test_data_5", "some_more_data": "test_data_5"}
            }
        ]))
        res.append(call('test_file', [
            {"file_complete": "true", "file_name": "test_file_2.csv", "total_expected_records": 1},
            {"file_complete": "true", "file_name": "test_file_3.csv", "total_expected_records": 1},
            {"file_complete": "true", "file_name": "test_file_4.csv", "total_expected_records": 1}
        ]))
        
        # Chunk 3: rows 6, 7, 8 (row 5 removed)
        res.append(call('test_file', [
            {
                "row_index": 6,
                "found_folder": "test_folder_6",
                "file_name": "test_file_6.csv",
                "content": {"more_data": "test_data_6", "some_more_data": "test_data_6"}
            },
            {
                "row_index": 7,
                "found_folder": "test_folder_7",
                "file_name": "test_file_7.csv",
                "content": {"more_data": "test_data_7", "some_more_data": "test_data_7"}
            },
            {
                "row_index": 8,
                "found_folder": "test_folder_8",
                "file_name": "test_file_8.csv",
                "content": {"more_data": "test_data_8", "some_more_data": "test_data_8"}
            }
        ]))
        res.append(call('test_file', [
            {"file_complete": "true", "file_name": "test_file_5.csv", "total_expected_records": 1},
            {"file_complete": "true", "file_name": "test_file_6.csv", "total_expected_records": 1},
            {"file_complete": "true", "file_name": "test_file_7.csv", "total_expected_records": 1}
        ]))
        
        # Chunk 4: row 9 (row 8 removed)
        res.append(call('test_file', [
            {
                "row_index": 9,
                "found_folder": "test_folder_9",
                "file_name": "test_file_9.csv",
                "content": {"more_data": "test_data_9", "some_more_data": "test_data_9"}
            }
        ]))
        res.append(call('test_file', [
            {"file_complete": "true", "file_name": "test_file_8.csv", "total_expected_records": 1}
        ]))
        
        # Final EOF for the last file
        res.append(call('test_file', [
            {"file_complete": "true", "file_name": "test_file_9.csv", "total_expected_records": 1}
        ]))
        
        assert mock_database_manager._insert_to_destinations.call_args_list == res

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
