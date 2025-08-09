import os
import json
import tempfile
import pytest
from unittest.mock import patch, MagicMock, mock_open
from managers.short_term_database_manager import ShortTermDBDatasetManager


class DummyUploader:
    def _insert_to_destinations(self, table_name, records):
        self.last_table = table_name
        self.last_records = records

    def restart_database(self):
        self.restarted = True


@pytest.fixture
def temp_outputs_folder():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def dummy_uploader():
    return DummyUploader()


@pytest.fixture
def dummy_cache_state():
    class DummyCache:
        def get_pushed_timestamps(self, fname):
            return []

        def update_pushed_timestamps(self, fname, ts):
            self.updated = (fname, ts)

    return DummyCache()


def test_push_parser_status_reads_from_outputs_folder(
    temp_outputs_folder, dummy_uploader, dummy_cache_state
):
    # Prepare a fake parser-status.json in the temp folder
    parser_status_path = os.path.join(temp_outputs_folder, "parser-status.json")
    fake_data = [
        {
            "when_date": "20240101",
            "limit": 10,
            "store_enum": "STORE1",
            "file_type": "TYPE1",
            "data_folder": "folder1",
            "output_folder": "out1",
            "status": "OK",
            "response": "done",
        }
    ]
    with open(parser_status_path, "w") as f:
        json.dump(fake_data, f)

    # Patch ParserStatus to_dict to avoid dependency
    with patch("managers.short_term_database_manager.ParserStatus") as MockParserStatus:
        instance = MockParserStatus.return_value
        instance.to_dict.return_value = {"mocked": True}
        manager = ShortTermDBDatasetManager(
            app_folder="/tmp",
            outputs_folder=temp_outputs_folder,
            status_folder="/tmp",
            short_term_db_target=dummy_uploader,
            enabled_scrapers=["scraper1", "scraper2"],
            enabled_converters=["converter1", "converter2"],
        )
        manager._push_parser_status(dummy_cache_state)
        # Check that DummyUploader got the mocked record
        assert dummy_uploader.last_records == [{"mocked": True}]
