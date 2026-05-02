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
