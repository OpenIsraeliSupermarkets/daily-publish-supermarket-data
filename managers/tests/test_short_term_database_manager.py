import json
import os
import tempfile

import pytest

from managers.short_term_database_manager import (
    VERIFIED_SCRAPER_DOWNLOADS_TABLE,
    ShortTermDBDatasetManager,
)


class RecordingUploader:
    def __init__(self):
        self.inserts = []

    def _insert_to_destinations(self, table_name, records):
        self.inserts.append((table_name, list(records)))

    def restart_database(self, enabled_scrapers, enabled_file_types):
        self.restarted = (enabled_scrapers, enabled_file_types)


class DummyCache:
    def __init__(self):
        self._pushed = {}

    def get_pushed_timestamps(self, fname):
        return list(self._pushed.get(fname, []))

    def update_pushed_timestamps(self, fname, ts):
        self._pushed[fname] = list(ts)


@pytest.fixture
def scraper_status_payload():
    task_id = "task-1"
    return {
        "global_status": [
            {
                "task_id": task_id,
                "status": "started",
                "system_timestamp": "2026-09-14T12:00:00+03:00",
                "limit": 1,
            }
        ],
        "events": [
            {
                "task_id": task_id,
                "status": "saw",
                "system_timestamp": "2026-09-14T12:00:01+03:00",
                "file_name": "PriceFull7290000000000-000-202609141200",
                "link": "https://example.com/file",
                "entry_id": "entry-1",
            },
            {
                "task_id": task_id,
                "status": "collected",
                "system_timestamp": "2026-09-14T12:00:02+03:00",
                "file_name": "PriceFull7290000000000-000-202609141200",
                "link_collected": "https://example.com/file",
                "entry_id": "entry-1",
            },
            {
                "task_id": task_id,
                "status": "downloaded",
                "system_timestamp": "2026-09-14T12:00:03+03:00",
                "file_name": "PriceFull7290000000000-000-202609141200",
                "downloaded_successfully": True,
                "extracted_successfully": True,
                "content_sha256": "abc123",
                "save_decision": "created",
                "entry_id": "entry-1",
            },
        ],
        "verified_downloads": [
            {
                "task_id": task_id,
                "file_name": "PriceFull7290000000000-000-202609141200",
                "system_timestamp": "2026-09-14T12:00:04+03:00",
                "content_sha256": "abc123",
                "save_decision": "created",
                "listing_hash": "listing-1",
                "entry_id": "entry-1",
            }
        ],
    }


def test_push_scraper_status_persists_verified_downloads(scraper_status_payload):
    uploader = RecordingUploader()
    cache = DummyCache()

    with tempfile.TemporaryDirectory() as tmpdir:
        scraping_status_folder = os.path.join(tmpdir, "scraping_status")
        os.makedirs(scraping_status_folder)
        with open(
            os.path.join(scraping_status_folder, "bareket.json"), "w", encoding="utf-8"
        ) as handle:
            json.dump(scraper_status_payload, handle)

        manager = ShortTermDBDatasetManager(
            app_folder=tmpdir,
            outputs_folder=os.path.join(tmpdir, "outputs"),
            status_folder=os.path.join(tmpdir, "status"),
            short_term_db_target=uploader,
            enabled_scrapers=["bareket"],
            enabled_file_types=["price_full"],
            scraping_status_folder=scraping_status_folder,
            converting_status_folder=os.path.join(tmpdir, "converting_status"),
        )
        os.makedirs(manager.converting_status_folder, exist_ok=True)

        manager._push_scraper_status(cache)

    tables = {table for table, _ in uploader.inserts}
    assert "ScraperStatus" in tables
    assert "GlobalScraperStatus" in tables
    assert VERIFIED_SCRAPER_DOWNLOADS_TABLE in tables

    verified_rows = [
        row
        for table, rows in uploader.inserts
        if table == VERIFIED_SCRAPER_DOWNLOADS_TABLE
        for row in rows
    ]
    assert len(verified_rows) == 1
    assert verified_rows[0]["listing_hash"] == "listing-1"
    assert verified_rows[0]["task_id"] == "task-1"
    assert "index" in verified_rows[0]
