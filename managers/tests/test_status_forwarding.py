"""Status files are validated by their model and every field of the model
goes to its own status_<stage>_<section> destination (#127)."""

import asyncio
import json

from il_supermarket_parsers import ParserStatusOutput
from il_supermarket_scarper import ScraperStatusOutput

from managers.short_term_database_manager import ShortTermDBDatasetManager
from remotes.short_term.kafka_db import KafkaDbUploader


class RecordingUploader:
    def __init__(self):
        self.inserts = []

    def _insert_to_destinations(self, table_name, records):
        self.inserts.append((table_name, list(records)))


class DummyCache:
    def __init__(self):
        self._pushed = {}

    def get_pushed_timestamps(self, fname):
        return list(self._pushed.get(fname, []))

    def update_pushed_timestamps(self, fname, ts):
        self._pushed[fname] = list(ts)


SCRAPER_STATUS = {
    "global_status": [
        {
            "task_id": "t1",
            "status": "started",
            "system_timestamp": "2026-10-22T10:00:00+03:00",
            "limit": 1,
        }
    ],
    "events": [
        {
            "task_id": "t1",
            "status": "saw",
            "system_timestamp": "2026-10-22T10:00:01+03:00",
            "file_name": "Stores7290027600007-000.gz",
            "link": "https://example.com/file",
            "entry_id": "entry-1",
        },
        {
            "task_id": "t1",
            "status": "collected",
            "system_timestamp": "2026-10-22T10:00:02+03:00",
            "file_name": "Stores7290027600007-000.gz",
            "link_collected": "https://example.com/file",
            "entry_id": "entry-1",
        },
    ],
    "verified_downloads": [
        {
            "task_id": "t1",
            "file_name": "Stores7290027600007-000.xml",
            "system_timestamp": "2026-10-22T10:00:04+03:00",
            "content_sha256": "abc123",
            "save_decision": "created",
            "listing_hash": "listing-1",
            "entry_id": "entry-1",
        }
    ],
    # not a field of the model: validation leaves it out
    "_metadata": {"last_modified": "2026-10-22T10:00:00+03:00"},
}

PARSER_STATUS = {
    "global_status": [
        {
            "status": "started",
            "system_timestamp": "2026-10-22 10:05:00.000001",
            "limit": None,
            "scraper": "KING_STORE",
            "files_types": "STORE_FILE",
            "task_id": "p1",
        }
    ],
    "events": [
        {
            "status": "processed",
            "system_timestamp": "2026-10-22 10:05:01.000001",
            "file_name": "Stores7290058108879-000.xml",
            "store_folder": "/app/data/dumps/KingStore",
            "file_type": "STORE_FILE",
            "row_count": 3,
            "task_id": "p1",
        }
    ],
}


def _manager(tmp_path, uploader):
    scraping = tmp_path / "scraping_status"
    converting = tmp_path / "converting_status"
    scraping.mkdir()
    converting.mkdir()
    (scraping / "shufersal.json").write_text(json.dumps(SCRAPER_STATUS))
    (converting / "king_store_store_file.json").write_text(json.dumps(PARSER_STATUS))
    (converting / "notes.txt").write_text("ignored")
    return ShortTermDBDatasetManager(
        app_folder=str(tmp_path),
        outputs_folder=str(tmp_path / "outputs"),
        status_folder=str(tmp_path / "status"),
        short_term_db_target=uploader,
        enabled_scrapers=["SHUFERSAL", "KING_STORE", "KING"],
        enabled_file_types=["STORE_FILE"],
        scraping_status_folder=str(scraping),
        converting_status_folder=str(converting),
    )


def test_every_model_field_reaches_its_destination(tmp_path):
    uploader = RecordingUploader()
    _manager(tmp_path, uploader)._push_status_files(DummyCache())

    by_table = {table: rows for table, rows in uploader.inserts}
    assert set(by_table) == {
        "status_scraper_global_status",
        "status_scraper_events",
        "status_scraper_verified_downloads",
        "status_parser_global_status",
        "status_parser_events",
    }
    scraper = ScraperStatusOutput(**SCRAPER_STATUS)
    for section in ScraperStatusOutput.model_fields:
        envelopes = by_table[f"status_scraper_{section}"]
        # the rows are the model's validated rows, in order
        assert [e["data"] for e in envelopes] == [
            json.loads(row.model_dump_json()) for row in getattr(scraper, section)
        ]
        assert {e["section"] for e in envelopes} == {section}
        assert {e["stage"] for e in envelopes} == {"scraper"}
        assert {e["chain"] for e in envelopes} == {"shufersal"}
        assert {e["source_status_file"] for e in envelopes} == {"shufersal.json"}

    parser_events = by_table["status_parser_events"]
    # longest enabled chain wins: king_store, not king
    assert {e["chain"] for e in parser_events} == {"king_store"}
    assert [e["data"]["row_count"] for e in parser_events] == [3]
    assert ParserStatusOutput(
        global_status=[e["data"] for e in by_table["status_parser_global_status"]],
        events=[e["data"] for e in parser_events],
    )


def test_index_is_stable_and_rows_are_pushed_once(tmp_path):
    cache = DummyCache()
    first = RecordingUploader()
    manager = _manager(tmp_path, first)
    manager._push_status_files(cache)

    again = RecordingUploader()
    manager.uploader = again
    manager._push_status_files(cache)
    assert again.inserts == []

    fresh = RecordingUploader()
    manager.uploader = fresh
    manager._push_status_files(DummyCache())
    assert [e["index"] for _, rows in fresh.inserts for e in rows] == [
        e["index"] for _, rows in first.inserts for e in rows
    ]
    indexes = [e["index"] for _, rows in first.inserts for e in rows]
    assert len(indexes) == len(set(indexes))


class RecordingProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, key, value):
        self.sent.append((topic, key, value))
        future = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future

    async def send_and_wait(self, topic, key, value):
        self.sent.append((topic, key, value))


def test_status_rows_are_keyed_by_chain_without_flush(tmp_path):
    collect = RecordingUploader()
    _manager(tmp_path, collect)._push_status_files(DummyCache())
    topic, envelopes = collect.inserts[0]

    uploader = KafkaDbUploader(kafka_bootstrap_servers="unused:9092")
    uploader.producer = RecordingProducer()
    asyncio.run(uploader._async_insert_to_destinations(topic, envelopes))

    assert [(t, k) for t, k, _ in uploader.producer.sent] == [
        (topic, envelopes[0]["chain"].encode())
    ] * len(envelopes)
