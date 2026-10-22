"""Kafka mode forwards status sections untouched, one topic per stage and key (#127)."""

import asyncio
import json

from managers.short_term_database_manager import ShortTermDBDatasetManager
from remotes.short_term.kafka_db import KafkaDbUploader


class ForwardingUploader:
    forwards_raw_status = True

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
    "_metadata": {"last_modified": "2026-10-22T10:00:00+03:00"},
    "global_status": [{"task_id": "t1", "status": "started", "limit": 1}],
    "events": [
        {"task_id": "t1", "status": "saw", "file_name": "Stores729-000.gz"},
        {"task_id": "t1", "status": "downloaded", "file_name": "Stores729-000.gz"},
    ],
    "verified_downloads": [{"task_id": "t1", "file_name": "Stores729-000.xml"}],
    # a key the fetcher has never heard of, with fields it doesn't know
    "brand_new_section": [{"anything": {"nested": [1, 2]}, "goes": None}],
}

PARSER_STATUS = {
    "global_status": [{"task_id": "p1", "status": "started", "scraper": "KING_STORE"}],
    "events": [{"task_id": "p1", "status": "processed", "row_count": 3}],
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


def test_every_section_reaches_its_topic_untouched(tmp_path):
    uploader = ForwardingUploader()
    _manager(tmp_path, uploader)._push_status_files(DummyCache())

    by_topic = {topic: rows for topic, rows in uploader.inserts}
    assert set(by_topic) == {
        "status_scraper_global_status",
        "status_scraper_events",
        "status_scraper_verified_downloads",
        "status_scraper_brand_new_section",
        "status_parser_global_status",
        "status_parser_events",
    }
    for section in (
        "global_status",
        "events",
        "verified_downloads",
        "brand_new_section",
    ):
        envelopes = by_topic[f"status_scraper_{section}"]
        assert [e["data"] for e in envelopes] == SCRAPER_STATUS[section]
        assert {e["section"] for e in envelopes} == {section}
        assert {e["stage"] for e in envelopes} == {"scraper"}
        assert {e["chain"] for e in envelopes} == {"shufersal"}
        assert {e["source_status_file"] for e in envelopes} == {"shufersal.json"}
    # longest enabled chain wins: king_store, not king
    assert {e["chain"] for e in by_topic["status_parser_events"]} == {"king_store"}
    assert [e["data"] for e in by_topic["status_parser_events"]] == PARSER_STATUS[
        "events"
    ]


def test_index_is_stable_and_rows_are_pushed_once(tmp_path):
    cache = DummyCache()
    first = ForwardingUploader()
    manager = _manager(tmp_path, first)
    manager._push_status_files(cache)

    again = ForwardingUploader()
    manager.uploader = again
    manager._push_status_files(cache)
    assert again.inserts == []

    fresh = ForwardingUploader()
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


def test_status_envelopes_are_keyed_by_chain_without_flush(tmp_path):
    collect = ForwardingUploader()
    _manager(tmp_path, collect)._push_status_files(DummyCache())
    topic, envelopes = collect.inserts[0]

    uploader = KafkaDbUploader(kafka_bootstrap_servers="unused:9092")
    uploader.producer = RecordingProducer()
    asyncio.run(uploader._async_insert_to_destinations(topic, envelopes))

    assert [(t, k) for t, k, _ in uploader.producer.sent] == [
        (topic, envelopes[0]["chain"].encode())
    ] * len(envelopes)
