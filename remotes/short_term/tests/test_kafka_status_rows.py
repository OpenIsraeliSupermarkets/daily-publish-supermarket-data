"""Status rows without a file_name reach their Kafka topic (#115)."""

import asyncio
import json

from il_supermarket_scarper.utils.scraping.scraper_status_contract import (
    ScraperStatusOutput,
)

from managers.short_term_database_manager import ShortTermDBDatasetManager
from remotes.short_term.kafka_db import KafkaDbUploader


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


def _global_status_rows():
    """Scraper global_status rows exactly as api_update builds them."""
    output = ScraperStatusOutput(
        global_status=[
            {"task_id": "20261021120000", "status": "started", "limit": 10},
            {
                "task_id": "20261021120000",
                "status": "estimated_size",
                "folder_size": {
                    "task_id": "20261021120000",
                    "size": 1.5,
                    "unit": "MB",
                    "folder": "/app/data/Shufersal",
                },
            },
        ]
    )
    pushed = []

    class Collect:
        def _insert_to_destinations(self, table_name, items):
            pushed.extend(items)

    manager = ShortTermDBDatasetManager.__new__(ShortTermDBDatasetManager)
    manager.uploader = Collect()
    envelope = {
        "chain": "shufersal",
        "stage": "scraper",
        "section": "global_status",
        "source_status_file": "Shufersal.json",
    }
    manager._push_status_rows(output.global_status, envelope, set(), [])
    return pushed


def test_status_rows_without_file_name_reach_the_topic():
    rows = _global_status_rows()
    assert len(rows) == 2 and all("file_name" not in row for row in rows)

    uploader = KafkaDbUploader(kafka_bootstrap_servers="unused:9092")
    uploader.producer = RecordingProducer()
    topic = "status_scraper_global_status"
    asyncio.run(uploader._async_insert_to_destinations(topic, rows))

    sent = uploader.producer.sent
    # every row is sent, keyed by its chain, with no per-file flush marker
    assert [value for _, _, value in sent] == rows
    assert [key for _, key, _ in sent] == [b"shufersal"] * len(rows)
    assert {t for t, _, _ in sent} == {topic}
    json.dumps([value for _, _, value in sent])  # serializable as the producer does


def test_rows_with_file_name_keep_file_key_and_flush():
    uploader = KafkaDbUploader(kafka_bootstrap_servers="unused:9092")
    uploader.producer = RecordingProducer()
    items = [{"file_name": "a.xml", "index": "x", "n": 1}]

    asyncio.run(uploader._async_insert_to_destinations("topic", items))

    assert uploader.producer.sent == [
        ("topic", b"a.xml", items[0]),
        ("topic", b"a.xml", {"flush": "true"}),
    ]


def test_row_without_file_name_or_index_is_sent_unkeyed():
    uploader = KafkaDbUploader(kafka_bootstrap_servers="unused:9092")
    uploader.producer = RecordingProducer()

    asyncio.run(uploader._async_insert_to_destinations("topic", [{"n": 1}]))

    assert uploader.producer.sent == [("topic", None, {"n": 1})]
