"""A push killed mid-file resumes from the last chunk, not from row 0 (#108)."""

import asyncio
import json
import os

import numpy as np
import pandas as pd
import pytest

from managers.cache_manager import CacheManager
from managers.large_file_push_manager import LargeFilePushManager
from remotes.short_term.kafka_db import KafkaDbUploader


class RecordingDb:
    """Collects pushed items; optionally dies on the n-th insert."""

    def __init__(self, die_on_call=None):
        self.items = []
        self.calls = 0
        self.die_on_call = die_on_call

    def _insert_to_destinations(self, table_name, items):
        self.calls += 1
        if self.die_on_call is not None and self.calls == self.die_on_call:
            raise KeyboardInterrupt("killed mid-push")
        self.items.extend(items)


def _write_csv(folder):
    # Like the parser's CSV writer: values repeated within a source file are
    # masked with NaN and restored by ffill.
    rows = []
    for source, count in [("a.xml", 5), ("b.xml", 4)]:
        for i in range(count):
            first = i == 0
            rows.append(
                {
                    "found_folder": "dump" if first else np.nan,
                    "file_name": source if first else np.nan,
                    "chainid": "729" if first else np.nan,
                    "itemcode": f"{source}-{i}",
                }
            )
    name = "price_full_file_test.csv"
    pd.DataFrame(rows).to_csv(os.path.join(folder, name), index=False)
    return name


def _push(folder, file, db, kill=False):
    manager = CacheManager(folder)
    cache = manager.__enter__()
    try:
        LargeFilePushManager(folder, db, chunk_size=2).process_file(file, cache)
    except KeyboardInterrupt:
        # a killed process never runs CacheManager.__exit__
        manager._lock.release()
        return
    manager.__exit__(None, None, None)


def _rows(items):
    return [
        (
            i["file_name"],
            i.get("row_index"),
            i.get("content"),
            i.get("total_expected_records"),
        )
        for i in items
    ]


def test_killed_push_resumes_at_last_completed_source_file(tmp_path):
    clean_dir, killed_dir = tmp_path / "clean", tmp_path / "killed"
    clean_dir.mkdir()
    killed_dir.mkdir()

    clean = RecordingDb()
    _push(str(clean_dir), _write_csv(str(clean_dir)), clean)

    # chunk_size=2: chunk 3 holds a.xml's last row and b.xml's first, so a.xml
    # completes there (inserts 3 and 4); die on chunk 4 (insert 5)
    file = _write_csv(str(killed_dir))
    _push(str(killed_dir), file, RecordingDb(die_on_call=5), kill=True)

    with open(killed_dir / ".push_cache") as cache_file:
        saved = json.load(cache_file)
    assert saved["last_pushed"][file] == 4  # a.xml is rows 0-4

    second = RecordingDb()
    _push(str(killed_dir), file, second)

    # the restart re-sends b.xml from its first (unmasked) row, as a clean run
    def data_rows(items):
        return _rows(
            i for i in items if i["file_name"] == "b.xml" and "file_complete" not in i
        )

    assert data_rows(second.items) == data_rows(clean.items)
    assert [i["file_name"] for i in second.items if "file_complete" in i] == ["b.xml"]


def test_finished_push_does_not_resend_file_complete(tmp_path):
    file = _write_csv(str(tmp_path))
    _push(str(tmp_path), file, RecordingDb())

    again = RecordingDb()
    _push(str(tmp_path), file, again)

    assert again.items == []


class OrderedProducer:
    def __init__(self):
        self.log = []

    async def send(self, topic, key, value):
        self.log.append(("send", value))
        future = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future

    async def send_and_wait(self, topic, key, value):
        self.log.append(("send_and_wait", value))


def test_records_are_batched_and_flushed_after():
    uploader = KafkaDbUploader(kafka_bootstrap_servers="unused:9092")
    uploader.producer = OrderedProducer()
    items = [{"file_name": "a.xml", "n": n} for n in range(3)]

    asyncio.run(uploader._async_insert_to_destinations("topic", items))

    assert uploader.producer.log == [("send", item) for item in items] + [
        ("send_and_wait", {"flush": "true"})
    ]
