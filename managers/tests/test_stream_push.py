"""Streaming push: rows go out as they are parsed, each file's file_complete
only after its rows are acked, and api_update skips streamed files (#114)."""

import asyncio
import os
import queue

import numpy as np
import pandas as pd

from managers.cache_manager import CacheManager
from managers.large_file_push_manager import LargeFilePushManager
from managers.stream_push_manager import StreamPushManager, row_to_item
from remotes.short_term.kafka_db import KafkaDbUploader
from remotes.short_term.file_db import DummyDocumentDbUploader
from aiokafka.errors import KafkaError


class Rows:
    """Stands in for the parser's ParsedRowsQueue."""

    def __init__(self, messages):
        self._queue = queue.Queue()
        for message in messages + [None]:
            self._queue.put(message)

    def get(self):
        return self._queue.get()


class DelayedAckProducer:
    """Logs sends; acks resolve on the next loop turns, or fail for chosen rows."""

    def __init__(self, fail_rows=()):
        self.log = []
        self.fail_rows = set(fail_rows)

    async def send(self, topic, key, value):
        loop = asyncio.get_running_loop()
        ack = loop.create_future()
        self.log.append(("send", topic, value))

        def resolve():
            if value.get("row_index") in self.fail_rows:
                ack.set_exception(KafkaError("broker said no"))
            else:
                self.log.append(("ack", topic, value))
                ack.set_result(None)

        loop.call_later(0.01, resolve)
        return ack

    async def send_and_wait(self, topic, key, value):
        await (await self.send(topic, key, value))


def _row(file_name, n):
    return {
        "found_folder": "dumps/Test",
        "file_name": file_name,
        "itemcode": f"{file_name}-{n}",
        "itemprice": "",
        "groups": {"group": {"id": "1"}},
    }


def _file(file_name, count):
    return [_row(file_name, n) for n in range(count)] + [
        {
            "file_complete": "true",
            "file_name": file_name,
            "total_expected_records": count,
        }
    ]


def _uploader(producer):
    uploader = KafkaDbUploader(kafka_bootstrap_servers="unused:9092")
    uploader.producer = producer
    uploader._loop = asyncio.new_event_loop()
    return uploader


def _stream(tmp_path, uploader, queues):
    with CacheManager(str(tmp_path)) as cache:
        stream = StreamPushManager(uploader, read_batch=3).push(queues, cache)
        streamed = list(cache.get_streamed_files())
    return stream, streamed


def test_row_is_shaped_like_the_csv_push():
    item = row_to_item(_row("a.xml", 0), 7)
    assert item == {
        "row_index": 7,
        "found_folder": "dumps/Test",
        "file_name": "a.xml",
        "content": {
            "itemcode": "a.xml-0",
            "itemprice": None,
            "groups": '{"group": {"id": "1"}}',
        },
    }


def test_file_complete_follows_the_files_acked_rows(tmp_path):
    producer = DelayedAckProducer()
    queues = {
        ("TEST", "PRICE_FULL_FILE"): Rows(_file("a.xml", 3) + _file("b.xml", 2)),
        ("TEST", "STORE_FILE"): Rows(_file("s.xml", 1)),
    }

    stream, streamed = _stream(tmp_path, _uploader(producer), queues)

    assert sorted(stream.stored_files) == ["a.xml", "b.xml", "s.xml"]
    assert sorted(streamed) == ["a.xml", "b.xml", "s.xml"]
    sent = [(t, v) for kind, t, v in producer.log if kind == "send"]
    for topic, file_name, first, count in [
        ("price_full_file_test", "a.xml", 0, 3),
        # row_index runs on across the table's files, as in its CSV
        ("price_full_file_test", "b.xml", 3, 2),
        ("store_file_test", "s.xml", 0, 1),
    ]:
        mine = [
            v
            for t, v in sent
            if t == topic and v.get("file_name", file_name) == file_name
        ]
        values = [v for v in mine if "flush" not in v]
        assert [v.get("row_index") for v in values] == list(
            range(first, first + count)
        ) + [None]
        assert values[-1] == {
            "file_complete": "true",
            "file_name": file_name,
            "total_expected_records": count,
        }
        # the marker is sent only after every row of its file was acked
        marker_at = producer.log.index(("send", topic, values[-1]))
        acked_rows = [
            v
            for kind, t, v in producer.log[:marker_at]
            if kind == "ack" and t == topic and v.get("file_name") == file_name
        ]
        assert len(acked_rows) == count


def test_row_index_continues_from_earlier_cycles(tmp_path):
    with CacheManager(str(tmp_path)) as cache:
        # the CSV push already sent rows 0-4 of this table's CSV
        cache.update_last_processed_row("price_full_file_test.csv", 4)
    producer = DelayedAckProducer()
    queues = {("TEST", "PRICE_FULL_FILE"): Rows(_file("a.xml", 2))}

    _stream(tmp_path, _uploader(producer), queues)
    queues = {("TEST", "PRICE_FULL_FILE"): Rows(_file("b.xml", 2))}
    _stream(tmp_path, _uploader(producer), queues)

    sent = [
        v["row_index"]
        for kind, _, v in producer.log
        if kind == "send" and "row_index" in v
    ]
    assert sent == [5, 6, 7, 8]


def test_failed_ack_leaves_no_file_complete_and_no_progress(tmp_path):
    producer = DelayedAckProducer(fail_rows={1})
    queues = {("TEST", "PRICE_FULL_FILE"): Rows(_file("a.xml", 3))}

    stream, streamed = _stream(tmp_path, _uploader(producer), queues)

    assert stream.failed_files == ["a.xml"]
    assert streamed == []
    assert not any("file_complete" in v for _, _, v in producer.log)


def test_stream_ending_inside_a_file_sends_no_file_complete(tmp_path):
    producer = DelayedAckProducer()
    queues = {("TEST", "PRICE_FULL_FILE"): Rows(_file("a.xml", 2)[:-1])}

    stream, streamed = _stream(tmp_path, _uploader(producer), queues)

    assert stream.stored_files == [] and streamed == []
    assert not any("file_complete" in v for _, _, v in producer.log)


def test_buffered_stream_for_databases_without_streaming(tmp_path):
    db = DummyDocumentDbUploader(str(tmp_path / "db"))
    db._create_destinations("row_index", "price_full_file_test")
    queues = {("TEST", "PRICE_FULL_FILE"): Rows(_file("a.xml", 3))}

    stream, streamed = _stream(tmp_path, db, queues)

    assert streamed == ["a.xml"]
    content = db.get_destinations_content("price_full_file_test")
    assert sorted(c["row_index"] for c in content if "row_index" in c) == [0, 1, 2]
    assert [c for c in content if "file_complete" in c] == [
        {"file_complete": "true", "file_name": "a.xml", "total_expected_records": 3}
    ]


def _write_csv(folder):
    rows = []
    for source, count in [("a.xml", 3), ("b.xml", 2), ("c.xml", 2)]:
        for i in range(count):
            first = i == 0
            rows.append(
                {
                    "found_folder": "dump" if first else np.nan,
                    "file_name": source if first else np.nan,
                    "itemcode": f"{source}-{i}",
                }
            )
    name = "price_full_file_test.csv"
    pd.DataFrame(rows).to_csv(os.path.join(folder, name), index=False)
    return name


class RecordingDb:
    def __init__(self):
        self.items = []

    def _insert_to_destinations(self, table_name, items):
        self.items.extend(items)


def test_csv_push_skips_files_the_stream_delivered(tmp_path):
    file = _write_csv(str(tmp_path))
    with CacheManager(str(tmp_path)) as cache:
        cache.mark_file_streamed("b.xml")
    db = RecordingDb()

    with CacheManager(str(tmp_path)) as cache:
        LargeFilePushManager(str(tmp_path), db, chunk_size=2).process_file(file, cache)

    sent = [
        (i["file_name"], i.get("row_index"), i.get("file_complete")) for i in db.items
    ]
    assert sent == [
        ("a.xml", 0, None),
        ("a.xml", 1, None),
        ("a.xml", 2, None),
        ("a.xml", None, "true"),
        ("c.xml", 5, None),
        ("c.xml", 6, None),
        ("c.xml", None, "true"),
    ]


def test_batch_insert_fails_when_a_record_is_not_acked():
    producer = DelayedAckProducer(fail_rows={1})
    uploader = KafkaDbUploader(kafka_bootstrap_servers="unused:9092")
    uploader.producer = producer
    items = [{"file_name": "a.xml", "row_index": n} for n in range(3)]

    try:
        asyncio.run(uploader._async_insert_to_destinations("topic", items))
    except KafkaError:
        pass
    else:
        raise AssertionError("a lost record must fail the push")
    assert not any("flush" in v for _, _, v in producer.log)
