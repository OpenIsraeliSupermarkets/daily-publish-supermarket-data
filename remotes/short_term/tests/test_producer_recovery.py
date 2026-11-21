"""The Kafka producer is recreated after failed acks, and the fetcher exits
when even a new producer cannot push (#190)."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from aiokafka.errors import (
    KafkaError,
    MessageSizeTooLargeError,
    OutOfOrderSequenceNumber,
)

from remotes.short_term.kafka_db import KafkaDbUploader


class FakeProducer:
    """Acks every send, or fails every send with `error`."""

    def __init__(self, error=None):
        self.error = error
        self.sent = []
        self.stopped = False

    async def start(self):
        pass

    async def stop(self):
        self.stopped = True

    async def send(self, topic, key, value):
        ack = asyncio.get_running_loop().create_future()
        if self.error is None:
            self.sent.append((topic, value))
            ack.set_result(None)
        else:
            ack.set_exception(self.error)
        return ack

    async def send_and_wait(self, topic, key, value):
        await (await self.send(topic, key, value))


def _uploader(first_producer, new_producers, max_consecutive_failures=3):
    """An uploader on `first_producer`; each reset starts the next of
    `new_producers`."""
    uploader = KafkaDbUploader(
        kafka_bootstrap_servers="broker:9092",
        max_consecutive_failures=max_consecutive_failures,
    )
    uploader.producer = first_producer
    uploader.admin_client = MagicMock()
    uploader._loop = asyncio.new_event_loop()
    factory = patch(
        "remotes.short_term.kafka_db.AIOKafkaProducer", side_effect=new_producers
    )
    admin = patch("remotes.short_term.kafka_db.KafkaAdminClient")
    return uploader, factory, admin


ROWS = [{"file_name": "a.xml", "row_index": n} for n in range(3)]


def test_failed_acks_recreate_the_producer_and_the_retry_delivers():
    broken = FakeProducer(error=OutOfOrderSequenceNumber())
    fresh = FakeProducer()
    uploader, factory, admin = _uploader(broken, [fresh])

    with factory, admin:
        uploader._insert_to_destinations("price_full_file_x", ROWS)

    # The broken producer was stopped, and every row went out on the new one.
    assert broken.stopped
    assert uploader.producer is fresh
    assert [v for _, v in fresh.sent if "row_index" in v] == ROWS
    assert uploader._consecutive_failed_pushes == 0


def test_a_push_that_fails_on_the_new_producer_too_is_raised():
    uploader, factory, admin = _uploader(
        FakeProducer(error=OutOfOrderSequenceNumber()),
        [FakeProducer(error=OutOfOrderSequenceNumber())],
    )

    with factory, admin, pytest.raises(KafkaError):
        uploader._insert_to_destinations("price_full_file_x", ROWS)
    assert uploader._consecutive_failed_pushes == 1


def test_the_process_exits_after_too_many_failed_pushes_in_a_row():
    always_broken = [FakeProducer(error=OutOfOrderSequenceNumber()) for _ in range(5)]
    uploader, factory, admin = _uploader(
        always_broken[0], always_broken[1:], max_consecutive_failures=2
    )

    with factory, admin:
        with pytest.raises(KafkaError):
            uploader._insert_to_destinations("price_full_file_x", ROWS)
        with pytest.raises(SystemExit):
            uploader._insert_to_destinations("price_full_file_x", ROWS)


def test_a_record_the_broker_refuses_is_raised_without_a_new_producer():
    too_large = FakeProducer(error=MessageSizeTooLargeError())
    uploader, factory, admin = _uploader(too_large, [])

    with factory, admin, pytest.raises(KafkaError):
        uploader._insert_to_destinations("price_full_file_x", ROWS)

    # Same producer, and the failure does not count toward the exit.
    assert uploader.producer is too_large
    assert uploader._consecutive_failed_pushes == 0


def test_a_failed_streamed_file_recreates_the_producer():
    broken = FakeProducer(error=OutOfOrderSequenceNumber())
    fresh = FakeProducer()
    uploader, factory, admin = _uploader(broken, [fresh])

    async def push(stream):
        for row in ROWS:
            await stream.add_row("price_full_file_x", row)
        await stream.end_file("price_full_file_x", "a.xml", len(ROWS))

    with factory, admin:
        stream = uploader.stream_rows(push)

    # The file is left for the CSV push, which then runs on the new producer.
    assert stream.failed_files == ["a.xml"]
    assert uploader.producer is fresh
