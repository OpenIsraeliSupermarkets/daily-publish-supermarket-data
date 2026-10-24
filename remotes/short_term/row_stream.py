"""Per-row streaming into a short-term database.

A row stream takes rows one at a time, grouped by source file. When a file
ends, the stream sends that file's ``file_complete`` message only after every
row of the file is stored, so the marker never overtakes its rows and a file
with a failed row gets no marker. Other files keep streaming meanwhile.
"""

import asyncio
from typing import Callable, Dict, List, Optional, Tuple

from utils import Logger

FileKey = Tuple[str, str]  # (table name, source file name)


def file_complete_message(file_name: str, total_expected_records: int) -> dict:
    """The end-of-file marker consumers wait for."""
    return {
        "file_complete": "true",
        "file_name": file_name,
        "total_expected_records": total_expected_records,
    }


class RowStream:
    """Base row stream. Subclasses store the rows."""

    def __init__(self, on_file_stored: Optional[Callable[[str], None]] = None):
        self._on_file_stored = on_file_stored
        self._tasks: List[asyncio.Task] = []
        self.stored_files: List[str] = []
        self.failed_files: List[str] = []

    async def add_row(self, table: str, item: dict) -> None:
        """Queue one row (a dict with ``file_name``) for ``table``."""
        raise NotImplementedError

    async def end_file(
        self, table: str, file_name: str, total_expected_records: int
    ) -> None:
        """Mark the end of ``file_name``; its marker goes out once its rows are stored."""
        self._tasks.append(
            asyncio.create_task(
                self._complete_file(table, file_name, total_expected_records)
            )
        )

    async def _complete_file(
        self, table: str, file_name: str, total_expected_records: int
    ) -> None:
        try:
            await self._store_file_end(table, file_name, total_expected_records)
        except Exception as e:  # pylint: disable=broad-except
            Logger.error("Streaming %s to %s failed: %s", file_name, table, e)
            self.failed_files.append(file_name)
            return
        self.stored_files.append(file_name)
        if self._on_file_stored is not None:
            self._on_file_stored(file_name)

    async def _store_file_end(
        self, table: str, file_name: str, total_expected_records: int
    ) -> None:
        """Wait until the file's rows are stored, then store its marker. Raise on failure."""
        raise NotImplementedError

    async def close(self) -> None:
        """Wait for every ended file. Rows of files that never ended get no marker."""
        await asyncio.gather(*self._tasks)
        await self._abandon_open_files()

    async def _abandon_open_files(self) -> None:
        """Hook for rows of files whose end never arrived."""


class BufferedRowStream(RowStream):
    """Row stream over a database that only has the batch insert
    (``_insert_to_destinations``). Rows are inserted in batches per file."""

    def __init__(self, uploader, batch_size: int = 10000, on_file_stored=None):
        super().__init__(on_file_stored)
        self._uploader = uploader
        self._batch_size = batch_size
        self._buffers: Dict[FileKey, List[dict]] = {}
        self._inserts: Dict[FileKey, List[asyncio.Future]] = {}

    def _insert(self, key: FileKey, items: List[dict]) -> None:
        self._inserts.setdefault(key, []).append(
            asyncio.ensure_future(
                asyncio.to_thread(self._uploader._insert_to_destinations, key[0], items)
            )
        )

    async def add_row(self, table: str, item: dict) -> None:
        key = (table, item["file_name"])
        buffer = self._buffers.setdefault(key, [])
        buffer.append(item)
        if len(buffer) >= self._batch_size:
            self._insert(key, self._buffers.pop(key))

    async def _store_file_end(self, table, file_name, total_expected_records):
        key = (table, file_name)
        if key in self._buffers:
            self._insert(key, self._buffers.pop(key))
        await asyncio.gather(*self._inserts.pop(key, []))
        await asyncio.to_thread(
            self._uploader._insert_to_destinations,
            table,
            [file_complete_message(file_name, total_expected_records)],
        )


class KafkaRowStream(RowStream):
    """Row stream over an aiokafka producer.

    Rows are handed to the producer without waiting for their acks, so the
    producer can batch them. Every message of a file uses the file name as its
    key, so the file stays on one partition, in order. At most
    ``max_pending_rows`` rows wait for an ack at a time; ``add_row`` waits
    when that many are in flight.
    """

    def __init__(
        self,
        producer,
        topic_for_table: Callable[[str], str],
        max_pending_rows: int = 100000,
        on_file_stored=None,
    ):
        super().__init__(on_file_stored)
        self._producer = producer
        self._topic_for_table = topic_for_table
        self._slots = asyncio.Semaphore(max_pending_rows)
        self._pending: Dict[FileKey, List[asyncio.Future]] = {}

    async def _send(self, table: str, file_name: str, value: dict) -> asyncio.Future:
        return await self._producer.send(
            topic=self._topic_for_table(table),
            key=file_name.encode("utf-8"),
            value=value,
        )

    async def add_row(self, table: str, item: dict) -> None:
        await self._slots.acquire()
        try:
            ack = await self._send(table, item["file_name"], item)
        except BaseException:
            self._slots.release()
            raise
        ack.add_done_callback(lambda _: self._slots.release())
        self._pending.setdefault((table, item["file_name"]), []).append(ack)

    async def _store_file_end(self, table, file_name, total_expected_records):
        acks = self._pending.pop((table, file_name), [])
        failed = [
            result
            for result in await asyncio.gather(*acks, return_exceptions=True)
            if isinstance(result, BaseException)
        ]
        if failed:
            raise RuntimeError(
                f"{len(failed)}/{len(acks)} rows were not acked, first error: {failed[0]!r}"
            )
        marker = await self._send(
            table,
            file_name,
            file_complete_message(file_name, total_expected_records),
        )
        flush = await self._send(table, file_name, {"flush": "true"})
        await marker
        await flush

    async def _abandon_open_files(self) -> None:
        for (table, file_name), acks in self._pending.items():
            Logger.warning(
                "Stream ended inside %s (%s): %d rows sent without file_complete",
                file_name,
                table,
                len(acks),
            )
            await asyncio.gather(*acks, return_exceptions=True)
        self._pending.clear()
