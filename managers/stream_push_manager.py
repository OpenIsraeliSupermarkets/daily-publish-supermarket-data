import asyncio
import json
import queue as queue_module
from concurrent.futures import ThreadPoolExecutor

from utils import Logger
from remotes import ShortTermDatabaseUploader
from managers.cache_manager import CacheState
from data_models.raw_schema import get_table_name
from utils.mongo_bson import sanitize_for_mongo

_ROW_FIELDS = ("found_folder", "file_name")


def _cell(value):
    """Match the values the CSV path sends: nested structures as JSON text
    (like the parser's CSV writer), empty cells as missing."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, ensure_ascii=False)
    if value == "":
        return None
    return value


def row_to_item(row: dict, row_index: int) -> dict:
    """Shape a parsed row like LargeFilePushManager's DataTable rows."""
    return sanitize_for_mongo(
        {
            "row_index": row_index,
            "found_folder": row["found_folder"],
            "file_name": row["file_name"],
            "content": {k: _cell(v) for k, v in row.items() if k not in _ROW_FIELDS},
        }
    )


def _get_batch(rows_queue, max_items: int) -> list:
    """Block for one message, then take whatever else is already queued."""
    batch = [rows_queue.get()]
    inner = getattr(rows_queue, "_queue", None)
    while inner is not None and batch[-1] is not None and len(batch) < max_items:
        try:
            batch.append(inner.get(block=False))
        except queue_module.Empty:
            break
    return batch


class StreamPushManager:
    """Push parsed rows to the short-term database while the parser runs.

    Reads the parser's queue output (``ConvertingTask.consume()``), one queue
    per (parser, file type), and streams each row as soon as it arrives. A
    source file is recorded in the push cache only once its file_complete is
    stored, so the CSV push in api_update skips it but resends anything that
    did not finish here.
    """

    def __init__(
        self,
        database_manager: ShortTermDatabaseUploader,
        read_batch: int = 1000,
    ):
        self.database_manager = database_manager
        self.read_batch = read_batch

    async def _drain(self, key, rows_queue, stream, executor):
        parser_name, file_type = key
        table = get_table_name(file_type, parser_name)
        loop = asyncio.get_running_loop()
        next_row_index = {}
        rows = 0
        while True:
            batch = await loop.run_in_executor(
                executor, _get_batch, rows_queue, self.read_batch
            )
            for message in batch:
                if message is None:
                    Logger.info("Streamed %d rows to %s", rows, table)
                    return
                file_name = message["file_name"]
                if message.get("file_complete") == "true":
                    await stream.end_file(
                        table, file_name, int(message["total_expected_records"])
                    )
                    continue
                row_index = next_row_index.get(file_name, 0)
                next_row_index[file_name] = row_index + 1
                await stream.add_row(table, row_to_item(message, row_index))
                rows += 1

    def push(self, queues: dict, local_cache: CacheState):
        """Stream every queue until its end-of-stream sentinel.

        Args:
            queues: {(parser name, file type): ParsedRowsQueue}
            local_cache: push cache; streamed files are recorded in it
        """
        if not queues:
            return None
        executor = ThreadPoolExecutor(
            max_workers=len(queues), thread_name_prefix="stream-push"
        )

        async def _push(stream):
            await asyncio.gather(
                *(
                    self._drain(key, rows_queue, stream, executor)
                    for key, rows_queue in queues.items()
                )
            )

        try:
            stream = self.database_manager.stream_rows(
                _push, on_file_stored=local_cache.mark_file_streamed
            )
        finally:
            executor.shutdown(wait=False, cancel_futures=True)
        Logger.info(
            "Streaming push done: %d files stored, %d failed",
            len(stream.stored_files),
            len(stream.failed_files),
        )
        if stream.failed_files:
            Logger.error(
                "Files left for the CSV push to resend: %s", stream.failed_files
            )
        return stream
