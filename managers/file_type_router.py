import threading

from il_supermarket_parsers.utils.loading_utils import (
    filename_to_file_type_and_chain_id,
)
from il_supermarket_scarper.utils.files.file_output import (
    InMemoryQueueHandler,
    QueueFileOutput,
)
from utils import Logger


def _file_type(file_name, fallback):
    """The type the parser's queue loader detects, from the name's prefix
    (as in il_supermarket_parsers' create_dumpfile_from_queue_message)."""
    prefix = file_name.split(".")[0].split("-")[0]
    try:
        return filename_to_file_type_and_chain_id(prefix)[0].name
    except ValueError:
        # the parser skips it with a warning, as it would without the router
        return fallback


class FileTypeRouter:
    """Split each scraper queue into one queue per file type.

    The parser runs one job per (chain, file type), and every job of a chain
    reads the chain's single scraper queue, putting back files of other types.
    When the matching job is not running at that moment, the file is put back
    once, comes back, and is skipped for good, so files are lost (#114). With
    one queue per (chain, file type), each job only sees its own files.
    """

    def __init__(self, scraper_queues: dict, file_types: list, storage_path: str):
        self.file_types = list(file_types)
        self._sources = scraper_queues
        self.queues = {
            file_type: {
                chain: QueueFileOutput(
                    InMemoryQueueHandler(f"{chain}_{file_type}"),
                    storage_path=storage_path,
                )
                for chain in scraper_queues
            }
            for file_type in self.file_types
        }
        self._threads = []

    def _route(self, chain):
        source = self._sources[chain].queue_handler._queue
        routed = 0
        while True:
            message = source.get()
            if message is None:
                break
            file_type = _file_type(message["file_name"], self.file_types[0])
            if file_type not in self.queues:
                file_type = self.file_types[0]
            self.queues[file_type][chain].queue_handler._queue.put(message)
            routed += 1
        for by_chain in self.queues.values():
            by_chain[chain].close_sync()
        Logger.info("Routed %d files of %s", routed, chain)

    def start(self):
        """Start one routing thread per chain."""
        for chain in self._sources:
            thread = threading.Thread(
                target=self._route, args=(chain,), name=f"route-{chain}", daemon=True
            )
            thread.start()
            self._threads.append(thread)

    def join(self):
        """Wait until every scraper queue has ended."""
        for thread in self._threads:
            thread.join()
