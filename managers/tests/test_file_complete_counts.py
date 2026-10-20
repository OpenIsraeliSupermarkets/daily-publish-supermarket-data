"""file_complete counts the rows actually sent for each source file (#110)."""

import os
from collections import Counter

import numpy as np
import pandas as pd

from managers.cache_manager import CacheState
from managers.large_file_push_manager import LargeFilePushManager


class RecordingDb:
    """Collects pushed items."""

    def __init__(self):
        self.items = []

    def _insert_to_destinations(self, _table_name, items):
        self.items.extend(items)


# chunk_size=3 gives chunks of rows [0-2] [3-5] [6-8] [9-11] [12-14] [15-17] [18]
SOURCES = [
    ("inside.xml", 2),  # rows 0-1: inside the first chunk
    ("spanning.xml", 8),  # rows 2-9: spans 4 chunks
    ("mid.xml", 4),  # rows 10-13: starts mid-chunk, spans 2 chunks
    ("tail.xml", 5),  # rows 14-18: spans 3 chunks, completed after the loop
]


def _write_csv(folder):
    # Like the parser's CSV writer: values repeated within a source file are
    # masked with NaN and restored by ffill.
    rows = []
    for source, count in SOURCES:
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


def test_total_expected_records_matches_rows_sent(tmp_path):
    """In one chunk, across 3+ chunks, from mid-chunk, and after the loop."""
    file = _write_csv(str(tmp_path))
    db = RecordingDb()
    LargeFilePushManager(str(tmp_path), db, chunk_size=3).process_file(
        file, CacheState({})
    )

    sent = Counter(i["file_name"] for i in db.items if "file_complete" not in i)
    expected = {
        i["file_name"]: i["total_expected_records"]
        for i in db.items
        if "file_complete" in i
    }
    completes = [i["file_name"] for i in db.items if "file_complete" in i]

    assert completes == [source for source, _ in SOURCES]
    assert expected == dict(SOURCES)
    assert dict(sent) == dict(SOURCES)

    # every row is sent exactly once
    row_indexes = [i["row_index"] for i in db.items if "file_complete" not in i]
    assert row_indexes == list(range(sum(count for _, count in SOURCES)))
