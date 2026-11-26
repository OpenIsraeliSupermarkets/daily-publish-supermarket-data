"""Repeated file blocks: one dump's rows written twice into a CSV (#196).

An output CSV holds one block of rows per parsed dump. Only the first row of a
block carries the dump's ``file_name``, so every non-empty ``file_name`` cell
starts a block. A name that starts two blocks was written twice:

- **duplicated**: the parser recorded fewer distinct content hashes for it than
  it has blocks. The same bytes were parsed again, for example leftover dumps of
  a killed scrape. These rows are copies and must not be published.
- **republished**: every block has its own content hash in the parser's
  ``parsed_files`` registry. The chain published new content under the same
  name and the scraper downloaded it again. Allowed, and reported.
"""

from __future__ import annotations

import csv
import os
import sys
from collections import Counter
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple

from managers.indicators.status_files import load_status_json

SAMPLE_FILES = 20


# ---------------------------------------------------------------------------
# Reading: where each block starts, and the hashes the parser recorded
# ---------------------------------------------------------------------------


def block_starts(path: str) -> Iterator[Tuple[int, str]]:
    """(data row index, file_name) of every block in one CSV.

    Streams with ``csv.reader`` so memory stays at about one row (#182/#183).
    A CSV without a ``file_name`` column has no blocks.
    """
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except (StopIteration, csv.Error):
            return
        if "file_name" not in header:
            return
        index = header.index("file_name")
        for row_number, row in enumerate(reader):
            if len(row) > index and row[index]:
                yield row_number, row[index]


def parsed_hashes(converting_status_folder: str) -> Dict[str, Set[Optional[str]]]:
    """file_name -> content hashes in every parser status's ``parsed_files``.

    None stands for an entry written before the parser recorded hashes.
    """
    hashes: Dict[str, Set[Optional[str]]] = {}
    if not converting_status_folder or not os.path.isdir(converting_status_folder):
        return hashes
    for name in sorted(os.listdir(converting_status_folder)):
        if not name.endswith(".json"):
            continue
        raw = load_status_json(os.path.join(converting_status_folder, name)) or {}
        for row in raw.get("parsed_files") or []:
            if row.get("file_name"):
                hashes.setdefault(row["file_name"], set()).add(row.get("content_sha256"))
    return hashes


# ---------------------------------------------------------------------------
# Deciding: duplicated or republished
# ---------------------------------------------------------------------------


def classify(blocks: int, hashes: Set[Optional[str]]) -> str:
    """``republished`` when each block has its own known hash, else ``duplicated``."""
    distinct = {value for value in hashes if value}
    return "republished" if len(distinct) >= blocks else "duplicated"


def scan_csv(path: str, hashes: Dict[str, Set[Optional[str]]]) -> Dict[str, Any]:
    """Repeated names of one CSV, split into duplicated and republished."""
    counts = Counter(name for _, name in block_starts(path))
    duplicated: List[str] = []
    republished: List[str] = []
    for name, blocks in sorted(counts.items()):
        if blocks < 2:
            continue
        bucket = classify(blocks, hashes.get(name, set()))
        (duplicated if bucket == "duplicated" else republished).append(name)
    return {"duplicated": duplicated, "republished": republished}


# ---------------------------------------------------------------------------
# The pipeline_health.json section
# ---------------------------------------------------------------------------


class RepeatedFileBlocks:
    """Computes the ``repeated_file_blocks`` section."""

    section = "repeated_file_blocks"

    @staticmethod
    def compute(outputs_folder: str, converting_status_folder: str) -> Dict[str, Any]:
        """Totals over every output CSV, and the CSVs that have repeats."""
        hashes = parsed_hashes(converting_status_folder)
        by_csv: List[Dict[str, Any]] = []
        if outputs_folder and os.path.isdir(outputs_folder):
            csv.field_size_limit(sys.maxsize)
            for root, _, files in os.walk(outputs_folder):
                for csv_name in sorted(files):
                    if not csv_name.endswith(".csv"):
                        continue
                    found = scan_csv(os.path.join(root, csv_name), hashes)
                    if found["duplicated"] or found["republished"]:
                        by_csv.append(
                            {
                                "csv": csv_name,
                                "duplicated_files": len(found["duplicated"]),
                                "republished_files": len(found["republished"]),
                                "duplicated": found["duplicated"][:SAMPLE_FILES],
                                "republished": found["republished"][:SAMPLE_FILES],
                            }
                        )
        by_csv.sort(key=lambda row: (-row["duplicated_files"], row["csv"]))
        return {
            "duplicated_files": sum(row["duplicated_files"] for row in by_csv),
            "republished_files": sum(row["republished_files"] for row in by_csv),
            "by_csv": by_csv,
        }
