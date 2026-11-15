"""Process memory readings for progress logs (#180)."""

from __future__ import annotations

import resource
import time
from contextlib import contextmanager
from typing import Iterator, Optional, Tuple

from utils.logging_config import Logger


def memory_mb() -> Tuple[Optional[int], int]:
    """(current RSS, peak RSS) of this process in MB.

    Reads /proc/self/status on Linux; elsewhere the current RSS is None and the
    peak comes from getrusage.
    """
    rss = peak = None
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1]) // 1024
                elif line.startswith("VmHWM:"):
                    peak = int(line.split()[1]) // 1024
    except OSError:
        pass
    if peak is None:
        # ru_maxrss is KB on Linux, bytes on macOS; only a rough fallback.
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024
    return rss, peak


@contextmanager
def log_step(name: str) -> Iterator[None]:
    """Log elapsed seconds and memory when ``name`` finishes (or raises)."""
    started = time.monotonic()
    try:
        yield
    finally:
        rss, peak = memory_mb()
        Logger.info(
            "Step %s: %.1fs rss=%sMB peak=%sMB",
            name,
            time.monotonic() - started,
            rss,
            peak,
        )
