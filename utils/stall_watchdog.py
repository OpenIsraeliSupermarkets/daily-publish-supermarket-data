"""Exit the fetcher when it stops making progress, so Docker restarts it (#192).

A hung fetcher raises nothing, so neither the DAG nor #190's exit-on-failure
notices it. The healthcheck marks the container unhealthy, but Docker does not
restart an unhealthy container. This watchdog runs the healthcheck's progress
test inside the process and exits when nothing has moved for too long.
"""

import json
import os
import threading
import time
from datetime import datetime
from typing import Callable, Optional

from utils.heartbeat import HeartbeatManager
from utils.logging_config import Logger


def stall_exit_minutes_from_env(default: int = 120) -> int:
    """FETCHER_STALL_EXIT_MINUTES: minutes without progress before the process
    exits. 0 turns the watchdog off."""
    raw = os.environ.get("FETCHER_STALL_EXIT_MINUTES", "").strip()
    if raw == "":
        return default
    try:
        minutes = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"FETCHER_STALL_EXIT_MINUTES must be an integer, but got {raw!r}"
        ) from exc
    if minutes < 0:
        raise ValueError(f"FETCHER_STALL_EXIT_MINUTES must be >= 0, but got {minutes}")
    return minutes


class StallWatchdog:
    """Checks progress every `check_every_seconds` and exits the process after
    `stall_minutes` without any.

    Progress is what the healthcheck counts: the heartbeat's `last_update`, or
    a scraping / converting status file or the push cache being written.
    Failed operations are not a stall: the DAG already logs and skips them
    (#122), so they never trigger an exit.
    """

    def __init__(
        self,
        app_data_path: str,
        stall_minutes: int,
        check_every_seconds: float = 60,
        exit_process: Callable[[int], None] = os._exit,
    ):
        self.heartbeat_path = os.path.join(app_data_path, "heartbeat.json")
        self.status_paths = HeartbeatManager.progress_status_paths(app_data_path)
        self.stall_seconds = stall_minutes * 60
        self.check_every_seconds = check_every_seconds
        # os._exit, not sys.exit: SystemExit raised in this thread would end
        # only the thread, and a hung process may hold locks a clean exit waits on.
        self.exit_process = exit_process
        self.started_at = datetime.now().astimezone()

    def _read_heartbeat(self) -> dict:
        """The heartbeat file's content, or {} when it is missing or unreadable."""
        try:
            with open(self.heartbeat_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:  # pylint: disable=broad-exception-caught
            return {}

    def idle_seconds(self) -> tuple[float, Optional[str]]:
        """Seconds since the last progress, and the current operation.

        Before the first heartbeat is written (or when it can't be read), the
        watchdog's own start counts as the last progress.
        """
        heartbeat = self._read_heartbeat()
        try:
            last_progress = HeartbeatManager.last_progress(heartbeat, self.status_paths)
        except Exception:  # pylint: disable=broad-exception-caught
            last_progress = self.started_at
        # A heartbeat left by an earlier container must not count as a stall.
        last_progress = max(last_progress, self.started_at)
        idle = (datetime.now().astimezone() - last_progress).total_seconds()
        return idle, heartbeat.get("current_operation")

    def check_once(self) -> bool:
        """Exit the process if it has stalled. Returns True when it called exit."""
        # Step 1: how long since anything moved.
        idle, operation = self.idle_seconds()
        if idle < self.stall_seconds:
            return False

        # Step 2: stalled. Say so loudly (CRITICAL also goes to Discord), then exit.
        Logger.critical(
            "No progress for %.0f minutes (current operation: %s). "
            "Exiting so the container restarts (#192).",
            idle / 60,
            operation,
        )
        self.exit_process(1)
        return True

    def _run(self):
        """The watchdog loop. A failed check is logged and retried next time."""
        while True:
            time.sleep(self.check_every_seconds)
            try:
                if self.check_once():
                    return
            except Exception as e:  # pylint: disable=broad-exception-caught
                Logger.error("Stall watchdog check failed: %s", e)

    def start(self) -> threading.Thread:
        """Start the watchdog in a daemon thread, so it never keeps the process alive."""
        thread = threading.Thread(target=self._run, name="stall-watchdog", daemon=True)
        thread.start()
        Logger.info(
            "Stall watchdog on: exit after %d minutes without progress",
            self.stall_seconds // 60,
        )
        return thread


def start_stall_watchdog(app_data_path: str) -> Optional[StallWatchdog]:
    """Start the watchdog from FETCHER_STALL_EXIT_MINUTES; None when it is 0."""
    minutes = stall_exit_minutes_from_env()
    if minutes == 0:
        Logger.info("Stall watchdog off (FETCHER_STALL_EXIT_MINUTES=0)")
        return None
    watchdog = StallWatchdog(app_data_path, minutes)
    watchdog.start()
    return watchdog
