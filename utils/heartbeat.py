"""
Heartbeat utility for tracking operation execution status.
Writes and reads a JSON file with operation statuses for health monitoring.

The health monitor thread (#192) runs the health check inside the process
every minute, writes the result to `health_status.json` for `healthcheck.py`,
and exits the process when nothing has made progress for too long, so Docker
restarts a hung fetcher instead of only marking it unhealthy.
"""

import glob
import json
import os
import threading
import time
from datetime import datetime
from typing import Callable, Dict, Iterable, Optional
from utils.logging_config import Logger

HEALTH_STATUS_FILE = "health_status.json"
# The monitor writes every 60 s; a status file older than this means it
# stopped. Shared by healthcheck.py and the API's /service_health (#204).
HEALTH_STATUS_MAX_AGE_SECONDS = 300


def stall_exit_minutes_from_env(default: int = 120) -> int:
    """FETCHER_STALL_EXIT_MINUTES: minutes without progress before the process
    exits. 0 keeps the health monitor writing the status, but never exits."""
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


class HeartbeatManager:
    """
    Manages heartbeat status file for health monitoring.

    Tracks individual operations and their execution status,
    allowing health checks to verify the process is running correctly.
    """

    def __init__(self, heartbeat_path: str):
        """
        Initialize the HeartbeatManager.

        Args:
            heartbeat_path: Path to the heartbeat JSON file
        """
        self.heartbeat_path = heartbeat_path
        os.makedirs(os.path.dirname(heartbeat_path), exist_ok=True)

    def _read_heartbeat(self) -> Dict:
        """
        Read the current heartbeat file.

        Returns:
            Dict containing heartbeat data, or empty dict if file doesn't exist
        """
        if os.path.exists(self.heartbeat_path):
            try:
                with open(self.heartbeat_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                Logger.error(f"Error reading heartbeat file: {e}")
                return {}
        return {}

    def _write_heartbeat(self, data: Dict):
        """
        Write heartbeat data to file.

        Args:
            data: Dictionary to write to heartbeat file
        """
        try:
            with open(self.heartbeat_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            Logger.error(f"Error writing heartbeat file: {e}")

    def reset(self):
        """
        Start a fresh heartbeat for a new process, so operations a previous
        container left running or failed don't count against this one.
        """
        self._write_heartbeat(
            {
                "last_update": datetime.now().isoformat(),
                "current_operation": None,
                "operations": {},
            }
        )

    def start_operation(self, operation: str):
        """
        Mark an operation as started.

        Args:
            operation: Name of the operation being started
        """
        data = self._read_heartbeat()
        data["last_update"] = datetime.now().isoformat()
        data["current_operation"] = operation

        if "operations" not in data:
            data["operations"] = {}

        data["operations"][operation] = {
            "status": "running",
            "started_at": datetime.now().isoformat(),
            "completed_at": None,
            "error": None,
        }

        self._write_heartbeat(data)
        Logger.info(f"Heartbeat: Started operation '{operation}'")

    def complete_operation(
        self, operation: str, success: bool = True, error: Optional[str] = None
    ):
        """
        Mark an operation as completed.

        Args:
            operation: Name of the operation being completed
            success: Whether the operation succeeded
            error: Error message if operation failed
        """
        data = self._read_heartbeat()
        data["last_update"] = datetime.now().isoformat()
        data["current_operation"] = None

        if "operations" not in data:
            data["operations"] = {}

        if operation in data["operations"]:
            data["operations"][operation]["status"] = "success" if success else "failed"
            data["operations"][operation]["completed_at"] = datetime.now().isoformat()
            if error:
                data["operations"][operation]["error"] = str(error)
        else:
            # Operation wasn't started properly, create entry
            data["operations"][operation] = {
                "status": "success" if success else "failed",
                "started_at": datetime.now().isoformat(),
                "completed_at": datetime.now().isoformat(),
                "error": str(error) if error else None,
            }

        self._write_heartbeat(data)
        Logger.info(
            f"Heartbeat: Completed operation '{operation}' with status '{data['operations'][operation]['status']}'"
        )

    def update_heartbeat(self):
        """
        Update the heartbeat timestamp without changing operation status.
        Useful for long-running operations to show the process is still alive.
        """
        data = self._read_heartbeat()
        data["last_update"] = datetime.now().isoformat()
        self._write_heartbeat(data)

    def start_health_monitor(self) -> Optional["HealthMonitor"]:
        """Start the health monitor thread for this heartbeat (#192). Settings
        come from HEALTHCHECK_MAX_AGE_SECONDS and FETCHER_STALL_EXIT_MINUTES."""
        app_data_path = os.path.dirname(self.heartbeat_path)
        monitor = HealthMonitor(
            app_data_path,
            max_age_seconds=int(os.environ.get("HEALTHCHECK_MAX_AGE_SECONDS", 3600)),
            stall_exit_minutes=stall_exit_minutes_from_env(),
        )
        monitor.start()
        return monitor

    @staticmethod
    def _as_aware(timestamp: str) -> datetime:
        """Parse an ISO timestamp; one without a timezone is in local time."""
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.astimezone()

    @staticmethod
    def last_status_update(status_paths: Iterable[str]) -> Optional[datetime]:
        """
        Latest `_metadata.last_modified` among the given status JSON files and the
        JSON files directly inside the given folders, or None if there is none.

        The scraper and parser status databases set it on every write, and the push
        cache sets it on every saved chunk, so it moves only while an operation makes
        progress. Only the most recently written file of each path is parsed.
        """
        latest = None
        for path in status_paths:
            candidates = (
                glob.glob(os.path.join(path, "*.json"))
                if os.path.isdir(path)
                else [path]
            )
            candidates = [c for c in candidates if os.path.isfile(c)]
            if not candidates:
                continue
            newest_file = max(candidates, key=os.path.getmtime)
            try:
                with open(newest_file, "r", encoding="utf-8") as f:
                    last_modified = json.load(f)["_metadata"]["last_modified"]
                changed = HeartbeatManager._as_aware(last_modified)
            except Exception:  # pylint: disable=broad-exception-caught
                continue
            if latest is None or changed > latest:
                latest = changed
        return latest

    @staticmethod
    def progress_status_paths(app_data_path: str) -> list:
        """The status files the operations update as they go: scraping and
        converting status, and the push cache."""
        return [
            os.path.join(app_data_path, "scraping_status"),
            os.path.join(app_data_path, "converting_status"),
            os.path.join(app_data_path, ".push_cache"),
        ]

    @staticmethod
    def last_progress(heartbeat: Dict, status_paths: Iterable[str]) -> datetime:
        """When the process last made progress: the heartbeat's `last_update`,
        or a status file's `_metadata.last_modified` if that is newer.

        Raises if the heartbeat has no parsable `last_update`.
        """
        last = HeartbeatManager._as_aware(heartbeat["last_update"])
        status_update = HeartbeatManager.last_status_update(status_paths)
        if status_update is not None and status_update > last:
            last = status_update
        return last

    @staticmethod
    def check_health(
        heartbeat_path: str,
        max_age_seconds: int = 300,
        status_paths: Optional[Iterable[str]] = None,
    ) -> tuple[bool, str]:
        """
        Check if the heartbeat indicates a healthy process.

        Args:
            heartbeat_path: Path to the heartbeat JSON file
            max_age_seconds: Maximum seconds without progress before considering unhealthy
            status_paths: Status JSON files or folders whose `_metadata.last_modified`
                counts as progress, along with the heartbeat's own last_update

        Returns:
            Tuple of (is_healthy, message)
        """
        if not os.path.exists(heartbeat_path):
            return False, "Heartbeat file does not exist"

        try:
            with open(heartbeat_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            return False, f"Error reading heartbeat file: {e}"

        # Check if heartbeat file has required fields
        if "last_update" not in data:
            return False, "Heartbeat file missing 'last_update' field"

        # Check how long since the process last made progress
        try:
            last_progress = HeartbeatManager.last_progress(data, status_paths or [])
        except Exception as e:
            return False, f"Error parsing last_update timestamp: {e}"
        idle_seconds = (datetime.now().astimezone() - last_progress).total_seconds()
        if idle_seconds > max_age_seconds:
            return (
                False,
                f"No progress for {idle_seconds:.0f}s "
                f"(current operation: {data.get('current_operation')})",
            )

        # Check for failed operations
        operations = data.get("operations", {})
        failed_ops = [
            op for op, status in operations.items() if status.get("status") == "failed"
        ]

        if failed_ops:
            errors = [operations[op].get("error", "Unknown error") for op in failed_ops]
            return (
                False,
                f"Operations failed: {', '.join(failed_ops)}. Errors: {'; '.join(errors)}",
            )

        # Check if there's a long-running operation
        current_op = data.get("current_operation")
        if current_op and current_op in operations:
            op_data = operations[current_op]
            if op_data.get("status") == "running":
                try:
                    started_at = datetime.fromisoformat(op_data["started_at"])
                    running_time = (datetime.now() - started_at).total_seconds()
                    # This is just informational, not a failure condition
                    return (
                        True,
                        f"Healthy. Currently running: {current_op} (for {running_time:.0f}s)",
                    )
                except Exception:
                    pass

        return True, "Healthy"


class HealthMonitor:  # pylint: disable=too-many-instance-attributes
    """Every `check_every_seconds`, runs the health check, writes the result to
    `health_status.json`, and exits the process after `stall_exit_minutes`
    without progress.

    Progress is what the health check counts: the heartbeat's `last_update`, or
    a scraping / converting status file or the push cache being written. A
    failed operation is written as unhealthy but never exits: the DAG already
    logs and skips it (#122).
    """

    def __init__(
        self,
        app_data_path: str,
        max_age_seconds: int,
        stall_exit_minutes: int,
        check_every_seconds: float = 60,
        exit_process: Callable[[int], None] = os._exit,
    ):
        self.heartbeat_path = os.path.join(app_data_path, "heartbeat.json")
        self.health_status_path = os.path.join(app_data_path, HEALTH_STATUS_FILE)
        self.status_paths = HeartbeatManager.progress_status_paths(app_data_path)
        self.max_age_seconds = max_age_seconds
        self.stall_seconds = stall_exit_minutes * 60
        self.check_every_seconds = check_every_seconds
        # os._exit, not sys.exit: SystemExit raised in this thread would end
        # only the thread, and a hung process may hold locks a clean exit waits on.
        self.exit_process = exit_process
        self.started_at = datetime.now().astimezone()

    def idle_seconds(self) -> float:
        """Seconds since the last progress. When the heartbeat can't be read,
        the monitor's own start counts as the last progress."""
        try:
            with open(self.heartbeat_path, "r", encoding="utf-8") as f:
                heartbeat = json.load(f)
            last_progress = HeartbeatManager.last_progress(heartbeat, self.status_paths)
        except Exception:  # pylint: disable=broad-exception-caught
            last_progress = self.started_at
        # Progress from before this process started (an earlier container's
        # files) counts as the start.
        last_progress = max(last_progress, self.started_at)
        return (datetime.now().astimezone() - last_progress).total_seconds()

    def check_once(self) -> bool:
        """Write the health status; exit the process if it has stalled.
        Returns True when it called exit."""
        # Step 1: the same check healthcheck.py used to run itself.
        healthy, message = HeartbeatManager.check_health(
            self.heartbeat_path, self.max_age_seconds, self.status_paths
        )
        idle = self.idle_seconds()

        # Step 2: write it down for healthcheck.py.
        self._write_status(healthy, message, idle)

        # Step 3: stalled for too long. Say so loudly (CRITICAL also goes to
        # Discord), then exit so Docker restarts the container.
        if self.stall_seconds and idle >= self.stall_seconds:
            Logger.critical(
                "No progress for %.0f minutes (%s). "
                "Exiting so the container restarts (#192).",
                idle / 60,
                message,
            )
            self.exit_process(1)
            return True  # pylint: disable=unreachable
        return False

    def _write_status(self, healthy: bool, message: str, idle: float):
        """Write the result atomically, so healthcheck.py never reads half a file."""
        status = {
            "healthy": healthy,
            "message": message,
            "idle_seconds": round(idle),
            "checked_at": datetime.now().astimezone().isoformat(),
        }
        tmp_path = self.health_status_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False)
        os.replace(tmp_path, self.health_status_path)

    def _run(self):
        """The monitor loop: check now, then every interval. A failed check is
        logged and retried next time."""
        while True:
            try:
                if self.check_once():
                    return
            except Exception as e:  # pylint: disable=broad-exception-caught
                Logger.error("Health monitor check failed: %s", e)
            time.sleep(self.check_every_seconds)

    def start(self) -> threading.Thread:
        """Start the monitor in a daemon thread, so it never keeps the process alive."""
        thread = threading.Thread(target=self._run, name="health-monitor", daemon=True)
        thread.start()
        if self.stall_seconds:
            Logger.info(
                "Health monitor on: exit after %d minutes without progress",
                self.stall_seconds // 60,
            )
        else:
            Logger.info("Health monitor on, never exits (FETCHER_STALL_EXIT_MINUTES=0)")
        return thread

    @staticmethod
    def read_report(app_data_path: str, max_age_seconds: float) -> Dict:
        """The fetcher's health for readers outside the process (#204):
        `healthcheck.py` through `read_status`, and the API's /service_health.

        Returns the monitor's last verdict (`healthy`, `message`, `checked_at`)
        and what each operation did (`current_operation`, `operations`, from
        `heartbeat.json`). Unhealthy when the status file is missing,
        unreadable, or older than `max_age_seconds` (the monitor stopped
        writing it); `checked_at` is None when it can't be read.
        """
        report = {
            "healthy": False,
            "message": "",
            "checked_at": None,
            "current_operation": None,
            "operations": {},
        }

        # Step 1: the monitor's last verdict, from health_status.json.
        path = os.path.join(app_data_path, HEALTH_STATUS_FILE)
        try:
            with open(path, "r", encoding="utf-8") as f:
                status = json.load(f)
            checked_at = HeartbeatManager._as_aware(  # pylint: disable=protected-access
                status["checked_at"]
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            report["message"] = f"No readable health status at {path}: {e}"
            checked_at = None

        # Step 2: a stale file means the monitor thread (or the process) is gone.
        if checked_at is not None:
            report["checked_at"] = checked_at.isoformat()
            age = (datetime.now().astimezone() - checked_at).total_seconds()
            if age > max_age_seconds:
                report["message"] = f"Health status not updated for {age:.0f}s"
            else:
                report["healthy"] = bool(status.get("healthy"))
                report["message"] = str(status.get("message", ""))

        # Step 3: what each operation did, from the heartbeat the DAG writes.
        heartbeat_path = os.path.join(app_data_path, "heartbeat.json")
        if os.path.exists(heartbeat_path):
            manager = HeartbeatManager(heartbeat_path)
            heartbeat = manager._read_heartbeat()  # pylint: disable=protected-access
            if not isinstance(heartbeat, dict):
                heartbeat = {}
            report["current_operation"] = heartbeat.get("current_operation")
            report["operations"] = heartbeat.get("operations", {})
        return report

    @staticmethod
    def read_status(app_data_path: str, max_age_seconds: float) -> tuple[bool, str]:
        """For healthcheck.py: the monitor's last verdict, as `read_report`
        gives it."""
        report = HealthMonitor.read_report(app_data_path, max_age_seconds)
        return report["healthy"], report["message"]
