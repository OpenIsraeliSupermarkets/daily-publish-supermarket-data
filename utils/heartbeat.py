"""
Heartbeat utility for tracking operation execution status.
Writes and reads a JSON file with operation statuses for health monitoring.
"""

import glob
import json
import os
from datetime import datetime
from typing import Dict, Iterable, Optional
from utils import Logger


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
            last_progress = HeartbeatManager._as_aware(data["last_update"])
        except Exception as e:
            return False, f"Error parsing last_update timestamp: {e}"
        status_update = HeartbeatManager.last_status_update(status_paths or [])
        if status_update is not None and status_update > last_progress:
            last_progress = status_update
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
