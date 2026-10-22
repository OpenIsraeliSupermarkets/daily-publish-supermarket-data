"""
Heartbeat utility for tracking operation execution status.
Writes and reads a JSON file with operation statuses for health monitoring.
"""

import json
import os
import threading
from datetime import datetime
from typing import Dict, Optional
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
        # the beating thread and the DAG both read-modify-write the file
        self._lock = threading.RLock()
        self._stop_beating = threading.Event()
        self._beat_thread = None

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
        # write then rename, so a health check never reads a half-written file
        tmp_path = f"{self.heartbeat_path}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            os.replace(tmp_path, self.heartbeat_path)
        except Exception as e:
            Logger.error(f"Error writing heartbeat file: {e}")

    def start_operation(self, operation: str):
        """
        Mark an operation as started.

        Args:
            operation: Name of the operation being started
        """
        with self._lock:
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
        with self._lock:
            data = self._read_heartbeat()
            data["last_update"] = datetime.now().isoformat()
            data["current_operation"] = None

            if "operations" not in data:
                data["operations"] = {}

            if operation in data["operations"]:
                data["operations"][operation]["status"] = (
                    "success" if success else "failed"
                )
                data["operations"][operation][
                    "completed_at"
                ] = datetime.now().isoformat()
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
        with self._lock:
            data = self._read_heartbeat()
            data["last_update"] = datetime.now().isoformat()
            self._write_heartbeat(data)

    def start_beating(self, interval_seconds: float = 60):
        """
        Refresh last_update every interval_seconds from a daemon thread, so the
        heartbeat only goes stale when the process is dead or frozen, not busy.
        Calling it again while the thread is alive does nothing.
        """
        if self._beat_thread is not None and self._beat_thread.is_alive():
            return
        self._stop_beating.clear()

        def _beat():
            while not self._stop_beating.is_set():
                self.update_heartbeat()
                self._stop_beating.wait(interval_seconds)

        self._beat_thread = threading.Thread(
            target=_beat, name="heartbeat", daemon=True
        )
        self._beat_thread.start()

    def stop_beating(self):
        """Stop the thread started by start_beating."""
        self._stop_beating.set()
        if self._beat_thread is not None:
            self._beat_thread.join(timeout=5)
            self._beat_thread = None

    @staticmethod
    def check_health(
        heartbeat_path: str,
        max_age_seconds: int = 300,
        max_operation_seconds: int = 0,
    ) -> tuple[bool, str]:
        """
        Check if the heartbeat indicates a healthy process.

        Args:
            heartbeat_path: Path to the heartbeat JSON file
            max_age_seconds: Maximum age of heartbeat in seconds before considering unhealthy
            max_operation_seconds: Maximum time one operation may stay running before
                considering unhealthy (a live process stuck in an operation keeps
                beating); 0 disables the limit

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

        # Check heartbeat age
        try:
            last_update = datetime.fromisoformat(data["last_update"])
            age_seconds = (datetime.now() - last_update).total_seconds()

            if age_seconds > max_age_seconds:
                return (
                    False,
                    f"Heartbeat is stale (last update: {age_seconds:.0f}s ago)",
                )
        except Exception as e:
            return False, f"Error parsing last_update timestamp: {e}"

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
                except Exception:
                    running_time = None
                if (
                    running_time is not None
                    and 0 < max_operation_seconds < running_time
                ):
                    return (
                        False,
                        f"Operation '{current_op}' has been running for {running_time:.0f}s "
                        f"(limit {max_operation_seconds}s)",
                    )
                if running_time is not None:
                    return (
                        True,
                        f"Healthy. Currently running: {current_op} (for {running_time:.0f}s)",
                    )

        return True, "Healthy"
