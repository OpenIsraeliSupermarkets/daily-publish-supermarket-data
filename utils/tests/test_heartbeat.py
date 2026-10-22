"""Heartbeat stays fresh while busy and flags an operation stuck for too long (#129)."""

import json
import threading
import time
from datetime import datetime, timedelta

from utils import HeartbeatManager


def _write(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _running_since(path, operation, started_at):
    _write(
        path,
        {
            "last_update": datetime.now().isoformat(),
            "current_operation": operation,
            "operations": {
                operation: {
                    "status": "running",
                    "started_at": started_at.isoformat(),
                    "completed_at": None,
                    "error": None,
                }
            },
        },
    )


def test_beating_keeps_a_long_operation_fresh(tmp_path):
    path = str(tmp_path / "heartbeat.json")
    heartbeat = HeartbeatManager(path)
    heartbeat.start_operation("scraping")
    with open(path, encoding="utf-8") as f:
        started = json.load(f)["last_update"]

    heartbeat.start_beating(interval_seconds=0.05)
    try:
        time.sleep(0.3)
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    finally:
        heartbeat.stop_beating()

    assert data["last_update"] > started
    assert data["current_operation"] == "scraping"
    assert data["operations"]["scraping"]["status"] == "running"


def test_start_beating_twice_keeps_one_thread(tmp_path):
    heartbeat = HeartbeatManager(str(tmp_path / "heartbeat.json"))
    heartbeat.start_beating(interval_seconds=10)
    try:
        heartbeat.start_beating(interval_seconds=10)
        beaters = [t for t in threading.enumerate() if t.name == "heartbeat"]
    finally:
        heartbeat.stop_beating()
    assert len(beaters) == 1


def test_beating_does_not_lose_operation_transitions(tmp_path):
    path = str(tmp_path / "heartbeat.json")
    heartbeat = HeartbeatManager(path)
    heartbeat.start_beating(interval_seconds=0)
    try:
        for i in range(50):
            heartbeat.start_operation(f"op{i}")
            heartbeat.complete_operation(f"op{i}", success=True)
    finally:
        heartbeat.stop_beating()

    with open(path, encoding="utf-8") as f:
        operations = json.load(f)["operations"]
    assert all(operations[f"op{i}"]["status"] == "success" for i in range(50))


def test_operation_over_the_limit_is_unhealthy(tmp_path):
    path = str(tmp_path / "heartbeat.json")
    _running_since(path, "scraping", datetime.now() - timedelta(hours=7))

    healthy, message = HeartbeatManager.check_health(
        path, max_age_seconds=300, max_operation_seconds=6 * 3600
    )

    assert not healthy
    assert "scraping" in message


def test_long_operation_within_the_limit_is_healthy(tmp_path):
    path = str(tmp_path / "heartbeat.json")
    _running_since(path, "api_update", datetime.now() - timedelta(hours=2))

    healthy, message = HeartbeatManager.check_health(
        path, max_age_seconds=300, max_operation_seconds=6 * 3600
    )

    assert healthy
    assert "api_update" in message


def test_operation_limit_zero_disables_the_check(tmp_path):
    path = str(tmp_path / "heartbeat.json")
    _running_since(path, "scraping", datetime.now() - timedelta(days=2))

    healthy, _ = HeartbeatManager.check_health(
        path, max_age_seconds=300, max_operation_seconds=0
    )

    assert healthy


def test_stale_heartbeat_is_unhealthy(tmp_path):
    path = str(tmp_path / "heartbeat.json")
    _write(
        path,
        {
            "last_update": (datetime.now() - timedelta(minutes=10)).isoformat(),
            "current_operation": None,
            "operations": {},
        },
    )

    healthy, message = HeartbeatManager.check_health(path, max_age_seconds=300)

    assert not healthy
    assert "stale" in message
