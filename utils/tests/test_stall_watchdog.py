"""The fetcher exits when nothing has made progress for too long (#192)."""

import json
import os
from datetime import datetime, timedelta

import pytest

from utils.stall_watchdog import StallWatchdog, stall_exit_minutes_from_env


def _write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _heartbeat(app_data, last_update, operation="scraping", status="running"):
    _write(
        os.path.join(app_data, "heartbeat.json"),
        {
            "last_update": last_update.isoformat(),
            "current_operation": operation,
            "operations": {operation: {"status": status}},
        },
    )


def _status(app_data, folder, last_modified):
    _write(
        os.path.join(app_data, folder, "status.json"),
        {"events": [], "_metadata": {"last_modified": last_modified.isoformat()}},
    )


def _watchdog(app_data, stall_minutes=120, started_minutes_ago=600):
    """A watchdog that records exits instead of exiting, started long ago so
    only the files decide."""
    exits = []
    watchdog = StallWatchdog(str(app_data), stall_minutes, exit_process=exits.append)
    watchdog.started_at = datetime.now().astimezone() - timedelta(
        minutes=started_minutes_ago
    )
    return watchdog, exits


def test_a_stale_heartbeat_exits_the_process(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=200))
    watchdog, exits = _watchdog(tmp_path)

    assert watchdog.check_once()
    assert exits == [1]


def test_a_recent_heartbeat_does_not_exit(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=10))
    watchdog, exits = _watchdog(tmp_path)

    assert not watchdog.check_once()
    assert exits == []


def test_a_recent_status_file_counts_as_progress(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=200))
    _status(tmp_path, "converting_status", datetime.now() - timedelta(minutes=5))
    watchdog, exits = _watchdog(tmp_path)

    assert not watchdog.check_once()
    assert exits == []


def test_a_failed_operation_alone_does_not_exit(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=10), "publishing", "failed")
    watchdog, exits = _watchdog(tmp_path)

    assert not watchdog.check_once()


def test_an_old_heartbeat_from_before_the_start_does_not_exit(tmp_path):
    # Left by an earlier container; this process started 5 minutes ago.
    _heartbeat(tmp_path, datetime.now() - timedelta(days=2))
    watchdog, exits = _watchdog(tmp_path, started_minutes_ago=5)

    assert not watchdog.check_once()


def test_no_heartbeat_yet_counts_from_the_start(tmp_path):
    watchdog, exits = _watchdog(tmp_path, started_minutes_ago=5)
    assert not watchdog.check_once()

    watchdog, exits = _watchdog(tmp_path, started_minutes_ago=200)
    assert watchdog.check_once()
    assert exits == [1]


@pytest.mark.parametrize(
    "value, expected", [(None, 120), ("", 120), ("0", 0), ("45", 45)]
)
def test_the_stall_minutes_come_from_the_environment(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("FETCHER_STALL_EXIT_MINUTES", raising=False)
    else:
        monkeypatch.setenv("FETCHER_STALL_EXIT_MINUTES", value)
    assert stall_exit_minutes_from_env() == expected


@pytest.mark.parametrize("value", ["soon", "-1"])
def test_a_bad_stall_minutes_value_is_refused(monkeypatch, value):
    monkeypatch.setenv("FETCHER_STALL_EXIT_MINUTES", value)
    with pytest.raises(ValueError):
        stall_exit_minutes_from_env()
