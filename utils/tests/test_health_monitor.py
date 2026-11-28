"""The health monitor writes the health status for healthcheck.py and exits the
fetcher when nothing has made progress for too long (#192)."""

import json
import os
from datetime import datetime, timedelta

import pytest

from utils.heartbeat import HealthMonitor, stall_exit_minutes_from_env


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
            "operations": {
                operation: {"status": status, "started_at": last_update.isoformat()}
            },
        },
    )


def _status(app_data, folder, last_modified):
    _write(
        os.path.join(app_data, folder, "status.json"),
        {"events": [], "_metadata": {"last_modified": last_modified.isoformat()}},
    )


def _monitor(app_data, stall_minutes=120, started_minutes_ago=600):
    """A monitor that records exits instead of exiting, started long ago so
    only the files decide."""
    exits = []
    monitor = HealthMonitor(
        str(app_data),
        max_age_seconds=3600,
        stall_exit_minutes=stall_minutes,
        exit_process=exits.append,
    )
    monitor.started_at = datetime.now().astimezone() - timedelta(
        minutes=started_minutes_ago
    )
    return monitor, exits


def _health_status(app_data):
    with open(os.path.join(app_data, "health_status.json"), encoding="utf-8") as f:
        return json.load(f)


# Exiting on a stall


def test_a_stale_heartbeat_exits_the_process(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=200))
    monitor, exits = _monitor(tmp_path)

    assert monitor.check_once()
    assert exits == [1]
    assert _health_status(tmp_path)["healthy"] is False


def test_a_recent_heartbeat_does_not_exit(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=10))
    monitor, exits = _monitor(tmp_path)

    assert not monitor.check_once()
    assert exits == []
    assert _health_status(tmp_path)["healthy"] is True


def test_a_recent_status_file_counts_as_progress(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=200))
    _status(tmp_path, "converting_status", datetime.now() - timedelta(minutes=5))
    monitor, exits = _monitor(tmp_path)

    assert not monitor.check_once()
    assert exits == []


def test_a_failed_operation_is_unhealthy_but_does_not_exit(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=10), "publishing", "failed")
    monitor, exits = _monitor(tmp_path)

    assert not monitor.check_once()
    assert exits == []
    assert _health_status(tmp_path)["healthy"] is False


def test_an_old_heartbeat_from_before_the_start_does_not_exit(tmp_path):
    # Left by an earlier container; this process started 5 minutes ago.
    _heartbeat(tmp_path, datetime.now() - timedelta(days=2))
    monitor, exits = _monitor(tmp_path, started_minutes_ago=5)

    assert not monitor.check_once()
    assert exits == []


def test_zero_writes_the_status_but_never_exits(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=200))
    monitor, exits = _monitor(tmp_path, stall_minutes=0)

    assert not monitor.check_once()
    assert exits == []
    assert _health_status(tmp_path)["healthy"] is False


# Reading the status in healthcheck.py


def test_healthcheck_reads_the_monitor_result(tmp_path):
    _heartbeat(tmp_path, datetime.now() - timedelta(minutes=10))
    monitor, _ = _monitor(tmp_path)
    monitor.check_once()

    healthy, _ = HealthMonitor.read_status(str(tmp_path), max_age_seconds=300)
    assert healthy


def test_healthcheck_is_unhealthy_without_a_status_file(tmp_path):
    healthy, message = HealthMonitor.read_status(str(tmp_path), max_age_seconds=300)
    assert not healthy
    assert "No readable health status" in message


def test_healthcheck_is_unhealthy_when_the_monitor_stopped_writing(tmp_path):
    _write(
        os.path.join(tmp_path, "health_status.json"),
        {
            "healthy": True,
            "message": "Healthy",
            "checked_at": (
                datetime.now().astimezone() - timedelta(minutes=10)
            ).isoformat(),
        },
    )
    healthy, message = HealthMonitor.read_status(str(tmp_path), max_age_seconds=300)
    assert not healthy
    assert "not updated" in message


# The setting


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


# The report served by the API's /service_health (#204)


def _monitor_status(app_data, healthy=True, message="Healthy", age_seconds=0):
    """health_status.json as the monitor writes it, age_seconds ago."""
    checked_at = datetime.now().astimezone() - timedelta(seconds=age_seconds)
    _write(
        os.path.join(app_data, "health_status.json"),
        {"healthy": healthy, "message": message, "checked_at": checked_at.isoformat()},
    )


def test_report_has_the_verdict_and_the_operations(tmp_path):
    _heartbeat(tmp_path, datetime.now(), operation="publishing", status="success")
    _monitor_status(tmp_path)

    report = HealthMonitor.read_report(str(tmp_path), max_age_seconds=300)

    assert report["healthy"]
    assert report["message"] == "Healthy"
    assert report["checked_at"]
    assert report["current_operation"] == "publishing"
    assert report["operations"]["publishing"]["status"] == "success"


def test_report_without_files_is_unhealthy_and_unchecked(tmp_path):
    report = HealthMonitor.read_report(str(tmp_path), max_age_seconds=300)

    assert not report["healthy"]
    assert "No readable health status" in report["message"]
    assert report["checked_at"] is None
    assert report["operations"] == {}


def test_report_of_a_failed_operation_carries_its_error(tmp_path):
    # The monitor turns a failed operation into unhealthy with its error.
    _heartbeat(tmp_path, datetime.now(), operation="publishing", status="failed")
    _monitor_status(
        tmp_path, healthy=False, message="Operations failed: publishing. Errors: 429"
    )

    report = HealthMonitor.read_report(str(tmp_path), max_age_seconds=300)

    assert not report["healthy"]
    assert "429" in report["message"]
    assert report["operations"]["publishing"]["status"] == "failed"


def test_report_of_a_stale_status_is_unhealthy_but_keeps_the_operations(tmp_path):
    # Prod between scrapes: the fetcher exited, its last operations stay readable.
    _heartbeat(tmp_path, datetime.now(), operation="scraping", status="success")
    _monitor_status(tmp_path, age_seconds=900)

    report = HealthMonitor.read_report(str(tmp_path), max_age_seconds=300)

    assert not report["healthy"]
    assert "not updated" in report["message"]
    assert report["operations"]["scraping"]["status"] == "success"


def test_read_status_gives_the_report_verdict(tmp_path):
    _monitor_status(tmp_path, healthy=False, message="No progress for 4000s")

    report = HealthMonitor.read_report(str(tmp_path), max_age_seconds=300)
    healthy, message = HealthMonitor.read_status(str(tmp_path), max_age_seconds=300)

    assert (healthy, message) == (report["healthy"], report["message"])
