"""Health is judged by when the status files last changed, not by heartbeat age alone (#129)."""

import json
import os
from datetime import datetime, timedelta, timezone

from utils import HeartbeatManager


def _write(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _heartbeat(path, last_update, current_operation="scraping"):
    _write(
        path,
        {
            "last_update": last_update.isoformat(),
            "current_operation": current_operation,
            "operations": {
                current_operation: {
                    "status": "running",
                    "started_at": last_update.isoformat(),
                    "completed_at": None,
                    "error": None,
                }
            },
        },
    )


def _status(path, last_modified):
    # the scraper / parser JsonDataBase shape: collections plus _metadata
    _write(
        path,
        {"events": [], "_metadata": {"last_modified": last_modified.isoformat()}},
    )


def test_busy_operation_with_recent_status_is_healthy(tmp_path):
    heartbeat = str(tmp_path / "heartbeat.json")
    _heartbeat(heartbeat, datetime.now() - timedelta(hours=2))
    status_dir = tmp_path / "scraping_status"
    status_dir.mkdir()
    # scraper writes Israel-time aware stamps
    _status(
        str(status_dir / "shufersal.json"),
        datetime.now(timezone(timedelta(hours=3))) - timedelta(minutes=5),
    )

    healthy, message = HeartbeatManager.check_health(
        heartbeat, max_age_seconds=3600, status_paths=[str(status_dir)]
    )

    assert healthy, message


def test_no_status_change_for_too_long_is_unhealthy(tmp_path):
    # Oct 20: every chain done scraping, then the process hung inside the operation
    heartbeat = str(tmp_path / "heartbeat.json")
    _heartbeat(heartbeat, datetime.now() - timedelta(hours=3))
    status_dir = tmp_path / "scraping_status"
    status_dir.mkdir()
    _status(
        str(status_dir / "shuk_ahir.json"),
        datetime.now(timezone.utc) - timedelta(hours=2),
    )

    healthy, message = HeartbeatManager.check_health(
        heartbeat, max_age_seconds=3600, status_paths=[str(status_dir)]
    )

    assert not healthy
    assert "scraping" in message


def test_push_cache_stamp_counts_as_progress(tmp_path):
    heartbeat = str(tmp_path / "heartbeat.json")
    _heartbeat(heartbeat, datetime.now() - timedelta(hours=4), "api_update")
    push_cache = str(tmp_path / ".push_cache")
    _write(
        push_cache,
        {
            "last_pushed": {"price_full_file_a.csv": 10},
            "_metadata": {"last_modified": datetime.now().astimezone().isoformat()},
        },
    )

    healthy, message = HeartbeatManager.check_health(
        heartbeat, max_age_seconds=3600, status_paths=[push_cache]
    )

    assert healthy, message


def test_reads_the_most_recently_written_status_file(tmp_path):
    status_dir = tmp_path / "converting_status"
    status_dir.mkdir()
    old, new = str(status_dir / "a.json"), str(status_dir / "b.json")
    recent = datetime.now(timezone.utc) - timedelta(minutes=1)
    _status(old, datetime.now(timezone.utc) - timedelta(days=1))
    _status(new, recent)
    os.utime(old, (1, 1))

    assert HeartbeatManager.last_status_update([str(status_dir)]) == recent


def test_missing_or_unstamped_status_is_ignored(tmp_path):
    status_dir = tmp_path / "scraping_status"
    status_dir.mkdir()
    _write(str(status_dir / "a.json"), {"events": []})

    assert (
        HeartbeatManager.last_status_update(
            [str(status_dir), str(tmp_path / "missing")]
        )
        is None
    )


def test_recent_heartbeat_without_status_files_is_healthy(tmp_path):
    # between DAG runs only the heartbeat moves
    heartbeat = str(tmp_path / "heartbeat.json")
    _heartbeat(heartbeat, datetime.now() - timedelta(minutes=20), "clean_dump_files")

    healthy, message = HeartbeatManager.check_health(
        heartbeat, max_age_seconds=3600, status_paths=[str(tmp_path / "missing")]
    )

    assert healthy, message
