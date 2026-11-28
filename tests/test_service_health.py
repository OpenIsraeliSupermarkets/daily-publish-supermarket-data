"""/service_health serves the fetcher's health files (#204), and the system
test's check reads it."""

import asyncio
import json
import os
from datetime import datetime

# api.py builds its telemetry client at import; the endpoint doesn't use it.
os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "test")

import api  # pylint: disable=wrong-import-position
from system_tests.fetcher_published import DONE, FAILED, WAITING, published_state


def _write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _service_health(app_data, monkeypatch):
    monkeypatch.setenv("APP_DATA_PATH", str(app_data))
    return asyncio.run(api.service_health_check()).model_dump()


def _fetcher_files(app_data, operations, healthy=True, message="Healthy"):
    now = datetime.now().astimezone().isoformat()
    _write(
        os.path.join(app_data, "heartbeat.json"),
        {
            "last_update": now,
            "current_operation": None,
            "operations": {
                op: {"status": status, "started_at": now, "completed_at": now}
                for op, status in operations.items()
            },
        },
    )
    _write(
        os.path.join(app_data, "health_status.json"),
        {"healthy": healthy, "message": message, "checked_at": now},
    )


# The endpoint


def test_service_health_includes_the_fetcher(tmp_path, monkeypatch):
    _fetcher_files(tmp_path, {"publishing": "success"})

    health = _service_health(tmp_path, monkeypatch)

    assert health["status"] == "healthy"
    assert health["fetcher"]["healthy"]
    assert health["fetcher"]["operations"]["publishing"]["status"] == "success"


def test_service_health_without_fetcher_files_keeps_the_api_healthy(
    tmp_path, monkeypatch
):
    health = _service_health(tmp_path, monkeypatch)

    assert health["status"] == "healthy"
    assert not health["fetcher"]["healthy"]
    assert health["fetcher"]["checked_at"] is None


def test_service_health_shows_a_failure_type_but_no_details(tmp_path, monkeypatch):
    # The heartbeat keeps the full error; the public endpoint shows only its type.
    _fetcher_files(
        tmp_path,
        {"publishing": "failed"},
        healthy=False,
        message="Operations failed: publishing. Errors: mongodb://user:secret@host",
    )
    heartbeat_path = os.path.join(tmp_path, "heartbeat.json")
    with open(heartbeat_path, encoding="utf-8") as f:
        heartbeat = json.load(f)
    heartbeat["operations"]["publishing"].update(
        error="mongodb://user:secret@host refused", error_type="ConnectionError"
    )
    _write(heartbeat_path, heartbeat)

    health = _service_health(tmp_path, monkeypatch)

    assert health["status"] == "healthy"
    assert not health["fetcher"]["healthy"]
    publishing = health["fetcher"]["operations"]["publishing"]
    assert publishing == {
        "status": "failed",
        "started_at": publishing["started_at"],
        "completed_at": publishing["completed_at"],
        "error_type": "ConnectionError",
    }
    assert "message" not in health["fetcher"]
    assert "secret" not in json.dumps(health)


def test_service_health_survives_a_malformed_heartbeat(tmp_path, monkeypatch):
    _fetcher_files(tmp_path, {})
    _write(
        os.path.join(tmp_path, "heartbeat.json"), {"operations": ["not", "a", "dict"]}
    )

    health = _service_health(tmp_path, monkeypatch)

    assert health["status"] == "healthy"
    assert not health["fetcher"]["healthy"]


# The system test's check


def _health(operations, healthy=True, checked_at="2026-11-28T15:00:00+02:00"):
    return {
        "status": "healthy",
        "fetcher": {
            "healthy": healthy,
            "message": "",
            "checked_at": checked_at,
            "operations": {op: {"status": s} for op, s in operations.items()},
        },
    }


def test_done_when_both_final_operations_succeeded():
    health = _health({"publishing": "success", "clean_all_source_data": "success"})
    assert published_state(health) == DONE


def test_waiting_while_cleanup_runs():
    health = _health({"publishing": "success", "clean_all_source_data": "running"})
    assert published_state(health) == WAITING


def test_waiting_before_the_monitor_wrote():
    assert published_state(_health({}, healthy=False, checked_at=None)) == WAITING


def test_failed_when_the_fetcher_is_unhealthy():
    assert published_state(_health({"publishing": "failed"}, healthy=False)) == FAILED
