"""One look at the fetcher's progress through the API's /service_health (#204).

local_test.sh runs it in persistent mode, inside the raw-data-api container,
through its `wait_until` helper, until the loop has published and cleaned up.

Exit codes:
    0: publishing and clean_all_source_data both succeeded (done)
    1: not there yet (keep waiting)
    2: the fetcher reports unhealthy (stop waiting, it failed)
"""

import json
import sys
import urllib.request

SERVICE_HEALTH_URL = "http://localhost:8000/service_health"
FINAL_OPERATIONS = ("publishing", "clean_all_source_data")

DONE, WAITING, FAILED = 0, 1, 2


def published_state(health: dict) -> int:
    """DONE, WAITING or FAILED for one /service_health response."""
    fetcher = health.get("fetcher") or {}

    # Step 1: the monitor hasn't written its first verdict yet.
    if not fetcher.get("checked_at"):
        return WAITING

    # Step 2: unhealthy covers a failed operation (its error is in `message`)
    # and a fetcher that stopped making progress.
    if not fetcher.get("healthy"):
        return FAILED

    # Step 3: done once both final operations succeeded.
    operations = fetcher.get("operations") or {}
    if all(
        (operations.get(op) or {}).get("status") == "success" for op in FINAL_OPERATIONS
    ):
        return DONE
    return WAITING


def main() -> int:
    """Ask the API once, print what it said, and exit with the state."""
    try:
        with urllib.request.urlopen(SERVICE_HEALTH_URL, timeout=10) as response:
            health = json.load(response)
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"/service_health not answering yet: {e}")
        return WAITING

    fetcher = health.get("fetcher") or {}
    print(
        f"fetcher: healthy={fetcher.get('healthy')} "
        f"current_operation={fetcher.get('current_operation')} "
        f"message={fetcher.get('message')!r}"
    )
    state = published_state(health)
    if state == FAILED:
        print(json.dumps(health, indent=2, ensure_ascii=False))
    return state


if __name__ == "__main__":
    sys.exit(main())
