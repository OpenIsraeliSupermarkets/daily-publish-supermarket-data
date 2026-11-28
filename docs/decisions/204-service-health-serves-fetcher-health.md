# 204: `/service_health` serves the fetcher's health, and the system test waits on it

**Context.** In persistent mode `local_test.sh` slept a fixed 120 s before
checking that the loop had published and cleaned up. When publishing took
longer, Step 8.5 failed without saying why. `/service_health` always answered
`healthy` and said nothing about the fetcher.

**Decision (Sefi, 2026-11-28).** Expose the fetcher's existing health check
through the API, read from the files it already writes, so it works for every
short-term and long-term setup and touches no database.

1. `HealthMonitor` (the fetcher's monitor thread, #192) stays the only place
   health is decided. `HealthMonitor.read_report` returns its last verdict
   from `health_status.json` plus each operation from `heartbeat.json`.
   `read_status`, which `healthcheck.py` uses, is a thin wrapper over it, and
   both use `HEALTH_STATUS_MAX_AGE_SECONDS` from `utils/heartbeat.py`, so
   Docker and the API can't disagree.
2. `/service_health` adds a `fetcher` section. `status` stays the API's own
   liveness: in prod the fetcher is a one-shot job, so between scrapes its
   health reads "not updated" while the API is fine. A malformed file reads as
   an unhealthy fetcher, never as an error of the endpoint.
3. Compose mounts `app_data` into `raw-data-api` read-only.
4. `local_test.sh` has one polling helper, `wait_until`, used by every wait.
   Persistent mode runs `system_tests/fetcher_published.py` in the API
   container until `publishing` and `clean_all_source_data` are both
   `success` (at most 3 minutes). It stops at once when the fetcher is
   unhealthy, which already covers a failed operation and its error.

**Rejected:** a fixed extra minute (#206), polling the fetcher's logs or a
Docker health log line, and a Mongo `fetcher_status` collection (Mongo is only
one of the short-term setups).

**Not changed:** `healthcheck.py`'s behaviour, the stall exit, and
`pipeline_health.json` (data quality per run, read by the quality gate).
