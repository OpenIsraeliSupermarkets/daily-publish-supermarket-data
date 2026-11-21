# 192: Exit the fetcher when it stops making progress

**Decision (Sefi, 2026-11-21, "Yes the watchdog"):** a hung fetcher raises nothing, so #190's exit-on-failure never fires. The healthcheck marks the container unhealthy (`No progress for 13000s` on 2026-11-21; about 51 h in the Oct 20 hang), but Docker does not restart an unhealthy container. Now:

1. `main.py` starts a daemon thread (`utils/stall_watchdog.py`). Every minute it runs the healthcheck's progress test: the heartbeat's `last_update`, or a scraping / converting status file or the push cache being written (`HeartbeatManager.last_progress`, shared with `healthcheck.py`).
2. After `FETCHER_STALL_EXIT_MINUTES` (default 120, twice the prod healthcheck's 60 min) without progress, it logs CRITICAL (Discord) with the current operation and calls `os._exit(1)`. `os._exit` because `SystemExit` in a thread ends only the thread, and a hung process may hold locks a clean exit waits on. `main.py` is PID 1, so the container stops and `restart: unless-stopped` starts it again; the push cache and status files resume the work.
3. Failed operations are not a stall and never trigger an exit: the DAG already logs and skips them (#122).
4. Progress older than the process start (a heartbeat left by an earlier container) counts as the start.
5. `0` turns the watchdog off.

**Rejected:** an autoheal sidecar. It needs the Docker socket on the prod host, and the healthcheck is also unhealthy after a failed operation, which would restart the fetcher in a loop. No other prod container had a hang that a restart would have fixed.

**Default:** the longest quiet stretch seen is the end-of-day publishing to Kaggle, about 50 min in the prod publishing job (run #609, 2026-11-21). 120 min leaves room for that.

**Until:** permanent.
