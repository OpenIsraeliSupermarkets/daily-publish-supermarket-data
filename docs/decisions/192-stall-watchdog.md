# 192: Exit the fetcher when it stops making progress

**Decision (Sefi, 2026-11-21, "Yes the watchdog"; reshaped 2026-11-22):** a hung fetcher raises nothing, so #190's exit-on-failure never fires. The healthcheck marks the container unhealthy (`No progress for 13000s` on 2026-11-21; about 51 h in the Oct 20 hang), but Docker does not restart an unhealthy container. Now the health check lives in the heartbeat code:

1. `main.py` calls `publisher.heartbeat.start_health_monitor()`. It starts a daemon thread (`HealthMonitor` in `utils/heartbeat.py`) that, every minute:
   1. runs the health check (`HeartbeatManager.check_health`): heartbeat, scraping / converting status files and the push cache as progress, and failed operations;
   2. writes the result to `$APP_DATA_PATH/health_status.json` (`healthy`, `message`, `idle_seconds`, `checked_at`), atomically;
   3. after `FETCHER_STALL_EXIT_MINUTES` (default 120, twice the prod healthcheck's 60 min) without progress, logs CRITICAL (Discord) and calls `os._exit(1)`. `os._exit` because `SystemExit` in a thread ends only the thread, and a hung process may hold locks a clean exit waits on. `main.py` is PID 1, so the container stops and `restart: unless-stopped` starts it again; the push cache and status files resume the work.
2. `healthcheck.py` only reads `health_status.json`. It is unhealthy when the file says so, or when the file is missing or older than 5 minutes (the monitor or the process is gone).
3. Failed operations are reported as unhealthy but never trigger an exit: the DAG already logs and skips them (#122).
4. Progress older than the process start (a heartbeat left by an earlier container) counts as the start.
5. `0` keeps writing the health status but never exits.

**Rejected:**
- An autoheal sidecar. It needs the Docker socket on the prod host, and the healthcheck is also unhealthy after a failed operation, which would restart the fetcher in a loop. No other prod container had a hang that a restart would have fixed.
- A deadline extended only by heartbeat writes. The heartbeat is written only when an operation starts or ends, so the limit would have to outlast a whole scrape (up to about 5.5 h) and hangs would be caught much later.
- A Docker HEALTHCHECK that kills `main.py`. It needs an init process as PID 1, and supermarket-pipeline's compose overrides the image's HEALTHCHECK.

**Default:** the longest quiet stretch seen is the end-of-day publishing to Kaggle, about 50 min in the prod publishing job (run #609, 2026-11-21). 120 min leaves room for that.

**Until:** permanent.
