# 201: check data-fetcher health every 5 s while it starts

**Context.** Scraper 1.1.3 (scrapers #241) made the system test's run take 23 s,
down from about 4.5 min. The compose healthcheck checked every 30 s, so the
container exited before its first check, and `local_test.sh`, which requires one
healthy report, failed a healthy run.

**Decision.** Add `start_interval: 5s` to the `data_processor` healthcheck in
`docker-compose.yml`. During `start_period` (60 s) Docker checks every 5 s, and
after that every 30 s as before.

**Why here.** The image has no `HEALTHCHECK`. Each consumer sets its own in its
compose file, and supermarket-pipeline's long-running fetcher isn't affected.
The test still requires a healthy report.
