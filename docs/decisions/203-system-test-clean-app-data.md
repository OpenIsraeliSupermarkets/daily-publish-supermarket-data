# 203: the system test empties `$APP_DATA_PATH` itself before each run

**Context.** Compose binds the `app_data` volume to `$APP_DATA_PATH`, so the
container's dumps, outputs, status and push cache live directly in it. Step 4
of `local_test.sh` deleted `$APP_DATA_PATH/app_data`, a folder nothing writes
to. A run that stopped early left its files behind, and the next test started
on top of them. On #202 a test with LIMIT=10 parsed 20 Bareket files, and the
leftover task's incomplete status failed `ScraperStatusOutput` validation.

**Decision.** Step 4 deletes everything inside `$APP_DATA_PATH`, hidden files
included, and keeps the folder for the bind mount. It deletes from an
`alpine` container, because the data-fetcher writes as root.

**Guards.** Step 4 refuses to run, and fails the test, when `APP_DATA_PATH` is
empty or `/`, or when the runner is `home-server`. Prod uses the same
`_work/.../app_data` path there.

**Why this is enough on a timeout.** Any run, however it ends, is cleaned at
the start of the next one, so a stopped run needs no clean step of its own.
