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

**Guards.** The system test uses its own folders,
`<runner workspace>/system_test_app_data` and `system_test_mongo_data`, set in
`system_test_pr.yml`. Prod uses `app_data` and `mongo_data` under the same
workspace, so the test can't empty prod's data whichever runner it lands on.
`runs-on: [self-hosted, ci]` remains the main protection. Step 4 also refuses
an empty `APP_DATA_PATH` or `/`, for local runs with the variable unset.

**Why this is enough on a timeout.** Any run, however it ends, is cleaned at
the start of the next one, so a stopped run needs no clean step of its own.
