# 184: The PR system test also runs main on the same files and fails on a significant CPU or memory increase

**Decision (Sefi, 2026-11-15):** beyond unit tests, every PR compares the data-fetcher's resource use on main against the PR, using the existing system test (`local_test.sh`), which runs the real Docker images and simulates production. A significant increase fails the PR from day one. This replaces the narrow approach of #180 / #181 (memory logging for one function). The same principle is to be rolled out to the project's other repos, each with its own issue.

**How (`.github/workflows/system_test_pr.yml`):**
- The PR's system test runs as before, with `PERF_OUT` set. `local_test.sh` then samples the `data-fetcher` container's cgroup every second while each operation runs. It records peak memory (counted the way `docker stats` counts it, without inactive page cache), CPU-seconds and wall time into `scrape.json` and `publish.json`. It also writes the list of files the scrape downloaded to `files.txt`.
- main (the PR's base SHA) is then built and run through the PR's `local_test.sh`, with `SCRAPE_FILE_NAME_REGEX` (#186) set to exactly those files, so both sides process the same data.
- If main can't scrape exactly the same files (a site stopped listing one, or the run failed), it retries up to 3 times and then fails the job (Sefi).
- The steps are scripts next to `local_test.sh` (Sefi): `perf_collect_files.sh` (called by `local_test.sh` after the scrape), `perf_run_main.sh` and `perf_compare.sh`. `perf_compare.sh` fails when, for either operation, peak memory is up more than 15% and more than 50 MB, or CPU is up more than 20% and more than 10 s. A metric that wasn't measured also fails. The table goes to the job summary.
- The main run skips the step 10 system tests (`SKIP_SYSTEM_TESTS`): the PR run already validated the data, and this halves the Kaggle reads.

**Not chosen:**
- Custom synthetic workloads: too much code (Sefi).
- Live-scraping both sides independently: they would see different files.
- Pinned Kaggle files: Kaggle holds only parsed CSVs, so scraping and parsing would not be exercised.
- The Shufersal fat-row CSV is not part of the input (Sefi).

**Bootstrap:** if main doesn't understand `SCRAPE_FILE_NAME_REGEX` yet (before #186 merges), the comparison is skipped with a notice.

**Cost:** about 10 more minutes per PR on the self-hosted CI runner, plus one more publish to the test Kaggle dataset.

**Until:** permanent. If false failures show up, widen the limits in `perf_compare.sh`. Making the system test a required check is a branch-protection setting.
