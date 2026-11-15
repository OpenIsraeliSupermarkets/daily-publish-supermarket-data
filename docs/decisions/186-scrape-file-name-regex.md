# 186: `SCRAPE_FILE_NAME_REGEX` restricts the scrape to the files it names

**Decision (Sefi, 2026-11-15):** the data-fetcher reads `SCRAPE_FILE_NAME_REGEX` and passes it to the scraper as `ScarpingTask(file_name_regex=...)`, which matches file names with `re.search`. Unset or empty, nothing changes. It lands on its own, before the main-vs-PR usage comparison (#184), so that comparison can make main scrape exactly the files the PR run scraped (Sefi).

**Why not `re_download`:** it adds the named files on top of `LIMIT` and doesn't restrict to them, so main would scrape more files than the PR run.

**Until:** permanent.
