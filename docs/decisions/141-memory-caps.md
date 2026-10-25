# 141: Memory controls for the scrape pipeline

**Decision (2026-10-25):** `scraping` reads `NUM_OF_PARSING_PROCESSES`, `SCRAPE_QUEUE_MAX_FILES`, `PARSE_QUEUE_MAX_FILES` and `PARSED_ROWS_QUEUE_MAX`, and compose reads `NUM_OF_PROCESSES` and `DATA_FETCHER_MEMORY_LIMIT`. Unset values keep the old behaviour. `template.yml` sets 5 / 3 / 8 / 4 / 2000 / 4G, each overridable by a repository variable of the same name. Requires il-supermarket-scraper 1.0.18 and israeli-supermarket-parsers#121.

The prod scrape stays pinned to v0.0.11 (decision 142). The caps alone do not keep the pipeline inside 4G.

**Evidence:** a load test of the data-fetcher image with the prod compose limits (4G, 2 CPUs). Workload: SHUFERSAL, RAMI_LEVY, VICTORY_NEW_SOURCE, YOHANANOF, TIV_TAAM, HAZI_HINAM, YAYNO_BITAN_AND_CARREFOUR, `PRICE_FULL_FILE,PROMO_FULL_FILE`, `LIMIT=150`, Mongo output.

| Settings (scrapers / parsers / scrape cap / parse cap / rows cap) | Result |
|---|---|
| 5 / 5 / unbounded | Kernel OOM-killed the scraper's queue Manager after 77 s (1.15 GB RSS). Every router then failed and the run hung. Same failure as prod runs 4189 and 40109084395. |
| 5 / 3 / 8 / 4 / 2000 | OOM kill after about 2 min. |
| 3 / 2 / 3 / 2 / 2000 | OOM kill (the scraper Manager, about 860 MB, and a parse worker, about 970 MB). The run then hung in `StreamPushManager.push`. |

With caps, the memory is held per process, not in the queues. A parse worker holds a whole extracted file plus its parse, 0.5 to 1.2 GB on the largest chains, and keeps that peak afterwards. The scraper's Manager keeps its peak too. A full queue makes each scraper worker hold its in-flight downloads instead.

**Constraints found:**
- `SCRAPE_QUEUE_MAX_FILES` must be 0 or at least 3. After the parser's router reads the end-of-stream marker, it puts the marker back, and the scraper closes the queue twice more (`_scrape_one`, `_close_file_outputs`). A smaller cap blocks the last `put` forever. Reproduced with cap 2 (hangs) vs 3 (passes).
- When a parse worker is killed, its parsed-rows queue never gets an end-of-stream marker, so `StreamPushManager` waits forever.

**Until:** the libraries stop holding whole files in memory across processes (queue file paths, or parse from a stream), or the container gets more memory. Then rerun the load test above and remove `image_tag` from `_prod_scrape.yml`.
