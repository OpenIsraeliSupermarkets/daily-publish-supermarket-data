# 141: Go back to the v0.0.11 disk pipeline

**Decision (2026-10-25):** the publisher runs `scraping` (to `dumps/` on disk), then `converting` (to CSV in `outputs/`), then `api_update` (pushes the CSVs), as in v0.0.11. The streaming path from #117 (in-memory queues from the scraper to the parser to Mongo) is no longer used. Requires il-supermarket-scraper 1.0.18 and il-supermarket-parser 1.0.14, both released.

Controls: `NUM_OF_PROCESSES` (scrapers, default 5), `NUM_OF_PARSING_PROCESSES` (parsers; unset means the same as `NUM_OF_PROCESSES`), and `DATA_FETCHER_MEMORY_LIMIT` (compose, default 4G). `template.yml` reads each one from a repository variable of the same name.

**Evidence:** a load test of the data-fetcher image on a VM about the size of [home-server](https://github.com/OpenIsraeliSupermarkets/runners-profiles/blob/main/home-server) (7.65 GiB RAM, 1 GiB swap, 2 CPUs). Workload: SHUFERSAL, RAMI_LEVY, VICTORY_NEW_SOURCE, YOHANANOF, TIV_TAAM, HAZI_HINAM, YAYNO_BITAN_AND_CARREFOUR, `PRICE_FULL_FILE,PROMO_FULL_FILE`, `LIMIT=150`. Memory is the container's anonymous memory (page cache excluded).

| Pipeline | Settings (scrapers / parsers / queue caps) | Limit | Result |
|---|---|---|---|
| Streaming | 5 / 5 / unbounded | 4G | Kernel OOM-killed the scraper's queue Manager after 77 s (1.15 GB). The run then hung, like prod runs 4189 and 40109084395. |
| Streaming | 5 / 3 / 8, 4, 2000 | 4G | OOM kill after about 2 min. |
| Streaming | 3 / 2 / 3, 2, 2000 | 4G | OOM kill, then a hang in `StreamPushManager.push`. |
| Streaming | 5 / 3 / 8, 4, 2000 | 7G | Reached 6.2 GiB, then broken pipes and the VM went down. |
| **Disk (this decision)** | 5 / 5 | 4G | Exit 0, no OOM, peak 1.4 GiB, 13.5 min. 7.1 GB of dumps and 2.3 GB of CSV on disk. |

Streaming holds memory per process, not in the queues: a parse worker keeps a whole extracted file plus its parse (0.5 to 1.2 GB on the largest chains), the scraper's Manager keeps its peak, and blocked scraper workers keep their downloads. On disk, each stage frees its memory before the next starts.

**Cost:** each stage waits for the previous one, so rows reach Mongo later than with streaming, and the run needs disk for the dumps until `clean_dump_files`.

**Bugs found in the streaming path:** israeli-supermarket-scarpers#207, #208, #209; israeli-supermarket-parsers#124, #125, #126; daily-publish-supermarket-data#147.

**Until:** the scraper hands off file paths on disk instead of file contents, with a byte budget checked before download (option 3 on #141). Then rerun the load test above with the streaming pipeline.
