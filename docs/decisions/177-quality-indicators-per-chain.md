# 177: Compute scraper and parser quality one chain at a time

**Decision (Sefi, 2026-11-15):** `compute_scraper_quality` and `compute_parser_quality` load one chain's status (its scraper JSON and its parser JSONs), reduce it to that chain's metrics and release it before the next chain. They used to load every chain's raw JSON and pydantic model first and compute afterwards. The output is unchanged.

**Why:** on 2026-11-15 the data-fetcher was OOM-killed (exit 137, 4G limit) inside `write_quality_indicators()` in both the scrape job and the publish job, so nothing reached Kaggle. Peak memory was about 5 to 6 times the total status JSON size, and status files pile up across scrape runs until a publish cleans them. Computing metrics should be the cheapest step, so raising the memory limit was rejected. With per-chain processing, peak memory follows the largest chain: 1,154 MB down to 300 MB (scraper) and 1,298 MB down to 298 MB (parser) on the Nov 13 status × 4.

**Until:** permanent. A new indicator that reads status files should follow the same per-chain pattern. `managers/tests/test_quality_indicators_memory.py` checks this for these two.
