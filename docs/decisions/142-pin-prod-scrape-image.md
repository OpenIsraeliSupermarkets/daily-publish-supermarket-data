# 142: Pin the prod scrape cron to v0.0.11

**Decision (Sefi, 2026-10-25):** `_prod_scrape.yml` runs `erlichsefi/data-fetcher:v0.0.11`, the last release before the streaming pipeline (#117), through the new `image_tag` input of `template.yml`. Other workflows keep the latest release.

**Why:** from v0.0.12, prod scrape runs on home-server lose a multiprocessing Manager mid-run (unbounded in-memory queues under the 4G limit). They then hang for hours (run 40109084395) or get killed (run 4189, exit 137), which blocks the single prod runner.

**Until:** the memory caps in #141 (with israeli-supermarket-scarpers#205 and israeli-supermarket-parsers#120) are released. Then remove `image_tag` from `_prod_scrape.yml`.
