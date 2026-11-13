# 142: Pin the prod scrape cron to v0.0.11

**Decision (Sefi, 2026-10-25):** `_prod_scrape.yml` runs `erlichsefi/data-fetcher:v0.0.11`, the last release before the streaming pipeline (#117), through the new `image_tag` input of `template.yml`. Other workflows keep the latest release.

**Why:** from v0.0.12, prod scrape runs on home-server lose a multiprocessing Manager mid-run (unbounded in-memory queues under the 4G limit). They then hang for hours (run 40109084395) or get killed (run 4189, exit 137), which blocks the single prod runner.

**Until:** a release that includes #141 (back to the disk pipeline, decision 141) is published. Then remove `image_tag` from `_prod_scrape.yml`. Removing it earlier would run the latest release, which still streams.

**Lifted (Sefi, 2026-11-12, #171):** #141 shipped in v0.0.17 (2026-10-25), so `image_tag` is removed from `_prod_scrape.yml` and the prod scrape runs the latest release. Rollback: restore `image_tag: "v0.0.11"` in `_prod_scrape.yml`.
