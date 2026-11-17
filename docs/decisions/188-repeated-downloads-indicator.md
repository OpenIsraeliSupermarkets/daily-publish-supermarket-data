# 188: Report repeated downloads (same sha256, not re-parsed) per chain

**Decision (Sefi, 2026-11-17):** the scraper section of `pipeline_health.json` reports, for each chain, how many download rows had `save_decision == rewrote_same`. That means the file was downloaded again even though the same bytes were already stored, so it was not sent to the parser. The report also gives that count's share of the chain's downloads. A chain whose share is above `QUALITY_SCRAPER_MAX_REPEATED_DOWNLOAD_SHARE` (default 0.2) is listed under `repeated_downloads_warnings` and logged. The idea came from Sefi after scrapers #233, where Shufersal re-downloaded every file on every run and no indicator showed it.

**Why report-only, not a gate indicator:** a scraper upgrade that changes listing identity (like scrapers #235) makes every file of that chain download once more, so a one-day spike is expected. The same is true of `download_published_lag`. Making it a gate row is one line in `INDICATORS` once its normal range is known.

**Until:** permanent; revisit promoting it to the gate after a few weeks of data.
