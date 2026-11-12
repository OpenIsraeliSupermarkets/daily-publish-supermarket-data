# 161: Recompute the quality indicators when publishing

**Decision (Sefi, 2026-11-12):** the publishing operation (`_upload_to_kaggle`) recomputes `pipeline_health.json`, `scraper_quality.json` and `parser_quality.json` from the status files and CSVs on disk before composing the dataset. The converting step still writes them too.

**Why:** the prod scrape job runs the pinned `v0.0.11` image (decision 142), and it is the job that wrote the indicators. The publishing job only uploaded them, so prod published v0.0.11's numbers. On 2026-11-11 that showed 3,195 "downloaded but never parsed" files, and the real number was 0. Publishing runs the latest release, so the published numbers now come from the current indicator code.

**Until:** permanent. It is harmless once the scrape pin is removed.
