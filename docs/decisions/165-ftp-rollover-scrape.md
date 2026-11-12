# 165: Late-evening scrape for the Cerberus FTP chains

**Decision (Sefi, 2026-11-12):** `_prod_scrape_ftp_rollover.yml` scrapes RAMI_LEVY and YOHANANOF daily at 20:30 UTC (22:30 IST / 23:30 IDT), with the same pinned image as the main scrape. `template.yml` takes an optional `enabled_scrapers` input for this, and other workflows still use the `ENABLED_SCRAPERS` secret.

**Why:** the Cerberus FTP removes the previous day's files at about 01:13 Israel time (seen on 2026-11-03 and 2026-11-12). Prod runs only about 3 scrape iterations a day, so the 17:00–23:59 files waited for an after-midnight run. On 2026-11-11 that lost 279 Rami Levy and 19 Yohananof files.

**Trade-off:** there is one runner, so if a full scrape is running at 20:30 UTC this job waits for it, and the job also comes before the 21:17 UTC publish in the queue. Once the main scrape runs often enough to cover the evening, retire this workflow.
