# 173: Chain prod scrape runs instead of relying on the cron

**Decision (Sefi, 2026-11-12):** when a `_prod_scrape.yml` run succeeds, it dispatches the next run (`chain-next` job, on a GitHub-hosted runner). The `17 */3` cron stays as a fallback. The concurrency group `prod-scrape` (no cancel) allows one running and one waiting. Set the repository variable `PROD_SCRAPE_CHAIN=false` to stop the chain.

**Why:** GitHub started the cron only about 3.3 times a day (runs 4196–4255, 2026-10-25..11-12), with gaps of 2–8 h after each run, while the runner sat idle. Each run carried hours of backlog, and the run after midnight lost Rami Levy and Yohananof evening files to the Cerberus FTP's ~01:13 rollover (#165). Back-to-back runs keep each run small. This replaces the dedicated late-evening scrape proposed in #166.

**Trade-off:** the single runner is busy most of the day. Publishing and release deploys queue behind the current scrape, for at most one run's length. A failed run doesn't re-dispatch. The cron restarts the chain within about 3 h, or whenever GitHub gets to it.
