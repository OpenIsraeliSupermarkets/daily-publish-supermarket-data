# 122: A failed operation or a killed data-fetcher fails the job

**Decision (Sefi, 2026-11-15):** when the data-fetcher runs `OPERATION` once (all prod and CI workflows), it still runs every operation in the list. If any of them raised, it logs their names and exits with code 1. `template.yml` runs `docker compose up --exit-code-from data_processor`, so a non-zero exit fails the step and the job. That includes an OOM kill (137).

**Why:** `docker compose up` exited 0 whatever the container did. Publishing #576 (2026-10-22, upload failed) and #600 (2026-11-15, OOM-killed, see #177) were both green with nothing on Kaggle, and the scrape run before #600 was killed the same way, also green.

**Not changed:** the long-running mode (no `OPERATION`, the loop used by supermarket-pipeline's fetcher) still keeps going after a failed operation, because exiting there would stop the service. `clean_all_source_data` still runs after a failed `publishing`. `_upload_and_clean` already cleans in its `finally`, so skipping the DAG step alone would change nothing. Keeping the data after a failed upload is a separate choice: a re-run could publish it, but status files would keep piling up, and that pile-up is what drove the #177 OOM.

**Until:** permanent.
