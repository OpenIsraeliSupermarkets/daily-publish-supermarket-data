# 167: Retry Kaggle 429s with backoff

**Decision (Sefi, 2026-11-12):** every `kagglehub` download and upload in `KaggleUploader` (`remotes/long_term/kaggle.py`) goes through `retry_on_rate_limit`, a thin wrapper over the generic `utils.retry.retry_on_429`. On HTTP 429 it waits and retries; any other error is raised on the first try, and after the last attempt the original 429 is raised so the operation still fails loudly.

Defaults: 6 attempts, waits of 30s, 60s, 120s, 240s, 300s (doubling, capped at 300s), so about 12.5 minutes in total before giving up. Kaggle's `Retry-After` header wins when it is sent (still capped). Override with `KAGGLE_RETRY_ATTEMPTS`, `KAGGLE_RETRY_BASE_DELAY`, `KAGGLE_RETRY_MAX_DELAY`.

**Why:** Kaggle throttles per account, and the CI system test uses the same account as prod. On 2026-11-12 three back-to-back re-runs of the #160 system test hit `429 Too Many Requests` on `DownloadDataset` and failed the publishing operation on the first throttled call. The prod publish runs the same code and would fail the same way.

**Not covered:** `was_updated_in_last` and `list_files` already swallow errors and return a default; they are left as they are.
