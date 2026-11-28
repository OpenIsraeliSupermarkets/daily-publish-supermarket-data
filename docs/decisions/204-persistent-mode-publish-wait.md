# 204: persistent mode waits one more minute for the loop's publishing

**Context.** In persistent mode `local_test.sh` waits 2 minutes, then checks
health, the quality indicators the loop published and that `clean_all_source_data`
removed the dumps. On #202 the loop's publishing (the Kaggle upload) wasn't
done by then, so Step 8.5 failed with "dumps still exists".

**Decision (Sefi, 2026-11-28).** After the health checks, wait another fixed
60 seconds (Step 8.2.1), then run the quality gate and Step 8.5 as before.

**Why not wait on a signal.** Waiting for the loop's `Done publishing,...` log
line was proposed and turned down in favour of a simple fixed wait.

**On a timeout.** The step fails and the teardown stops the loop. The next
test's Step 4 empties `system_test_app_data` before it starts (#203), so a
timed-out run needs no clean step of its own.
