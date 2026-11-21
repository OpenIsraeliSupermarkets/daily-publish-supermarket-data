# 190: Recreate the Kafka producer after failed acks, exit when that does not help

**Decision (Sefi, 2026-11-21, "solve all" on the prod triage):** after a Kafka broker restart the fetcher's idempotent producer was left with sequence numbers the broker no longer knew (`OutOfOrderSequenceNumber`), and no send was acked again. The fetcher kept running for weeks while every topic stayed empty. Now:

1. A batch push whose records are not acked stops the producer, starts a new one (new producer id, sequences from zero) and pushes once more. Rows acked the first time are sent again; consumers already tolerate that (#108).
2. A streamed file that failed gets a new producer before the CSV push resends it.
3. A push that fails on the new producer too is counted. After `KAFKA_MAX_CONSECUTIVE_FAILURES` (default 3) failed pushes in a row the process logs CRITICAL (Discord) and exits with code 1. Raising is not enough, because the DAG logs and skips a failed operation (#122). Docker restarts the container and the push cache resumes where it stopped.
4. Errors about the records themselves (message or record too large) are raised as before: a new producer would fail the same way, and they do not count toward the exit.

**Until:** permanent.
