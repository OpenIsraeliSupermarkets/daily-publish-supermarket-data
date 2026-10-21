"""Measure the scraper-to-Kafka latency KPI.

For every data row in Kafka, the latency is the time the broker appended the
row minus the time the scraper first saw the row's source file (the ``saw``
event in the scraper status folder). Run it after a cycle, against a broker
whose topics use ``LogAppendTime`` (broker setting
``log.message.timestamp.type=LogAppendTime``); with ``CreateTime`` the
producer's send time is used instead, which hides time spent waiting for acks.

    python scripts/kpi_scraper_to_kafka.py \
        --scraping-status-folder app_data/scraping_status \
        --bootstrap localhost:9092 --out kpi.json
"""

import argparse
import asyncio
import datetime
import json
import os
import statistics

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from kafka import KafkaAdminClient

LOG_APPEND_TIME = 1


def _stem(file_name: str) -> str:
    return file_name.rsplit(".", 1)[0]


def load_saw_times(status_folder: str) -> dict:
    """Return {source file stem: first 'saw' time (epoch seconds)}."""
    saw = {}
    for name in os.listdir(status_folder):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(status_folder, name), encoding="utf-8") as f:
            status = json.load(f)
        for event in status.get("events", []):
            if event.get("status") != "saw":
                continue
            when = datetime.datetime.fromisoformat(
                event["system_timestamp"]
            ).timestamp()
            stem = _stem(event["file_name"])
            saw[stem] = min(when, saw.get(stem, when))
    return saw


async def read_rows(bootstrap: str, topics: list) -> tuple:
    """Return ({(file stem, row_index): append time}, timestamp types seen)."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        fetch_max_bytes=50 * 1024 * 1024,
        max_partition_fetch_bytes=20 * 1024 * 1024,
    )
    await consumer.start()
    rows, ts_types = {}, set()
    try:
        partitions = [
            TopicPartition(t, p)
            for t in topics
            for p in (consumer.partitions_for_topic(t) or [])
        ]
        if not partitions:
            await consumer._client.force_metadata_update()  # pylint: disable=protected-access
            partitions = [
                TopicPartition(t, p)
                for t in topics
                for p in (consumer.partitions_for_topic(t) or [])
            ]
        consumer.assign(partitions)
        await consumer.seek_to_beginning(*partitions)
        end = await consumer.end_offsets(partitions)
        remaining = {tp for tp in partitions if end[tp] > 0}
        while remaining:
            batch = await consumer.getmany(timeout_ms=2000, max_records=50000)
            for tp, messages in batch.items():
                for msg in messages:
                    ts_types.add(msg.timestamp_type)
                    value = json.loads(msg.value)
                    if "row_index" in value and "file_name" in value:
                        key = (_stem(value["file_name"]), value["row_index"])
                        if key not in rows:
                            rows[key] = msg.timestamp / 1000.0
                if messages and messages[-1].offset + 1 >= end[tp]:
                    remaining.discard(tp)
    finally:
        await consumer.stop()
    return rows, ts_types


def _pct(values: list, q: float) -> float:
    values = sorted(values)
    return values[min(len(values) - 1, int(round(q * (len(values) - 1))))]


def summarize(rows: dict, saw: dict) -> dict:
    """Latency statistics, in seconds, over every row with a known saw time."""
    latencies, missing = [], set()
    for (stem, _), appended in rows.items():
        if stem not in saw:
            missing.add(stem)
            continue
        latencies.append(appended - saw[stem])
    if not latencies:
        return {"rows": 0, "files_without_saw_event": len(missing)}
    files = {stem for stem, _ in rows if stem in saw}
    return {
        "rows": len(latencies),
        "files": len(files),
        "files_without_saw_event": len(missing),
        "latency_seconds": {
            "mean": round(statistics.fmean(latencies), 1),
            "p50": round(_pct(latencies, 0.50), 1),
            "p90": round(_pct(latencies, 0.90), 1),
            "p99": round(_pct(latencies, 0.99), 1),
            "max": round(max(latencies), 1),
            "min": round(min(latencies), 1),
        },
        "first_saw_to_last_row_seconds": round(
            max(rows[k] for k in rows if k[0] in saw) - min(saw[s] for s in files),
            1,
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scraping-status-folder", required=True)
    parser.add_argument(
        "--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--out", help="write the summary JSON here too")
    args = parser.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap)
    topics = [t for t in admin.list_topics() if not t.startswith("__")]
    admin.close()

    saw = load_saw_times(args.scraping_status_folder)
    rows, ts_types = asyncio.run(read_rows(args.bootstrap, topics))
    summary = summarize(rows, saw)
    summary["broker_log_append_time"] = ts_types == {LOG_APPEND_TIME}
    print(json.dumps(summary, indent=2))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)


if __name__ == "__main__":
    main()
