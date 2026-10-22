"""Consumes wiki.recentchange from Redpanda, validates events and writes micro-batches
as append-only Parquet files (the raw layer). Invalid events go to a dead-letter topic.
Offsets are committed only after the Parquet file is safely written (at-least-once);
duplicates after a replay are removed at read time, keyed on event_id."""
import os
import time
import uuid
from datetime import datetime, timezone

import duckdb
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from src.models import parse_event

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "wiki.recentchange")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "wiki.recentchange.dlq")
RAW_DIR = os.getenv("RAW_DIR", "data/raw")
BATCH_SIZE = 200
FLUSH_SECONDS = 5

COLUMNS = [
    "event_id", "rc_id", "event_type", "wiki", "title", "user_name", "is_bot",
    "is_minor", "namespace", "length_old", "length_new", "comment", "event_ts",
    "kafka_partition", "kafka_offset", "ingested_at",
]

BATCH_DDL = """
CREATE TABLE batch (
    event_id VARCHAR,
    rc_id BIGINT,
    event_type VARCHAR,
    wiki VARCHAR,
    title VARCHAR,
    user_name VARCHAR,
    is_bot BOOLEAN,
    is_minor BOOLEAN,
    namespace INTEGER,
    length_old INTEGER,
    length_new INTEGER,
    comment VARCHAR,
    event_ts TIMESTAMP,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    ingested_at TIMESTAMP
)
"""


def write_batch(batch, raw_dir: str = RAW_DIR):
    """Write one micro-batch as a Parquet file. Written to a temp name first and then
    renamed, so readers never see a half-written file."""
    if not batch:
        return None
    os.makedirs(raw_dir, exist_ok=True)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con = duckdb.connect(":memory:")
    try:
        con.execute(BATCH_DDL)
        placeholders = ", ".join("?" for _ in COLUMNS)
        con.executemany(
            f"INSERT INTO batch ({', '.join(COLUMNS)}) VALUES ({placeholders})",
            [[row.get(c, now) if c == "ingested_at" else row[c] for c in COLUMNS] for row in batch],
        )
        name = f"part-{now:%Y%m%dT%H%M%S%f}-{uuid.uuid4().hex[:8]}"
        tmp = os.path.join(raw_dir, f".{name}.tmp")
        final = os.path.join(raw_dir, f"{name}.parquet")
        con.execute(f"COPY batch TO '{tmp.replace(chr(92), '/')}' (FORMAT PARQUET)")
    finally:
        con.close()
    os.replace(tmp, final)
    return final


def flush(consumer, dlq, batch):
    if batch:
        write_batch(batch)
    dlq.flush()
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as exc:
        # Nothing consumed since the last commit - nothing to commit, not an error.
        if exc.args[0].code() != KafkaError._NO_OFFSET:
            raise


def main():
    os.makedirs(RAW_DIR, exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "duckdb-sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    dlq = Producer({"bootstrap.servers": BOOTSTRAP})

    batch, good, bad, dirty = [], 0, 0, False
    last_flush = time.time()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                else:
                    try:
                        batch.append(parse_event(msg.value(), msg.partition(), msg.offset()))
                    except ValueError as exc:
                        dlq.produce(DLQ_TOPIC, key=msg.key(), value=msg.value(),
                                    headers={"error": str(exc)})
                        dlq.poll(0)
                        bad += 1
                    dirty = True

            due = len(batch) >= BATCH_SIZE or (dirty and time.time() - last_flush >= FLUSH_SECONDS)
            if due:
                flush(consumer, dlq, batch)
                good += len(batch)
                print(f"Flushed {len(batch)} rows | total good={good} bad={bad}")
                batch, dirty, last_flush = [], False, time.time()
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        flush(consumer, dlq, batch)
        consumer.close()


if __name__ == "__main__":
    main()
