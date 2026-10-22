"""Consumes wiki.recentchange from Redpanda, validates events and writes them to DuckDB.
Invalid events go to a dead-letter topic. Offsets are committed only after the DuckDB
write succeeds (at-least-once) and inserts are idempotent on event_id (so effectively once)."""
import os
import time

import duckdb
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from src.models import parse_event

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "wiki.recentchange")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "wiki.recentchange.dlq")
DB_PATH = os.getenv("DB_PATH", "data/stream.duckdb")
BATCH_SIZE = 200
FLUSH_SECONDS = 5

COLUMNS = [
    "event_id", "rc_id", "event_type", "wiki", "title", "user_name", "is_bot",
    "is_minor", "namespace", "length_old", "length_new", "comment", "event_ts",
    "kafka_partition", "kafka_offset",
]

DDL = """
CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR PRIMARY KEY,
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
    ingested_at TIMESTAMP DEFAULT current_timestamp
)
"""


def write_batch(batch):
    """Open DuckDB just for this write, so other processes can read between flushes."""
    con = duckdb.connect(DB_PATH)
    try:
        con.execute(DDL)
        if not batch:
            return
        placeholders = ", ".join("?" for _ in COLUMNS)
        sql = f"INSERT OR IGNORE INTO raw_events ({', '.join(COLUMNS)}) VALUES ({placeholders})"
        con.executemany(sql, [[row[c] for c in COLUMNS] for row in batch])
    finally:
        con.close()


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
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    write_batch([])  # create the table if it doesn't exist yet

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
