"""One-off: move rows from the old data/stream.duckdb into data/raw/ as Parquet."""
import os
import sys

import duckdb

SRC = "data/stream.duckdb"
if not os.path.exists(SRC):
    sys.exit("Nothing to migrate: data/stream.duckdb not found.")

os.makedirs("data/raw", exist_ok=True)
con = duckdb.connect(SRC, read_only=True)
n = con.execute("SELECT count(*) FROM raw_events").fetchall()[0][0]
con.execute("""
    COPY (SELECT event_id, rc_id, event_type, wiki, title, user_name, is_bot, is_minor,
                 namespace, length_old, length_new, comment, event_ts,
                 kafka_partition, kafka_offset, ingested_at
          FROM raw_events)
    TO 'data/raw/part-migrated.parquet' (FORMAT PARQUET)
""")
con.close()
print(f"Migrated {n} rows to data/raw/part-migrated.parquet")
