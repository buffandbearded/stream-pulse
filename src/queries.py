"""Read-side helpers: a connection over the raw Parquet files, and the Stage 4 aggregations.

The raw layer is append-only Parquet, so there are no file locks between the consumer
(writer) and any readers. Duplicates (possible after a consumer restart/replay) are removed
here with a window function keyed on event_id."""
import os

import duckdb

RAW_DIR = os.getenv("RAW_DIR", "data/raw")


def connect_read(raw_dir: str = RAW_DIR):
    """In-memory DuckDB with a deduplicated `raw_events` view over the Parquet files."""
    glob = os.path.join(raw_dir, "*.parquet").replace("\\", "/")
    con = duckdb.connect(":memory:")
    con.execute(f"""
        CREATE VIEW raw_events AS
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingested_at) AS rn
            FROM read_parquet('{glob}')
        ) WHERE rn = 1
    """)
    return con


# All windows are anchored on the newest event in the table, not on the wall clock,
# so the queries behave the same on live data and on replayed data.
EDITS_PER_MINUTE = """
SELECT date_trunc('minute', event_ts) AS minute,
       count(*)                       AS edits,
       count(*) FILTER (WHERE is_bot) AS bot_edits
FROM raw_events
WHERE event_ts >= (SELECT max(event_ts) FROM raw_events) - INTERVAL {minutes} MINUTE
GROUP BY 1
ORDER BY 1
"""

TOP_WIKIS = """
SELECT wiki, count(*) AS edits
FROM raw_events
WHERE event_ts >= (SELECT max(event_ts) FROM raw_events) - INTERVAL {minutes} MINUTE
GROUP BY 1
ORDER BY edits DESC
LIMIT {limit}
"""

BOT_VS_HUMAN = """
SELECT CASE WHEN is_bot THEN 'bot' ELSE 'human' END AS actor,
       count(*)                                     AS edits,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM raw_events
WHERE event_ts >= (SELECT max(event_ts) FROM raw_events) - INTERVAL {minutes} MINUTE
GROUP BY 1
ORDER BY edits DESC
"""

EVENT_TYPES = """
SELECT event_type, count(*) AS events
FROM raw_events
WHERE event_ts >= (SELECT max(event_ts) FROM raw_events) - INTERVAL {minutes} MINUTE
GROUP BY 1
ORDER BY events DESC
"""
