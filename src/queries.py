"""Read-side helpers: a lock-tolerant DuckDB connection and the Stage 4 aggregations."""
import os
import time

import duckdb

DB_PATH = os.getenv("DB_PATH", "data/stream.duckdb")


def connect_read(retries: int = 20, delay: float = 0.25):
    """Open the DB read-only, retrying while the consumer holds the write lock."""
    last = None
    for _ in range(retries):
        try:
            return duckdb.connect(DB_PATH, read_only=True)
        except duckdb.IOException as exc:
            last = exc
            time.sleep(delay)
    raise last


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
