from datetime import datetime, timedelta

import duckdb

from src.consumer import COLUMNS, DDL
from src.queries import BOT_VS_HUMAN, EDITS_PER_MINUTE, TOP_WIKIS


def make_db():
    con = duckdb.connect(":memory:")
    con.execute(DDL)
    base = datetime(2026, 10, 22, 12, 0, 0)
    rows = []
    for i in range(30):  # 30 events, one every 10s -> 5 minutes, alternating bot/human
        rows.append({
            "event_id": f"e{i}", "rc_id": i, "event_type": "edit",
            "wiki": "enwiki" if i % 3 else "dewiki", "title": "t", "user_name": "u",
            "is_bot": i % 2 == 0, "is_minor": False, "namespace": 0,
            "length_old": 1, "length_new": 2, "comment": "",
            "event_ts": base + timedelta(seconds=10 * i),
            "kafka_partition": 0, "kafka_offset": i,
        })
    ph = ", ".join("?" for _ in COLUMNS)
    con.executemany(f"INSERT INTO raw_events ({', '.join(COLUMNS)}) VALUES ({ph})",
                    [[r[c] for c in COLUMNS] for r in rows])
    return con


def test_edits_per_minute_buckets():
    con = make_db()
    rows = con.sql(EDITS_PER_MINUTE.format(minutes=10)).fetchall()
    assert [r[1] for r in rows] == [6, 6, 6, 6, 6]
    assert sum(r[2] for r in rows) == 15


def test_top_wikis_order():
    con = make_db()
    rows = con.sql(TOP_WIKIS.format(minutes=10, limit=5)).fetchall()
    assert rows[0][0] == "enwiki" and rows[0][1] == 20


def test_bot_vs_human_sums_to_100():
    con = make_db()
    rows = con.sql(BOT_VS_HUMAN.format(minutes=10)).fetchall()
    assert round(sum(r[2] for r in rows)) == 100
