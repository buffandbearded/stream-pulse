from datetime import datetime, timedelta

from src.consumer import write_batch
from src.queries import BOT_VS_HUMAN, EDITS_PER_MINUTE, TOP_WIKIS, connect_read


def make_rows():
    base = datetime(2026, 10, 22, 12, 0, 0)
    return [{
        "event_id": f"e{i}", "rc_id": i, "event_type": "edit",
        "wiki": "enwiki" if i % 3 else "dewiki", "title": "t", "user_name": "u",
        "is_bot": i % 2 == 0, "is_minor": False, "namespace": 0,
        "length_old": 1, "length_new": 2, "comment": "",
        "event_ts": base + timedelta(seconds=10 * i),  # 30 events, one per 10s = 5 minutes
        "kafka_partition": 0, "kafka_offset": i,
    } for i in range(30)]


def test_edits_per_minute_buckets(tmp_path):
    write_batch(make_rows(), str(tmp_path))
    con = connect_read(str(tmp_path))
    rows = con.execute(EDITS_PER_MINUTE.format(minutes=10)).fetchall()
    assert [r[1] for r in rows] == [6, 6, 6, 6, 6]
    assert sum(r[2] for r in rows) == 15


def test_top_wikis_order(tmp_path):
    write_batch(make_rows(), str(tmp_path))
    rows = connect_read(str(tmp_path)).execute(TOP_WIKIS.format(minutes=10, limit=5)).fetchall()
    assert rows[0][0] == "enwiki" and rows[0][1] == 20


def test_bot_vs_human_sums_to_100(tmp_path):
    write_batch(make_rows(), str(tmp_path))
    rows = connect_read(str(tmp_path)).execute(BOT_VS_HUMAN.format(minutes=10)).fetchall()
    assert round(sum(r[2] for r in rows)) == 100


def test_replayed_batch_is_deduplicated(tmp_path):
    rows = make_rows()
    write_batch(rows, str(tmp_path))
    write_batch(rows, str(tmp_path))  # simulate at-least-once replay after a crash
    con = connect_read(str(tmp_path))
    assert con.execute("SELECT count(*) FROM raw_events").fetchall()[0][0] == 30


def test_no_temp_files_left_behind(tmp_path):
    write_batch(make_rows(), str(tmp_path))
    assert [p.suffix for p in tmp_path.iterdir()] == [".parquet"]
