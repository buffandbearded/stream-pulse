"""Smoke test: the dashboard renders against a tiny hand-made warehouse."""
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

APP = str(Path(__file__).resolve().parent.parent / "app" / "dashboard.py")


def make_warehouse(path):
    con = duckdb.connect(str(path))
    base = datetime.utcnow().replace(second=0, microsecond=0)
    mins = [base - timedelta(minutes=i) for i in range(10) if i != 4]  # minute 4 missing = gap
    con.execute("CREATE TABLE fct_edits_per_minute (event_minute TIMESTAMP, events BIGINT, "
                "bot_events BIGINT, human_events BIGINT, new_pages BIGINT)")
    con.executemany("INSERT INTO fct_edits_per_minute VALUES (?, 100, 40, 60, 5)", [[m] for m in mins])
    con.execute("CREATE TABLE mart_wiki_leaderboard (wiki VARCHAR, wiki_family VARCHAR, events BIGINT, "
                "distinct_users BIGINT, pct_bot DOUBLE)")
    con.execute("INSERT INTO mart_wiki_leaderboard VALUES ('enwiki','wikipedia',500,50,10.0),"
                "('commonswiki','commons',300,20,30.0)")
    con.execute("CREATE TABLE mart_top_editors (user_name VARCHAR, edits BIGINT, wikis_touched BIGINT, net_bytes BIGINT)")
    con.execute("INSERT INTO mart_top_editors VALUES ('Alice',20,1,500)")
    con.execute("CREATE TABLE stg_wiki_events (event_type VARCHAR, wiki VARCHAR, wiki_family VARCHAR, "
                "is_bot BOOLEAN, event_ts TIMESTAMP)")
    con.executemany("INSERT INTO stg_wiki_events VALUES ('edit','enwiki','wikipedia', ?, ?)",
                    [[i % 2 == 0, base - timedelta(seconds=i)] for i in range(50)])
    con.close()


def test_dashboard_renders(tmp_path, monkeypatch):
    wh = tmp_path / "wh.duckdb"
    make_warehouse(wh)
    monkeypatch.setenv("WAREHOUSE_PATH", str(wh))
    at = AppTest.from_file(APP, default_timeout=60)
    at.run()
    assert not at.exception, [e.value for e in at.exception]
    labels = [m.label for m in at.metric]
    assert "Events processed" in labels and "Bot share" in labels
    assert at.metric[0].value == "50"
