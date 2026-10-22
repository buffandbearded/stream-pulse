"""stream-pulse dashboard: a live view of the dbt marts built on the Wikipedia edit stream.

Run from the repo root:   streamlit run app/dashboard.py
"""
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

WAREHOUSE = os.getenv("WAREHOUSE_PATH", "data/warehouse.duckdb")
DBT_ARGS = ["build", "--project-dir", "dbt", "--profiles-dir", "dbt"]
REFRESH_SECONDS = 10
DBT_EVERY_SECONDS = 60

st.set_page_config(page_title="stream-pulse", layout="wide")


# ---------- data access ----------
def _query(con, sql: str) -> pd.DataFrame:
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)


def load(window_minutes: int) -> dict:
    """Read everything the page needs in one short-lived read-only connection.
    Retries briefly if dbt is rewriting the warehouse at that moment."""
    last = None
    for _ in range(40):
        try:
            con = duckdb.connect(WAREHOUSE, read_only=True)
        except duckdb.IOException as exc:
            last = exc
            time.sleep(0.25)
            continue
        try:
            return {
                "minutes": _query(con, f"""
                    SELECT event_minute, events, bot_events, human_events, new_pages
                    FROM fct_edits_per_minute
                    WHERE event_minute >= (SELECT max(event_minute) FROM fct_edits_per_minute)
                                          - INTERVAL {int(window_minutes)} MINUTE
                    ORDER BY 1"""),
                "wikis": _query(con, """
                    SELECT wiki, wiki_family, events, distinct_users, pct_bot
                    FROM mart_wiki_leaderboard ORDER BY events DESC LIMIT 15"""),
                "families": _query(con, """
                    SELECT wiki_family, count(*) AS events
                    FROM stg_wiki_events GROUP BY 1 ORDER BY 2 DESC"""),
                "types": _query(con, """
                    SELECT event_type, count(*) AS events
                    FROM stg_wiki_events GROUP BY 1 ORDER BY 2 DESC"""),
                "editors": _query(con, """
                    SELECT user_name, edits, wikis_touched, net_bytes
                    FROM mart_top_editors LIMIT 15"""),
                "totals": _query(con, """
                    SELECT count(*) AS total, max(event_ts) AS newest,
                           sum(is_bot::int) AS bots, count(DISTINCT wiki) AS wikis
                    FROM stg_wiki_events"""),
            }
        finally:
            con.close()
    raise last


def run_dbt() -> None:
    """Rebuild the marts by running the dbt executable that lives next to this Python."""
    exe = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    cmd = [str(exe) if exe.exists() else (shutil.which("dbt") or "dbt")] + DBT_ARGS
    proc = subprocess.run(cmd, capture_output=True, text=True)
    st.session_state["dbt_ok"] = proc.returncode == 0
    st.session_state["dbt_log"] = (proc.stdout + proc.stderr)[-6000:]
    st.session_state["dbt_time"] = time.time()


def fmt_age(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 90:
        return f"{seconds}s ago"
    if seconds < 5400:
        return f"{seconds // 60} min ago"
    return f"{seconds // 3600} h ago"


# ---------- sidebar ----------
with st.sidebar:
    st.header("Controls")
    window = st.select_slider("Time window (minutes)", options=[15, 30, 60, 120, 240], value=60)
    auto_refresh = st.toggle(f"Auto-refresh every {REFRESH_SECONDS}s", value=True)
    auto_dbt = st.toggle(f"Rebuild marts (dbt) every {DBT_EVERY_SECONDS}s", value=True)
    if st.button("Run dbt build now", width="stretch"):
        with st.spinner("Running dbt build..."):
            run_dbt()
    st.caption("Source: Wikimedia recent-changes stream, via Redpanda, Parquet and dbt.")
    if "dbt_log" in st.session_state:
        ok = st.session_state["dbt_ok"]
        st.write(("Last dbt build: OK" if ok else "Last dbt build: FAILED"))
        with st.expander("dbt log"):
            st.code(st.session_state["dbt_log"])

st.title("stream-pulse")
st.caption("Live Wikipedia edit activity. All times UTC.")

if not Path(WAREHOUSE).exists():
    with st.spinner("No warehouse yet, running dbt build..."):
        run_dbt()
    if not Path(WAREHOUSE).exists():
        st.error("Warehouse not found and dbt build failed. Is the consumer producing data/raw/*.parquet?")
        st.stop()


# ---------- the live part ----------
def live_view() -> None:
    wh = Path(WAREHOUSE)
    if auto_dbt and time.time() - wh.stat().st_mtime > DBT_EVERY_SECONDS:
        run_dbt()

    d = load(window)
    totals = d["totals"].iloc[0]
    minutes = d["minutes"].copy()

    # Fill minutes with no events so gaps show up as zero instead of being hidden.
    if not minutes.empty:
        full = pd.date_range(minutes["event_minute"].min(), minutes["event_minute"].max(), freq="min")
        minutes = (minutes.set_index("event_minute").reindex(full, fill_value=0)
                   .rename_axis("minute"))

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    newest_age = (now - pd.Timestamp(totals["newest"]).to_pydatetime()).total_seconds()
    built_age = time.time() - wh.stat().st_mtime
    complete = minutes.iloc[:-1] if len(minutes) > 1 else minutes  # last minute is still filling
    recent = complete.tail(5)
    per_min = float(recent["events"].mean()) if not recent.empty else 0.0
    bot_pct = 100.0 * totals["bots"] / totals["total"] if totals["total"] else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Events processed", f"{int(totals['total']):,}")
    c2.metric("Events / min (last 5 full min)", f"{per_min:,.0f}")
    c3.metric("Bot share", f"{bot_pct:.1f}%")
    c4.metric("Wikis seen", f"{int(totals['wikis'])}")
    c5.metric("Newest event", fmt_age(newest_age), help=f"Marts last built {fmt_age(built_age)}")
    if newest_age > 300:
        st.warning("No new events for over 5 minutes. Are the producer and consumer running?")

    left, right = st.columns([2, 1])
    with left:
        st.subheader("Events per minute")
        if minutes.empty:
            st.info("No data in this window yet.")
        else:
            chart = minutes[["human_events", "bot_events"]].rename(
                columns={"human_events": "human", "bot_events": "bot"})
            st.bar_chart(chart, color=["#4c9be8", "#e8704c"], height=320)
            st.caption("Zero bars are minutes with no data (for example the producer was stopped). "
                       "The newest minute is still filling.")
    with right:
        st.subheader("Event types")
        st.bar_chart(d["types"].set_index("event_type"), height=320, color="#7a8cff")

    a, b = st.columns([3, 2])
    with a:
        st.subheader("Most active wikis")
        st.dataframe(
            d["wikis"], hide_index=True, width="stretch",
            column_config={
                "wiki": "Wiki", "wiki_family": "Family",
                "events": st.column_config.ProgressColumn(
                    "Events", format="%d", min_value=0, max_value=int(d["wikis"]["events"].max() or 1)),
                "distinct_users": "Users",
                "pct_bot": st.column_config.NumberColumn("Bot %", format="%.1f"),
            })
    with b:
        st.subheader("By project family")
        st.bar_chart(d["families"].set_index("wiki_family"), height=330, color="#5fc79a")

    st.subheader("Top human editors")
    st.dataframe(d["editors"], hide_index=True, width="stretch",
                 column_config={"user_name": "Editor", "edits": "Edits",
                                "wikis_touched": "Wikis", "net_bytes": "Net bytes"})


run_every = REFRESH_SECONDS if auto_refresh else None
st.fragment(run_every=run_every)(live_view)()
