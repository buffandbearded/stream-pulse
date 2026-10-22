"""Terminal live view: refreshes the Stage 4 aggregations every few seconds."""
import contextlib
import io
import sys
import time

import duckdb

from src.queries import (BOT_VS_HUMAN, EDITS_PER_MINUTE, EVENT_TYPES, TOP_WIKIS,
                         connect_read)

REFRESH_SECONDS = 5
WINDOW_MINUTES = 10


def render():
    con = connect_read()
    try:
        total = con.execute("SELECT count(*) FROM raw_events").fetchall()[0][0]
        newest = con.execute("SELECT max(event_ts) FROM raw_events").fetchall()[0][0]
        print(f"stream-pulse | total events: {total} | newest event (UTC): {newest}\n")
        print(f"Edits per minute (last {WINDOW_MINUTES} min)")
        print(con.sql(EDITS_PER_MINUTE.format(minutes=WINDOW_MINUTES)))
        print("Top wikis (last 5 min)")
        print(con.sql(TOP_WIKIS.format(minutes=5, limit=5)))
        print("Bot vs human (last 5 min)")
        print(con.sql(BOT_VS_HUMAN.format(minutes=5)))
        print("Event types (last 5 min)")
        print(con.sql(EVENT_TYPES.format(minutes=5)))
    finally:
        con.close()


def main():
    try:
        while True:
            try:
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    render()
                text = out.getvalue()
            except duckdb.IOException:
                text = "Waiting for data in data/raw/ ... (is the consumer running?)\n"
            sys.stdout.write("\033[2J\033[H" + text)  # draw only when ready: no blank flash
            sys.stdout.flush()
            time.sleep(REFRESH_SECONDS)
    except KeyboardInterrupt:
        print("Bye")


if __name__ == "__main__":
    main()
