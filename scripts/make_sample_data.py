"""Write deterministic synthetic events to data/raw/ (used by CI to exercise dbt without Kafka).

Includes duplicated event ids and one unknown event type on purpose, so the dedup logic and the
accepted_values warning are both exercised.
"""
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.consumer import write_batch  # noqa: E402

N = 3000
WIKIS = (["enwiki"] * 5 + ["dewiki"] * 2 + ["commonswiki"] * 4 + ["wikidatawiki"] * 3
         + ["zhwikisource"] * 4 + ["enwiktionary", "metawiki", "frwikibooks"])
TYPES = ["edit"] * 6 + ["new"] + ["log"] * 2 + ["categorize"] * 3


def make_rows(seed: int = 1) -> list[dict]:
    rnd = random.Random(seed)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []
    for i in range(N):
        kind = rnd.choice(TYPES)
        old = rnd.randint(100, 5000)
        rows.append({
            "event_id": f"sample-{i}", "rc_id": i, "event_type": kind, "wiki": rnd.choice(WIKIS),
            "title": "Sample", "user_name": rnd.choice([f"user{k}" for k in range(60)]),
            "is_bot": rnd.random() < 0.45, "is_minor": rnd.random() < 0.3, "namespace": 0,
            "length_old": old if kind == "edit" else None,
            "length_new": None if kind in ("log", "categorize") else old + rnd.randint(-300, 600),
            "comment": "", "event_ts": now - timedelta(seconds=rnd.randint(0, 1800)),
            "kafka_partition": i % 3, "kafka_offset": i,
        })
    return rows


if __name__ == "__main__":
    rows = make_rows()
    write_batch(rows[:2000], "data/raw")
    write_batch(rows[1500:], "data/raw")  # 500 events deliberately written twice
    write_batch([{**rows[0], "event_id": "sample-odd", "event_type": "unknown-type"}], "data/raw")
    print(f"wrote {N + 1} unique events (+500 duplicates) to data/raw/")
