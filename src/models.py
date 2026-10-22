"""Validation and flattening of raw Wikimedia recentchange events."""
import json
from datetime import datetime, timezone

REQUIRED = ("type", "wiki", "user", "title", "timestamp")


def parse_event(raw: bytes | str, partition: int, offset: int) -> dict:
    """Turn a raw Kafka message value into a flat row. Raises ValueError if invalid."""
    try:
        e = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc
    if not isinstance(e, dict):
        raise ValueError("payload is not a JSON object")

    missing = [f for f in REQUIRED if e.get(f) in (None, "")]
    if missing:
        raise ValueError(f"missing fields: {missing}")

    event_id = (e.get("meta") or {}).get("id")
    if not event_id:
        raise ValueError("missing meta.id")

    try:
        event_ts = datetime.fromtimestamp(int(e["timestamp"]), tz=timezone.utc).replace(tzinfo=None)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError(f"bad timestamp: {e['timestamp']!r}") from exc

    length = e.get("length") or {}
    return {
        "event_id": event_id,
        "rc_id": e.get("id"),
        "event_type": e["type"],
        "wiki": e["wiki"],
        "title": e["title"],
        "user_name": e["user"],
        "is_bot": bool(e.get("bot", False)),
        "is_minor": bool(e.get("minor", False)),
        "namespace": e.get("namespace"),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
        "comment": e.get("comment"),
        "event_ts": event_ts,
        "kafka_partition": partition,
        "kafka_offset": offset,
    }
