import json

import pytest

from src.models import parse_event

BASE = {
    "meta": {"id": "abc-123"},
    "id": 1, "type": "edit", "wiki": "enwiki", "user": "Bob",
    "title": "Foo", "timestamp": 1792685577, "bot": True,
    "length": {"old": 10, "new": 20},
}


def test_valid_edit():
    row = parse_event(json.dumps(BASE), 0, 5)
    assert row["event_id"] == "abc-123"
    assert row["is_bot"] is True
    assert row["length_old"] == 10 and row["length_new"] == 20
    assert row["kafka_offset"] == 5


def test_new_page_has_no_old_length():
    e = {**BASE, "type": "new", "length": {"new": 289}}
    row = parse_event(json.dumps(e), 0, 1)
    assert row["length_old"] is None and row["length_new"] == 289


def test_invalid_json():
    with pytest.raises(ValueError):
        parse_event(b"{not json", 0, 1)


def test_missing_field():
    e = {k: v for k, v in BASE.items() if k != "user"}
    with pytest.raises(ValueError):
        parse_event(json.dumps(e), 0, 1)


def test_missing_meta_id():
    e = {**BASE, "meta": {}}
    with pytest.raises(ValueError):
        parse_event(json.dumps(e), 0, 1)


def test_bad_timestamp():
    e = {**BASE, "timestamp": "yesterday"}
    with pytest.raises(ValueError):
        parse_event(json.dumps(e), 0, 1)
