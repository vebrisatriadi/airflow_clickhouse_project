import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from flink_jobs.utils import parse_event


def test_parse_event_parses_json():
    raw = '{"movie_id": 3, "rating": 9.5}'
    assert parse_event(raw) == (3, 9.5)
