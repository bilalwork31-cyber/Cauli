"""Unit sanity for driver.py math (no redis, no workers needed).

Run:  pytest -q test_driver.py
"""

import random

import pytest

from driver import parse_celery_date_done, percentile, summarize_latencies


def test_percentile_1_to_100():
    vals = list(range(1, 101))
    random.Random(7).shuffle(vals)  # must not require sorted input
    assert percentile(vals, 50) == pytest.approx(50.5)
    assert percentile(vals, 90) == pytest.approx(90.1)
    assert percentile(vals, 95) == pytest.approx(95.05)
    assert percentile(vals, 99) == pytest.approx(99.01)
    assert percentile(vals, 0) == 1.0
    assert percentile(vals, 100) == 100.0


def test_percentile_single_and_pair():
    assert percentile([42], 50) == 42.0
    assert percentile([42], 99) == 42.0
    assert percentile([10, 20], 50) == pytest.approx(15.0)
    assert percentile([20, 10], 75) == pytest.approx(17.5)


def test_percentile_empty():
    assert percentile([], 50) is None


def test_summarize_fixed_tail():
    # 99 fast tasks at 100ms and one 1000ms straggler
    lat = [100.0] * 99 + [1000.0]
    s = summarize_latencies(lat)
    assert s["count"] == 100
    assert s["p50"] == pytest.approx(100.0)
    assert s["p95"] == pytest.approx(100.0)
    assert s["p99"] == pytest.approx(109.0)  # interpolated toward the straggler
    assert s["max"] == pytest.approx(1000.0)
    assert s["mean"] == pytest.approx(109.0)


def test_summarize_empty():
    s = summarize_latencies([])
    assert s["count"] == 0
    assert s["p50"] is None
    assert s["max"] is None


def test_parse_celery_date_done_naive_is_utc():
    # celery stores date_done in UTC; naive strings must be read as UTC
    ms_naive = parse_celery_date_done("2026-01-02T03:04:05.500000")
    ms_aware = parse_celery_date_done("2026-01-02T03:04:05.500000+00:00")
    assert ms_naive == ms_aware
    assert ms_naive == pytest.approx(1767323045500.0)
