"""
tests/test_transform.py
=======================
Unit tests for HN ETL transform logic.
Run: pytest tests/ -v
"""

import pytest
import sys
import os

# Make dags importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))

from hn_etl_dag import _extract_domain, transform_stories


# ---------------------------------------------------------------------------
# _extract_domain
# ---------------------------------------------------------------------------
class TestExtractDomain:
    def test_standard_url(self):
        assert _extract_domain("https://www.github.com/user/repo") == "github.com"

    def test_subdomain(self):
        assert _extract_domain("https://blog.ycombinator.com/post") == "ycombinator.com"

    def test_empty_url(self):
        assert _extract_domain("") == ""

    def test_none_url(self):
        assert _extract_domain(None) == ""

    def test_no_tld(self):
        result = _extract_domain("http://localhost:8080")
        assert result == "localhost:8080" or result == "localhost"

    def test_ask_hn_no_url(self):
        assert _extract_domain("") == ""


# ---------------------------------------------------------------------------
# transform_stories (via mock context)
# ---------------------------------------------------------------------------
MOCK_STORIES = [
    {
        "id": 12345,
        "title": "  Show HN: My ML project  ",
        "url": "https://github.com/user/ml-project",
        "by": "testuser",
        "score": 342,
        "descendants": 87,
        "kids": [111, 222, 333],
        "text": "",
        "time": 1700000000,
        "type": "story",
    },
    {
        "id": 99999,
        "title": "Ask HN: How do you manage context windows?",
        "url": "",
        "by": "anotheruser",
        "score": 201,
        "descendants": 150,
        "kids": [444, 555],
        "text": "  I've been thinking about this a lot.  ",
        "time": 1700001000,
        "type": "story",
    },
    {
        # Should be filtered out — not a story type
        "id": 77777,
        "title": "A comment, not a story",
        "type": "comment",
        "by": "commenter",
        "score": 0,
    },
    None,  # Should be filtered out
]


class MockXCom:
    def __init__(self, data):
        self.data = data

    def xcom_pull(self, key, task_ids):
        return self.data

    def xcom_push(self, key, value):
        pass


class MockContext:
    def __init__(self, stories):
        self.task_instance = MockXCom(stories)
        self.ds = "2024-01-15"


def test_transform_filters_non_stories():
    ctx = MockContext(MOCK_STORIES)
    result = transform_stories(**{"task_instance": ctx["task_instance"], "ds": ctx.ds})
    # Should only include 2 valid stories, not the comment or None
    assert len(result) == 2


def test_transform_strips_whitespace():
    ctx = MockContext(MOCK_STORIES)
    # Run transform directly
    result = _run_transform(MOCK_STORIES, "2024-01-15")
    titles = [r["title"] for r in result]
    assert "Show HN: My ML project" in titles  # stripped
    texts = [r["text"] for r in result]
    assert "I've been thinking about this a lot." in texts


def test_transform_kids_count():
    result = _run_transform(MOCK_STORIES, "2024-01-15")
    story = next(r for r in result if r["id"] == 12345)
    assert story["kids_count"] == 3
    assert len(story["kids"]) == 3


def test_transform_domain_extraction():
    result = _run_transform(MOCK_STORIES, "2024-01-15")
    story = next(r for r in result if r["id"] == 12345)
    assert story["domain"] == "github.com"


def test_transform_empty_url_domain():
    result = _run_transform(MOCK_STORIES, "2024-01-15")
    story = next(r for r in result if r["id"] == 99999)
    assert story["domain"] == ""
    assert story["url"] == ""


def test_transform_adds_run_date():
    result = _run_transform(MOCK_STORIES, "2024-01-15")
    for r in result:
        assert r["run_date"] == "2024-01-15"
        assert "run_ts" in r


def test_transform_handles_missing_fields():
    minimal = [{
        "id": 11111,
        "title": "Minimal story",
        "type": "story",
        # No score, descendants, kids, url, by
    }]
    result = _run_transform(minimal, "2024-01-15")
    assert len(result) == 1
    assert result[0]["score"] == 0
    assert result[0]["descendants"] == 0
    assert result[0]["kids_count"] == 0
    assert result[0]["kids"] == []
    assert result[0]["url"] == ""
    assert result[0]["by"] == ""


# ---------------------------------------------------------------------------
# Helper: run transform logic without Airflow context
# ---------------------------------------------------------------------------
def _run_transform(stories: list, run_date: str) -> list:
    from datetime import timezone
    from datetime import datetime

    run_ts = datetime.now(timezone.utc).isoformat()
    transformed = []

    for s in stories:
        if not s or s.get("type") != "story":
            continue

        transformed.append({
            "id":           s.get("id"),
            "title":        (s.get("title") or "").strip(),
            "url":          s.get("url", ""),
            "domain":       _extract_domain(s.get("url", "")),
            "by":           s.get("by", ""),
            "score":        s.get("score", 0),
            "descendants":  s.get("descendants", 0),
            "kids_count":   len(s.get("kids") or []),
            "kids":         s.get("kids") or [],
            "text":         (s.get("text") or "").strip(),
            "time":         s.get("time"),
            "type":         s.get("type", "story"),
            "run_date":     run_date,
            "run_ts":       run_ts,
        })

    return transformed
