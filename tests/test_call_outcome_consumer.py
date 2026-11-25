import asyncio
import json
from uuid import uuid4

import pytest
from aiokafka.errors import CommitFailedError
from pydantic import ValidationError

from app.services import call_outcome_consumer
from app.services.call_outcome_consumer import (
    AUDIT_COLUMNS,
    CORE_COLUMNS,
    CallAnalyzed,
    CallCompleted,
    audit_rows,
    core_row,
    EVENT_ADAPTER,
)

LEAD_ID = "11111111-1111-4111-8111-111111111111"
RUN_ID = "22222222-2222-4222-8222-222222222222"
EVENT_ID = "33333333-3333-4333-8333-333333333333"


def completed(**overrides) -> bytes:
    event = {
        "event_type": "call.completed",
        "schema_version": 1,
        "event_id": EVENT_ID,
        "call_id": "call-1",
        "client_id": 1000000000,
        "lead_id": LEAD_ID,
        "customer_id": "c1",
        "campaign_id": "mock-campaign",
        "call_status": "answered",
        "duration_seconds": 30,
        "language": "en",
        "call_started_at": "2026-11-25T02:30:00.000+05:30",
        "event_version": 1,
    }
    event.update(overrides)
    return json.dumps(event).encode()


def analyzed(**overrides) -> bytes:
    event = {
        "event_type": "call.analyzed",
        "schema_version": 1,
        "event_id": str(uuid4()),
        "call_id": "call-1",
        "client_id": 1000000000,
        "call_started_at": "2026-11-25T02:30:00.000+05:30",
        "analysis_run_id": RUN_ID,
        "analysis_version": 1,
        "parameters": [
            {
                "audit_parameter_id": "greeting",
                "score": 8,
                "normalized_score": 0.8,
                "answer": "yes",
                "justification": "greeting scored 8 out of 10",
            },
            {
                "audit_parameter_id": "closing",
                "score": 2,
                "normalized_score": 0.2,
                "answer": "no",
                "justification": "closing scored 2 out of 10",
            },
        ],
    }
    event.update(overrides)
    return json.dumps(event).encode()


def test_completed_row_matches_column_order_and_utc_date():
    event = EVENT_ADAPTER.validate_json(completed())
    assert isinstance(event, CallCompleted)
    row = core_row(event)
    assert len(row) == len(CORE_COLUMNS)
    values = dict(zip(CORE_COLUMNS, row, strict=True))
    assert values["call_date"].isoformat() == "2026-11-24"
    assert values["call_started_at"].isoformat() == "2026-11-24T21:00:00+00:00"
    assert values["lead_id"].hex == LEAD_ID.replace("-", "")
    assert values["event_version"] == 1


def test_analyzed_rows_share_the_run_and_match_column_order():
    event = EVENT_ADAPTER.validate_json(analyzed())
    assert isinstance(event, CallAnalyzed)
    rows = audit_rows(event)
    assert len(rows) == 2
    for row in rows:
        assert len(row) == len(AUDIT_COLUMNS)
        values = dict(zip(AUDIT_COLUMNS, row, strict=True))
        assert values["call_id"] == "call-1"
        assert values["client_id"] == 1000000000
        assert values["call_date"].isoformat() == "2026-11-24"
        assert str(values["analysis_run_id"]) == RUN_ID
        assert values["analysis_version"] == 1
    assert [row[AUDIT_COLUMNS.index("audit_parameter_id")] for row in rows] == [
        "greeting",
        "closing",
    ]


@pytest.mark.parametrize(
    "payload",
    [
        b"not-json",
        b'{"event_type": "call.other"}',
        completed(schema_version=2),
        completed(call_status="dropped"),
        completed(call_started_at="2026-11-25T02:30:00.000"),
        completed(event_version=0),
        completed(duration_seconds=-1),
        completed(client_id=999_999_999),
        completed(client_id=10_000_000_000),
        analyzed(parameters=[]),
        analyzed(analysis_version=0),
        analyzed(client_id=-1),
    ],
)
def test_one_validation_error_for_bad_messages(payload):
    with pytest.raises(ValidationError):
        EVENT_ADAPTER.validate_json(payload)


def test_commit_failure_returns_to_the_poll_loop(monkeypatch):
    attempts = 0

    async def fail(*args):
        nonlocal attempts
        attempts += 1
        raise CommitFailedError()

    async def explode(delay):
        raise AssertionError(f"retried after {delay}s")

    monkeypatch.setattr(call_outcome_consumer, "_apply_batch", fail)
    monkeypatch.setattr(call_outcome_consumer.asyncio, "sleep", explode)
    asyncio.run(call_outcome_consumer._apply_batch_with_retry({}, None, None, None))
    assert attempts == 1
