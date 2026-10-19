import logging
import uuid
from types import SimpleNamespace

import pytest

from app.services import s3_ingestion
from app.services.s3_ingestion import process_s3_object

IDENTITY_MAPPING = [
    {"file_column": "customer_id", "lead_attribute": "customer_id"},
    {"file_column": "phone_number", "lead_attribute": "phone_number"},
]

CSV = b"customer_id,phone_number\nc1,9999999991\nc2,bad\n"


def _connector(mapping=None):
    return SimpleNamespace(
        id=uuid.uuid4(),
        org_id=1000000000,
        field_mapping=IDENTITY_MAPPING if mapping is None else mapping,
        connection_config={"prefix": ""},
    )


def _batch():
    return SimpleNamespace(id=uuid.uuid4())


@pytest.mark.asyncio
async def test_missing_identity_mapping_leaves_the_file(monkeypatch):
    async def get_object_bytes(connector, key):
        raise AssertionError("file should stay in ingestion")

    monkeypatch.setattr(s3_ingestion, "get_object_bytes", get_object_bytes)

    assert await process_s3_object(_connector(mapping=[]), "ingestion/leads.csv", object()) is None


@pytest.mark.asyncio
async def test_missing_object_returns_without_opening_a_batch(monkeypatch):
    async def get_object_bytes(connector, key):
        raise FileNotFoundError(key)

    async def open_batch(*args, **kwargs):
        raise AssertionError("batch should not be opened")

    monkeypatch.setattr(s3_ingestion, "get_object_bytes", get_object_bytes)
    monkeypatch.setattr(s3_ingestion, "open_batch", open_batch)

    assert await process_s3_object(_connector(), "ingestion/leads.csv", object()) is None


@pytest.mark.asyncio
async def test_unusable_file_is_copied_then_deleted(monkeypatch):
    batch = _batch()
    calls = []

    async def get_object_bytes(connector, key):
        return b"\xff\xfe not text"

    async def open_batch(*args, **kwargs):
        calls.append("open")
        return batch

    async def fail_batch(leads_db, stored, reason, **kwargs):
        calls.append(("fail", reason))

    async def put_object_bytes(connector, key, data):
        calls.append(("put", key, data))

    async def delete_object(connector, key):
        calls.append(("delete", key))

    monkeypatch.setattr(s3_ingestion, "get_object_bytes", get_object_bytes)
    monkeypatch.setattr(s3_ingestion, "open_batch", open_batch)
    monkeypatch.setattr(s3_ingestion, "fail_batch", fail_batch)
    monkeypatch.setattr(s3_ingestion, "put_object_bytes", put_object_bytes)
    monkeypatch.setattr(s3_ingestion, "delete_object", delete_object)

    assert await process_s3_object(_connector(), "ingestion/leads.csv", object()) is batch

    assert calls[0] == "open"
    assert calls[1][0] == "fail"
    assert calls[1][1] == "Object is not valid UTF-8 CSV text."
    assert calls[2] == ("put", f"failure/{batch.id}_leads.csv", b"\xff\xfe not text")
    assert calls[3] == ("delete", "ingestion/leads.csv")


@pytest.mark.asyncio
async def test_header_only_file_is_unusable(monkeypatch):
    batch = _batch()
    reasons = []

    async def get_object_bytes(connector, key):
        return b"customer_id,phone_number\n"

    async def open_batch(*args, **kwargs):
        return batch

    async def fail_batch(leads_db, stored, reason, **kwargs):
        reasons.append(reason)

    async def put_object_bytes(connector, key, data):
        return None

    async def delete_object(connector, key):
        return None

    monkeypatch.setattr(s3_ingestion, "get_object_bytes", get_object_bytes)
    monkeypatch.setattr(s3_ingestion, "open_batch", open_batch)
    monkeypatch.setattr(s3_ingestion, "fail_batch", fail_batch)
    monkeypatch.setattr(s3_ingestion, "put_object_bytes", put_object_bytes)
    monkeypatch.setattr(s3_ingestion, "delete_object", delete_object)

    assert await process_s3_object(_connector(), "ingestion/empty.csv", object()) is batch

    assert reasons == ["No data found in the object."]


@pytest.mark.asyncio
async def test_accounting_mismatch_leaves_the_original(monkeypatch):
    batch = _batch()
    calls = []

    async def get_object_bytes(connector, key):
        return CSV

    async def open_batch(*args, **kwargs):
        return batch

    error_log = [{"row_number": 2, "reason": "phone_number must be a valid 10-digit number"}]

    async def process_rows(*args, **kwargs):
        assert kwargs["batch_size"] == 500
        return [], error_log

    async def finalize_batch(*args, **kwargs):
        calls.append("finalize")
        return batch

    async def fail_batch(leads_db, stored, reason, **kwargs):
        calls.append(("fail", reason, kwargs.get("error_messages")))

    async def put_object_bytes(connector, key, data):
        calls.append("put")

    async def delete_object(connector, key):
        calls.append("delete")

    monkeypatch.setattr(s3_ingestion, "get_object_bytes", get_object_bytes)
    monkeypatch.setattr(s3_ingestion, "open_batch", open_batch)
    monkeypatch.setattr(s3_ingestion, "process_rows", process_rows)
    monkeypatch.setattr(s3_ingestion, "finalize_batch", finalize_batch)
    monkeypatch.setattr(s3_ingestion, "fail_batch", fail_batch)
    monkeypatch.setattr(s3_ingestion, "put_object_bytes", put_object_bytes)
    monkeypatch.setattr(s3_ingestion, "delete_object", delete_object)

    assert await process_s3_object(_connector(), "ingestion/leads.csv", object()) is None

    assert calls[0][0] == "fail"
    assert "left in place" in calls[0][1]
    assert calls[0][2] == error_log
    assert "finalize" not in calls
    assert "put" not in calls
    assert "delete" not in calls


@pytest.mark.asyncio
async def test_fail_batch_keeps_the_original_row_errors():
    from app.models.lead_ingestion_batch import LeadIngestionBatchStatus
    from app.services.ingestion import fail_batch

    batch = SimpleNamespace()

    class Session:
        async def commit(self):
            return None

    error_log = [{"row_number": 2, "reason": "phone_number must be a valid 10-digit number"}]
    await fail_batch(
        Session(),
        batch,
        "Row accounting mismatch. Original file left in place.",
        total_leads=2,
        error_messages=error_log,
    )

    assert batch.status == LeadIngestionBatchStatus.FAILED
    assert batch.total_leads == 2
    assert batch.error_messages == [
        {"reason": "Row accounting mismatch. Original file left in place."},
        *error_log,
    ]


@pytest.mark.asyncio
async def test_writes_both_sides_then_deletes_original(monkeypatch):
    batch = _batch()
    calls = []

    async def get_object_bytes(connector, key):
        return CSV

    async def open_batch(*args, **kwargs):
        assert kwargs["source_ref"] == "ingestion/leads.csv"
        return batch

    async def process_rows(*args, **kwargs):
        return (
            [{"row_number": 1}],
            [{"row_number": 2, "reason": "phone_number must be a valid 10-digit number"}],
        )

    async def finalize_batch(*args, **kwargs):
        return batch

    async def put_object_bytes(connector, key, data):
        calls.append(("put", key))

    async def delete_object(connector, key):
        calls.append(("delete", key))

    monkeypatch.setattr(s3_ingestion, "get_object_bytes", get_object_bytes)
    monkeypatch.setattr(s3_ingestion, "open_batch", open_batch)
    monkeypatch.setattr(s3_ingestion, "process_rows", process_rows)
    monkeypatch.setattr(s3_ingestion, "finalize_batch", finalize_batch)
    monkeypatch.setattr(s3_ingestion, "put_object_bytes", put_object_bytes)
    monkeypatch.setattr(s3_ingestion, "delete_object", delete_object)

    assert await process_s3_object(_connector(), "ingestion/leads.csv", object()) is batch

    assert calls == [
        ("put", f"success/{batch.id}_leads.csv"),
        ("put", f"failure/{batch.id}_leads.csv"),
        ("delete", "ingestion/leads.csv"),
    ]


@pytest.mark.asyncio
async def test_unexpected_error_fails_the_batch_and_leaves_the_file(monkeypatch, caplog):
    batch = _batch()
    session = SimpleNamespace(rolled_back=False, loaded=None)

    async def rollback():
        session.rolled_back = True

    async def get(model, batch_id):
        session.loaded = batch_id
        return batch

    session.rollback = rollback
    session.get = get
    reasons = []

    async def get_object_bytes(connector, key):
        return CSV

    async def open_batch(*args, **kwargs):
        return batch

    async def process_rows(*args, **kwargs):
        raise RuntimeError("connection reset")

    async def fail_batch(leads_db, stored, reason, **kwargs):
        reasons.append((stored, reason))

    async def delete_object(connector, key):
        raise AssertionError("file should stay in ingestion")

    monkeypatch.setattr(s3_ingestion, "get_object_bytes", get_object_bytes)
    monkeypatch.setattr(s3_ingestion, "open_batch", open_batch)
    monkeypatch.setattr(s3_ingestion, "process_rows", process_rows)
    monkeypatch.setattr(s3_ingestion, "fail_batch", fail_batch)
    monkeypatch.setattr(s3_ingestion, "delete_object", delete_object)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="connection reset"):
            await process_s3_object(_connector(), "ingestion/leads.csv", session)

    assert session.rolled_back
    assert session.loaded == batch.id
    assert reasons == [(batch, "Ingestion failed unexpectedly (RuntimeError).")]
    assert "connection reset" not in reasons[0][1]
    assert "connection reset" in caplog.text


@pytest.mark.asyncio
async def test_output_write_failure_leaves_the_finished_batch(monkeypatch):
    batch = _batch()
    session = SimpleNamespace()

    async def rollback():
        raise AssertionError("finished batch should not be rolled back")

    async def get(model, batch_id):
        raise AssertionError("finished batch should not be re-fetched")

    session.rollback = rollback
    session.get = get

    async def get_object_bytes(connector, key):
        return CSV

    async def open_batch(*args, **kwargs):
        return batch

    async def process_rows(*args, **kwargs):
        return (
            [{"row_number": 1}],
            [{"row_number": 2, "reason": "phone_number must be a valid 10-digit number"}],
        )

    async def finalize_batch(*args, **kwargs):
        return batch

    async def fail_batch(leads_db, stored, reason, **kwargs):
        raise AssertionError("finished batch should stay finished")

    async def put_object_bytes(connector, key, data):
        raise RuntimeError("upload failed")

    async def delete_object(connector, key):
        raise AssertionError("file should stay in ingestion")

    monkeypatch.setattr(s3_ingestion, "get_object_bytes", get_object_bytes)
    monkeypatch.setattr(s3_ingestion, "open_batch", open_batch)
    monkeypatch.setattr(s3_ingestion, "process_rows", process_rows)
    monkeypatch.setattr(s3_ingestion, "finalize_batch", finalize_batch)
    monkeypatch.setattr(s3_ingestion, "fail_batch", fail_batch)
    monkeypatch.setattr(s3_ingestion, "put_object_bytes", put_object_bytes)
    monkeypatch.setattr(s3_ingestion, "delete_object", delete_object)

    with pytest.raises(RuntimeError, match="upload failed"):
        await process_s3_object(_connector(), "ingestion/leads.csv", session)
