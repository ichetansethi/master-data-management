import csv
import io
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import AsyncSessionLocal
from app.database_app import AsyncSessionLocal_app
from app.models.connector import Connector, ConnectorType
from app.models.lead_ingestion_batch import LeadIngestionBatch
from app.models.leads import LeadSource
from app.services.ingestion import (
    fail_batch,
    finalize_batch,
    open_batch,
    process_rows,
    require_identity_mapping,
)
from app.services.s3_client import (
    delete_object,
    folder_key,
    get_object_bytes,
    list_keys,
    put_object_bytes,
)

logger = logging.getLogger(__name__)


def split_rows(rows, fieldnames, success_rows, error_log) -> tuple[bytes | None, bytes | None]:
    """Return (success_csv, failure_csv). Either is None when that side has no rows.

    Success rows keep fieldnames, in that order. Failure rows append
    failure_reason. Both sides are sorted by row_number so the bank's row
    order is preserved.

    A data row with more cells than the header is stored by csv.DictReader
    under the key None. Those cells have no column name, so they are dropped
    (DictWriter extrasaction="ignore") rather than written into the bank's file.
    """
    success_csv = _write_rows(
        rows,
        list(fieldnames),
        success_rows,
    )
    failure_csv = _write_rows(
        rows,
        [*fieldnames, "failure_reason"],
        error_log,
        reason_field="failure_reason",
    )
    return success_csv, failure_csv


def _write_rows(rows, fieldnames, entries, reason_field: str | None = None) -> bytes | None:
    if not entries:
        return None

    ordered = sorted(entries, key=lambda entry: entry["row_number"])
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for entry in ordered:
        source = rows[entry["row_number"] - 1]
        if reason_field is None:
            writer.writerow(source)
        else:
            writer.writerow({**source, reason_field: entry["reason"]})
    return buffer.getvalue().encode("utf-8")


def _filename(key: str) -> str:
    return key.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]


async def _archive_unusable(connector, leads_db, batch, key: str, raw: bytes, reason: str):
    """Keep the original bytes under failure/, then remove the source so it is not retried."""
    await fail_batch(leads_db, batch, reason)
    name = _filename(key)
    await put_object_bytes(
        connector,
        folder_key(connector, "failure", f"{batch.id}_{name}"),
        raw,
    )
    await delete_object(connector, key)
    return batch


async def process_s3_object(
    connector, key: str, leads_db: AsyncSession
) -> LeadIngestionBatch | None:
    """Ingest one object from the connector's ingestion folder.

    Returns the batch when a file was handled, including an unusable file
    archived to failure/. Returns None for a missing identity mapping, a
    missing object, or a row-count mismatch. The source key is deleted only
    after its replacement files have been written, or after that unusable
    copy. An unexpected error while parsing or loading rows marks the batch
    failed with a generic message, leaves the file in place, and is raised
    again. A later failure while writing the output files leaves the finished
    batch as it is.
    """
    try:
        require_identity_mapping(connector)
    except ValueError:
        return None

    try:
        raw = await get_object_bytes(connector, key)
    except FileNotFoundError:
        return None

    batch = await open_batch(
        leads_db,
        org_id=connector.org_id,
        connector_id=connector.id,
        source=LeadSource.S3,
        source_ref=key,
    )
    batch_id = batch.id

    try:
        try:
            text = raw.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)
            fieldnames = reader.fieldnames
        except UnicodeDecodeError:
            return await _archive_unusable(
                connector,
                leads_db,
                batch,
                key,
                raw,
                "Object is not valid UTF-8 CSV text.",
            )
        except csv.Error:
            return await _archive_unusable(
                connector,
                leads_db,
                batch,
                key,
                raw,
                "Object is not valid CSV.",
            )

        if not fieldnames:
            return await _archive_unusable(
                connector,
                leads_db,
                batch,
                key,
                raw,
                "No header found in the object.",
            )
        if not rows:
            return await _archive_unusable(
                connector,
                leads_db,
                batch,
                key,
                raw,
                "No data found in the object.",
            )

        success_rows, error_log = await process_rows(
            leads_db,
            rows,
            connector.org_id,
            LeadSource.S3,
            field_mapping=connector.field_mapping,
            batch_size=500,
        )
        if len(success_rows) + len(error_log) != len(rows):
            await fail_batch(
                leads_db,
                batch,
                (
                    "Row accounting mismatch: "
                    f"{len(success_rows)} succeeded and {len(error_log)} failed "
                    f"out of {len(rows)} parsed rows. Original file left in place."
                ),
                total_leads=len(rows),
                error_messages=error_log,
            )
            return None

        batch = await finalize_batch(leads_db, batch, len(rows), success_rows, error_log)
    except Exception as exc:
        logger.exception("Failed to ingest %s", key)
        await leads_db.rollback()
        failed = await leads_db.get(LeadIngestionBatch, batch_id)
        await fail_batch(
            leads_db,
            failed,
            f"Ingestion failed unexpectedly ({type(exc).__name__}).",
        )
        raise

    success_csv, failure_csv = split_rows(rows, fieldnames, success_rows, error_log)
    name = _filename(key)
    if success_csv is not None:
        await put_object_bytes(
            connector,
            folder_key(connector, "success", f"{batch.id}_{name}"),
            success_csv,
        )
    if failure_csv is not None:
        await put_object_bytes(
            connector,
            folder_key(connector, "failure", f"{batch.id}_{name}"),
            failure_csv,
        )
    await delete_object(connector, key)
    return batch


async def _load_active_s3_connectors() -> list[Connector]:
    """Load active S3 connectors and detach them before the session closes."""
    async with AsyncSessionLocal_app() as session:
        result = await session.execute(
            select(Connector).where(
                Connector.type == ConnectorType.S3,
                Connector.is_active.is_(True),
            )
        )
        connectors = list(result.scalars().all())
        for connector in connectors:
            session.expunge(connector)
        return connectors


async def poll_once() -> dict:
    """Process .csv objects under ingestion/ for every active S3 connector.

    The app database session is closed before any file is touched. Each
    connector and each file has its own error boundary, so one failure does
    not stop the rest of the pass.
    """
    summary = {
        "connectors_processed": 0,
        "connectors_skipped": 0,
        "connectors_failed": 0,
        "files_processed": 0,
        "files_skipped": 0,
        "files_failed": 0,
    }
    connectors = await _load_active_s3_connectors()
    for connector in connectors:
        try:
            try:
                require_identity_mapping(connector)
            except ValueError as exc:
                logger.warning("Skipping connector %s: %s", connector.id, exc)
                summary["connectors_skipped"] += 1
                continue

            keys = await list_keys(connector, "ingestion")
            summary["connectors_processed"] += 1
            for key in keys:
                if not key.lower().endswith(".csv"):
                    logger.info(
                        "Skipping non-csv key %s for connector %s", key, connector.id
                    )
                    summary["files_skipped"] += 1
                    continue
                try:
                    async with AsyncSessionLocal() as leads_db:
                        ingested = await process_s3_object(connector, key, leads_db)
                    if ingested is None:
                        summary["files_skipped"] += 1
                    else:
                        summary["files_processed"] += 1
                except Exception:
                    logger.exception(
                        "Failed to process %s for connector %s", key, connector.id
                    )
                    summary["files_failed"] += 1
        except Exception:
            logger.exception("Skipping connector %s after an error", connector.id)
            summary["connectors_failed"] += 1
    return summary
