import asyncio
import logging
from datetime import timezone
from typing import Annotated, Literal
from uuid import UUID

import clickhouse_connect
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import CommitFailedError
from pydantic import AwareDatetime, BaseModel, Field, TypeAdapter, ValidationError

from app.config import settings
from app.org_id import ORG_ID_MAX, ORG_ID_MIN

logger = logging.getLogger(__name__)

RETRY_START_SECONDS = 1
RETRY_MAX_SECONDS = 30

CORE_COLUMNS = [
    "call_id",
    "client_id",
    "lead_id",
    "customer_id",
    "campaign_id",
    "call_status",
    "duration_seconds",
    "language",
    "call_started_at",
    "call_date",
    "event_version",
]
AUDIT_COLUMNS = [
    "call_id",
    "client_id",
    "call_date",
    "audit_parameter_id",
    "score",
    "normalized_score",
    "answer",
    "justification",
    "analysis_run_id",
    "analysis_version",
]


# Unsigned in ClickHouse. A value it would reject must fail here, or the retry loop stalls the partition.
ClientId = Annotated[int, Field(ge=ORG_ID_MIN, le=ORG_ID_MAX)]


class AuditParameter(BaseModel):
    audit_parameter_id: str
    score: float
    normalized_score: float
    answer: str
    justification: str


class CallCompleted(BaseModel):
    event_type: Literal["call.completed"]
    schema_version: Literal[1]
    event_id: UUID
    call_id: str
    client_id: ClientId
    lead_id: UUID
    customer_id: str
    campaign_id: str
    call_status: Literal["answered", "no_answer", "busy", "failed"]
    duration_seconds: int = Field(ge=0)
    language: str
    call_started_at: AwareDatetime
    event_version: int = Field(ge=1)


class CallAnalyzed(BaseModel):
    event_type: Literal["call.analyzed"]
    schema_version: Literal[1]
    event_id: UUID
    call_id: str
    client_id: ClientId
    call_started_at: AwareDatetime
    analysis_run_id: UUID
    analysis_version: int = Field(ge=1)
    parameters: list[AuditParameter] = Field(min_length=1)


CallEvent = Annotated[CallCompleted | CallAnalyzed, Field(discriminator="event_type")]
EVENT_ADAPTER = TypeAdapter(CallEvent)


def failure_reason(exc: ValidationError) -> str:
    parts = []
    for error in exc.errors():
        where = ".".join(str(part) for part in error["loc"])
        parts.append(f"{where}: {error['msg']}" if where else error["msg"])
    return "; ".join(parts) or str(exc)


def core_row(event: CallCompleted) -> tuple:
    started = event.call_started_at.astimezone(timezone.utc)
    return (
        event.call_id,
        event.client_id,
        event.lead_id,
        event.customer_id,
        event.campaign_id,
        event.call_status,
        event.duration_seconds,
        event.language,
        started,
        started.date(),
        event.event_version,
    )


def audit_rows(event: CallAnalyzed) -> list[tuple]:
    call_date = event.call_started_at.astimezone(timezone.utc).date()
    return [
        (
            event.call_id,
            event.client_id,
            call_date,
            parameter.audit_parameter_id,
            parameter.score,
            parameter.normalized_score,
            parameter.answer,
            parameter.justification,
            event.analysis_run_id,
            event.analysis_version,
        )
        for parameter in event.parameters
    ]


def _rows_for(event: CallCompleted | CallAnalyzed) -> tuple[list[tuple], list[tuple]]:
    if isinstance(event, CallCompleted):
        return [core_row(event)], []
    return [], audit_rows(event)


async def _apply_batch(fetched, client, producer, consumer) -> None:
    core_rows: list[tuple] = []
    audit: list[tuple] = []
    dead: list[tuple] = []
    for records in fetched.values():
        for record in records:
            try:
                event = EVENT_ADAPTER.validate_json(record.value)
            except ValidationError as exc:
                dead.append((record, failure_reason(exc)))
                continue
            completed, analyzed = _rows_for(event)
            core_rows.extend(completed)
            audit.extend(analyzed)

    if core_rows:
        await client.insert("call_core", core_rows, column_names=CORE_COLUMNS)
    if audit:
        await client.insert(
            "call_audit_parameters", audit, column_names=AUDIT_COLUMNS
        )
    for record, reason in dead:
        delivery = await producer.send(
            settings.kafka_call_outcomes_dlq_topic,
            value=record.value,
            key=record.key,
            headers=[
                ("failure_reason", reason.encode()),
                ("partition", str(record.partition).encode()),
                ("offset", str(record.offset).encode()),
            ],
        )
        await delivery
    await consumer.commit()
    logger.info(
        "committed core=%d audit=%d dead=%d", len(core_rows), len(audit), len(dead)
    )


async def _apply_batch_with_retry(fetched, client, producer, consumer) -> None:
    delay = RETRY_START_SECONDS
    while True:
        try:
            await _apply_batch(fetched, client, producer, consumer)
            return
        except CommitFailedError:
            # The group already dropped this member, so retrying commit never rejoins it.
            # getmany on the next loop does. Inserted rows are identical versions and collapse under FINAL.
            logger.exception("commit failed; leaving this batch for redelivery")
            return
        except Exception:
            logger.exception("batch failed; retrying in %ss", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, RETRY_MAX_SECONDS)


async def consume() -> None:
    consumer = AIOKafkaConsumer(
        settings.kafka_call_outcomes_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await consumer.start()
    try:
        await producer.start()
        try:
            client = await clickhouse_connect.get_async_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
                database=settings.clickhouse_database,
                autogenerate_session_id=False,
            )
            try:
                while True:
                    fetched = await consumer.getmany(timeout_ms=1000, max_records=500)
                    if not fetched:
                        continue
                    await _apply_batch_with_retry(fetched, client, producer, consumer)
            finally:
                await client.close()
        finally:
            await producer.stop()
    finally:
        await consumer.stop()
