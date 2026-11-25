import argparse
import asyncio
import json
import logging
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from aiokafka import AIOKafkaProducer
from sqlalchemy import func, select

from app.config import settings
from app.database import AsyncSessionLocal, engine
from app.models.leads import Leads

logger = logging.getLogger("mock_kafka_producer")

OUTCOMES = ("answered", "no_answer", "busy", "failed")
PARAMETERS = ("greeting", "identity_verification", "disclosure", "closing")
CAMPAIGN_ID = "mock-campaign"


def events_for(lead) -> list[dict]:
    """One call.completed event, plus call.analyzed when the call was answered."""
    call_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 3 * 24 * 60 * 60))
    call_started_at = started.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    call_status = random.choice(OUTCOMES)
    events = [
        {
            "event_type": "call.completed",
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "call_id": call_id,
            "client_id": lead.org_id,
            "lead_id": str(lead.id),
            "customer_id": lead.customer_id,
            "campaign_id": CAMPAIGN_ID,
            "call_status": call_status,
            "duration_seconds": random.randint(15, 180) if call_status == "answered" else 0,
            "language": random.choice(("hi", "en")),
            "call_started_at": call_started_at,
            "event_version": 1,
        }
    ]
    if call_status != "answered":
        return events

    parameters = []
    for name in PARAMETERS:
        score = random.randint(0, 10)
        parameters.append(
            {
                "audit_parameter_id": name,
                "score": score,
                "normalized_score": score / 10,
                "answer": "yes" if score >= 5 else "no",
                "justification": f"{name} scored {score} out of 10",
            }
        )
    events.append(
        {
            "event_type": "call.analyzed",
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "call_id": call_id,
            "client_id": lead.org_id,
            "call_started_at": call_started_at,
            "analysis_run_id": str(uuid.uuid4()),
            "analysis_version": 1,
            "parameters": parameters,
        }
    )
    return events


async def main(leads: int, org_id: int | None) -> None:
    try:
        async with AsyncSessionLocal() as session:
            stmt = select(Leads.id, Leads.org_id, Leads.customer_id)
            if org_id is not None:
                stmt = stmt.where(Leads.org_id == org_id)
            # shortcut: random() reads the whole leads table, replace before this runs against a production-sized table
            stmt = stmt.order_by(func.random()).limit(leads)
            rows = (await session.execute(stmt)).all()
        if len(rows) < leads:
            logger.info("selected %d leads, fewer than %d requested", len(rows), leads)
        calls = [events_for(row) for row in rows]
        if not calls:
            return

        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            key_serializer=lambda key: key.encode(),
            value_serializer=lambda value: json.dumps(value).encode(),
            acks="all",
            enable_idempotence=True,
        )
        await producer.start()
        try:
            futures = []
            for events in calls:
                for event in events:
                    futures.append(
                        await producer.send(
                            settings.kafka_call_outcomes_topic,
                            value=event,
                            key=event["call_id"],
                        )
                    )
            await asyncio.gather(*futures)
            logger.info("sent %d messages", len(futures))
            counts = {outcome: 0 for outcome in OUTCOMES}
            analyzed = 0
            answered_call_id = None
            for events in calls:
                completed = events[0]
                counts[completed["call_status"]] += 1
                analyzed += len(events) - 1
                if answered_call_id is None and completed["call_status"] == "answered":
                    answered_call_id = completed["call_id"]
            for outcome in OUTCOMES:
                logger.info("%s %d", outcome, counts[outcome])
            logger.info("call.analyzed %d", analyzed)
            logger.info("answered call_id %s", answered_call_id or "none")
        finally:
            await producer.stop()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock call-outcome producer.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--leads", type=int, metavar="N", help="Select N random leads.")
    parser.add_argument("--org-id", type=int, help="Limit the selection to one org.")
    args = parser.parse_args()
    if args.leads < 1:
        parser.error("--leads must be at least 1")
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main(args.leads, args.org_id))
