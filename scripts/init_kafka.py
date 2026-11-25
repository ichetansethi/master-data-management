import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, for_code

from app.config import settings

logger = logging.getLogger("init_kafka")


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    topics = [
        (settings.kafka_call_outcomes_topic, settings.kafka_call_outcomes_partitions),
        (settings.kafka_call_outcomes_dlq_topic, 1),
    ]
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
        response = await admin.create_topics(
            [
                NewTopic(name, num_partitions=partitions, replication_factor=1)
                for name, partitions in topics
            ]
        )
        for entry in response.topic_errors:
            code = entry[1]
            if code == TopicAlreadyExistsError.errno:
                logger.info("%s already exists", entry[0])
            elif code != 0:
                message = entry[2] if len(entry) > 2 and entry[2] else entry[0]
                raise for_code(code)(message)
            else:
                logger.info("created %s", entry[0])

        for name, expected in topics:
            described = await admin.describe_topics([name])
            partitions = len(described[0]["partitions"])
            if partitions != expected:
                raise SystemExit(
                    f"{name} has {partitions} partitions, settings expect {expected}. "
                    "Kafka will not shrink a topic, and this script will not add partitions."
                )
            logger.info("%s has %d partitions", name, partitions)
    finally:
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())
