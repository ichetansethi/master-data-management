import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from app.services.call_outcome_consumer import consume

logger = logging.getLogger("consumer")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        logger.info("consumer stopped")


if __name__ == "__main__":
    main()
