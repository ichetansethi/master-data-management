import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from app.config import settings
from app.services.s3_ingestion import poll_once

logger = logging.getLogger("poller")


async def run(once: bool) -> None:
    interval = settings.s3_poll_interval_seconds
    if interval <= 0:
        logger.error("S3_POLL_INTERVAL_SECONDS must be greater than zero")
        raise SystemExit(1)
    while True:
        try:
            summary = await poll_once()
            logger.info("poll complete %s", summary)
        except Exception:
            logger.exception("poll pass failed")
        if once:
            return
        await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll S3 ingestion folders.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single pass and exit.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run(args.once))
    except KeyboardInterrupt:
        logger.info("poller stopped")


if __name__ == "__main__":
    main()
