import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from app.config import settings
from app.services.s3_client import get_s3_client


async def main():
    access_key = os.environ.get("TEST_CONNECTOR_S3_ACCESS_KEY_ID")
    secret_key = os.environ.get("TEST_CONNECTOR_S3_SECRET_ACCESS_KEY")
    bucket = os.environ.get("TEST_CONNECTOR_S3_BUCKET")
    if not all([access_key, secret_key, bucket]):
        print(
            "Set TEST_CONNECTOR_S3_BUCKET, TEST_CONNECTOR_S3_ACCESS_KEY_ID, "
            "and TEST_CONNECTOR_S3_SECRET_ACCESS_KEY in .env first."
        )
        return

    async with get_s3_client(access_key, secret_key) as s3:
        response = await s3.list_objects_v2(Bucket=bucket, MaxKeys=5)
        contents = response.get("Contents") or []
        print(f"endpoint={settings.s3_endpoint_url}")
        print(f"bucket={bucket}")
        print(f"count={len(contents)}")
        for obj in contents:
            print(obj["Key"])


if __name__ == "__main__":
    asyncio.run(main())
