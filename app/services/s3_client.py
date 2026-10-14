from contextlib import asynccontextmanager

from botocore.client import Config
import aioboto3

from app.config import settings


@asynccontextmanager
async def get_s3_client(access_key: str, secret_key: str, endpoint_url: str | None = None):
    """Async S3 client for a specific connector's bucket. Each connector owns
    its own bucket + credentials (connector.connection_config), so the caller
    always passes that connector's access_key/secret_key rather than any
    app-wide credential. endpoint_url defaults to the shared S3-compatible
    endpoint used for this deployment (Cloudflare R2); pass an override for a
    connector backed by a different endpoint (e.g. real AWS S3).

    aioboto3 has no module-level client(); Session().client() is the async
    context manager. region_name="auto" and virtual-hosted-style addressing
    (the boto3 default) match Cloudflare R2's S3-compatible API.

    Usage:
        async with get_s3_client(access_key, secret_key) as client:
            response = await client.get_object(...)
    """
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=endpoint_url or settings.s3_endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    ) as client:
        yield client


async def list_pending_files(connector) -> list[str]:
    """Object keys waiting under the connector's ingestion prefix."""
    config = connector.connection_config
    prefix = config.get("prefix", "")
    ingestion_prefix = f"{prefix}ingestion/"

    async with get_s3_client(
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        endpoint_url=config.get("endpoint_url"),
    ) as s3:
        response = await s3.list_objects_v2(Bucket=config["bucket"], Prefix=ingestion_prefix)
        contents = response.get("Contents", [])
        return [obj["Key"] for obj in contents if obj["Key"] != ingestion_prefix]
