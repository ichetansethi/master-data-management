from contextlib import asynccontextmanager

from botocore.client import Config
from botocore.exceptions import ClientError
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


def folder_key(connector, folder: str, filename: str = "") -> str:
    """Object key under the connector prefix: ``{prefix}/{folder}/`` or
    ``{prefix}/{folder}/{filename}``."""
    config = connector.connection_config or {}
    prefix = (config.get("prefix") or "").strip("/")
    folder_name = folder.strip("/")
    name = filename.strip("/")
    head = "/".join(part for part in (prefix, folder_name) if part)
    if not name:
        return f"{head}/"
    return f"{head}/{name}"


def _open_client(connector):
    config = connector.connection_config or {}
    return get_s3_client(
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        endpoint_url=config.get("endpoint_url"),
    )


async def list_keys(connector, folder: str) -> list[str]:
    """Keys under ``folder``, excluding the folder placeholder itself.

    list_objects_v2 returns at most 1000 keys, which is fine for now.
    """
    prefix = folder_key(connector, folder)
    async with _open_client(connector) as client:
        response = await client.list_objects_v2(
            Bucket=connector.connection_config["bucket"],
            Prefix=prefix,
        )
    contents = response.get("Contents", [])
    return [obj["Key"] for obj in contents if obj["Key"] != prefix]


async def get_object_bytes(connector, key: str) -> bytes:
    async with _open_client(connector) as client:
        try:
            response = await client.get_object(
                Bucket=connector.connection_config["bucket"],
                Key=key,
            )
            return await response["Body"].read()
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code == "NoSuchKey":
                raise FileNotFoundError(key) from exc
            raise


async def put_object_bytes(connector, key: str, data: bytes) -> None:
    async with _open_client(connector) as client:
        await client.put_object(
            Bucket=connector.connection_config["bucket"],
            Key=key,
            Body=data,
        )


async def delete_object(connector, key: str) -> None:
    """Delete one object. S3 does not complain if the key is already gone,
    which is useful if a poll is retried."""
    async with _open_client(connector) as client:
        await client.delete_object(
            Bucket=connector.connection_config["bucket"],
            Key=key,
        )


async def list_pending_files(connector) -> list[str]:
    """Object keys waiting under the connector's ingestion prefix."""
    return await list_keys(connector, "ingestion")
