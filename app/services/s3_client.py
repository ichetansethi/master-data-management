import boto3
from botocore.client import Config

from app.config import settings


def get_s3_client(access_key: str, secret_key: str, endpoint_url: str | None = None):
    """Boto3 S3 client for a specific connector's bucket. Each connector owns
    its own bucket + credentials (connector.connection_config), so the caller
    always passes that connector's access_key/secret_key rather than any
    app-wide credential. endpoint_url defaults to the shared S3-compatible
    endpoint used for this deployment (Cloudflare R2); pass an override for a
    connector backed by a different endpoint (e.g. real AWS S3).

    region_name="auto" and virtual-hosted-style addressing (the boto3
    default) match Cloudflare R2's S3-compatible API, per Cloudflare's own
    boto3 example.
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url or settings.s3_endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )
