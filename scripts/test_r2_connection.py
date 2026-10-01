import asyncio
import aioboto3

# R2 needs an account-specific endpoint URL, not AWS's default —
# you'll need this from your Cloudflare dashboard
R2_ENDPOINT_URL = "https://a12374b2a553b7aea70675086bfc3dc8.r2.cloudflarestorage.com"

async def main():
    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id="820cbdbf07b129d7d5eef448b0836c2e",
        aws_secret_access_key="31c532d1372866bda7a5f86ce01fdb2af4713e80b57be00f0227d7be96226230",
    ) as s3:
        response = await s3.list_objects_v2(Bucket="cr-platform-dev")
        print(response.get("Contents", "Bucket is empty or no objects found"))

if __name__ == "__main__":
    asyncio.run(main())
