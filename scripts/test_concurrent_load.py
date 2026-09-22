import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx

BASE_URL = "http://127.0.0.1:8000"

async def main():
    async with httpx.AsyncClient() as client:
        login_resp = await client.post(
            f"{BASE_URL}/auth/login",
            data={"username": "admin", "password": "some-test-password"},
        )
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        csv_content = b"customer_id,phone_number\nRACE001,9876543210\n"

        # Fire two identical uploads concurrently using asyncio.gather,
        # each with the same csv_content, same org_id, same headers

        result1, result2 = await asyncio.gather(
            client.post(
                f"{BASE_URL}/leads/upload",
                files={"file": ("race.csv", csv_content, "text/csv")},
                data={"org_id": "1000000000"},
                headers=headers,
            ),
            client.post(
                f"{BASE_URL}/leads/upload",
                files={"file": ("race.csv", csv_content, "text/csv")},
                data={"org_id": "1000000000"},
                headers=headers,
            ),
        )
        print("Request 1:", result1.status_code, result1.json())
        print("Request 2:", result2.status_code, result2.json())


if __name__ == "__main__":
    asyncio.run(main())