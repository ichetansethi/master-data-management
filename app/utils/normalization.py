import re
import hashlib

def normalize_phone_number(phone: str) -> str:
    digits_only = re.sub(r"[^\d]", "", phone)

    if len(digits_only) == 12 and digits_only.startswith("91"):
        return digits_only[2:]

    return digits_only

def normalize_customer_id(customer_id: str) -> str:
    return customer_id

def compute_dedup_hash(org_id: int, customer_id: str, phone_number: str) -> str:
    normalized_customer_id = normalize_customer_id(customer_id)
    normalized_phone = normalize_phone_number(phone_number)
    raw = f"{org_id}{normalized_customer_id}{normalized_phone}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()