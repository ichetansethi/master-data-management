import re
import hashlib


def normalize_customer_id(customer_id: str | None) -> str:
    if customer_id is None:
        raise ValueError("customer_id is required")
    value = str(customer_id).strip()
    if not value:
        raise ValueError("customer_id is required")
    return value


def normalize_phone_number(phone_number: str | None) -> str:
    if phone_number is None:
        raise ValueError("phone_number is required")

    digits = re.sub(r"[^\d]", "", str(phone_number).strip())

    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]

    if len(digits) != 10:
        raise ValueError("phone_number must be a valid 10-digit number")

    return digits


def compute_dedup_hash(org_id: int, customer_id: str, phone_number: str) -> str:
    normalized_phone = normalize_phone_number(phone_number)
    raw = f"{org_id}{normalize_customer_id(customer_id)}{normalized_phone}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def translate_payload(raw_payload: dict, field_mapping: list[dict]) -> dict:
    translated = {}
    for mapping in field_mapping:
        source_key = mapping["file_column"]
        target_key = mapping["lead_attribute"]
        data_type = mapping.get("data_type")
        is_mandatory = mapping.get("mandatory", False) or target_key == "customer_id"

        if source_key not in raw_payload or raw_payload[source_key] in (None, ""):
            if is_mandatory:
                raise ValueError(f"{source_key} is required but missing from payload")
            continue

        value = raw_payload[source_key]

        if data_type == "PRIMARY_PHONE_NUMBER":
            value = normalize_phone_number(value)
        elif target_key == "customer_id":
            value = normalize_customer_id(value)

        translated[target_key] = value

    return translated