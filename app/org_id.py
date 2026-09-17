ORG_ID_MIN = 1_000_000_000
ORG_ID_MAX = 9_999_999_999


def is_ten_digit_org_id(value: int) -> bool:
    return ORG_ID_MIN <= value <= ORG_ID_MAX
