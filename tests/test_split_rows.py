import csv
import io

from app.services.s3_ingestion import split_rows

FIELDNAMES = ["customer_id", "phone_number", "name"]

ROWS = [
    {"customer_id": "c1", "phone_number": "9999999991", "name": "Asha"},
    {"customer_id": "c2", "phone_number": "9999999992", "name": "Bela"},
    {"customer_id": "c3", "phone_number": "bad", "name": "Chitra"},
    {"customer_id": "", "phone_number": "9999999994", "name": "Diya"},
    {"customer_id": "c1", "phone_number": "9999999995", "name": "Esha"},
]

SUCCESS_ROWS = [
    {"row_number": 2, "lead_id": "lead-2"},
    {"row_number": 1, "lead_id": "lead-1"},
]

ERROR_LOG = [
    {"row_number": 5, "reason": "Duplicate found"},
    {"row_number": 3, "reason": "phone_number must be a valid 10-digit number"},
    {"row_number": 4, "reason": "customer_id is required"},
]


def _parse(data: bytes):
    reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
    return list(reader.fieldnames), list(reader)


def test_splits_five_rows_preserving_order_and_reasons():
    success_csv, failure_csv = split_rows(ROWS, FIELDNAMES, SUCCESS_ROWS, ERROR_LOG)

    success_fields, success_body = _parse(success_csv)
    failure_fields, failure_body = _parse(failure_csv)

    assert len(success_body) + len(failure_body) == len(ROWS)
    assert success_fields == FIELDNAMES
    assert [row["customer_id"] for row in success_body] == ["c1", "c2"]
    assert [row["name"] for row in success_body] == ["Asha", "Bela"]

    assert failure_fields == [*FIELDNAMES, "failure_reason"]
    assert [row["failure_reason"] for row in failure_body] == [
        "phone_number must be a valid 10-digit number",
        "customer_id is required",
        "Duplicate found",
    ]
    assert [row["customer_id"] for row in failure_body] == ["c3", "", "c1"]
    assert [row["name"] for row in failure_body] == ["Chitra", "Diya", "Esha"]


def test_all_success_omits_failure_file():
    rows = ROWS[:2]
    success_csv, failure_csv = split_rows(
        rows,
        FIELDNAMES,
        [{"row_number": 1}, {"row_number": 2}],
        [],
    )

    assert failure_csv is None
    fields, body = _parse(success_csv)
    assert fields == FIELDNAMES
    assert len(body) == 2


def test_all_failure_omits_success_file():
    rows = ROWS[2:]
    success_csv, failure_csv = split_rows(
        rows,
        FIELDNAMES,
        [],
        [
            {"row_number": 2, "reason": "customer_id is required"},
            {"row_number": 1, "reason": "phone_number must be a valid 10-digit number"},
        ],
    )

    assert success_csv is None
    fields, body = _parse(failure_csv)
    assert fields[-1] == "failure_reason"
    assert [row["failure_reason"] for row in body] == [
        "phone_number must be a valid 10-digit number",
        "customer_id is required",
    ]


def test_extra_cell_does_not_crash():
    fieldnames = ["customer_id", "phone_number"]
    rows = [
        {
            "customer_id": "c9",
            "phone_number": "9999999999",
            None: ["spill"],
        }
    ]

    success_csv, failure_csv = split_rows(
        rows,
        fieldnames,
        [{"row_number": 1}],
        [],
    )

    assert failure_csv is None
    fields, body = _parse(success_csv)
    assert fields == fieldnames
    assert body == [{"customer_id": "c9", "phone_number": "9999999999"}]
