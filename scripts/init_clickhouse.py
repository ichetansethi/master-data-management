"""Apply the ClickHouse schema in clickhouse/migrations/.

Runs every NNN_*.sql file in order, every time. ClickHouse has no Alembic, so
the rules are:
- one statement per file (clickhouse-connect runs a single statement per call)
- every statement must be safe to re-run (CREATE ... IF NOT EXISTS,
  ALTER ... ADD COLUMN IF NOT EXISTS, ...)
- never edit an applied file; a schema change is a new, higher-numbered file
"""
import logging
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import clickhouse_connect

from app.config import settings

MIGRATIONS_DIR = ROOT / "clickhouse" / "migrations"
MIGRATION_FILE = re.compile(r"^\d{3}_[a-z0-9_]+\.sql$")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

logger = logging.getLogger("init_clickhouse")


def load_statement(path: Path) -> str:
    statement = path.read_text(encoding="utf-8").strip().rstrip(";").strip()
    if not statement:
        raise ValueError(f"{path.name} is empty")
    return statement


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    database = settings.clickhouse_database
    if not IDENTIFIER.match(database):
        raise SystemExit(f"Invalid CLICKHOUSE_DATABASE name: {database!r}")

    files = sorted(p for p in MIGRATIONS_DIR.iterdir() if MIGRATION_FILE.match(p.name))
    if not files:
        raise SystemExit(f"No migration files found in {MIGRATIONS_DIR}")
    statements = [(p.name, load_statement(p)) for p in files]

    connection = dict(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )

    admin = clickhouse_connect.get_client(**connection)
    try:
        admin.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    finally:
        admin.close()

    client = clickhouse_connect.get_client(**connection, database=database)
    try:
        for name, statement in statements:
            client.command(statement)
            logger.info("applied %s", name)
    finally:
        client.close()

    logger.info("ClickHouse schema up to date (%d files)", len(statements))


if __name__ == "__main__":
    main()
