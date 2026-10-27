"""Generate job/init.sql, the Aurora mart DDL, from the Redshift mart columns.

The Aurora mart tables mirror the Redshift mart tables column for column, so the
DDL is generated from the Redshift catalog (svv_columns). The output is reviewed,
committed, and run by hand as the Aurora master user. It isn't run by the job.

Run from the module root with an AWS session that can query the workgroup:

    python job/generate_init_sql.py --workgroup orcahouse-dev \\
        --secret-arn 'redshift!orcahouse-dev-admin' --output job/init.sql

or `make init-sql`. Without --secret-arn, the caller's IAM identity is used, which
only sees the mart tables it has privileges on.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from spreadsheet_mart_tables import (
    MART_TABLES_DEFAULT,
    REGION_NAME,
    SCHEMA_NAME,
    get_boto3_client,
    parse_tables,
    validate_identifier,
)

COLUMNS_SQL = f"""SELECT
    table_name,
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM svv_columns
WHERE table_schema = '{SCHEMA_NAME}'
ORDER BY table_name, ordinal_position"""

# Redshift types with the same meaning and name in PostgreSQL, or a shorter one.
TYPE_NAMES = {
    "smallint": "smallint",
    "integer": "integer",
    "bigint": "bigint",
    "real": "real",
    "double precision": "double precision",
    "boolean": "boolean",
    "date": "date",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
    "time without time zone": "time",
    "time with time zone": "timetz",
}

STATEMENT_TIMEOUT_SECONDS = 5 * 60
POLL_INTERVAL_SECONDS = 2


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    character_maximum_length: int | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None


def pg_type(column: Column) -> str:
    """The PostgreSQL type for a Redshift column."""
    if column.data_type == "character varying":
        return f"varchar({column.character_maximum_length})"
    if column.data_type == "character":
        return f"char({column.character_maximum_length})"
    if column.data_type == "numeric":
        return f"numeric({column.numeric_precision},{column.numeric_scale})"
    if column.data_type in TYPE_NAMES:
        return TYPE_NAMES[column.data_type]
    raise ValueError(
        f"No PostgreSQL type for Redshift column {column.name} ({column.data_type}). "
        "Add one to generate_init_sql.py, and check that the CSV unload of it imports."
    )


def quote_identifier(name: str) -> str:
    return f'"{validate_identifier(name)}"'


def ddl_for(table: str, columns: list[Column]) -> str:
    """DROP and CREATE TABLE for one mart table, columns in Redshift order."""
    name = f"{SCHEMA_NAME}.{validate_identifier(table)}"
    quoted = [quote_identifier(column.name) for column in columns]
    width = max(len(column) for column in quoted)
    lines = ",\n".join(
        f"    {column.ljust(width)} {pg_type(spec)}"
        for column, spec in zip(quoted, columns, strict=True)
    )
    return f"DROP TABLE IF EXISTS {name};\n\nCREATE TABLE {name}\n(\n{lines}\n);"


def init_sql(tables: dict[str, list[Column]], source: str) -> str:
    header = f"""--
-- init.sql
--
-- README:
--   The Aurora PostgreSQL DDL for the spreadsheet-mart-tables load target, the
--   {SCHEMA_NAME} schema of the orcanexus database. Generated from the Redshift mart
--   columns (svv_columns) by job/generate_init_sql.py, see `make init-sql`.
--   Do not edit it by hand. Regenerate it when a mart's columns change.
--
--   Run it as the Aurora master user (sadmin) in the orcanexus database, after
--   orcahouse infra/aurora/dba/04_rw.sql has created dbuser_rw, and before the
--   first Glue load.
--
--   It drops and recreates every table, which deletes the loaded rows. Don't
--   run it while the Glue job or the OrcaNexus API is using the tables. The next
--   Glue run reloads them.
--
--   Generated from: {source}.
--

CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
"""
    tables_ddl = "\n\n".join(
        ddl_for(table, columns) for table, columns in tables.items()
    )
    footer = f"""-- The sadmin default privileges cover these. The grants make the script work
-- whoever runs it.
GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA_NAME} TO dbuser_ro;
GRANT SELECT, INSERT, TRUNCATE, MAINTAIN ON ALL TABLES IN SCHEMA {SCHEMA_NAME} TO dbuser_rw;

SELECT '{SCHEMA_NAME} tables ready' AS status;
"""
    return f"{header}\n{tables_ddl}\n\n{footer}"


def field_value(field: dict[str, Any]) -> Any:
    if field.get("isNull"):
        return None
    return next(iter(field.values()))


def fetch_columns(
    client: Any,
    workgroup: str,
    database: str,
    secret_arn: str | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, list[Column]]:
    """Query the Redshift mart columns through the Redshift Data API."""
    kwargs = {"WorkgroupName": workgroup, "Database": database, "Sql": COLUMNS_SQL}
    if secret_arn:
        kwargs["SecretArn"] = secret_arn
    statement_id = client.execute_statement(**kwargs)["Id"]

    deadline = monotonic() + STATEMENT_TIMEOUT_SECONDS
    while True:
        status = client.describe_statement(Id=statement_id)
        if status["Status"] == "FINISHED":
            break
        if status["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Redshift statement {statement_id} failed: {status.get('Error')}"
            )
        if monotonic() >= deadline:
            raise TimeoutError(
                f"Redshift statement {statement_id} is still {status['Status']}"
            )
        sleep(POLL_INTERVAL_SECONDS)

    tables: dict[str, list[Column]] = {}
    kwargs = {"Id": statement_id}
    while True:
        page = client.get_statement_result(**kwargs)
        for record in page["Records"]:
            table, name, data_type, length, precision, scale = map(field_value, record)
            tables.setdefault(table, []).append(
                Column(name, data_type, length, precision, scale)
            )
        if not page.get("NextToken"):
            return tables
        kwargs["NextToken"] = page["NextToken"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--workgroup", default="orcahouse-dev")
    parser.add_argument("--database", default="orcavault")
    parser.add_argument(
        "--secret-arn", help="Redshift secret name or ARN to connect as"
    )
    parser.add_argument("--tables", default=",".join(MART_TABLES_DEFAULT))
    parser.add_argument("--output", help="Write to this file instead of stdout")
    args = parser.parse_args()

    tables = parse_tables(args.tables)
    client = get_boto3_client("redshift-data")
    columns = fetch_columns(client, args.workgroup, args.database, args.secret_arn)

    missing = [table for table in tables if table not in columns]
    if missing:
        print(f"Not in Redshift {SCHEMA_NAME}: {', '.join(missing)}", file=sys.stderr)
        return 1

    sql = init_sql(
        {table: columns[table] for table in tables},
        source=f"Redshift workgroup {args.workgroup}, database {args.database}, region {REGION_NAME}",
    )
    if args.output:
        with open(args.output, "w", newline="") as handle:
            handle.write(sql)
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(sql)
    return 0


if __name__ == "__main__":
    sys.exit(main())
