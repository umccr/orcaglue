"""Generate job/mart_tables.json, the Aurora mart DDL, from the Redshift mart columns.

The Aurora mart tables mirror the Redshift mart tables column for column, so their
columns are generated from the Redshift catalog (svv_columns). The job creates each
table from this file at every run, so a reviewed and deployed change reaches Aurora at
the next swap. Indexes are added by hand, as lists of column names, and regenerating
keeps them.

Run from the module root with an AWS session that can query the workgroup:

    python job/generate_mart_ddl.py --workgroup orcahouse-dev \\
        --secret-arn 'redshift!orcahouse-dev-admin' --output job/mart_tables.json

or `make ddl`. Without --secret-arn, the caller's IAM identity is used, which
only sees the mart tables it has privileges on.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from spreadsheet_mart_tables import (
    MART_TABLES_DEFAULT,
    REGION_NAME,
    SCHEMA_NAME,
    TableSpec,
    get_boto3_client,
    parse_ddl,
    parse_table_spec,
    parse_tables,
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

ABOUT = (
    "The Aurora mart tables, which the spreadsheet-mart-tables job creates at every run. "
    "The columns are generated from the Redshift mart by `make ddl`; don't edit them. "
    "The indexes are edited by hand, as lists of column names, and `make ddl` keeps them."
)


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
        "Add one to generate_mart_ddl.py, and check that the CSV unload of it imports."
    )


def table_spec(
    table: str, columns: list[Column], indexes: Iterable[tuple[str, ...]] = ()
) -> TableSpec:
    """One table's DDL: the Redshift columns in order, with PostgreSQL types."""
    return parse_table_spec(
        table,
        {
            "columns": [(column.name, pg_type(column)) for column in columns],
            "indexes": list(indexes),
        },
    )


def render_ddl(specs: dict[str, TableSpec], source: str) -> str:
    """The DDL file, with a column per line so a mart change reads well in a diff."""
    tables = []
    for spec in specs.values():
        columns = ",\n".join(
            f"        {json.dumps(list(column))}" for column in spec.columns
        )
        indexes = json.dumps([list(index) for index in spec.indexes])
        tables.append(
            f"    {json.dumps(spec.name)}: {{\n"
            f'      "columns": [\n{columns}\n      ],\n'
            f'      "indexes": {indexes}\n'
            "    }"
        )
    return (
        "{\n"
        f'  "about": {json.dumps(ABOUT)},\n'
        f'  "source": {json.dumps(source)},\n'
        '  "tables": {\n' + ",\n".join(tables) + "\n  }\n}\n"
    )


def read_indexes(path: str) -> dict[str, tuple[tuple[str, ...], ...]]:
    """The hand-written indexes of an existing DDL file, to keep them."""
    try:
        with open(path, encoding="utf-8") as handle:
            specs = parse_ddl(handle.read())
    except FileNotFoundError:
        return {}
    return {table: spec.indexes for table, spec in specs.items()}


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

    # An index on a column that Redshift dropped fails here, to be fixed by hand.
    indexes = read_indexes(args.output) if args.output else {}
    text = render_ddl(
        {
            table: table_spec(table, columns[table], indexes.get(table, ()))
            for table in tables
        },
        source=f"Redshift workgroup {args.workgroup}, database {args.database}, region {REGION_NAME}",
    )
    if args.output:
        with open(args.output, "w", newline="") as handle:
            handle.write(text)
        print(f"Wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
