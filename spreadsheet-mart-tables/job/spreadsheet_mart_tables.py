"""Reverse ETL: load the OrcaVault mart CSV unload into Aurora PostgreSQL.

A Redshift UNLOAD writes each mart table as gzip CSV parts with a manifest:

    <source_prefix><table>/<table>_<slice>_part_00.gz
    <source_prefix><table>/<table>_manifest

For every table, this job sets Content-Encoding: gzip on the parts, which the
Aurora aws_s3 import requires and UNLOAD doesn't set, then refreshes the Aurora
table with a blue-green swap. It creates a shadow table in the mart_stg schema
from the job's DDL file, imports the parts into it with one
aws_s3.table_import_from_s3 call per part, and builds its indexes and statistics.
Then it drops the live mart table and moves the shadow table into mart. Each
table is one transaction, so readers use the old table until the commit, and a
failed table keeps its rows. The job reaches the private cluster through a Glue
network connection in its VPC, and logs in over TLS with an IAM auth token.
"""

from __future__ import annotations

import functools
import json
import re
import ssl
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ModuleNotFoundError:  # pragma: no cover
    boto3 = None
    BotoCoreError = ClientError = Exception

try:
    import pg8000.native as pg8000_native
    from pg8000.exceptions import DatabaseError, InterfaceError
except ModuleNotFoundError:  # pragma: no cover - Glue installs it from the job's wheels
    pg8000_native = None

    class DatabaseError(Exception):
        """Stands in for the pg8000 error where pg8000 isn't installed, as in unit tests."""

    class InterfaceError(Exception):
        """Stands in for the pg8000 error where pg8000 isn't installed, as in unit tests."""


try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except ModuleNotFoundError:  # pragma: no cover - lets unit tests import helpers locally
    GlueContext = None
    Job = None

    def getResolvedOptions(argv: list[str], options: list[str]) -> dict[str, str]:
        parsed = {}
        for index, arg in enumerate(argv):
            if not arg.startswith("--"):
                continue
            name = arg[2:]
            if name in options and index + 1 < len(argv):
                parsed[name] = argv[index + 1]

        missing = [option for option in options if option not in parsed]
        if missing:
            raise ValueError(f"Missing required Glue options: {', '.join(missing)}")
        return parsed


try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:  # pragma: no cover - lets unit tests import helpers locally
    SparkSession = None


BASE_NAME_DEFAULT = "spreadsheet__mart_tables"
S3_MID_PATH_DEFAULT = f"orcaglue/{BASE_NAME_DEFAULT}/dev"
SCHEMA_NAME = "mart"
# The shadow tables are loaded here, where the API users can't see them, then moved into mart.
STAGE_SCHEMA_NAME = "mart_stg"
# The swap drops the old table with its grants, so each new table is granted SELECT to these.
MART_READERS_DEFAULT = ("dbuser_ro",)
DB_NAME_DEFAULT = "orcanexus"
DB_USER_DEFAULT = "dbuser_rw"
DB_PORT_DEFAULT = 5432
REGION_NAME = "ap-southeast-2"
APPLICATION_NAME = "orcaglue-spreadsheet-mart-tables"
SUMMARY_FILE_NAME = "orcanexus_mart_tables.manifest.json"
MANIFEST_SUFFIX = "_manifest"

# The mart tables unloaded from Redshift. Keep in sync with job/mart_tables.json and
# the mart-tables stack config.
MART_TABLES_DEFAULT = (
    "bam",
    "em_library_aliases",
    "em_library_dns",
    "em_library_services",
    "fastq",
    "lims",
    "vcf",
    "workflow",
    "workflow_cost_report",
    "workflow_output",
)

# Table, column, index and role names are lowercase identifiers of at most 63 bytes,
# PostgreSQL's limit. They are validated before they are put into SQL.
IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
# A column type in the DDL file, such as varchar(255), numeric(18,2) or double precision.
TYPE_PATTERN = re.compile(r"^[a-z][a-z ]*[a-z](\(\d+(,\d+)?\))?$")
IMPORTED_ROWS_PATTERN = re.compile(r"^(\d+) rows imported")

# The swap waits this long for readers of the live table to finish. Queries that arrive
# meanwhile queue behind it, so the wait is short, and the swap is retried instead.
LOCK_TIMEOUT = "5s"
SWAP_ATTEMPTS = 6
SWAP_RETRY_SECONDS = 10
# The server ends any statement that runs longer than this. The socket timeout only
# catches a dead connection, so it must be longer.
STATEMENT_TIMEOUT = "10min"
SOCKET_TIMEOUT_SECONDS = 15 * 60
WAKE_TIMEOUT_SECONDS = 180
WAKE_INTERVAL_SECONDS = 10
# SQLSTATE cannot_connect_now: the server is starting up, as when Aurora resumes.
STARTING_UP_SQLSTATE = "57P03"
# SQLSTATE lock_not_available: lock_timeout ran out.
LOCK_NOT_AVAILABLE_SQLSTATE = "55P03"

IMPORT_SQL = (
    "SELECT aws_s3.table_import_from_s3("
    ":table_name, :column_list, :options, "
    "aws_commons.create_s3_uri(:bucket, :key, :region)) AS result"
)
EXISTS_SQL = "SELECT to_regclass(:table_name) IS NOT NULL AS present"


@dataclass(frozen=True)
class Part:
    bucket: str
    key: str
    content_length: int


@dataclass(frozen=True)
class Manifest:
    key: str
    last_modified: str
    parts: tuple[Part, ...]
    empty_parts: int


@dataclass
class TableResult:
    table: str
    status: str = "pending"
    manifest: str | None = None
    manifest_last_modified: str | None = None
    parts: int = 0
    bytes: int = 0
    encodings_fixed: int = 0
    rows_before: int | None = None
    rows_after: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class AuroraLogin:
    host: str
    port: int
    user: str
    database: str


@dataclass(frozen=True)
class TableSpec:
    """A table from the DDL file: its columns in CSV header order, and its indexes."""

    name: str
    columns: tuple[tuple[str, str], ...]
    indexes: tuple[tuple[str, ...], ...] = ()

    @property
    def column_names(self) -> list[str]:
        return [name for name, _ in self.columns]


@dataclass(frozen=True)
class LoadOptions:
    readers: tuple[str, ...] = MART_READERS_DEFAULT
    null_marker: str = ""
    allow_empty: bool = False


LOAD_OPTIONS_DEFAULT = LoadOptions()


class LoadError(RuntimeError):
    """A table load that was rolled back."""


# The failures that fail one table but let the others load: AWS and database errors,
# a missing or malformed manifest (OSError, ValueError, KeyError), and rolled back loads.
TABLE_ERRORS = (
    ClientError,
    BotoCoreError,
    DatabaseError,
    InterfaceError,
    LoadError,
    OSError,
    ValueError,
    KeyError,
)


def parse_bool(value: str | bool | None, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value

    value = value.strip().lower()
    if value in {"1", "true", "t", "yes", "y"}:
        return True
    if value in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"Cannot parse boolean value: {value}")


def validate_identifier(name: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(name):
        raise ValueError(f"Not a valid lowercase identifier: {name!r}")
    return name


def validate_type(data_type: str) -> str:
    if not TYPE_PATTERN.fullmatch(data_type):
        raise ValueError(f"Not a column type: {data_type!r}")
    return data_type


def quote_identifier(name: str) -> str:
    """Quote a column name. The Redshift columns include keywords, such as key and type."""
    return f'"{validate_identifier(name)}"'


def parse_names(
    value: str | None, default: tuple[str, ...], option: str
) -> tuple[str, ...]:
    if not value:
        return default

    names = [name.strip() for name in value.split(",") if name.strip()]
    if not names:
        raise ValueError(f"{option} names nothing")
    return tuple(dict.fromkeys(validate_identifier(name) for name in names))


def parse_tables(value: str | None) -> tuple[str, ...]:
    return parse_names(value, MART_TABLES_DEFAULT, "--mart_tables")


def parse_readers(value: str | None) -> tuple[str, ...]:
    return parse_names(value, MART_READERS_DEFAULT, "--mart_readers")


def qualified_name(table: str) -> str:
    return f"{SCHEMA_NAME}.{validate_identifier(table)}"


def staged_name(table: str) -> str:
    return f"{STAGE_SCHEMA_NAME}.{validate_identifier(table)}"


def index_name(table: str, columns: tuple[str, ...]) -> str:
    return f"{table}_{'_'.join(columns)}_idx"


def parse_table_spec(table: str, spec: dict[str, Any]) -> TableSpec:
    """Validate one table of the DDL file. Its names and types go into SQL as they are."""
    columns = tuple(
        (validate_identifier(name), validate_type(data_type))
        for name, data_type in spec["columns"]
    )
    names = [name for name, _ in columns]
    if not names or len(set(names)) != len(names):
        raise ValueError(f"{table} needs columns with distinct names")

    indexes = tuple(
        tuple(validate_identifier(column) for column in index)
        for index in spec.get("indexes", [])
    )
    for index in indexes:
        if not index or not set(index) <= set(names):
            raise ValueError(f"Index {list(index)} of {table} isn't on its columns")
        validate_identifier(index_name(table, index))
    return TableSpec(validate_identifier(table), columns, indexes)


def parse_ddl(text: str) -> dict[str, TableSpec]:
    """Read the DDL file, job/mart_tables.json, which `make ddl` generates."""
    return {
        table: parse_table_spec(table, spec)
        for table, spec in json.loads(text)["tables"].items()
    }


def create_table_sql(spec: TableSpec) -> str:
    columns = ", ".join(
        f"{quote_identifier(name)} {data_type}" for name, data_type in spec.columns
    )
    return f"CREATE TABLE {staged_name(spec.name)} ({columns})"


def create_index_sql(spec: TableSpec) -> list[str]:
    """B-tree indexes, named for their columns. They move into mart with the table."""
    return [
        f"CREATE INDEX {index_name(spec.name, index)} ON {staged_name(spec.name)} "
        f"({', '.join(quote_identifier(column) for column in index)})"
        for index in spec.indexes
    ]


def get_boto3_client(service: str, config: Any = None):
    if boto3 is None:
        raise RuntimeError("boto3 is required for AWS operations")
    return boto3.client(service, region_name=REGION_NAME, config=config)


def error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None) or {}
    return response.get("Error", {}).get("Code")


def manifest_key(prefix: str, table: str) -> str:
    return f"{prefix}{table}/{table}{MANIFEST_SUFFIX}"


def parse_s3_url(url: str) -> tuple[str, str]:
    if not url.startswith("s3://"):
        raise ValueError(f"Not an S3 URL: {url}")
    bucket, _, key = url[len("s3://") :].partition("/")
    if not bucket or not key:
        raise ValueError(f"Not an S3 object URL: {url}")
    return bucket, key


def read_text(s3: Any, location: str) -> str:
    """Read an S3 object, or a local file in a local run."""
    if not location.startswith("s3://"):
        with open(location, encoding="utf-8") as handle:
            return handle.read()

    bucket, key = parse_s3_url(location)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def read_manifest(s3: Any, bucket: str, prefix: str, table: str) -> Manifest:
    """Read the UNLOAD manifest for a table, skipping empty parts.

    UNLOAD can write empty objects, and the Aurora import fails on a 0 byte file.
    """
    key = manifest_key(prefix, table)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error_code(error) in ("NoSuchKey", "404"):
            raise FileNotFoundError(
                f"No UNLOAD manifest at s3://{bucket}/{key}"
            ) from error
        raise

    entries = json.loads(response["Body"].read()).get("entries", [])
    last_modified = response.get("LastModified")
    if isinstance(last_modified, datetime):
        last_modified = last_modified.astimezone(timezone.utc).isoformat()

    parts = []
    empty_parts = 0
    for entry in entries:
        part_bucket, part_key = parse_s3_url(entry["url"])
        if part_bucket != bucket:
            raise ValueError(
                f"Manifest s3://{bucket}/{key} lists a part in another bucket: {entry['url']}"
            )
        content_length = int(entry.get("meta", {}).get("content_length", 0))
        if content_length == 0:
            empty_parts += 1
            continue
        parts.append(
            Part(bucket=part_bucket, key=part_key, content_length=content_length)
        )

    return Manifest(
        key=key,
        last_modified=str(last_modified),
        parts=tuple(parts),
        empty_parts=empty_parts,
    )


def list_manifest_tables(s3: Any, bucket: str, prefix: str) -> list[str]:
    """List the tables that have an UNLOAD manifest under the prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    tables = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            table, _, name = obj["Key"][len(prefix) :].partition("/")
            if name == f"{table}{MANIFEST_SUFFIX}":
                tables.append(table)
    return sorted(tables)


def ensure_gzip_encoding(s3: Any, part: Part) -> bool:
    """Set Content-Encoding: gzip on a part, returning whether it changed.

    The Aurora aws_s3 import only decompresses objects with that metadata, and
    UNLOAD doesn't set it. The object is copied onto itself with new metadata.
    """
    head = s3.head_object(Bucket=part.bucket, Key=part.key)
    if head.get("ContentEncoding") == "gzip":
        return False

    s3.copy_object(
        Bucket=part.bucket,
        Key=part.key,
        CopySource={"Bucket": part.bucket, "Key": part.key},
        MetadataDirective="REPLACE",
        Metadata=head.get("Metadata", {}),
        ContentEncoding="gzip",
        ContentType="text/csv",
    )
    return True


def tls_context(ca_bundle: str) -> ssl.SSLContext:
    """Trust only the RDS CAs, and check Aurora's certificate and host name.

    This is libpq's sslmode=verify-full. The bundle is the RDS regional CA bundle.
    """
    return ssl.create_default_context(cadata=ca_bundle)


def iam_auth_token(login: AuroraLogin) -> str:
    """A 15 minute IAM auth token, used as the password. It is signed locally."""
    return get_boto3_client("rds").generate_db_auth_token(
        DBHostname=login.host,
        Port=login.port,
        DBUsername=login.user,
        Region=REGION_NAME,
    )


class Database:
    """One PostgreSQL session on Aurora, through the pg8000 native API."""

    def __init__(self, connection: Any):
        self.connection = connection

    def execute(
        self, sql: str, parameters: dict[str, str] | None = None
    ) -> list[dict[str, Any]]:
        """Run one statement and return its rows as dicts."""
        rows = self.connection.run(sql, **(parameters or {}))
        names = [column["name"] for column in self.connection.columns or []]
        return [dict(zip(names, row)) for row in rows or []]

    def begin(self) -> None:
        self.execute("BEGIN")

    def commit(self) -> None:
        self.execute("COMMIT")

    def rollback(self) -> None:
        self.execute("ROLLBACK")

    def close(self) -> None:
        try:
            self.connection.close()
        except (InterfaceError, OSError) as error:
            # The server ends the session, and rolls back any open transaction, on its own.
            print(f"WARNING: closing the Aurora connection failed: {error}")


def connect(
    login: AuroraLogin,
    ssl_context: ssl.SSLContext,
    password: Callable[[AuroraLogin], str] = iam_auth_token,
) -> Database:
    """Log in to Aurora over TLS, with a new IAM auth token as the password."""
    if pg8000_native is None:
        raise RuntimeError("pg8000 is required; Glue installs it from the job's wheels")

    connection = pg8000_native.Connection(
        user=login.user,
        host=login.host,
        port=login.port,
        database=login.database,
        password=password(login),
        ssl_context=ssl_context,
        timeout=SOCKET_TIMEOUT_SECONDS,
        application_name=APPLICATION_NAME,
        startup_params={"statement_timeout": STATEMENT_TIMEOUT},
    )
    return Database(connection)


def server_error_fields(error: Exception) -> dict[str, str]:
    """The server's error fields on a pg8000 DatabaseError, such as C (SQLSTATE) and M."""
    fields = error.args[0] if error.args else None
    return fields if isinstance(fields, dict) else {}


def sqlstate(error: Exception) -> str | None:
    return server_error_fields(error).get("C")


def describe_error(error: Exception) -> str:
    """The error for the logs and the summary. A server error is its SQLSTATE and message."""
    fields = server_error_fields(error) if isinstance(error, DatabaseError) else {}
    if "M" in fields:
        detail = f" ({fields['D']})" if fields.get("D") else ""
        return f"{type(error).__name__}: {fields.get('C')} {fields['M']}{detail}"
    return f"{type(error).__name__}: {error}"


def wait_for_aurora(
    connect_db: Callable[[], Database],
    timeout_seconds: float = WAKE_TIMEOUT_SECONDS,
    interval_seconds: float = WAKE_INTERVAL_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    """Wait until Aurora accepts a login. It pauses at 0 ACU, and a login resumes it.

    Connection failures are retried. A database error other than "starting up", such
    as a refused login, is raised at once.
    """
    deadline = monotonic() + timeout_seconds
    while True:
        try:
            connect_db().close()
            return
        except (InterfaceError, DatabaseError) as error:
            if (
                isinstance(error, DatabaseError)
                and sqlstate(error) != STARTING_UP_SQLSTATE
            ):
                raise
            if monotonic() >= deadline:
                raise TimeoutError(
                    f"Aurora didn't accept a login within {timeout_seconds} seconds"
                ) from error
            print(f"Waiting for Aurora to accept a login: {describe_error(error)}")
            sleep(interval_seconds)


def import_options(null_marker: str = "") -> str:
    """The COPY options. HEADER MATCH fails if the CSV header and columns differ."""
    options = ["format csv", "header match"]
    if null_marker:
        options.append("null '{}'".format(null_marker.replace("'", "''")))
    return f"({', '.join(options)})"


def import_parameters(
    table: str, columns: list[str], part: Part, null_marker: str = ""
) -> dict[str, str]:
    """Import a part into the table's shadow table."""
    return {
        "table_name": staged_name(table),
        "column_list": ",".join(columns),
        "options": import_options(null_marker),
        "bucket": part.bucket,
        "key": part.key,
        "region": REGION_NAME,
    }


def parse_imported_rows(result: str) -> int:
    match = IMPORTED_ROWS_PATTERN.match(result or "")
    if not match:
        raise LoadError(f"Unexpected aws_s3 import result: {result!r}")
    return int(match.group(1))


def count_rows(db: Database, name: str) -> int:
    rows = db.execute(f"SELECT count(*) AS row_count FROM {name}")
    return int(rows[0]["row_count"])


def table_exists(db: Database, name: str) -> bool:
    return bool(db.execute(EXISTS_SQL, {"table_name": name})[0]["present"])


def swap_in(
    db: Database,
    table: str,
    attempts: int = SWAP_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Replace the live table with the shadow table, returning the attempt that did it.

    The old table is dropped first, which frees its index names, then the shadow table
    moves into mart with its indexes, statistics and grants. Both need the live table's
    exclusive lock, which waits for the queries using it, up to LOCK_TIMEOUT. A timeout
    rolls back to the savepoint and retries, without redoing the load.
    """
    live = qualified_name(table)
    db.execute("SAVEPOINT swap")
    for attempt in range(1, attempts + 1):
        try:
            db.execute(f"DROP TABLE IF EXISTS {live}")
            db.execute(f"ALTER TABLE {staged_name(table)} SET SCHEMA {SCHEMA_NAME}")
            return attempt
        except DatabaseError as error:
            if sqlstate(error) != LOCK_NOT_AVAILABLE_SQLSTATE or attempt == attempts:
                raise
            db.execute("ROLLBACK TO SAVEPOINT swap")
            print(
                f"Readers held {live} for {LOCK_TIMEOUT}; retrying the swap in "
                f"{SWAP_RETRY_SECONDS}s ({attempt} of {attempts})"
            )
            sleep(SWAP_RETRY_SECONDS)
    raise ValueError(f"attempts must be at least 1, not {attempts}")


def load_table(
    db: Database,
    spec: TableSpec,
    parts: tuple[Part, ...],
    result: TableResult,
    options: LoadOptions = LOAD_OPTIONS_DEFAULT,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Load the parts into a shadow table, then swap it in for the live table.

    It is one transaction, so no other session sees the shadow table, and an error
    leaves nothing behind. Until the swap, the live table is only read, so the API's
    queries carry on. The swap holds the live table's lock from the drop to the commit,
    a few milliseconds.
    """
    table = spec.name
    live, stage = qualified_name(table), staged_name(table)
    db.begin()
    try:
        db.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
        # A swap moves the shadow table out, so one here was left by hand.
        db.execute(f"DROP TABLE IF EXISTS {stage}")
        db.execute(create_table_sql(spec))

        imported = 0
        for part in parts:
            started = monotonic()
            rows = db.execute(
                IMPORT_SQL,
                import_parameters(table, spec.column_names, part, options.null_marker),
            )
            part_rows = parse_imported_rows(rows[0]["result"] if rows else "")
            imported += part_rows
            print(
                f"Imported {part_rows} rows from s3://{part.bucket}/{part.key} "
                f"in {monotonic() - started:.1f}s"
            )

        rows_after = count_rows(db, stage)
        if rows_after != imported:
            raise LoadError(f"{stage} has {rows_after} rows after importing {imported}")
        result.rows_before = count_rows(db, live) if table_exists(db, live) else None
        if rows_after == 0 and result.rows_before and not options.allow_empty:
            raise LoadError(
                f"{live} would go from {result.rows_before} rows to 0. "
                "Check the unload, or rerun with --allow_empty true."
            )

        # Built after the load, which is faster than keeping them up to date during it.
        # The statistics are ready before the first query of the new table.
        for sql in create_index_sql(spec):
            db.execute(sql)
        db.execute(f"ANALYZE {stage}")
        for reader in options.readers:
            db.execute(f"GRANT SELECT ON {stage} TO {validate_identifier(reader)}")

        attempt = swap_in(db, table, sleep=sleep)
        db.commit()
    except Exception:
        try:
            db.rollback()
        except (DatabaseError, InterfaceError, OSError) as rollback_error:
            # Keep the original error. The server rolls back when the session ends.
            print(f"Rollback of {live} failed: {describe_error(rollback_error)}")
        raise

    result.rows_after = rows_after
    before = "new table" if result.rows_before is None else result.rows_before
    print(
        f"Loaded {live}: {before} -> {rows_after} rows, swapped in on attempt {attempt}"
    )


def process_table(
    s3: Any,
    connect_db: Callable[[], Database] | None,
    bucket: str,
    prefix: str,
    table: str,
    specs: dict[str, TableSpec],
    dry_run: bool,
    options: LoadOptions = LOAD_OPTIONS_DEFAULT,
) -> TableResult:
    """Prepare one table's parts and load them, returning the outcome."""
    result = TableResult(table=table)
    try:
        if table not in specs:
            raise LoadError(
                f"The DDL file has no {table} table. Add it with `make ddl`."
            )
        manifest = read_manifest(s3, bucket, prefix, table)
        result.manifest = f"s3://{bucket}/{manifest.key}"
        result.manifest_last_modified = manifest.last_modified
        result.parts = len(manifest.parts)
        result.bytes = sum(part.content_length for part in manifest.parts)

        for part in manifest.parts:
            if ensure_gzip_encoding(s3, part):
                result.encodings_fixed += 1

        if dry_run or connect_db is None:
            result.status = "dry_run"
            return result

        # A session per table, so a dropped connection fails only that table.
        db = connect_db()
        try:
            load_table(db, specs[table], manifest.parts, result, options)
        finally:
            db.close()
        result.status = "loaded"
    except TABLE_ERRORS as error:
        # Every table is attempted; load_marts fails the job at the end.
        result.status = "failed"
        result.error = describe_error(error)
        print(f"Failed {qualified_name(table)}: {result.error}")
    return result


def build_summary(
    results: list[TableResult],
    source_uri: str,
    unlisted_tables: list[str],
    dry_run: bool,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source_uri,
        "target_schema": SCHEMA_NAME,
        "dry_run": dry_run,
        "tables": [asdict(result) for result in results],
        "unlisted_tables": unlisted_tables,
    }


def upload_summary(
    s3: Any, bucket: str, s3_mid_path: str, summary: dict[str, Any]
) -> str:
    key = f"{s3_mid_path}/{SUMMARY_FILE_NAME}"
    body = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    s3.put_object(
        Bucket=bucket, Key=key, Body=body.encode(), ContentType="application/json"
    )
    uri = f"s3://{bucket}/{key}"
    print(f"Uploaded summary: {uri}")
    return uri


def load_marts(
    s3: Any,
    connect_db: Callable[[], Database] | None,
    source_bucket: str,
    source_prefix: str,
    tables: tuple[str, ...],
    specs: dict[str, TableSpec],
    lz_bucket: str,
    s3_mid_path: str,
    dry_run: bool,
    options: LoadOptions = LOAD_OPTIONS_DEFAULT,
) -> list[TableResult]:
    """Load every table, upload the summary, and raise if any table failed."""
    if connect_db is not None and not dry_run:
        wait_for_aurora(connect_db)

    results = [
        process_table(
            s3,
            connect_db,
            source_bucket,
            source_prefix,
            table,
            specs,
            dry_run,
            options,
        )
        for table in tables
    ]

    unlisted = sorted(
        set(list_manifest_tables(s3, source_bucket, source_prefix)) - set(tables)
    )
    if unlisted:
        print(f"Manifests not in --mart_tables, not loaded: {', '.join(unlisted)}")

    summary = build_summary(
        results, f"s3://{source_bucket}/{source_prefix}", unlisted, dry_run
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    upload_summary(s3, lz_bucket, s3_mid_path, summary)

    failed = [result.table for result in results if result.status == "failed"]
    if failed:
        raise RuntimeError(
            f"{len(failed)} of {len(results)} mart tables failed: {', '.join(failed)}"
        )
    return results


GlueJobBase = Job if Job is not None else object


class GlueSpreadsheetMartTables(GlueJobBase):
    def __init__(self, glue_context: GlueContext):
        if Job is None:
            raise RuntimeError(
                "GlueSpreadsheetMartTables must run inside AWS Glue or Glue libs"
            )

        super().__init__(glue_context)

        self.glue_context: GlueContext = glue_context
        self.spark: SparkSession = glue_context.spark_session

        params = [
            "lz_bucket",
            "source_bucket",
            "source_prefix",
            "mart_ddl",
        ]

        optional = [
            "JOB_NAME",
            "base_name",
            "s3_mid_path",
            "aurora_host",
            "aurora_port",
            "aurora_user",
            "aurora_database",
            "aurora_ca_bundle",
            "mart_tables",
            "mart_readers",
            "null_marker",
            "allow_empty",
            "dry_run",
        ]
        params += [name for name in optional if f"--{name}" in sys.argv]

        args = getResolvedOptions(sys.argv, params)

        self.lz_bucket = args["lz_bucket"]
        self.source_bucket = args["source_bucket"]
        self.source_prefix = args["source_prefix"]
        self.mart_ddl = args["mart_ddl"]
        self.tables = parse_tables(args.get("mart_tables"))
        self.options = LoadOptions(
            readers=parse_readers(args.get("mart_readers")),
            null_marker=args.get("null_marker", ""),
            allow_empty=parse_bool(args.get("allow_empty")),
        )
        self.dry_run = parse_bool(args.get("dry_run"))
        self.s3_mid_path = args.get("s3_mid_path", S3_MID_PATH_DEFAULT)

        # A dry run doesn't touch Aurora, so it runs without the Aurora options.
        self.login = None
        self.ca_bundle = args.get("aurora_ca_bundle")
        if not self.dry_run:
            if "aurora_host" not in args or not self.ca_bundle:
                raise ValueError(
                    "--aurora_host and --aurora_ca_bundle are required unless --dry_run is true"
                )
            self.login = AuroraLogin(
                host=args["aurora_host"],
                port=int(args.get("aurora_port", DB_PORT_DEFAULT)),
                user=args.get("aurora_user", DB_USER_DEFAULT),
                database=args.get("aurora_database", DB_NAME_DEFAULT),
            )

        job_name = args.get("JOB_NAME", "GlueSpreadsheetMartTables")
        self.init(job_name, args)

    def run(self) -> None:
        s3 = get_boto3_client("s3")
        # A dry run reads the DDL file too, so it catches a table the file lacks.
        specs = parse_ddl(read_text(s3, self.mart_ddl))
        connect_db = None
        if self.login is None:
            print("Dry run enabled: skipping the Aurora load")
        else:
            ssl_context = tls_context(read_text(s3, self.ca_bundle))
            connect_db = functools.partial(connect, self.login, ssl_context)

        load_marts(
            s3=s3,
            connect_db=connect_db,
            source_bucket=self.source_bucket,
            source_prefix=self.source_prefix,
            tables=self.tables,
            specs=specs,
            lz_bucket=self.lz_bucket,
            s3_mid_path=self.s3_mid_path,
            dry_run=self.dry_run,
            options=self.options,
        )
        self.commit()


if __name__ == "__main__":
    if GlueContext is None or SparkSession is None:
        raise RuntimeError("Run this script with AWS Glue libs or spark-submit")

    session = SparkSession.builder.getOrCreate()
    gc = GlueContext(session.sparkContext)
    GlueSpreadsheetMartTables(gc).run()
