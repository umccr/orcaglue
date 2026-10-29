"""Reverse ETL: load the OrcaVault mart CSV unload into Aurora PostgreSQL.

A Redshift UNLOAD writes each mart table as gzip CSV parts with a manifest:

    <source_prefix><table>/<table>_<slice>_part_00.gz
    <source_prefix><table>/<table>_manifest

For every table, this job sets Content-Encoding: gzip on the parts, which the
Aurora aws_s3 import requires and UNLOAD doesn't set, then replaces the Aurora
table. The job reaches the private cluster through a Glue network connection in
its VPC, and logs in over TLS with an IAM auth token. TRUNCATE and one
aws_s3.table_import_from_s3 call per part run in one transaction per table, so
a failed table keeps its previous rows.
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
DB_NAME_DEFAULT = "orcanexus"
DB_USER_DEFAULT = "dbuser_rw"
DB_PORT_DEFAULT = 5432
REGION_NAME = "ap-southeast-2"
APPLICATION_NAME = "orcaglue-spreadsheet-mart-tables"
SUMMARY_FILE_NAME = "orcanexus_mart_tables.manifest.json"
MANIFEST_SUFFIX = "_manifest"

# The mart tables unloaded from Redshift. Keep in sync with job/init.sql and the
# mart-tables stack config.
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

# Table and column names are lowercase identifiers that need no quoting. They are
# validated before they are put into SQL.
IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
IMPORTED_ROWS_PATTERN = re.compile(r"^(\d+) rows imported")

LOCK_TIMEOUT = "30s"
# The server ends any statement that runs longer than this. The socket timeout only
# catches a dead connection, so it must be longer.
STATEMENT_TIMEOUT = "10min"
SOCKET_TIMEOUT_SECONDS = 15 * 60
WAKE_TIMEOUT_SECONDS = 180
WAKE_INTERVAL_SECONDS = 10
# SQLSTATE cannot_connect_now: the server is starting up, as when Aurora resumes.
STARTING_UP_SQLSTATE = "57P03"

IMPORT_SQL = (
    "SELECT aws_s3.table_import_from_s3("
    ":table_name, :column_list, :options, "
    "aws_commons.create_s3_uri(:bucket, :key, :region)) AS result"
)
COLUMNS_SQL = (
    "SELECT column_name FROM information_schema.columns "
    "WHERE table_schema = :schema_name AND table_name = :table_name "
    "ORDER BY ordinal_position"
)


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


def parse_tables(value: str | None) -> tuple[str, ...]:
    if not value:
        return MART_TABLES_DEFAULT

    tables = [table.strip() for table in value.split(",") if table.strip()]
    if not tables:
        raise ValueError("--mart_tables names no tables")
    return tuple(dict.fromkeys(validate_identifier(table) for table in tables))


def qualified_name(table: str) -> str:
    return f"{SCHEMA_NAME}.{validate_identifier(table)}"


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


def read_s3_text(s3: Any, url: str) -> str:
    bucket, key = parse_s3_url(url)
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


def table_columns(db: Database, table: str) -> list[str]:
    rows = db.execute(
        COLUMNS_SQL,
        {"schema_name": SCHEMA_NAME, "table_name": validate_identifier(table)},
    )
    if not rows:
        raise LoadError(
            f"{qualified_name(table)} doesn't exist in Aurora. Run job/init.sql first."
        )
    return [validate_identifier(row["column_name"]) for row in rows]


def import_options(null_marker: str = "") -> str:
    """The COPY options. HEADER MATCH fails if the CSV header and columns differ."""
    options = ["format csv", "header match"]
    if null_marker:
        options.append("null '{}'".format(null_marker.replace("'", "''")))
    return f"({', '.join(options)})"


def import_parameters(
    table: str, columns: list[str], part: Part, null_marker: str = ""
) -> dict[str, str]:
    return {
        "table_name": qualified_name(table),
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


def count_rows(db: Database, table: str) -> int:
    rows = db.execute(f"SELECT count(*) AS row_count FROM {qualified_name(table)}")
    return int(rows[0]["row_count"])


def load_table(
    db: Database,
    table: str,
    columns: list[str],
    parts: tuple[Part, ...],
    result: TableResult,
    null_marker: str = "",
    allow_empty: bool = False,
    monotonic: Callable[[], float] = time.monotonic,
) -> None:
    """Replace the table with the parts in one transaction, then analyze it."""
    name = qualified_name(table)
    db.begin()
    try:
        db.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
        result.rows_before = count_rows(db, table)
        db.execute(f"TRUNCATE TABLE {name}")

        imported = 0
        for part in parts:
            started = monotonic()
            rows = db.execute(
                IMPORT_SQL, import_parameters(table, columns, part, null_marker)
            )
            part_rows = parse_imported_rows(rows[0]["result"] if rows else "")
            imported += part_rows
            print(
                f"Imported {part_rows} rows from s3://{part.bucket}/{part.key} "
                f"in {monotonic() - started:.1f}s"
            )

        rows_after = count_rows(db, table)
        if rows_after != imported:
            raise LoadError(f"{name} has {rows_after} rows after importing {imported}")
        if rows_after == 0 and result.rows_before and not allow_empty:
            raise LoadError(
                f"{name} would go from {result.rows_before} rows to 0. "
                "Check the unload, or rerun with --allow_empty true."
            )
        db.commit()
    except Exception:
        try:
            db.rollback()
        except (DatabaseError, InterfaceError, OSError) as rollback_error:
            # Keep the original error. The server rolls back when the session ends.
            print(f"Rollback of {name} failed: {describe_error(rollback_error)}")
        raise

    result.rows_after = rows_after
    print(f"Loaded {name}: {result.rows_before} -> {rows_after} rows")
    try:
        db.execute(f"ANALYZE {name}")
    except (DatabaseError, InterfaceError, OSError) as error:
        # The rows are committed. Stale statistics only slow queries until autovacuum runs.
        print(
            f"WARNING: ANALYZE {name} failed; the load is kept: {describe_error(error)}"
        )


def process_table(
    s3: Any,
    connect_db: Callable[[], Database] | None,
    bucket: str,
    prefix: str,
    table: str,
    dry_run: bool,
    null_marker: str = "",
    allow_empty: bool = False,
) -> TableResult:
    """Prepare one table's parts and load them, returning the outcome."""
    result = TableResult(table=table)
    try:
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
            columns = table_columns(db, table)
            load_table(
                db, table, columns, manifest.parts, result, null_marker, allow_empty
            )
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
    lz_bucket: str,
    s3_mid_path: str,
    dry_run: bool,
    null_marker: str = "",
    allow_empty: bool = False,
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
            dry_run,
            null_marker,
            allow_empty,
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
            "null_marker",
            "allow_empty",
            "dry_run",
        ]
        params += [name for name in optional if f"--{name}" in sys.argv]

        args = getResolvedOptions(sys.argv, params)

        self.lz_bucket = args["lz_bucket"]
        self.source_bucket = args["source_bucket"]
        self.source_prefix = args["source_prefix"]
        self.tables = parse_tables(args.get("mart_tables"))
        self.null_marker = args.get("null_marker", "")
        self.allow_empty = parse_bool(args.get("allow_empty"))
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
        connect_db = None
        if self.login is None:
            print("Dry run enabled: skipping the Aurora load")
        else:
            ssl_context = tls_context(read_s3_text(s3, self.ca_bundle))
            connect_db = functools.partial(connect, self.login, ssl_context)

        load_marts(
            s3=s3,
            connect_db=connect_db,
            source_bucket=self.source_bucket,
            source_prefix=self.source_prefix,
            tables=self.tables,
            lz_bucket=self.lz_bucket,
            s3_mid_path=self.s3_mid_path,
            dry_run=self.dry_run,
            null_marker=self.null_marker,
            allow_empty=self.allow_empty,
        )
        self.commit()


if __name__ == "__main__":
    if GlueContext is None or SparkSession is None:
        raise RuntimeError("Run this script with AWS Glue libs or spark-submit")

    session = SparkSession.builder.getOrCreate()
    gc = GlueContext(session.sparkContext)
    GlueSpreadsheetMartTables(gc).run()
