"""Reverse ETL: load the OrcaVault mart CSV unload into Aurora PostgreSQL.

A Redshift UNLOAD writes each mart table as gzip CSV parts with a manifest:

    <source_prefix><table>/<table>_<slice>_part_00.gz
    <source_prefix><table>/<table>_manifest

For every table, this job sets Content-Encoding: gzip on the parts, which the
Aurora aws_s3 import requires and UNLOAD doesn't set, then replaces the Aurora
table through the RDS Data API. TRUNCATE and one aws_s3.table_import_from_s3
call per part run in one transaction per table, so a failed table keeps its
previous rows.
"""

from __future__ import annotations

import json
import re
import sys
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError
except ModuleNotFoundError:  # pragma: no cover
    boto3 = None
    Config = None
    BotoCoreError = ClientError = Exception

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
REGION_NAME = "ap-southeast-2"
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
WAKE_TIMEOUT_SECONDS = 180
WAKE_INTERVAL_SECONDS = 10
# The Data API cancels a call after 45 seconds. Warn when an import gets close.
SLOW_IMPORT_SECONDS = 30

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


class LoadError(RuntimeError):
    """A table load that was rolled back."""


# The failures that fail one table but let the others load: AWS errors, a missing
# or malformed manifest (OSError, ValueError, KeyError), and rolled back loads.
TABLE_ERRORS = (ClientError, BotoCoreError, LoadError, OSError, ValueError, KeyError)


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


def get_rds_data_client():
    # Every call waits up to the 45 second Data API limit, and is never retried
    # by the SDK, so a statement can't run twice.
    config = Config(read_timeout=60, connect_timeout=10, retries={"max_attempts": 1})
    return get_boto3_client("rds-data", config=config)


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


class DataApi:
    """A small RDS Data API client for one database."""

    def __init__(self, client: Any, cluster_arn: str, secret_arn: str, database: str):
        self.client = client
        self.target = {"resourceArn": cluster_arn, "secretArn": secret_arn}
        self.database = database

    def execute(
        self,
        sql: str,
        parameters: dict[str, str] | None = None,
        transaction_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Run one statement and return its rows as dicts."""
        kwargs: dict[str, Any] = {
            **self.target,
            "database": self.database,
            "sql": sql,
            "formatRecordsAs": "JSON",
        }
        if parameters:
            kwargs["parameters"] = [
                {"name": name, "value": {"stringValue": value}}
                for name, value in parameters.items()
            ]
        if transaction_id:
            kwargs["transactionId"] = transaction_id

        response = self.client.execute_statement(**kwargs)
        records = response.get("formattedRecords")
        return json.loads(records) if records else []

    def begin(self) -> str:
        response = self.client.begin_transaction(**self.target, database=self.database)
        return response["transactionId"]

    def commit(self, transaction_id: str) -> None:
        self.client.commit_transaction(**self.target, transactionId=transaction_id)

    def rollback(self, transaction_id: str) -> None:
        self.client.rollback_transaction(**self.target, transactionId=transaction_id)

    def wake(
        self,
        timeout_seconds: float = WAKE_TIMEOUT_SECONDS,
        interval_seconds: float = WAKE_INTERVAL_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        """Wait for the cluster to answer. It pauses at 0 ACU and resumes on demand."""
        deadline = monotonic() + timeout_seconds
        while True:
            try:
                self.execute("SELECT 1")
                return
            except ClientError as error:
                if error_code(error) != "DatabaseResumingException":
                    raise
                if monotonic() >= deadline:
                    raise TimeoutError(
                        f"Aurora didn't resume within {timeout_seconds} seconds"
                    ) from error
                print("Aurora is resuming; waiting...")
                sleep(interval_seconds)


def table_columns(api: DataApi, table: str) -> list[str]:
    rows = api.execute(
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


def count_rows(api: DataApi, table: str, transaction_id: str | None = None) -> int:
    rows = api.execute(
        f"SELECT count(*) AS row_count FROM {qualified_name(table)}",
        transaction_id=transaction_id,
    )
    return int(rows[0]["row_count"])


def load_table(
    api: DataApi,
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
    transaction_id = api.begin()
    try:
        api.execute(
            f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'", transaction_id=transaction_id
        )
        result.rows_before = count_rows(api, table, transaction_id)
        api.execute(f"TRUNCATE TABLE {name}", transaction_id=transaction_id)

        imported = 0
        for part in parts:
            started = monotonic()
            rows = api.execute(
                IMPORT_SQL,
                import_parameters(table, columns, part, null_marker),
                transaction_id,
            )
            part_rows = parse_imported_rows(rows[0]["result"] if rows else "")
            elapsed = monotonic() - started
            imported += part_rows
            slow = (
                " (slow, close to the 45 second Data API limit)"
                if elapsed > SLOW_IMPORT_SECONDS
                else ""
            )
            print(
                f"Imported {part_rows} rows from s3://{part.bucket}/{part.key} in {elapsed:.1f}s{slow}"
            )

        rows_after = count_rows(api, table, transaction_id)
        if rows_after != imported:
            raise LoadError(f"{name} has {rows_after} rows after importing {imported}")
        if rows_after == 0 and result.rows_before and not allow_empty:
            raise LoadError(
                f"{name} would go from {result.rows_before} rows to 0. "
                "Check the unload, or rerun with --allow_empty true."
            )
        api.commit(transaction_id)
    except Exception:
        try:
            api.rollback(transaction_id)
        except (ClientError, BotoCoreError) as rollback_error:
            # Keep the original error; the transaction times out on its own.
            print(f"Rollback of {name} failed: {rollback_error}")
        raise

    result.rows_after = rows_after
    print(f"Loaded {name}: {result.rows_before} -> {rows_after} rows")
    try:
        api.execute(f"ANALYZE {name}")
    except (ClientError, BotoCoreError) as error:
        # The rows are committed. Stale statistics only slow queries until autovacuum runs.
        print(f"WARNING: ANALYZE {name} failed; the load is kept: {error}")


def process_table(
    s3: Any,
    api: DataApi | None,
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

        if dry_run or api is None:
            result.status = "dry_run"
            return result

        columns = table_columns(api, table)
        load_table(
            api, table, columns, manifest.parts, result, null_marker, allow_empty
        )
        result.status = "loaded"
    except TABLE_ERRORS as error:
        # Every table is attempted; load_marts fails the job at the end.
        result.status = "failed"
        result.error = f"{type(error).__name__}: {error}"
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
    api: DataApi | None,
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
    if api is not None and not dry_run:
        api.wake()

    results = [
        process_table(
            s3,
            api,
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
            "aurora_cluster_arn",
            "aurora_secret_arn",
        ]

        optional = [
            "JOB_NAME",
            "base_name",
            "s3_mid_path",
            "aurora_database",
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
        self.cluster_arn = args["aurora_cluster_arn"]
        self.secret_arn = args["aurora_secret_arn"]
        self.database = args.get("aurora_database", DB_NAME_DEFAULT)
        self.tables = parse_tables(args.get("mart_tables"))
        self.null_marker = args.get("null_marker", "")
        self.allow_empty = parse_bool(args.get("allow_empty"))
        self.dry_run = parse_bool(args.get("dry_run"))
        self.s3_mid_path = args.get("s3_mid_path", S3_MID_PATH_DEFAULT)

        job_name = args.get("JOB_NAME", "GlueSpreadsheetMartTables")
        self.init(job_name, args)

    def run(self) -> None:
        s3 = get_boto3_client("s3")
        api = None
        if not self.dry_run:
            api = DataApi(
                get_rds_data_client(), self.cluster_arn, self.secret_arn, self.database
            )
        else:
            print("Dry run enabled: skipping the Aurora load")

        load_marts(
            s3=s3,
            api=api,
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
