from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

try:
    import boto3
except ModuleNotFoundError:  # pragma: no cover
    boto3 = None

try:
    from libumccr.aws import libs3
except ModuleNotFoundError:  # pragma: no cover
    libs3 = None

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


BASE_NAME_DEFAULT = "csv__ica_usage_report"
S3_MID_PATH_DEFAULT = f"orcaglue/{BASE_NAME_DEFAULT}/dev"
S3_SOURCE_PREFIX_DEFAULT = "ica-usage-reports/"
SCHEMA_NAME = "tsa"
DB_NAME = "orcavault"
REGION_NAME = "ap-southeast-2"
REDSHIFT_STATEMENT_TIMEOUT_SECONDS = 10 * 60
REDSHIFT_POLL_INTERVAL_SECONDS = 2

EXPECTED_COLUMNS = (
    "usage_id",
    "uc_name",
    "billable_account_id",
    "account_name",
    "account_type",
    "usage_context",
    "usage_context_type",
    "user_name",
    "product",
    "usage_type_description",
    "quantity",
    "usage_unit",
    "price_per_unit",
    "cost",
    "cost_unit",
    "category",
    "usage_timestamp",
    "region",
    "metadata",
    "billing_date",
)

# Resolved at runtime in GlueIcaUsageReport constructor.
BASE_NAME = BASE_NAME_DEFAULT
S3_MID_PATH = S3_MID_PATH_DEFAULT
S3_SOURCE_PREFIX = S3_SOURCE_PREFIX_DEFAULT
OUT_NAME = f"{DB_NAME}_{SCHEMA_NAME}_{BASE_NAME}"
OUT_NAME_DOT = f"{DB_NAME}.{SCHEMA_NAME}.{BASE_NAME}"
OUT_PATH = f"/tmp/{OUT_NAME}"


@dataclass(frozen=True)
class SourceObject:
    key: str
    size: int
    etag: str
    last_modified: str


@dataclass(frozen=True)
class DownloadedObject:
    source: SourceObject
    local_path: str


@dataclass(frozen=True)
class TransformResult:
    csv_file: str
    sql_file: str
    manifest_file: str
    row_count: int
    duplicate_usage_billing_count: int
    source_count: int


def resolve_output_paths(base_name: str) -> tuple[str, str, str]:
    out_name = f"{DB_NAME}_{SCHEMA_NAME}_{base_name}"
    out_name_dot = f"{DB_NAME}.{SCHEMA_NAME}.{base_name}"
    out_path = f"/tmp/{out_name}"
    return out_name, out_name_dot, out_path


def parse_bool(value: str | bool | None, default: bool = True) -> bool:
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


def get_boto3_client(service: str):
    if boto3 is None:
        raise RuntimeError("boto3 is required for AWS operations")
    return boto3.client(service, region_name=REGION_NAME)


def get_s3_client():
    if libs3 is not None:
        return libs3.s3_client()
    return get_boto3_client("s3")


def normalise_column_name(name: str) -> str:
    normalised = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    return re.sub(r"_+", "_", normalised).strip("_")


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def table_name(suffix: str = "", include_database: bool = False) -> str:
    prefix = f"{DB_NAME}." if include_database else ""
    return f"{prefix}{SCHEMA_NAME}.{BASE_NAME}{suffix}"


def quoted_columns() -> str:
    return ", ".join(f'"{column}"' for column in EXPECTED_COLUMNS)


def create_table_sql(name: str) -> str:
    columns = ",\n    ".join(
        f"{column.ljust(28)} varchar(65535)" for column in EXPECTED_COLUMNS
    )
    return f"""CREATE TABLE IF NOT EXISTS {name}
(
    {columns}
);"""


def init_sql() -> str:
    return "\n\n".join(
        [
            create_table_sql(table_name(include_database=True)),
            create_table_sql(table_name("__staging", include_database=True)),
            create_table_sql(table_name("__previous", include_database=True)),
        ]
    )


def build_staging_load_sql(s3_csv_uri: str, role: str) -> list[str]:
    staging_table = table_name("__staging")
    return [
        f"DELETE FROM {staging_table};",
        f"""
COPY {staging_table} ({quoted_columns()})
FROM '{sql_literal(s3_csv_uri)}'
IAM_ROLE '{sql_literal(role)}'
FORMAT AS CSV
IGNOREHEADER 1
EMPTYASNULL
BLANKSASNULL
REGION '{REGION_NAME}';
""",
    ]


def build_target_swap_sql() -> list[str]:
    target_table = table_name()
    staging_table = table_name("__staging")
    previous_table = table_name("__previous")
    columns = quoted_columns()

    return [
        f"DELETE FROM {previous_table};",
        (
            f"INSERT INTO {previous_table} ({columns}) "
            f"SELECT {columns} FROM {target_table};"
        ),
        f"DELETE FROM {target_table};",
        (
            f"INSERT INTO {target_table} ({columns}) "
            f"SELECT {columns} FROM {staging_table};"
        ),
    ]


def extract(s3_bucket_name: str, source_prefix: str) -> list[DownloadedObject]:
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket_name, Prefix=source_prefix)

    source_objects = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith(".csv"):
                continue
            last_modified = obj["LastModified"]
            if isinstance(last_modified, datetime):
                last_modified = last_modified.astimezone(timezone.utc).isoformat()
            source_objects.append(
                SourceObject(
                    key=key,
                    size=int(obj.get("Size", 0)),
                    etag=str(obj.get("ETag", "")).strip('"'),
                    last_modified=str(last_modified),
                )
            )

    source_objects = sorted(source_objects, key=lambda item: item.key)
    if not source_objects:
        raise RuntimeError(
            f"No CSV files found under s3://{s3_bucket_name}/{source_prefix}"
        )

    downloaded = []
    for index, source in enumerate(source_objects, start=1):
        filename = Path(source.key).name
        local_path = f"{OUT_PATH}__{index:04d}__{filename}"
        s3_client.download_file(s3_bucket_name, source.key, local_path)
        downloaded.append(DownloadedObject(source=source, local_path=local_path))
        print(f"Downloaded: s3://{s3_bucket_name}/{source.key} -> {local_path}")

    return downloaded


def _drop_helper_columns(df: pl.DataFrame) -> pl.DataFrame:
    helper_columns = [
        column
        for column in df.columns
        if column == ""
        or column.startswith("unnamed")
        or column.startswith("duplicated")
        or column.startswith("_duplicated")
    ]
    return df.drop(helper_columns) if helper_columns else df


def _read_source_csv(path: str) -> pl.DataFrame:
    df = pl.read_csv(path, infer_schema_length=False, infer_schema=False)
    df.columns = [normalise_column_name(column) for column in df.columns]
    df = _drop_helper_columns(df)

    extra_columns = sorted(set(df.columns) - set(EXPECTED_COLUMNS))
    if extra_columns:
        raise ValueError(
            f"Unexpected columns in {path}: {', '.join(extra_columns)}. "
            "Update the TSA table and EXPECTED_COLUMNS before loading this report."
        )

    for column in EXPECTED_COLUMNS:
        if column not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.String).alias(column))

    df = df.select(list(EXPECTED_COLUMNS))
    df = df.with_columns(pl.col(pl.String).str.strip_chars())
    df = df.with_columns(
        pl.when(pl.col(pl.String).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.String))
        .name.keep()
    )
    return df.filter(~pl.all_horizontal(pl.all().is_null()))


def transform(downloaded: list[DownloadedObject]) -> TransformResult:
    if not downloaded:
        raise RuntimeError("No downloaded CSV files to transform")

    frames = []
    for item in downloaded:
        df = _read_source_csv(item.local_path)
        frames.append(df)
        print(item.local_path, df.columns, f"rows={df.height}")

    df = pl.concat(frames)
    duplicate_count = (
        0
        if df.is_empty()
        else int(
            df.select(
                pl.struct(["usage_id", "billing_date"]).is_duplicated().sum()
            ).item()
        )
    )

    csv_file = f"{OUT_PATH}.csv"
    sql_file = f"{OUT_PATH}.sql"
    manifest_file = f"{OUT_PATH}.manifest.json"

    df.write_csv(csv_file)

    with open(sql_file, "w", newline="") as handle:
        handle.write(init_sql())

    manifest = {
        "base_name": BASE_NAME,
        "target_table": table_name(include_database=True),
        "staging_table": table_name("__staging", include_database=True),
        "previous_table": table_name("__previous", include_database=True),
        "source_prefix": S3_SOURCE_PREFIX,
        "source_objects": [asdict(item.source) for item in downloaded],
        "source_count": len(downloaded),
        "row_count": df.height,
        "duplicate_usage_billing_count": duplicate_count,
        "expected_columns": list(EXPECTED_COLUMNS),
        "csv_sha256": sha256_file(csv_file),
    }

    with open(manifest_file, "w", newline="") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print(init_sql())
    print(json.dumps(manifest, indent=2, sort_keys=True))

    return TransformResult(
        csv_file=csv_file,
        sql_file=sql_file,
        manifest_file=manifest_file,
        row_count=df.height,
        duplicate_usage_billing_count=duplicate_count,
        source_count=len(downloaded),
    )


def upload_artifacts(bucket: str, result: TransformResult) -> dict[str, str]:
    s3_client = get_s3_client()
    artifacts = {}

    for path in [result.csv_file, result.sql_file, result.manifest_file]:
        key = f"{S3_MID_PATH}/{os.path.basename(path)}"
        s3_client.upload_file(path, bucket, key)
        artifacts[Path(path).suffix.lstrip(".") or "file"] = f"s3://{bucket}/{key}"
        print(f"Uploaded: {path} -> s3://{bucket}/{key}")

    return artifacts


def _wait_for_query(
    client: Any,
    statement_id: str,
    timeout_seconds: int = REDSHIFT_STATEMENT_TIMEOUT_SECONDS,
    poll_interval_seconds: int = REDSHIFT_POLL_INTERVAL_SECONDS,
) -> None:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than zero")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be greater than zero")

    deadline = time.monotonic() + timeout_seconds
    last_status = "UNKNOWN"

    while True:
        response = client.describe_statement(Id=statement_id)
        last_status = response["Status"]
        if last_status == "FINISHED":
            print(f"Statement {statement_id} finished successfully.")
            return
        if last_status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Redshift Data API statement {statement_id} failed. "
                f"Error: {response.get('Error')}"
            )

        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            raise TimeoutError(
                f"Timed out after {timeout_seconds} seconds waiting for "
                f"Redshift Data API statement {statement_id}; "
                f"last status={last_status}"
            )

        print(f"Statement {statement_id} status: {last_status}; waiting...")
        time.sleep(min(poll_interval_seconds, remaining_seconds))


def _batch_execute(client: Any, workgroup: str, sqls: list[str], name: str) -> None:
    response = client.batch_execute_statement(
        WorkgroupName=workgroup,
        Database=DB_NAME,
        Sqls=sqls,
        StatementName=name,
    )
    _wait_for_query(client, response["Id"])


def _fetch_count(client: Any, workgroup: str, sql: str) -> int:
    response = client.execute_statement(
        WorkgroupName=workgroup,
        Database=DB_NAME,
        Sql=sql,
    )
    statement_id = response["Id"]
    _wait_for_query(client, statement_id)

    result = client.get_statement_result(Id=statement_id)
    value = result["Records"][0][0]
    if "longValue" in value:
        return int(value["longValue"])
    if "stringValue" in value:
        return int(value["stringValue"])
    raise RuntimeError(f"Cannot parse Redshift count result: {value}")


def load_to_redshift(
    workgroup: str,
    role: str,
    csv_s3_uri: str,
    expected_rows: int,
) -> None:
    redshift_data_client = get_boto3_client("redshift-data")
    target_table = table_name()
    staging_table = table_name("__staging")

    print(f"Loading staging table: {staging_table} from {csv_s3_uri}")
    _batch_execute(
        redshift_data_client,
        workgroup,
        build_staging_load_sql(csv_s3_uri, role),
        f"{BASE_NAME}-stage-load",
    )

    staging_count = _fetch_count(
        redshift_data_client,
        workgroup,
        f"SELECT COUNT(*) FROM {staging_table};",
    )
    if staging_count != expected_rows:
        raise RuntimeError(
            f"Staging row count mismatch: expected {expected_rows}, got {staging_count}"
        )

    print(f"Replacing target table: {target_table}")
    _batch_execute(
        redshift_data_client,
        workgroup,
        build_target_swap_sql(),
        f"{BASE_NAME}-target-swap",
    )

    target_count = _fetch_count(
        redshift_data_client,
        workgroup,
        f"SELECT COUNT(*) FROM {target_table};",
    )
    if target_count != expected_rows:
        raise RuntimeError(
            f"Target row count mismatch: expected {expected_rows}, got {target_count}"
        )

    print(f"Load complete: {target_table} rows={target_count}")


def load(bucket: str, workgroup: str, role: str, result: TransformResult) -> None:
    artifact_uris = upload_artifacts(bucket, result)
    load_to_redshift(
        workgroup=workgroup,
        role=role,
        csv_s3_uri=artifact_uris["csv"],
        expected_rows=result.row_count,
    )


def clean_up() -> None:
    pass


GlueJobBase = Job if Job is not None else object


class GlueIcaUsageReport(GlueJobBase):
    def __init__(self, glue_context: GlueContext):
        if Job is None:
            raise RuntimeError(
                "GlueIcaUsageReport must run inside AWS Glue or Glue libs"
            )

        super().__init__(glue_context)

        self.glue_context: GlueContext = glue_context
        self.spark: SparkSession = glue_context.spark_session

        params = ["lz_bucket", "rs_workgroup", "rs_role"]

        if "--JOB_NAME" in sys.argv:
            params.append("JOB_NAME")
        if "--base_name" in sys.argv:
            params.append("base_name")
        if "--s3_mid_path" in sys.argv:
            params.append("s3_mid_path")
        if "--source_prefix" in sys.argv:
            params.append("source_prefix")
        if "--load_enabled" in sys.argv:
            params.append("load_enabled")

        args = getResolvedOptions(sys.argv, params)

        self.bucket = args["lz_bucket"]
        self.workgroup = args["rs_workgroup"]
        self.role = args["rs_role"]
        self.source_prefix = args.get("source_prefix", S3_SOURCE_PREFIX_DEFAULT)
        self.load_enabled = parse_bool(args.get("load_enabled"), default=False)

        job_name = args.get("JOB_NAME", "GlueIcaUsageReport")
        self.init(job_name, args)

        global BASE_NAME, S3_MID_PATH, S3_SOURCE_PREFIX
        global OUT_NAME, OUT_NAME_DOT, OUT_PATH

        BASE_NAME = args.get("base_name", BASE_NAME_DEFAULT)
        S3_MID_PATH = args.get("s3_mid_path", S3_MID_PATH_DEFAULT)
        S3_SOURCE_PREFIX = self.source_prefix
        OUT_NAME, OUT_NAME_DOT, OUT_PATH = resolve_output_paths(BASE_NAME)

    def run(self) -> None:
        downloaded = extract(self.bucket, self.source_prefix)
        result = transform(downloaded)

        if self.load_enabled:
            load(
                bucket=self.bucket,
                workgroup=self.workgroup,
                role=self.role,
                result=result,
            )
        else:
            upload_artifacts(self.bucket, result)
            print("Skipping Redshift load because load_enabled=false")

        clean_up()
        self.commit()


if __name__ == "__main__":
    if GlueContext is None or SparkSession is None:
        raise RuntimeError("Run this script with AWS Glue libs or spark-submit")

    session = SparkSession.builder.getOrCreate()
    gc = GlueContext(session.sparkContext)
    GlueIcaUsageReport(gc).run()
