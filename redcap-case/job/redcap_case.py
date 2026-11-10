import os
import sys
import time

import boto3
import polars as pl
import requests
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from libumccr.aws import libs3, libssm
from pyspark.sql import SparkSession

# The datasource REDCap configuration
REDCAP_ENDPOINT = "https://redcap.unimelb.edu.au/api/"
REDCAP_TOKEN_PARAMETER_NAME = "/orcabus/case-manager/redcap/redcap-api-token"
REQUEST_TIMEOUT = 300  # seconds

# NOTE: this is intended db table naming convention
# i.e. <datasource>__<suffix_meaningful_naming_convention>
# e.g. <spreadsheet>__<some_research_data_collection>
BASE_NAME_DEFAULT = "redcap__case"
S3_MID_PATH_DEFAULT = f"orcaglue/{BASE_NAME_DEFAULT}/dev"
SCHEMA_NAME = "tsa"
DB_NAME = "orcavault"

# Resolved at runtime in GlueRedcapCase constructor
BASE_NAME = None
S3_MID_PATH = None
OUT_NAME = None
OUT_NAME_DOT = None
OUT_PATH = None

REGION_NAME = "ap-southeast-2"
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

def extract():
    """Export every record of the REDCap project as a flat CSV."""
    token = libssm.get_secret(REDCAP_TOKEN_PARAMETER_NAME)

    payload = {
        "token": token,
        "content": "record",
        "action": "export",
        "format": "csv",
        "type": "flat",
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "returnFormat": "json",
    }

    response = requests.post(REDCAP_ENDPOINT, data=payload, timeout=REQUEST_TIMEOUT)

    # Do not log the payload or the response body: they hold the token and the data.
    if response.status_code != 200:
        raise RuntimeError(
            f"REDCap API request failed with status {response.status_code}"
        )

    # Write the raw bytes so quotes and newlines inside free-text fields stay intact.
    with open(f"{OUT_PATH}__raw.csv", "wb") as f:
        f.write(response.content)

def transform():
    # treat all columns as string value, do not automatically infer the dataframe dtype
    # https://stackoverflow.com/questions/77318631/how-to-read-all-columns-as-strings-in-polars
    df = pl.read_csv(
        f"{OUT_PATH}__raw.csv", infer_schema_length=False, infer_schema=False
    )

    # strip whitespaces, carriage return
    df = df.with_columns(pl.col(pl.String).str.strip_chars())

    # blank cells to null (after stripping, so whitespace-only cells count as blank)
    df = df.with_columns(
        pl.when(pl.col(pl.String).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.String))
        .name.keep()
    )

    # drop row iff all values are null
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))

    # drop all unnamed (blank) columns
    for col in df.columns:
        if col.startswith("__UNNAMED__"):
            df = df.drop(col)
        if col.startswith("_duplicated"):
            df = df.drop(col)
        if col == "":
            df = df.drop(col)

    # REDCap variable names are already lowercase snake_case. Checkbox columns (field___1)
    # and repeating instruments are kept as exported: reshaping belongs to later layers.

    # Never truncate the target table and then load nothing.
    if df.height == 0:
        raise RuntimeError("REDCap export returned no records; refusing to load")

    print(f"Exported {df.height} rows, {len(df.columns)} columns")
    print(df.columns)

    df.write_csv(f"{OUT_PATH}.csv")

    # generate sql schema script
    sql = ",\n".join(
        f'"{col}"\tvarchar(65535)'
        for col in df.columns
        if col not in ("record_source", "load_datetime")
    )

    sql_schema = f"""CREATE TABLE IF NOT EXISTS {OUT_NAME_DOT}
    (
    {sql}
    );"""

    with open(f"{OUT_PATH}.sql", "w", newline="") as f:
        f.write(sql_schema)

    print(sql_schema)

def _wait_for_query(client, statement_id: str):
    """Poll Redshift Data API until the statement reaches a terminal state."""
    while True:
        response = client.describe_statement(Id=statement_id)
        status = response["Status"]
        if status == "FINISHED":
            print(f"Statement {statement_id} finished successfully.")
            break
        elif status in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Redshift Data API statement {statement_id} failed.\n"
                f"Error: {response.get('Error')}"
            )
        print(f"Statement {statement_id} status: {status} — waiting...")
        time.sleep(2)

def upload_artifacts(bucket: str) -> str:
    """Upload the generated CSV and SQL artefacts to S3 and return the CSV S3 URI."""

    csv_file = f"{OUT_PATH}.csv"
    sql_file = f"{OUT_PATH}.sql"

    csv_s3_object_name = f"{S3_MID_PATH}/{os.path.basename(csv_file)}"
    sql_s3_object_name = f"{S3_MID_PATH}/{os.path.basename(sql_file)}"

    s3_client = libs3.s3_client()
    s3_client.upload_file(csv_file, bucket, csv_s3_object_name)
    s3_client.upload_file(sql_file, bucket, sql_s3_object_name)
    print(f"Uploaded CSV  → s3://{bucket}/{csv_s3_object_name}")
    print(f"Uploaded SQL  → s3://{bucket}/{sql_s3_object_name}")

    return f"s3://{bucket}/{csv_s3_object_name}"

def load_to_redshift(workgroup: str, role: str, s3_csv_uri: str):
    """TRUNCATE the target table then COPY the staged CSV via the Redshift Data API."""

    table_name = f"{SCHEMA_NAME}.{BASE_NAME}"

    truncate_sql = f"TRUNCATE TABLE {table_name};"

    copy_sql = f"""
        COPY {table_name}
        FROM '{s3_csv_uri}'
        IAM_ROLE '{role}'
        FORMAT AS CSV
        IGNOREHEADER 1
        EMPTYASNULL
        BLANKSASNULL
        REGION '{REGION_NAME}';
    """

    redshift_data_client = boto3.client("redshift-data", region_name=REGION_NAME)

    common_kwargs = {
        "WorkgroupName": workgroup,
        "Database": DB_NAME,
    }

    print(f"Truncating table: {table_name}")
    truncate_response = redshift_data_client.execute_statement(
        **common_kwargs,
        Sql=truncate_sql,
    )
    _wait_for_query(redshift_data_client, truncate_response["Id"])

    print(f"Running COPY into: {table_name} from {s3_csv_uri}")
    copy_response = redshift_data_client.execute_statement(
        **common_kwargs,
        Sql=copy_sql,
    )
    _wait_for_query(redshift_data_client, copy_response["Id"])

    print(f"Load complete → {table_name}")

def load(bucket: str, workgroup: str, role: str):
    """
    Load staged CSV data into Redshift Serverless via:
      1. Upload CSV and SQL artefacts to S3
      2. TRUNCATE the target table
      3. COPY from S3 into Redshift Serverless using the Data API
    """

    s3_csv_uri = upload_artifacts(bucket)
    load_to_redshift(workgroup=workgroup, role=role, s3_csv_uri=s3_csv_uri)

def clean_up():
    # Remove the raw export: it may hold fields that are not in the transformed CSV.
    raw_file = f"{OUT_PATH}__raw.csv"
    if os.path.exists(raw_file):
        os.remove(raw_file)

class GlueRedcapCase(Job):
    def __init__(self, glue_context: GlueContext):
        super().__init__(glue_context)

        self.glue_context: GlueContext = glue_context
        self.spark: SparkSession = glue_context.spark_session

        # Pass-in parameters
        params = ["lz_bucket", "rs_workgroup", "rs_role"]

        if "--JOB_NAME" in sys.argv:
            params.append("JOB_NAME")
        if "--base_name" in sys.argv:
            params.append("base_name")
        if "--s3_mid_path" in sys.argv:
            params.append("s3_mid_path")
        if "--dry_run" in sys.argv:
            params.append("dry_run")

        args = getResolvedOptions(sys.argv, params)

        self.bucket = args["lz_bucket"]
        self.workgroup = args["rs_workgroup"]
        self.role = args["rs_role"]
        self.dry_run = parse_bool(args.get("dry_run"))

        job_name = args.get("JOB_NAME", "GlueRedcapCase")
        self.init(job_name, args)

        # Resolve optional parameters with defaults
        global BASE_NAME, S3_MID_PATH, OUT_NAME, OUT_NAME_DOT, OUT_PATH

        BASE_NAME = args.get("base_name", BASE_NAME_DEFAULT)
        S3_MID_PATH = args.get("s3_mid_path", S3_MID_PATH_DEFAULT)

        # Prepare out path with naming convention
        OUT_NAME = f"{DB_NAME}_{SCHEMA_NAME}_{BASE_NAME}"
        OUT_NAME_DOT = f"{DB_NAME}.{SCHEMA_NAME}.{BASE_NAME}"
        OUT_PATH = f"/tmp/{OUT_NAME}"

    def run(self):

        extract()

        transform()

        # Use --dry_run true to upload the artefacts without touching Redshift.
        if self.dry_run:
            upload_artifacts(self.bucket)
            print("Dry run enabled: skipping Redshift load")
        else:
            load(bucket=self.bucket, workgroup=self.workgroup, role=self.role)

        clean_up()

        self.commit()

if __name__ == "__main__":
    session = SparkSession.builder.getOrCreate()
    gc = GlueContext(session.sparkContext)
    GlueRedcapCase(gc).run()
