import csv
import json
import os
import sys
import time

import boto3
import gspread
import polars as pl
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from libumccr.aws import libs3, libssm
from pyspark.sql import SparkSession

# The datasource spreadsheet configuration
GDRIVE_SERVICE_ACCOUNT = "/umccr/google/drive/lims_service_account_json"
LIMS_SHEET_ID = "/umccr/google/drive/lims_sheet_id"
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SHEET_NAME = "Sheet1"

# NOTE: this is intended db table naming convention
# i.e. <datasource>__<suffix_meaningful_naming_convention>
# e.g. <spreadsheet>__<some_research_data_collection>
BASE_NAME_DEFAULT = "spreadsheet__google_lims"
S3_MID_PATH_DEFAULT = f"orcaglue/{BASE_NAME_DEFAULT}/dev"
SCHEMA_NAME = "tsa"
DB_NAME = "orcavault"

# Resolved at runtime in GlueGoogleLIMS constructor
BASE_NAME = None
S3_MID_PATH = None
OUT_NAME = None
OUT_NAME_DOT = None
OUT_PATH = None

REGION_NAME = "ap-southeast-2"


def extract():
    spreadsheet_id = libssm.get_secret(LIMS_SHEET_ID)
    account_info = libssm.get_secret(GDRIVE_SERVICE_ACCOUNT)

    gs = gspread.service_account_from_dict(json.loads(account_info))
    sh = gs.open_by_key(spreadsheet_id)

    worksheet = sh.worksheet(SHEET_NAME)
    filename = f"{OUT_PATH}__{SHEET_NAME}.csv"
    with open(filename, "w") as f:
        writer = csv.writer(f)
        writer.writerows(worksheet.get_all_values())


def transform():
    # treat all columns as string value, do not automatically infer the dataframe dtype i.e. infer_schema_length=0
    # https://github.com/pola-rs/polars/pull/16840
    # https://stackoverflow.com/questions/77318631/how-to-read-all-columns-as-strings-in-polars
    df = pl.read_csv(
        f"{OUT_PATH}__{SHEET_NAME}.csv", infer_schema_length=False, infer_schema=False
    )

    # replace all cells that contain well-known placeholder characters, typically derived formula columns
    df = df.with_columns(pl.col(pl.String).str.replace("^_$", ""))
    df = df.with_columns(pl.col(pl.String).str.replace("^__$$", ""))
    df = df.with_columns(pl.col(pl.String).str.replace("^-$", ""))
    df = df.with_columns(
        pl.when(pl.col(pl.String).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.String))
        .name.keep()
    )

    # strip whitespaces, carriage return
    df = df.with_columns(pl.col(pl.String).str.strip_chars())

    # drop row iff all values are null
    # https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.drop_nulls.html
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))

    # sort the columns
    # df = df.select(sorted(df.columns))

    # drop all unnamed (blank) columns
    for col in df.columns:
        if col.startswith("__UNNAMED__"):
            df = df.drop(col)
        if col.startswith("_duplicated"):
            df = df.drop(col)
        if col == "":
            df = df.drop(col)

    # add sheet name as a column
    df = df.with_columns(pl.lit(SHEET_NAME).alias("sheet_name"))

    print(SHEET_NAME, df.columns)

    # final column rename
    df = df.rename(
        {
            "IlluminaID": "illumina_id",
            "Run": "run",
            "Timestamp": "timestamp",
            "SubjectID": "subject_id",
            "SampleID": "sample_id",
            "LibraryID": "library_id",
            "ExternalSubjectID": "external_subject_id",
            "ExternalSampleID": "external_sample_id",
            "ExternalLibraryID": "external_library_id",
            "SampleName": "sample_name",
            "ProjectOwner": "project_owner",
            "ProjectName": "project_name",
            "ProjectCustodian": "project_custodian",
            "Type": "type",
            "Assay": "assay",
            "OverrideCycles": "override_cycles",
            "Phenotype": "phenotype",
            "Source": "source",
            "Quality": "quality",
            "Topup": "topup",
            "SecondaryAnalysis": "secondary_analysis",
            "Workflow": "workflow",
            "Tags": "tags",
            "FASTQ": "fastq",
            "NumberFASTQS": "number_fastqs",
            "Results": "results",
            "Trello": "trello",
            "Notes": "notes",
            "Todo": "todo",
        }
    )

    df.write_csv(f"{OUT_PATH}.csv")

    # generate sql schema script
    sql = ""
    i = 1
    for col in df.columns:
        if col in ["record_source", "load_datetime"]:
            continue
        if i == len(df.columns):
            sql += f"{col}\tvarchar"
        else:
            sql += f"{col}\tvarchar,\n"
        i += 1

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


def load(bucket: str, workgroup: str, role: str):
    """
    Load staged CSV data into Redshift Serverless via:
      1. Upload CSV and SQL artefacts to S3
      2. TRUNCATE the target table
      3. COPY from S3 into Redshift Serverless using the Data API
    """

    csv_file = f"{OUT_PATH}.csv"
    sql_file = f"{OUT_PATH}.sql"

    csv_s3_object_name = f"{S3_MID_PATH}/{os.path.basename(csv_file)}"
    sql_s3_object_name = f"{S3_MID_PATH}/{os.path.basename(sql_file)}"

    # --- Step 1: Upload artefacts to S3 ---

    s3_client = libs3.s3_client()
    s3_client.upload_file(csv_file, bucket, csv_s3_object_name)
    s3_client.upload_file(sql_file, bucket, sql_s3_object_name)
    print(f"Uploaded CSV  → s3://{bucket}/{csv_s3_object_name}")
    print(f"Uploaded SQL  → s3://{bucket}/{sql_s3_object_name}")

    # --- Step 2: Truncate + COPY via Redshift Data API ---

    table_name = f"{SCHEMA_NAME}.{BASE_NAME}"
    s3_csv_uri = f"s3://{bucket}/{csv_s3_object_name}"

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

    common_kwargs = dict(
        WorkgroupName=workgroup,
        Database=DB_NAME,
    )

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


def clean_up():
    # os.remove(LOCAL_TEMP_FILE)
    pass  # for now


class GlueGoogleLIMS(Job):
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

        args = getResolvedOptions(sys.argv, params)

        self.bucket = args["lz_bucket"]
        self.workgroup = args["rs_workgroup"]
        self.role = args["rs_role"]

        job_name = args.get("JOB_NAME", "GlueGoogleLIMS")
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

        # comment out the following line to skip loading into Redshift
        load(bucket=self.bucket, workgroup=self.workgroup, role=self.role)

        clean_up()

        self.commit()


if __name__ == "__main__":
    session = SparkSession.builder.getOrCreate()
    gc = GlueContext(session.sparkContext)
    GlueGoogleLIMS(gc).run()
