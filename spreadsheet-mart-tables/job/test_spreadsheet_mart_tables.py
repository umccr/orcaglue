import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import generate_init_sql as ddl
import pytest
import spreadsheet_mart_tables as job
from botocore.exceptions import ClientError

BUCKET = "orcavault-dev-warehouse-115253169271-ap-southeast-2-an"
PREFIX = "v1/csv/mart/"
LZ_BUCKET = "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an"
LIMS_COLUMNS = ["sequencing_run_id", "library_id", "load_datetime"]


def client_error(code: str, operation: str = "Operation") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


def manifest_body(*entries: tuple[str, int]) -> bytes:
    return json.dumps(
        {
            "entries": [
                {"url": f"s3://{BUCKET}/{key}", "meta": {"content_length": size}}
                for key, size in entries
            ]
        }
    ).encode()


class FakeS3:
    """An in-memory S3 holding the unload objects."""

    def __init__(self):
        self.objects = {}
        self.copies = []
        self.puts = []

    def add(self, key: str, body: bytes = b"", encoding: str | None = None):
        self.objects[key] = {"Body": body, "ContentEncoding": encoding, "Metadata": {}}

    def add_table(self, table: str, sizes=(100, 0, 50)):
        keys = []
        for index, size in enumerate(sizes):
            key = f"{PREFIX}{table}/{table}_{index:04d}_part_00.gz"
            self.add(key)
            keys.append((key, size))
        self.add(f"{PREFIX}{table}/{table}_manifest", manifest_body(*keys))

    def get_object(self, Bucket: str, Key: str):
        if Key not in self.objects:
            raise client_error("NoSuchKey", "GetObject")
        return {
            "Body": io.BytesIO(self.objects[Key]["Body"]),
            "LastModified": datetime(2026, 10, 23, 7, 14, tzinfo=timezone.utc),
        }

    def head_object(self, Bucket: str, Key: str):
        obj = self.objects[Key]
        response = {"Metadata": obj["Metadata"]}
        if obj["ContentEncoding"]:
            response["ContentEncoding"] = obj["ContentEncoding"]
        return response

    def copy_object(self, **kwargs):
        assert kwargs["CopySource"] == {
            "Bucket": kwargs["Bucket"],
            "Key": kwargs["Key"],
        }
        assert kwargs["MetadataDirective"] == "REPLACE"
        self.objects[kwargs["Key"]]["ContentEncoding"] = kwargs["ContentEncoding"]
        self.copies.append(kwargs["Key"])

    def put_object(self, **kwargs):
        self.puts.append(kwargs)

    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        objects = self.objects

        class Paginator:
            def paginate(self, Bucket: str, Prefix: str):
                yield {
                    "Contents": [
                        {"Key": key} for key in objects if key.startswith(Prefix)
                    ]
                }

        return Paginator()


class FakeRdsData:
    """Answers the job's Data API calls and records the SQL that ran."""

    def __init__(self, counts=(5, 3), imported=(2, 1), resuming=0, fail_on=None):
        self.counts = list(counts)
        self.imported = list(imported)
        self.resuming = resuming
        self.fail_on = fail_on
        self.statements = []
        self.calls = []

    def execute_statement(self, **kwargs):
        sql = kwargs["sql"]
        self.statements.append(
            (sql, kwargs.get("parameters"), kwargs.get("transactionId"))
        )
        if self.fail_on and self.fail_on in sql:
            raise client_error("BadRequestException", "ExecuteStatement")
        if sql == "SELECT 1" and self.resuming:
            self.resuming -= 1
            raise client_error("DatabaseResumingException", "ExecuteStatement")
        if sql == job.COLUMNS_SQL:
            records = [{"column_name": name} for name in LIMS_COLUMNS]
        elif sql.startswith("SELECT count(*)"):
            records = [{"row_count": self.counts.pop(0)}]
        elif sql == job.IMPORT_SQL:
            records = [
                {"result": f"{self.imported.pop(0)} rows imported into relation"}
            ]
        else:
            return {}
        return {"formattedRecords": json.dumps(records)}

    def begin_transaction(self, **kwargs):
        self.calls.append("begin")
        return {"transactionId": "tx-1"}

    def commit_transaction(self, **kwargs):
        self.calls.append("commit")

    def rollback_transaction(self, **kwargs):
        self.calls.append("rollback")

    def sql(self):
        return [statement[0] for statement in self.statements]


def data_api(client: FakeRdsData) -> job.DataApi:
    return job.DataApi(client, "arn:cluster", "arn:secret", "orcanexus")


def parts(*sizes: int) -> tuple[job.Part, ...]:
    return tuple(
        job.Part(BUCKET, f"{PREFIX}lims/part_{i}.gz", size)
        for i, size in enumerate(sizes)
    )


def test_parse_tables_defaults_and_validates():
    assert job.parse_tables(None) == job.MART_TABLES_DEFAULT
    assert job.parse_tables(" lims, fastq ,lims") == ("lims", "fastq")
    with pytest.raises(ValueError, match="identifier"):
        job.parse_tables("lims;drop table x")


def test_read_manifest_skips_empty_parts():
    s3 = FakeS3()
    s3.add_table("lims", sizes=(100, 0, 50))

    manifest = job.read_manifest(s3, BUCKET, PREFIX, "lims")

    assert manifest.key == f"{PREFIX}lims/lims_manifest"
    assert [part.content_length for part in manifest.parts] == [100, 50]
    assert manifest.empty_parts == 1
    assert manifest.last_modified == "2026-10-23T07:14:00+00:00"


def test_read_manifest_missing_is_a_clear_error():
    with pytest.raises(FileNotFoundError, match="No UNLOAD manifest"):
        job.read_manifest(FakeS3(), BUCKET, PREFIX, "lims")


def test_read_manifest_rejects_parts_in_another_bucket():
    s3 = FakeS3()
    body = json.dumps(
        {"entries": [{"url": "s3://other/x.gz", "meta": {"content_length": 1}}]}
    )
    s3.add(f"{PREFIX}lims/lims_manifest", body.encode())

    with pytest.raises(ValueError, match="another bucket"):
        job.read_manifest(s3, BUCKET, PREFIX, "lims")


def test_ensure_gzip_encoding_only_copies_when_missing():
    s3 = FakeS3()
    s3.add("a.gz")
    s3.add("b.gz", encoding="gzip")

    assert job.ensure_gzip_encoding(s3, job.Part(BUCKET, "a.gz", 1)) is True
    assert job.ensure_gzip_encoding(s3, job.Part(BUCKET, "a.gz", 1)) is False
    assert job.ensure_gzip_encoding(s3, job.Part(BUCKET, "b.gz", 1)) is False
    assert s3.copies == ["a.gz"]


def test_import_options_match_the_header_and_escape_the_null_marker():
    assert job.import_options() == "(format csv, header match)"
    assert job.import_options(r"\N") == r"(format csv, header match, null '\N')"
    assert job.import_options("it's") == "(format csv, header match, null 'it''s')"


def test_import_parameters_name_the_part_and_columns():
    part = job.Part(BUCKET, f"{PREFIX}lims/lims_0000_part_00.gz", 10)

    assert job.import_parameters("lims", LIMS_COLUMNS, part) == {
        "table_name": "mart.lims",
        "column_list": "sequencing_run_id,library_id,load_datetime",
        "options": "(format csv, header match)",
        "bucket": BUCKET,
        "key": f"{PREFIX}lims/lims_0000_part_00.gz",
        "region": "ap-southeast-2",
    }


def test_load_table_replaces_rows_in_one_transaction():
    client = FakeRdsData(counts=(5, 3), imported=(2, 1))
    result = job.TableResult("lims")

    job.load_table(data_api(client), "lims", LIMS_COLUMNS, parts(10, 20), result)

    assert client.sql() == [
        "SET LOCAL lock_timeout = '30s'",
        "SELECT count(*) AS row_count FROM mart.lims",
        "TRUNCATE TABLE mart.lims",
        job.IMPORT_SQL,
        job.IMPORT_SQL,
        "SELECT count(*) AS row_count FROM mart.lims",
        "ANALYZE mart.lims",
    ]
    # Everything but ANALYZE runs in the transaction.
    assert [statement[2] for statement in client.statements] == ["tx-1"] * 6 + [None]
    assert client.calls == ["begin", "commit"]
    assert (result.rows_before, result.rows_after) == (5, 3)


def test_load_table_keeps_the_load_when_analyze_fails(capsys):
    client = FakeRdsData(counts=(5, 3), imported=(2, 1), fail_on="ANALYZE")
    result = job.TableResult("lims")

    job.load_table(data_api(client), "lims", LIMS_COLUMNS, parts(10, 20), result)

    assert client.calls == ["begin", "commit"]
    assert result.rows_after == 3
    assert "WARNING: ANALYZE mart.lims failed" in capsys.readouterr().out


def test_load_table_rolls_back_when_counts_differ():
    client = FakeRdsData(counts=(5, 4), imported=(2, 1))

    with pytest.raises(job.LoadError, match="4 rows after importing 3"):
        job.load_table(
            data_api(client),
            "lims",
            LIMS_COLUMNS,
            parts(10, 20),
            job.TableResult("lims"),
        )

    assert client.calls == ["begin", "rollback"]
    assert "ANALYZE mart.lims" not in client.sql()


def test_load_table_rolls_back_when_an_import_fails():
    client = FakeRdsData(fail_on="aws_s3.table_import_from_s3")

    with pytest.raises(ClientError):
        job.load_table(
            data_api(client), "lims", LIMS_COLUMNS, parts(10), job.TableResult("lims")
        )

    assert client.calls == ["begin", "rollback"]


def test_load_table_refuses_to_empty_a_table_unless_allowed():
    with pytest.raises(job.LoadError, match="from 5 rows to 0"):
        job.load_table(
            data_api(FakeRdsData(counts=(5, 0), imported=())),
            "lims",
            LIMS_COLUMNS,
            (),
            job.TableResult("lims"),
        )

    client = FakeRdsData(counts=(5, 0), imported=())
    result = job.TableResult("lims")
    job.load_table(data_api(client), "lims", LIMS_COLUMNS, (), result, allow_empty=True)
    assert client.calls == ["begin", "commit"]
    assert result.rows_after == 0


def test_table_columns_requires_the_table():
    client = FakeRdsData()
    assert job.table_columns(data_api(client), "lims") == LIMS_COLUMNS

    client.execute_statement = lambda **kwargs: {"formattedRecords": "[]"}
    with pytest.raises(job.LoadError, match="Run job/init.sql"):
        job.table_columns(data_api(client), "lims")


def test_wake_waits_while_aurora_resumes():
    client = FakeRdsData(resuming=2)
    sleeps = []

    data_api(client).wake(sleep=sleeps.append, monotonic=lambda: 0)

    assert sleeps == [job.WAKE_INTERVAL_SECONDS] * 2
    assert client.sql() == ["SELECT 1"] * 3


def test_wake_raises_other_errors_and_times_out():
    client = FakeRdsData(fail_on="SELECT 1")
    with pytest.raises(ClientError):
        data_api(client).wake(sleep=lambda seconds: None)

    ticks = iter([0, 200])
    with pytest.raises(TimeoutError):
        data_api(FakeRdsData(resuming=5)).wake(
            sleep=lambda seconds: None, monotonic=lambda: next(ticks)
        )


def test_dry_run_prepares_parts_without_touching_aurora():
    s3 = FakeS3()
    s3.add_table("lims")
    s3.add_table("new_mart")

    results = job.load_marts(
        s3, None, BUCKET, PREFIX, ("lims",), LZ_BUCKET, "orcaglue/x/dev", dry_run=True
    )

    assert [
        (result.table, result.status, result.parts, result.encodings_fixed)
        for result in results
    ] == [("lims", "dry_run", 2, 2)]
    summary = json.loads(s3.puts[0]["Body"])
    assert s3.puts[0]["Key"] == f"orcaglue/x/dev/{job.SUMMARY_FILE_NAME}"
    assert summary["dry_run"] is True
    assert summary["unlisted_tables"] == ["new_mart"]


def test_load_marts_attempts_every_table_then_fails():
    s3 = FakeS3()
    s3.add_table("lims", sizes=(10, 20))
    client = FakeRdsData(counts=(5, 3), imported=(2, 1))

    with pytest.raises(RuntimeError, match="1 of 2 mart tables failed: fastq"):
        job.load_marts(
            s3,
            data_api(client),
            BUCKET,
            PREFIX,
            ("fastq", "lims"),
            LZ_BUCKET,
            "orcaglue/x/dev",
            dry_run=False,
        )

    summary = json.loads(s3.puts[0]["Body"])
    statuses = {
        table["table"]: (table["status"], table["error"]) for table in summary["tables"]
    }
    assert statuses["lims"] == ("loaded", None)
    assert statuses["fastq"][0] == "failed"
    assert "No UNLOAD manifest" in statuses["fastq"][1]
    assert client.sql()[0] == "SELECT 1"


def test_pg_type_maps_the_redshift_mart_types():
    assert ddl.pg_type(ddl.Column("a", "character varying", 255)) == "varchar(255)"
    assert ddl.pg_type(ddl.Column("a", "character", 16)) == "char(16)"
    assert ddl.pg_type(ddl.Column("a", "numeric", None, 18, 2)) == "numeric(18,2)"
    assert ddl.pg_type(ddl.Column("a", "timestamp with time zone")) == "timestamptz"
    assert ddl.pg_type(ddl.Column("a", "smallint")) == "smallint"
    with pytest.raises(ValueError, match="No PostgreSQL type"):
        ddl.pg_type(ddl.Column("a", "super"))


def test_ddl_for_keeps_the_redshift_column_order():
    columns = [
        ddl.Column("portal_run_id", "character", 16),
        ddl.Column("key", "character varying", 1024),
    ]

    assert ddl.ddl_for("bam", columns) == (
        "DROP TABLE IF EXISTS mart.bam;\n\n"
        "CREATE TABLE mart.bam\n"
        "(\n"
        '    "portal_run_id" char(16),\n'
        '    "key"           varchar(1024)\n'
        ");"
    )


def test_init_sql_file_creates_exactly_the_default_tables():
    init_sql = Path(__file__).with_name("init.sql").read_text(encoding="utf-8")

    created = re.findall(r"^CREATE TABLE mart\.(\w+)$", init_sql, flags=re.MULTILINE)
    assert tuple(created) == job.MART_TABLES_DEFAULT
    assert "TO dbuser_rw;" in init_sql
