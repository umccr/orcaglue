import io
import json
import re
import ssl
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import generate_init_sql as ddl
import pytest
import spreadsheet_mart_tables as job
from botocore.exceptions import ClientError

BUCKET = "orcavault-dev-warehouse-115253169271-ap-southeast-2-an"
PREFIX = "v1/csv/mart/"
LZ_BUCKET = "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an"
LIMS_COLUMNS = ["sequencing_run_id", "library_id", "load_datetime"]
CA_BUNDLE = Path(__file__).with_name("rds-ca-ap-southeast-2-bundle.pem")
LOGIN = job.AuroraLogin(
    host="orcanexus-dev.cluster-cpyama2ogufr.ap-southeast-2.rds.amazonaws.com",
    port=5432,
    user="dbuser_rw",
    database="orcanexus",
)


def client_error(code: str, operation: str = "Operation") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


def database_error(code: str) -> job.DatabaseError:
    return job.DatabaseError({"S": "FATAL", "C": code, "M": code})


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


class FakeConnection:
    """Answers the job's SQL like a pg8000 native connection, and records it."""

    def __init__(self, counts=(5, 3), imported=(2, 1), fail_on=None, columns=None):
        self.counts = list(counts)
        self.imported = list(imported)
        self.fail_on = fail_on
        self.table_columns = LIMS_COLUMNS if columns is None else columns
        self.statements = []
        self.columns = None
        self.closed = False

    def run(self, sql, **params):
        self.statements.append((sql, params))
        self.columns = None
        if self.fail_on and self.fail_on in sql:
            raise database_error("XX000")
        if sql == job.COLUMNS_SQL:
            return self.answer("column_name", [[name] for name in self.table_columns])
        if sql.startswith("SELECT count(*)"):
            return self.answer("row_count", [[self.counts.pop(0)]])
        if sql == job.IMPORT_SQL:
            result = f"{self.imported.pop(0)} rows imported into relation"
            return self.answer("result", [[result]])
        return None

    def answer(self, name, rows):
        self.columns = [{"name": name}]
        return rows

    def close(self):
        self.closed = True

    def sql(self):
        return [statement[0] for statement in self.statements]


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


def test_tls_context_trusts_only_the_rds_cas_and_checks_the_host_name():
    context = job.tls_context(CA_BUNDLE.read_text(encoding="utf-8"))

    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert sorted(
        dict(field[0] for field in ca["subject"])["commonName"]
        for ca in context.get_ca_certs()
    ) == [
        "Amazon RDS ap-southeast-2 Root CA ECC384 G1",
        "Amazon RDS ap-southeast-2 Root CA RSA2048 G1",
        "Amazon RDS ap-southeast-2 Root CA RSA4096 G1",
    ]


def test_read_s3_text_reads_the_ca_bundle():
    s3 = FakeS3()
    s3.add("orcaglue/x/dev/rds-ca.pem", b"pem")

    assert job.read_s3_text(s3, f"s3://{LZ_BUCKET}/orcaglue/x/dev/rds-ca.pem") == "pem"


def test_iam_auth_token_is_for_the_login(monkeypatch):
    calls = []

    class FakeRds:
        def generate_db_auth_token(self, **kwargs):
            calls.append(kwargs)
            return "token"

    monkeypatch.setattr(job, "get_boto3_client", lambda service: FakeRds())

    assert job.iam_auth_token(LOGIN) == "token"
    assert calls == [
        {
            "DBHostname": LOGIN.host,
            "Port": 5432,
            "DBUsername": "dbuser_rw",
            "Region": "ap-southeast-2",
        }
    ]


def test_connect_logs_in_over_tls_with_a_new_token(monkeypatch):
    connections = []
    monkeypatch.setattr(
        job,
        "pg8000_native",
        SimpleNamespace(Connection=lambda **kwargs: connections.append(kwargs)),
    )
    context = job.tls_context(CA_BUNDLE.read_text(encoding="utf-8"))
    tokens = {LOGIN: object()}

    job.connect(LOGIN, context, password=tokens.get)

    assert connections == [
        {
            "user": "dbuser_rw",
            "host": LOGIN.host,
            "port": 5432,
            "database": "orcanexus",
            "password": tokens[LOGIN],
            "ssl_context": context,
            "timeout": job.SOCKET_TIMEOUT_SECONDS,
            "application_name": "orcaglue-spreadsheet-mart-tables",
            "startup_params": {"statement_timeout": "10min"},
        }
    ]


def test_database_returns_rows_as_dicts():
    db = job.Database(FakeConnection())

    rows = db.execute(job.COLUMNS_SQL, {"schema_name": "mart", "table_name": "lims"})

    assert rows == [{"column_name": name} for name in LIMS_COLUMNS]
    assert db.connection.statements[0][1] == {
        "schema_name": "mart",
        "table_name": "lims",
    }
    assert db.execute("TRUNCATE TABLE mart.lims") == []


def test_database_close_tolerates_a_dropped_connection(capsys):
    connection = FakeConnection()

    def close():
        raise job.InterfaceError("network error")

    connection.close = close
    job.Database(connection).close()

    assert "closing the Aurora connection failed" in capsys.readouterr().out


def test_describe_error_gives_a_server_error_as_its_sqlstate_and_message():
    lock = job.DatabaseError(
        {
            "S": "ERROR",
            "C": "55P03",
            "M": "canceling statement due to lock timeout",
            "F": "postgres.c",
        }
    )
    denied = job.DatabaseError({"C": "XX000", "M": "import failed", "D": "HTTP 403"})

    assert job.describe_error(lock) == (
        "DatabaseError: 55P03 canceling statement due to lock timeout"
    )
    assert job.describe_error(denied) == "DatabaseError: XX000 import failed (HTTP 403)"
    assert job.describe_error(job.InterfaceError("network error")) == (
        "InterfaceError: network error"
    )


def test_load_table_replaces_rows_in_one_transaction():
    db = job.Database(FakeConnection(counts=(5, 3), imported=(2, 1)))
    result = job.TableResult("lims")

    job.load_table(db, "lims", LIMS_COLUMNS, parts(10, 20), result)

    assert db.connection.sql() == [
        "BEGIN",
        "SET LOCAL lock_timeout = '30s'",
        "SELECT count(*) AS row_count FROM mart.lims",
        "TRUNCATE TABLE mart.lims",
        job.IMPORT_SQL,
        job.IMPORT_SQL,
        "SELECT count(*) AS row_count FROM mart.lims",
        "COMMIT",
        "ANALYZE mart.lims",
    ]
    assert db.connection.statements[4][1] == job.import_parameters(
        "lims", LIMS_COLUMNS, parts(10, 20)[0]
    )
    assert (result.rows_before, result.rows_after) == (5, 3)


def test_load_table_keeps_the_load_when_analyze_fails(capsys):
    db = job.Database(FakeConnection(counts=(5, 3), imported=(2, 1), fail_on="ANALYZE"))
    result = job.TableResult("lims")

    job.load_table(db, "lims", LIMS_COLUMNS, parts(10, 20), result)

    assert "COMMIT" in db.connection.sql()
    assert result.rows_after == 3
    assert "WARNING: ANALYZE mart.lims failed" in capsys.readouterr().out


def test_load_table_rolls_back_when_counts_differ():
    db = job.Database(FakeConnection(counts=(5, 4), imported=(2, 1)))

    with pytest.raises(job.LoadError, match="4 rows after importing 3"):
        job.load_table(db, "lims", LIMS_COLUMNS, parts(10, 20), job.TableResult("lims"))

    assert db.connection.sql()[-1] == "ROLLBACK"
    assert "COMMIT" not in db.connection.sql()
    assert "ANALYZE mart.lims" not in db.connection.sql()


def test_load_table_rolls_back_when_an_import_fails():
    db = job.Database(FakeConnection(fail_on="aws_s3.table_import_from_s3"))

    with pytest.raises(job.DatabaseError):
        job.load_table(db, "lims", LIMS_COLUMNS, parts(10), job.TableResult("lims"))

    assert db.connection.sql()[-1] == "ROLLBACK"
    assert "COMMIT" not in db.connection.sql()


def test_load_table_refuses_to_empty_a_table_unless_allowed():
    db = job.Database(FakeConnection(counts=(5, 0), imported=()))
    with pytest.raises(job.LoadError, match="from 5 rows to 0"):
        job.load_table(db, "lims", LIMS_COLUMNS, (), job.TableResult("lims"))
    assert db.connection.sql()[-1] == "ROLLBACK"

    db = job.Database(FakeConnection(counts=(5, 0), imported=()))
    result = job.TableResult("lims")
    job.load_table(db, "lims", LIMS_COLUMNS, (), result, allow_empty=True)
    assert "COMMIT" in db.connection.sql()
    assert result.rows_after == 0


def test_table_columns_requires_the_table():
    assert job.table_columns(job.Database(FakeConnection()), "lims") == LIMS_COLUMNS

    with pytest.raises(job.LoadError, match="Run job/init.sql"):
        job.table_columns(job.Database(FakeConnection(columns=[])), "lims")


def failing_connect(*errors: Exception):
    """A connect function that raises each error in turn, then connects."""
    pending = list(errors)
    connections = []

    def connect_db():
        if pending:
            raise pending.pop(0)
        connections.append(FakeConnection())
        return job.Database(connections[-1])

    return connect_db, connections


def test_wait_for_aurora_retries_while_aurora_resumes():
    connect_db, connections = failing_connect(
        job.InterfaceError("Can't create a connection"),
        database_error(job.STARTING_UP_SQLSTATE),
    )
    sleeps = []

    job.wait_for_aurora(connect_db, sleep=sleeps.append, monotonic=lambda: 0)

    assert sleeps == [job.WAKE_INTERVAL_SECONDS] * 2
    assert len(connections) == 1
    assert connections[0].closed


def test_wait_for_aurora_raises_a_refused_login_and_times_out():
    # 28000 invalid_authorization_specification, as for a login without rds_iam
    connect_db, _ = failing_connect(database_error("28000"))
    with pytest.raises(job.DatabaseError):
        job.wait_for_aurora(connect_db, sleep=lambda seconds: None)

    connect_db, _ = failing_connect(*[job.InterfaceError("timed out")] * 5)
    ticks = iter([0, 200])
    with pytest.raises(TimeoutError):
        job.wait_for_aurora(
            connect_db, sleep=lambda seconds: None, monotonic=lambda: next(ticks)
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
    connect_db, connections = failing_connect()

    with pytest.raises(RuntimeError, match="1 of 2 mart tables failed: fastq"):
        job.load_marts(
            s3,
            connect_db,
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
    # One login to wait for Aurora, then one session for lims; fastq fails before it logs in.
    assert len(connections) == 2
    assert all(connection.closed for connection in connections)
    assert connections[0].statements == []


def test_process_table_fails_the_table_when_the_login_fails():
    s3 = FakeS3()
    s3.add_table("lims", sizes=(10,))
    connect_db, _ = failing_connect(job.InterfaceError("network error"))

    result = job.process_table(s3, connect_db, BUCKET, PREFIX, "lims", dry_run=False)

    assert (result.status, result.error) == ("failed", "InterfaceError: network error")


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
