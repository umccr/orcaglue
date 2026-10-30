import io
import json
import re
import ssl
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import generate_mart_ddl as ddl
import pytest
import spreadsheet_mart_tables as job
from botocore.exceptions import ClientError

BUCKET = "orcavault-dev-warehouse-115253169271-ap-southeast-2-an"
PREFIX = "v1/csv/mart/"
LZ_BUCKET = "orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an"
LIMS = job.TableSpec(
    "lims",
    (
        ("sequencing_run_id", "varchar(255)"),
        ("library_id", "varchar(255)"),
        ("load_datetime", "timestamptz"),
    ),
    (("sequencing_run_id",),),
)
SPECS = {"lims": LIMS}
LIMS_COLUMNS = LIMS.column_names
DDL_FILE = Path(__file__).with_name("mart_tables.json")
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
    """Answers the job's SQL like a pg8000 native connection, and records it.

    counts are the row counts of the shadow table, then of the live table. errors maps
    the start of a statement to the SQLSTATEs it raises, one per run of it, in turn.
    """

    def __init__(self, counts=(3, 5), imported=(2, 1), live=True, errors=None):
        self.counts = list(counts)
        self.imported = list(imported)
        self.live = live
        self.errors = {sql: list(codes) for sql, codes in (errors or {}).items()}
        self.statements = []
        self.columns = None
        self.closed = False

    def run(self, sql, **params):
        self.statements.append((sql, params))
        self.columns = None
        for start, codes in self.errors.items():
            if sql.startswith(start) and codes:
                raise database_error(codes.pop(0))
        if sql.startswith("SELECT count(*)"):
            return self.answer("row_count", [[self.counts.pop(0)]])
        if sql == job.EXISTS_SQL:
            return self.answer("present", [[self.live]])
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


def ddl_text(**tables) -> str:
    return json.dumps({"tables": tables})


def test_parse_names_default_and_validate():
    assert job.parse_tables(None) == job.MART_TABLES_DEFAULT
    assert job.parse_tables(" lims, fastq ,lims") == ("lims", "fastq")
    assert job.parse_readers(None) == ("dbuser_ro",)
    assert job.parse_readers("dbuser_ro,api_ro") == ("dbuser_ro", "api_ro")
    with pytest.raises(ValueError, match="identifier"):
        job.parse_tables("lims;drop table x")
    with pytest.raises(ValueError, match="identifier"):
        job.parse_readers("public to dbuser_rw")


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


def test_import_parameters_load_the_shadow_table():
    part = job.Part(BUCKET, f"{PREFIX}lims/lims_0000_part_00.gz", 10)

    assert job.import_parameters("lims", LIMS_COLUMNS, part) == {
        "table_name": "mart_stg.lims",
        "column_list": "sequencing_run_id,library_id,load_datetime",
        "options": "(format csv, header match)",
        "bucket": BUCKET,
        "key": f"{PREFIX}lims/lims_0000_part_00.gz",
        "region": "ap-southeast-2",
    }


def test_parse_ddl_reads_the_columns_and_indexes():
    text = ddl_text(
        lims={
            "columns": [list(column) for column in LIMS.columns],
            "indexes": [["sequencing_run_id"]],
        }
    )

    assert job.parse_ddl(text) == SPECS


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ({"columns": []}, "distinct names"),
        ({"columns": [["a", "text"], ["a", "text"]]}, "distinct names"),
        ({"columns": [["A", "text"]]}, "identifier"),
        ({"columns": [["a", "text); drop table x; --"]]}, "Not a column type"),
        ({"columns": [["a", "text"]], "indexes": [["b"]]}, "isn't on its columns"),
        ({"columns": [["a", "text"]], "indexes": [[]]}, "isn't on its columns"),
        # The index name, t_<60 a>_idx, is longer than PostgreSQL's 63 bytes.
        ({"columns": [["a" * 60, "text"]], "indexes": [["a" * 60]]}, "identifier"),
    ],
)
def test_parse_ddl_rejects_a_bad_table(spec, message):
    with pytest.raises(ValueError, match=message):
        job.parse_ddl(ddl_text(t=spec))


def test_create_table_and_index_sql_build_the_shadow_table():
    pair = job.TableSpec("t", (("a", "text"), ("b", "numeric(18,2)")), (("a", "b"),))

    assert job.create_table_sql(LIMS) == (
        'CREATE TABLE mart_stg.lims ("sequencing_run_id" varchar(255), '
        '"library_id" varchar(255), "load_datetime" timestamptz)'
    )
    assert job.create_index_sql(LIMS) == [
        'CREATE INDEX lims_sequencing_run_id_idx ON mart_stg.lims ("sequencing_run_id")'
    ]
    assert job.create_index_sql(pair) == [
        'CREATE INDEX t_a_b_idx ON mart_stg.t ("a", "b")'
    ]


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


def test_read_text_reads_s3_or_a_local_file(tmp_path):
    s3 = FakeS3()
    s3.add("orcaglue/x/dev/rds-ca.pem", b"pem")
    local = tmp_path / "mart_tables.json"
    local.write_text("{}", encoding="utf-8")

    assert job.read_text(s3, f"s3://{LZ_BUCKET}/orcaglue/x/dev/rds-ca.pem") == "pem"
    assert job.read_text(s3, str(local)) == "{}"


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
    db = job.Database(FakeConnection(live=False))

    rows = db.execute(job.EXISTS_SQL, {"table_name": "mart.lims"})

    assert rows == [{"present": False}]
    assert db.connection.statements[0][1] == {"table_name": "mart.lims"}
    assert db.execute("ANALYZE mart_stg.lims") == []


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


def test_load_table_swaps_in_a_shadow_table_in_one_transaction():
    db = job.Database(FakeConnection(counts=(3, 5), imported=(2, 1)))
    result = job.TableResult("lims")

    job.load_table(db, LIMS, parts(10, 20), result)

    assert db.connection.sql() == [
        "BEGIN",
        "SET LOCAL lock_timeout = '5s'",
        "DROP TABLE IF EXISTS mart_stg.lims",
        job.create_table_sql(LIMS),
        job.IMPORT_SQL,
        job.IMPORT_SQL,
        "SELECT count(*) AS row_count FROM mart_stg.lims",
        job.EXISTS_SQL,
        "SELECT count(*) AS row_count FROM mart.lims",
        *job.create_index_sql(LIMS),
        "ANALYZE mart_stg.lims",
        "GRANT SELECT ON mart_stg.lims TO dbuser_ro",
        "SAVEPOINT swap",
        "DROP TABLE IF EXISTS mart.lims",
        "ALTER TABLE mart_stg.lims SET SCHEMA mart",
        "COMMIT",
    ]
    assert db.connection.statements[4][1] == job.import_parameters(
        "lims", LIMS_COLUMNS, parts(10, 20)[0]
    )
    assert db.connection.statements[7][1] == {"table_name": "mart.lims"}
    assert (result.rows_before, result.rows_after) == (5, 3)


def test_load_table_creates_the_live_table_on_the_first_load(capsys):
    db = job.Database(FakeConnection(counts=(3,), live=False))
    result = job.TableResult("lims")

    job.load_table(db, LIMS, parts(10, 20), result)

    assert "SELECT count(*) AS row_count FROM mart.lims" not in db.connection.sql()
    assert db.connection.sql()[-3:] == [
        "DROP TABLE IF EXISTS mart.lims",
        "ALTER TABLE mart_stg.lims SET SCHEMA mart",
        "COMMIT",
    ]
    assert (result.rows_before, result.rows_after) == (None, 3)
    assert "Loaded mart.lims: new table -> 3 rows, swapped in on attempt 1" in (
        capsys.readouterr().out
    )


def test_load_table_grants_select_to_each_reader():
    db = job.Database(FakeConnection())
    options = job.LoadOptions(readers=("dbuser_ro", "api_ro"))

    job.load_table(db, LIMS, parts(10, 20), job.TableResult("lims"), options)

    assert [sql for sql in db.connection.sql() if sql.startswith("GRANT")] == [
        "GRANT SELECT ON mart_stg.lims TO dbuser_ro",
        "GRANT SELECT ON mart_stg.lims TO api_ro",
    ]


def test_load_table_retries_the_swap_while_readers_hold_the_table():
    timeouts = [job.LOCK_NOT_AVAILABLE_SQLSTATE] * 2
    db = job.Database(
        FakeConnection(errors={"DROP TABLE IF EXISTS mart.lims": timeouts})
    )
    sleeps = []
    result = job.TableResult("lims")

    job.load_table(db, LIMS, parts(10, 20), result, sleep=sleeps.append)

    assert db.connection.sql()[-8:] == [
        "SAVEPOINT swap",
        "DROP TABLE IF EXISTS mart.lims",
        "ROLLBACK TO SAVEPOINT swap",
        "DROP TABLE IF EXISTS mart.lims",
        "ROLLBACK TO SAVEPOINT swap",
        "DROP TABLE IF EXISTS mart.lims",
        "ALTER TABLE mart_stg.lims SET SCHEMA mart",
        "COMMIT",
    ]
    assert sleeps == [job.SWAP_RETRY_SECONDS] * 2
    # The savepoint keeps the load, so a retry doesn't import again.
    assert db.connection.sql().count(job.IMPORT_SQL) == 2
    assert result.rows_after == 3


def test_load_table_gives_up_the_swap_and_rolls_back():
    timeouts = [job.LOCK_NOT_AVAILABLE_SQLSTATE] * job.SWAP_ATTEMPTS
    db = job.Database(
        FakeConnection(errors={"DROP TABLE IF EXISTS mart.lims": timeouts})
    )
    sleeps = []

    with pytest.raises(job.DatabaseError) as raised:
        job.load_table(
            db, LIMS, parts(10, 20), job.TableResult("lims"), sleep=sleeps.append
        )

    assert job.sqlstate(raised.value) == job.LOCK_NOT_AVAILABLE_SQLSTATE
    assert len(sleeps) == job.SWAP_ATTEMPTS - 1
    assert db.connection.sql()[-1] == "ROLLBACK"
    assert "COMMIT" not in db.connection.sql()


def test_load_table_does_not_retry_other_swap_errors():
    # 2BP01 dependent_objects_still_exist, as for a view on the live table
    db = job.Database(
        FakeConnection(errors={"DROP TABLE IF EXISTS mart.lims": ["2BP01"]})
    )
    sleeps = []

    with pytest.raises(job.DatabaseError):
        job.load_table(
            db, LIMS, parts(10, 20), job.TableResult("lims"), sleep=sleeps.append
        )

    assert sleeps == []
    assert db.connection.sql()[-1] == "ROLLBACK"


def test_load_table_rolls_back_when_counts_differ():
    db = job.Database(FakeConnection(counts=(4,), imported=(2, 1)))

    with pytest.raises(
        job.LoadError, match="mart_stg.lims has 4 rows after importing 3"
    ):
        job.load_table(db, LIMS, parts(10, 20), job.TableResult("lims"))

    assert db.connection.sql()[-1] == "ROLLBACK"
    assert "SAVEPOINT swap" not in db.connection.sql()


def test_load_table_rolls_back_when_an_import_fails():
    db = job.Database(FakeConnection(errors={job.IMPORT_SQL: ["XX000"]}))

    with pytest.raises(job.DatabaseError):
        job.load_table(db, LIMS, parts(10), job.TableResult("lims"))

    assert db.connection.sql()[-1] == "ROLLBACK"
    assert "COMMIT" not in db.connection.sql()


def test_load_table_refuses_to_empty_a_table_unless_allowed():
    db = job.Database(FakeConnection(counts=(0, 5), imported=()))
    with pytest.raises(job.LoadError, match="from 5 rows to 0"):
        job.load_table(db, LIMS, (), job.TableResult("lims"))
    assert db.connection.sql()[-1] == "ROLLBACK"
    assert "SAVEPOINT swap" not in db.connection.sql()

    db = job.Database(FakeConnection(counts=(0, 5), imported=()))
    result = job.TableResult("lims")
    job.load_table(db, LIMS, (), result, job.LoadOptions(allow_empty=True))
    assert db.connection.sql()[-1] == "COMMIT"
    assert (result.rows_before, result.rows_after) == (5, 0)


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
        s3,
        None,
        BUCKET,
        PREFIX,
        ("lims",),
        SPECS,
        LZ_BUCKET,
        "orcaglue/x/dev",
        dry_run=True,
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
    specs = {**SPECS, "fastq": job.TableSpec("fastq", (("library_id", "text"),))}
    connect_db, connections = failing_connect()

    with pytest.raises(RuntimeError, match="1 of 2 mart tables failed: fastq"):
        job.load_marts(
            s3,
            connect_db,
            BUCKET,
            PREFIX,
            ("fastq", "lims"),
            specs,
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


def test_process_table_fails_a_table_the_ddl_file_lacks():
    s3 = FakeS3()
    s3.add_table("fastq", sizes=(10,))

    result = job.process_table(s3, None, BUCKET, PREFIX, "fastq", SPECS, dry_run=True)

    assert result.status == "failed"
    assert "The DDL file has no fastq table" in result.error
    assert s3.copies == []


def test_process_table_fails_the_table_when_the_login_fails():
    s3 = FakeS3()
    s3.add_table("lims", sizes=(10,))
    connect_db, _ = failing_connect(job.InterfaceError("network error"))

    result = job.process_table(
        s3, connect_db, BUCKET, PREFIX, "lims", SPECS, dry_run=False
    )

    assert (result.status, result.error) == ("failed", "InterfaceError: network error")


def test_pg_type_maps_the_redshift_mart_types():
    assert ddl.pg_type(ddl.Column("a", "character varying", 255)) == "varchar(255)"
    assert ddl.pg_type(ddl.Column("a", "character", 16)) == "char(16)"
    assert ddl.pg_type(ddl.Column("a", "numeric", None, 18, 2)) == "numeric(18,2)"
    assert ddl.pg_type(ddl.Column("a", "timestamp with time zone")) == "timestamptz"
    assert ddl.pg_type(ddl.Column("a", "smallint")) == "smallint"
    with pytest.raises(ValueError, match="No PostgreSQL type"):
        ddl.pg_type(ddl.Column("a", "super"))


def test_table_spec_keeps_the_redshift_column_order_and_the_indexes():
    columns = [
        ddl.Column("portal_run_id", "character", 16),
        ddl.Column("key", "character varying", 1024),
    ]

    assert ddl.table_spec("bam", columns, [("portal_run_id",)]) == job.TableSpec(
        "bam",
        (("portal_run_id", "char(16)"), ("key", "varchar(1024)")),
        (("portal_run_id",),),
    )


def test_render_ddl_round_trips_with_a_column_per_line(tmp_path):
    specs = {"lims": LIMS, "bam": job.TableSpec("bam", (("key", "varchar(1024)"),))}
    path = tmp_path / "mart_tables.json"

    text = ddl.render_ddl(specs, source="test")
    path.write_text(text, encoding="utf-8")

    assert job.parse_ddl(text) == specs
    assert '        ["library_id", "varchar(255)"],' in text.splitlines()
    assert ddl.read_indexes(str(path)) == {"lims": (("sequencing_run_id",),), "bam": ()}
    assert ddl.read_indexes(str(tmp_path / "missing.json")) == {}


def test_ddl_file_defines_exactly_the_default_tables():
    specs = job.parse_ddl(DDL_FILE.read_text(encoding="utf-8"))

    assert tuple(specs) == job.MART_TABLES_DEFAULT
    assert specs["lims"].indexes == (("sequencing_run_id",),)


@pytest.mark.parametrize("stack", ["dev", "prod"])
def test_stack_mart_tables_are_in_the_ddl_file(stack):
    config = (Path(__file__).parents[1] / f"Pulumi.{stack}.yaml").read_text(
        encoding="utf-8"
    )
    value = re.search(
        r"^\s*spreadsheet-mart-tables:mart-tables: (\S+)$", config, re.MULTILINE
    )

    tables = job.parse_tables(value.group(1))

    assert set(tables) <= set(job.parse_ddl(DDL_FILE.read_text(encoding="utf-8")))
