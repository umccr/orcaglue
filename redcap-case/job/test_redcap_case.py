import csv
from pathlib import Path

import pytest
import redcap_case as job

class FakeResponse:
    def __init__(self, status_code=200, content=b""):
        self.status_code = status_code
        self.content = content
        self.text = content.decode("utf-8", errors="replace")

@pytest.fixture
def out_path(tmp_path, monkeypatch):
    """Point every module-level output global at a temp directory."""
    base = tmp_path / "orcavault_tsa_redcap__case"
    monkeypatch.setattr(job, "BASE_NAME", job.BASE_NAME_DEFAULT)
    monkeypatch.setattr(job, "S3_MID_PATH", "orcaglue/redcap__case/test")
    monkeypatch.setattr(job, "OUT_NAME", "orcavault_tsa_redcap__case")
    monkeypatch.setattr(job, "OUT_NAME_DOT", "orcavault.tsa.redcap__case")
    monkeypatch.setattr(job, "OUT_PATH", str(base))
    return base

def write_raw(out_path: Path, content: str) -> None:
    Path(f"{out_path}__raw.csv").write_text(content, encoding="utf-8")

def read_rows(path: str) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))

# --- parse_bool ---

def test_parse_bool_defaults_and_values():
    assert job.parse_bool(None) is False
    assert job.parse_bool(None, default=True) is True
    assert job.parse_bool(True) is True
    assert job.parse_bool(" TRUE ") is True
    assert job.parse_bool("no") is False

def test_parse_bool_rejects_garbage():
    with pytest.raises(ValueError, match="Cannot parse boolean"):
        job.parse_bool("maybe")

# --- extract ---

def test_extract_posts_expected_payload_and_writes_raw_bytes(out_path, monkeypatch):
    calls = {}
    body = b'col_a,col_b\n1,"line one\nline two"\n'

    def fake_post(url, data, timeout):
        calls.update(url=url, data=data, timeout=timeout)
        return FakeResponse(200, body)

    monkeypatch.setattr(job.libssm, "get_secret", lambda name: "secret-token")
    monkeypatch.setattr(job.requests, "post", fake_post)

    job.extract()

    assert calls["url"] == job.REDCAP_ENDPOINT
    assert calls["timeout"] == job.REQUEST_TIMEOUT
    assert calls["data"]["token"] == "secret-token"
    assert calls["data"]["content"] == "record"
    assert calls["data"]["action"] == "export"
    assert calls["data"]["format"] == "csv"
    assert not any(key.startswith("fields[") for key in calls["data"])
    # raw bytes are kept untouched, including the newline inside the quoted cell
    assert Path(f"{out_path}__raw.csv").read_bytes() == body

# --- transform ---

def test_transform_ddl_lists_every_column_without_trailing_comma(out_path):
    write_raw(out_path, "col_a,col_b,col_c\n1,a,b\n")

    columns = job.transform()

    sql = Path(f"{out_path}.sql").read_text(encoding="utf-8")
    assert columns == ["col_a", "col_b", "col_c"]
    for column in columns:
        assert f'"{column}"' in sql
    assert not any(line.rstrip().endswith(",") for line in sql.splitlines()[-2:])

def test_transform_keeps_everything_as_text(out_path):
    write_raw(out_path, "col_a,col_b,col_c\n001,007,1.50\n")

    job.transform()

    row = read_rows(f"{out_path}.csv")[0]
    assert row == {"col_a": "001", "col_b": "007", "col_c": "1.50"}

def test_transform_nulls_blanks_strips_whitespace_and_drops_empty_rows(out_path):
    write_raw(
        out_path,
        "col_a,col_b\n  1  ,  value_a  \n2,\n , \n",
    )

    job.transform()

    rows = read_rows(f"{out_path}.csv")
    assert rows == [
        {"col_a": "1", "col_b": "value_a"},
        {"col_a": "2", "col_b": ""},
    ]

def test_transform_keeps_column_order_and_values(out_path):
    header = "col_a,col_b___x,col_b___y,col_c"
    write_raw(out_path, f"{header}\n1,1,0,2\n")

    job.transform()

    rows = read_rows(f"{out_path}.csv")
    assert list(rows[0]) == header.split(",")
    assert list(rows[0].values()) == ["1", "1", "0", "2"]

def test_transform_preserves_multiline_free_text(out_path):
    write_raw(out_path, 'col_a,col_b\n1,"first line\nsecond ""quoted"" line"\n')

    job.transform()

    assert read_rows(f"{out_path}.csv")[0]["col_b"] == 'first line\nsecond "quoted" line'

def test_transform_drops_unnamed_columns(out_path):
    write_raw(out_path, "col_a,,col_b\n1,junk,value_a\n")

    job.transform()

    columns = list(read_rows(f"{out_path}.csv")[0])
    assert columns == ["col_a", "col_b"]

def test_transform_refuses_an_export_with_no_records(out_path):
    write_raw(out_path, "col_a,col_b\n")

    with pytest.raises(RuntimeError, match="no records"):
        job.transform()

    assert not Path(f"{out_path}.csv").exists()

def test_transform_refuses_an_export_of_only_blank_rows(out_path):
    write_raw(out_path, "col_a,col_b\n,\n , \n")

    with pytest.raises(RuntimeError, match="no records"):
        job.transform()

def test_transform_writes_ddl_matching_the_csv_columns(out_path):
    write_raw(out_path, "col_a,timestamp,col_b\n1,x,y\n")

    job.transform()

    sql = Path(f"{out_path}.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS orcavault.tsa.redcap__case" in sql
    # quoted, in CSV order, wide enough for free text
    positions = [
        sql.index('"col_a"\tvarchar(65535)'),
        sql.index('"timestamp"\tvarchar(65535)'),
        sql.index('"col_b"\tvarchar(65535)'),
    ]
    assert positions == sorted(positions)
    # only the last column has no trailing comma
    assert sql.count("varchar(65535),") == 2

# --- load ---

class FakeRedshiftData:
    def __init__(self, statuses=("FINISHED",), error=None):
        self.statements = []
        self.statuses = list(statuses)
        self.error = error

    def execute_statement(self, **kwargs):
        self.statements.append(kwargs)
        return {"Id": f"stmt-{len(self.statements)}"}

    def describe_statement(self, Id):
        status = self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]
        response = {"Status": status}
        if self.error:
            response["Error"] = self.error
        return response

def test_load_to_redshift_truncates_then_copies(monkeypatch):
    fake = FakeRedshiftData()
    monkeypatch.setattr(job.boto3, "client", lambda *a, **k: fake)
    monkeypatch.setattr(job, "BASE_NAME", "redcap__case")

    job.load_to_redshift(
        workgroup="orcahouse-dev",
        role="arn:aws:iam::123456789012:role/test",
        s3_csv_uri="s3://bucket/path/file.csv",
        columns=["record_id", "col_b", "col_a"],
    )

    assert len(fake.statements) == 2
    truncate, copy = fake.statements
    assert truncate["Sql"].strip() == "TRUNCATE TABLE tsa.redcap__case;"
    assert 'COPY tsa.redcap__case ("record_id", "col_b", "col_a")' in copy["Sql"]
    assert "FROM 's3://bucket/path/file.csv'" in copy["Sql"]
    assert "IGNOREHEADER 1" in copy["Sql"]
    for statement in fake.statements:
        assert statement["WorkgroupName"] == "orcahouse-dev"
        assert statement["Database"] == job.DB_NAME

def test_wait_for_query_polls_until_finished(monkeypatch):
    fake = FakeRedshiftData(statuses=("SUBMITTED", "STARTED", "FINISHED"))
    sleeps = []
    monkeypatch.setattr(job.time, "sleep", lambda seconds: sleeps.append(seconds))

    job._wait_for_query(fake, "stmt-1")

    assert len(sleeps) == 2

@pytest.mark.parametrize("status", ["FAILED", "ABORTED"])
def test_wait_for_query_raises_on_terminal_failure(monkeypatch, status):
    fake = FakeRedshiftData(statuses=(status,), error="permission denied")
    monkeypatch.setattr(job.time, "sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="permission denied"):
        job._wait_for_query(fake, "stmt-1")

def test_load_does_not_copy_when_truncate_fails(monkeypatch):
    fake = FakeRedshiftData(statuses=("FAILED",), error="boom")
    monkeypatch.setattr(job.boto3, "client", lambda *a, **k: fake)
    monkeypatch.setattr(job.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(job, "BASE_NAME", "redcap__case")

    with pytest.raises(RuntimeError):
        job.load_to_redshift("wg", "role", "s3://bucket/file.csv", ["col_a"])

    assert len(fake.statements) == 1

# --- artefacts and clean up ---

def test_upload_artifacts_uploads_csv_and_sql(out_path, monkeypatch):
    uploads = []

    class FakeS3:
        def upload_file(self, filename, bucket, key):
            uploads.append((Path(filename).name, bucket, key))

    monkeypatch.setattr(job.libs3, "s3_client", lambda: FakeS3())

    uri = job.upload_artifacts("my-bucket")

    assert uri == "s3://my-bucket/orcaglue/redcap__case/test/orcavault_tsa_redcap__case.csv"
    assert uploads == [
        (
            "orcavault_tsa_redcap__case.csv",
            "my-bucket",
            "orcaglue/redcap__case/test/orcavault_tsa_redcap__case.csv",
        ),
        (
            "orcavault_tsa_redcap__case.sql",
            "my-bucket",
            "orcaglue/redcap__case/test/orcavault_tsa_redcap__case.sql",
        ),
    ]

def test_clean_up_removes_the_raw_export_only(out_path):
    raw = Path(f"{out_path}__raw.csv")
    cleaned = Path(f"{out_path}.csv")
    raw.write_text("secret\n")
    cleaned.write_text("kept\n")

    job.clean_up()

    assert not raw.exists()
    assert cleaned.exists()

def test_clean_up_is_safe_when_nothing_was_downloaded(out_path):
    job.clean_up()

