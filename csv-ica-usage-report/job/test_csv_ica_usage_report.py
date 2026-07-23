import csv
import json
from pathlib import Path

import pytest

import csv_ica_usage_report as job


def write_csv(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def downloaded(path: Path, key: str) -> job.DownloadedObject:
    return job.DownloadedObject(
        source=job.SourceObject(
            key=key,
            size=path.stat().st_size,
            etag="test-etag",
            last_modified="2026-07-14T00:00:00+00:00",
        ),
        local_path=str(path),
    )


def test_normalise_column_name():
    assert job.normalise_column_name("Usage ID") == "usage_id"
    assert job.normalise_column_name(" Price/Unit ") == "price_unit"
    assert job.normalise_column_name("__UNNAMED__: 1") == "unnamed_1"


def test_parse_bool_supports_dry_run_default():
    assert job.parse_bool(None) is False
    assert job.parse_bool(None, default=False) is False
    assert job.parse_bool("true", default=False) is True
    assert job.parse_bool("false", default=True) is False


def test_transform_combines_files_and_writes_expected_columns(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "BASE_NAME", job.BASE_NAME_DEFAULT)
    monkeypatch.setattr(
        job,
        "OUT_PATH",
        str(tmp_path / "orcavault_tsa_csv__ica_usage_report"),
    )

    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    execution_id = "123e4567-e89b-12d3-a456-426614174000"
    portal_run_id = "20260720ABCDEF12"
    structured_metadata = (
        f"id:{execution_id}|license:license-id|pipelineUuid:pipeline-id|"
        "status:Succeeded|"
        "reference:research--workflow--dragen-wgs--4.2.4--"
        f"{portal_run_id}-{execution_id}"
    )

    write_csv(
        first,
        "\n".join(
            [
                "Usage ID,Billing Date,Cost,Metadata",
                f" u-1 , 2026-07-01 , 1.20 , {structured_metadata} ",
                " , , , ",
            ]
        ),
    )
    write_csv(
        second,
        "\n".join(
            [
                "usage_id,billing_date,cost,metadata",
                "u-2,2026-07-02,2.40,workflow id: wfr.2",
            ]
        ),
    )

    result = job.transform(
        [
            downloaded(first, "ica-usage-reports/first.csv"),
            downloaded(second, "ica-usage-reports/second.csv"),
        ]
    )

    assert result.row_count == 2
    assert Path(result.csv_file).exists()
    assert Path(result.sql_file).exists()
    assert Path(result.manifest_file).exists()

    with Path(result.csv_file).open(encoding="utf-8", newline="") as handle:
        output = list(csv.DictReader(handle))

    assert list(output[0]) == list(job.OUTPUT_COLUMNS)
    assert output[0]["usage_id"] == "u-1"
    assert output[0]["metadata"] == structured_metadata
    assert output[0]["ica_execution_id"] == execution_id
    assert output[0]["portal_run_id"] == portal_run_id
    assert output[0]["ref_format"] == "umccr_workflow_run"
    assert output[0]["id_matches_reference"] == "true"
    assert output[1]["ref_format"] == "no_reference"

    manifest = json.loads(Path(result.manifest_file).read_text(encoding="utf-8"))
    assert manifest["target_table"] == "orcavault.tsa.csv__ica_usage_report"
    assert manifest["expected_columns"] == list(job.OUTPUT_COLUMNS)
    assert "staging_table" not in manifest
    assert "previous_table" not in manifest


def test_transform_rejects_schema_drift(tmp_path, monkeypatch):
    monkeypatch.setattr(
        job,
        "OUT_PATH",
        str(tmp_path / "orcavault_tsa_csv__ica_usage_report"),
    )

    source = tmp_path / "drift.csv"
    write_csv(
        source,
        "\n".join(
            [
                "usage_id,billing_date,unexpected_column",
                "u-1,2026-07-01,nope",
            ]
        ),
    )

    with pytest.raises(ValueError, match="Unexpected columns"):
        job.transform([downloaded(source, "ica-usage-reports/drift.csv")])


def test_load_sql_truncates_and_reloads_target_without_safety_tables():
    init_sql = job.init_sql()
    load_sql = "\n".join(
        job.build_target_load_sql(
            "s3://example-bucket/orcaglue/csv__ica_usage_report/dev/report.csv",
            "arn:aws:iam::115253169271:role/dev-redshift-namespace-role",
        )
    )

    assert "__staging" not in init_sql
    assert "__previous" not in init_sql
    assert "DROP TABLE IF EXISTS orcavault.tsa.csv__ica_usage_report;" in init_sql
    assert "CREATE TABLE IF NOT EXISTS orcavault.tsa.csv__ica_usage_report" in init_sql
    assert "__staging" not in load_sql
    assert "__previous" not in load_sql
    assert "DELETE FROM" not in load_sql.upper()
    assert "TRUNCATE TABLE tsa.csv__ica_usage_report" in load_sql
    assert "COPY tsa.csv__ica_usage_report" in load_sql
    assert '"ica_execution_id"' in load_sql
    assert '"id_matches_reference"' in load_sql


def test_load_to_redshift_uses_existing_shared_infra_permissions(monkeypatch):
    class ExistingPermissionClient:
        def __init__(self):
            self.sqls = []

        def execute_statement(self, **kwargs):
            self.sqls.append(kwargs["Sql"])
            return {"Id": f"statement-{len(self.sqls)}"}

        def describe_statement(self, Id: str) -> dict[str, str | int]:
            assert Id.startswith("statement-")
            return {"Status": "FINISHED", "ResultRows": 1}

    client = ExistingPermissionClient()
    monkeypatch.setattr(job, "get_boto3_client", lambda service: client)

    job.load_to_redshift(
        workgroup="orcahouse-dev",
        role="arn:aws:iam::115253169271:role/dev-redshift-namespace-role",
        csv_s3_uri="s3://example-bucket/orcaglue/csv__ica_usage_report/dev/report.csv",
        expected_rows=2,
    )

    assert len(client.sqls) == 3
    assert client.sqls[0] == "TRUNCATE TABLE tsa.csv__ica_usage_report;"
    assert "COPY tsa.csv__ica_usage_report" in client.sqls[1]
    assert "SELECT COUNT(*)" in client.sqls[2]


def test_wait_for_query_times_out_with_last_status(monkeypatch):
    class PendingClient:
        def describe_statement(self, Id: str) -> dict[str, str]:
            assert Id == "statement-1"
            return {"Status": "STARTED"}

    ticks = iter([0, 1, 3])
    sleeps = []

    monkeypatch.setattr(job.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(job.time, "sleep", lambda seconds: sleeps.append(seconds))

    with pytest.raises(
        TimeoutError,
        match=(
            "Timed out after 2 seconds waiting for Redshift Data API statement "
            "statement-1; last status=STARTED"
        ),
    ):
        job._wait_for_query(
            PendingClient(),
            "statement-1",
            timeout_seconds=2,
            poll_interval_seconds=1,
        )

    assert sleeps == [1]
