import csv
import json
from pathlib import Path

import pytest
import spreadsheet_ica_usage_report as job


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
        str(tmp_path / "orcavault_tsa_spreadsheet__ica_usage_report"),
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
        "usage_id,billing_date,cost,metadata\nu-2,2026-07-02,2.40,workflow id: wfr.2",
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
    assert manifest["target_table"] == "orcavault.tsa.spreadsheet__ica_usage_report"
    assert manifest["expected_columns"] == list(job.OUTPUT_COLUMNS)
    assert "staging_table" not in manifest
    assert "previous_table" not in manifest


def test_transform_rejects_schema_drift(tmp_path, monkeypatch):
    monkeypatch.setattr(
        job,
        "OUT_PATH",
        str(tmp_path / "orcavault_tsa_spreadsheet__ica_usage_report"),
    )

    source = tmp_path / "drift.csv"
    write_csv(
        source,
        "usage_id,billing_date,unexpected_column\nu-1,2026-07-01,nope",
    )

    with pytest.raises(ValueError, match="Unexpected columns"):
        job.transform([downloaded(source, "ica-usage-reports/drift.csv")])


def test_load_sql_truncates_and_reloads_target_without_safety_tables():
    init_sql = job.init_sql()
    load_sql = "\n".join(
        job.build_target_load_sql(
            "s3://example-bucket/orcaglue/spreadsheet__ica_usage_report/dev/report.csv",
            "arn:aws:iam::115253169271:role/dev-redshift-namespace-role",
        )
    )

    assert "__staging" not in init_sql
    assert "__previous" not in init_sql
    assert (
        "DROP TABLE IF EXISTS orcavault.tsa.spreadsheet__ica_usage_report;" in init_sql
    )
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "orcavault.tsa.spreadsheet__ica_usage_report" in init_sql
    )
    assert "__staging" not in load_sql
    assert "__previous" not in load_sql
    assert "DELETE FROM" not in load_sql.upper()
    assert "TRUNCATE TABLE tsa.spreadsheet__ica_usage_report" in load_sql
    assert "COPY tsa.spreadsheet__ica_usage_report" in load_sql
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
        csv_s3_uri=(
            "s3://example-bucket/orcaglue/spreadsheet__ica_usage_report/dev/report.csv"
        ),
        expected_rows=2,
    )

    assert len(client.sqls) == 3
    assert client.sqls[0] == "TRUNCATE TABLE tsa.spreadsheet__ica_usage_report;"
    assert "COPY tsa.spreadsheet__ica_usage_report" in client.sqls[1]
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


LEGACY_HEADER = ",".join(job.LEGACY_SOURCE_COLUMNS)
BIOINSIGHT_HEADER = ",".join(job.BIOINSIGHT_SOURCE_COLUMNS)

# One 2026-04 storage row: cost is the discounted amount, so quantity multiplied
# by the list price_per_unit does not reproduce it. The BioInsight Core layout is
# what finally makes that gap explicit.
LEGACY_ROW = (
    "711951760,University of Melbourne,acct-1,umccr-prod,DOMAIN,production,"
    "Project,services@umccr.org,BaseSpace Sequence Hub,Standard Storage,"
    "0.0627312336,TB-Months,22.4300372066,1.4070639035,iCredits,Storage,"
    "04/01/2026 00:00:00,Sydney,,04/01/2026 00:00:00"
)

# The same storage charge under the 2026-05 layout: cost is quantity multiplied
# by applied_rate exactly, and the discount shows up in cost_saved.
BIOINSIGHT_ROW = (
    "717169592,1,University of Melbourne,acct-1,umccr-prod,DOMAIN,production,"
    "Project,services@umccr.org,BaseSpace Sequence Hub,Standard Storage,"
    "0.0627312336,TB-Months,Storage,05/01/2026 00:00:00,Sydney,"
    "ica_v2:true|is_in_grace_period:false,05/01/2026 00:00:00,BIC,Standard Rate,"
    "22.4300372066,22.0630817979,1.3840443380,0.0230195654"
)


def test_detect_source_layout():
    assert job.detect_source_layout(list(job.LEGACY_SOURCE_COLUMNS)) == "legacy"
    assert job.detect_source_layout(list(job.BIOINSIGHT_SOURCE_COLUMNS)) == "bioinsight"
    assert job.detect_source_layout(list(job.EXPECTED_COLUMNS)) == "mixed"
    assert job.detect_source_layout(["usage_id", "billing_date", "cost"]) == "unknown"


def test_transform_loads_both_source_layouts_side_by_side(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "BASE_NAME", job.BASE_NAME_DEFAULT)
    monkeypatch.setattr(
        job,
        "OUT_PATH",
        str(tmp_path / "orcavault_tsa_spreadsheet__ica_usage_report"),
    )

    legacy = tmp_path / "ica-detailed-usage-2026-04.csv"
    bioinsight = tmp_path / "ica-detailed-usage-2026-05.csv"
    write_csv(legacy, f"{LEGACY_HEADER}\n{LEGACY_ROW}\n")
    write_csv(bioinsight, f"{BIOINSIGHT_HEADER}\n{BIOINSIGHT_ROW}\n")

    result = job.transform(
        [
            downloaded(legacy, "ica-usage-reports/ica-detailed-usage-2026-04.csv"),
            downloaded(bioinsight, "ica-usage-reports/ica-detailed-usage-2026-05.csv"),
        ]
    )

    assert result.row_count == 2
    assert result.source_layouts == {"legacy": 1, "bioinsight": 1}

    with Path(result.csv_file).open(encoding="utf-8", newline="") as handle:
        old_row, new_row = list(csv.DictReader(handle))

    # The legacy layout has no rate breakdown and no row sequence.
    assert old_row["price_per_unit"] == "22.4300372066"
    assert old_row["cost_unit"] == "iCredits"
    assert old_row["row_seq"] == ""
    assert old_row["list_rate"] == ""
    assert old_row["applied_rate"] == ""
    assert old_row["pricing_method"] == ""
    assert old_row["cost_saved"] == ""

    # The BioInsight Core layout splits the old price_per_unit into list and
    # applied rates, and states the saving outright.
    assert new_row["price_per_unit"] == ""
    assert new_row["row_seq"] == "1"
    assert new_row["cost_unit"] == "BIC"
    assert new_row["pricing_method"] == "Standard Rate"
    assert new_row["list_rate"] == "22.4300372066"
    assert new_row["applied_rate"] == "22.0630817979"
    assert new_row["cost_saved"] == "0.0230195654"

    # iCredits and BIC are stored exactly as the source wrote them. The 1:1
    # conversion means the values stay comparable across the cutover.
    assert old_row["quantity"] == new_row["quantity"]

    # Storage rows only started carrying these two metadata keys in 2026-05.
    assert old_row["ica_v2"] == ""
    assert old_row["is_in_grace_period"] == ""
    assert new_row["ica_v2"] == "true"
    assert new_row["is_in_grace_period"] == "false"

    manifest = json.loads(Path(result.manifest_file).read_text(encoding="utf-8"))
    assert manifest["source_layouts"] == {"legacy": 1, "bioinsight": 1}


def test_init_sql_covers_both_layouts():
    init_sql = job.init_sql()

    for column in job.LEGACY_SOURCE_COLUMNS + job.BIOINSIGHT_SOURCE_COLUMNS:
        assert f"    {column.ljust(28)} varchar(65535)" in init_sql

    assert "    ica_v2".ljust(32) in init_sql
    assert "is_in_grace_period" in init_sql


def test_init_sql_file_matches_generated_schema():
    """job/init.sql is generated from this module and must not drift from it."""
    deployed = Path(__file__).with_name("init.sql").read_text(encoding="utf-8")

    assert job.init_sql() in deployed
