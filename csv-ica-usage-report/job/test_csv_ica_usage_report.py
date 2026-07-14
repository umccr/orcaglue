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


def test_transform_combines_files_and_writes_expected_columns(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "BASE_NAME", job.BASE_NAME_DEFAULT)
    monkeypatch.setattr(
        job,
        "OUT_PATH",
        str(tmp_path / "orcavault_tsa_csv__ica_usage_report"),
    )

    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"

    write_csv(
        first,
        "\n".join(
            [
                "Usage ID,Billing Date,Cost,Metadata",
                " u-1 , 2026-07-01 , 1.20 , workflow id: wfr.1 ",
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

    output = Path(result.csv_file).read_text(encoding="utf-8").splitlines()
    assert output[0].split(",") == list(job.EXPECTED_COLUMNS)
    assert "u-1" in output[1]
    assert "workflow id: wfr.1" in output[1]


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


def test_load_sql_uses_delete_insert_not_truncate():
    stage_sql = "\n".join(
        job.build_staging_load_sql(
            "s3://example-bucket/orcaglue/csv__ica_usage_report/dev/report.csv",
            "arn:aws:iam::115253169271:role/dev-redshift-namespace-role",
        )
    )
    swap_sql = "\n".join(job.build_target_swap_sql())

    assert "TRUNCATE" not in stage_sql.upper()
    assert "TRUNCATE" not in swap_sql.upper()
    assert "COPY tsa.csv__ica_usage_report__staging" in stage_sql
    assert "INSERT INTO tsa.csv__ica_usage_report__previous" in swap_sql
    assert "INSERT INTO tsa.csv__ica_usage_report" in swap_sql
