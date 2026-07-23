from ica_cost_metadata import PARSED_METADATA_KEYS, parse_ica_cost_metadata


ICA_EXECUTION_ID = "123e4567-e89b-12d3-a456-426614174000"
PORTAL_RUN_ID = "20260720ABCDEF12"


def test_parse_structured_workflow_reference():
    metadata = (
        f"id:{ICA_EXECUTION_ID}|license:license-id|pipelineUuid:pipeline-id|"
        "status:Succeeded|"
        "reference:research--workflow--dragen-wgs--4.2.4--"
        f"{PORTAL_RUN_ID}-{ICA_EXECUTION_ID}"
    )

    assert parse_ica_cost_metadata(metadata) == {
        "id": ICA_EXECUTION_ID,
        "license": "license-id",
        "pipeline_uuid": "pipeline-id",
        "status": "Succeeded",
        "domain": "research",
        "type": "workflow",
        "workflow_name": "dragen-wgs",
        "workflow_version": "4.2.4",
        "portal_run_id": PORTAL_RUN_ID,
        "ref_format": "umccr_workflow_run",
        "reference_raw": None,
        "ref_uuid": ICA_EXECUTION_ID,
        "id_matches_reference": True,
    }


def test_parse_uuid_only_reference():
    result = parse_ica_cost_metadata(
        f"id:{ICA_EXECUTION_ID}|reference:legacy-reference-{ICA_EXECUTION_ID}"
    )

    assert result["ref_format"] == "uuid_only"
    assert result["reference_raw"] == "legacy-reference"
    assert result["ref_uuid"] == ICA_EXECUTION_ID
    assert result["id_matches_reference"] is True


def test_parse_unknown_and_missing_references():
    unknown = parse_ica_cost_metadata("id:execution-id|reference:unknown")
    missing = parse_ica_cost_metadata("id:execution-id|status:Succeeded")
    empty = parse_ica_cost_metadata(None)

    assert unknown["ref_format"] == "unknown"
    assert unknown["ref_uuid"] is None
    assert unknown["id_matches_reference"] is None
    assert missing["ref_format"] == "no_reference"
    assert empty["ref_format"] == "no_reference"
    assert tuple(empty) == PARSED_METADATA_KEYS


def test_parse_preserves_legacy_pair_handling():
    result = parse_ica_cost_metadata(
        "status|status:failed:retry|license:first|license:second|reference:"
    )

    assert result["status"] == "failed:retry"
    assert result["license"] == "second"
    assert result["ref_format"] == "unknown"
    assert result["id_matches_reference"] is None


def test_parse_key_only_and_empty_segments():
    result = parse_ica_cost_metadata("status|reference|license:||:ignored|")

    assert result["status"] is None
    assert result["license"] is None
    assert result["ref_format"] == "unknown"
    assert result["reference_raw"] is None
    assert result["ref_uuid"] is None
