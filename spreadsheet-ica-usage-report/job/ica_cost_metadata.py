from __future__ import annotations

import re


PARSED_METADATA_KEYS = (
    "id",
    "license",
    "pipeline_uuid",
    "status",
    "domain",
    "type",
    "workflow_name",
    "workflow_version",
    "portal_run_id",
    "ref_format",
    "reference_raw",
    "ref_uuid",
    "id_matches_reference",
)

STRUCTURED_REFERENCE_PATTERN = re.compile(
    r"^([^-]+)--([^-]+)--(.+)--(.+)--"
    r"([0-9]{8}[A-Za-z0-9]{8})-([0-9a-fA-F-]{36})$"
)
UUID_ONLY_REFERENCE_PATTERN = re.compile(r"^(.*)-([0-9a-fA-F-]{36})$")


def parse_ica_cost_metadata(metadata: str | None) -> dict[str, str | bool | None]:
    """Reproduce the legacy dbt ``parse_event`` macro for one metadata value."""
    data: dict[str, str | None] = {}

    if metadata is not None:
        for pair in metadata.split("|"):
            if not pair:
                continue

            if ":" in pair:
                key, value = pair.split(":", 1)
            else:
                key, value = pair, None

            if not key:
                continue

            data[key] = value if value else None

    reference_exists = "reference" in data
    reference = data.get("reference")

    structured_match = (
        STRUCTURED_REFERENCE_PATTERN.match(reference)
        if reference_exists and reference is not None
        else None
    )
    uuid_only_match = (
        UUID_ONLY_REFERENCE_PATTERN.match(reference)
        if reference_exists and reference is not None
        else None
    )

    if structured_match is not None:
        ref_format = "umccr_workflow_run"
        reference_raw = None
        ref_uuid = structured_match.group(6)
    elif uuid_only_match is not None:
        ref_format = "uuid_only"
        reference_raw = uuid_only_match.group(1)
        ref_uuid = uuid_only_match.group(2)
    elif reference_exists:
        ref_format = "unknown"
        reference_raw = None
        ref_uuid = None
    else:
        ref_format = "no_reference"
        reference_raw = None
        ref_uuid = None

    metadata_id = data.get("id")
    id_matches_reference = (
        metadata_id == ref_uuid
        if metadata_id is not None and ref_uuid is not None
        else None
    )

    return {
        "id": metadata_id,
        "license": data.get("license"),
        "pipeline_uuid": data.get("pipelineUuid"),
        "status": data.get("status"),
        "domain": structured_match.group(1) if structured_match is not None else None,
        "type": structured_match.group(2) if structured_match is not None else None,
        "workflow_name": structured_match.group(3)
        if structured_match is not None
        else None,
        "workflow_version": structured_match.group(4)
        if structured_match is not None
        else None,
        "portal_run_id": structured_match.group(5)
        if structured_match is not None
        else None,
        "ref_format": ref_format,
        "reference_raw": reference_raw,
        "ref_uuid": ref_uuid,
        "id_matches_reference": id_matches_reference,
    }
