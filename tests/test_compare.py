import json
from pathlib import Path

import pytest

from hms_export.compare import Record, compare_records, load_records, records_from_object


URN = "urn:li:dataset:(urn:li:dataPlatform:hive,hive.orders,UAT)"


def record(aspect_name: str, aspect: object, urn: str = URN, entity_type: str = "dataset") -> Record:
    return Record(entity_type, urn, aspect_name, aspect)


def test_reads_flat_and_wrapped_mcp() -> None:
    flat = {"entityType": "dataset", "entityUrn": URN, "aspectName": "status", "aspect": {"removed": False}}
    wrapped = {**flat, "aspect": {"value": json.dumps({"removed": False}), "contentType": "application/json"}}
    assert list(records_from_object(flat)) == list(records_from_object(wrapped))


def test_reads_datahub_file_sink_json_wrapper() -> None:
    wrapped = {
        "entityType": "dataset", "entityUrn": URN, "aspectName": "status",
        "aspect": {"json": {"removed": False}},
    }
    assert list(records_from_object(wrapped)) == [record("status", {"removed": False})]


def test_reads_mce() -> None:
    obj = {"proposedSnapshot": {"com.linkedin.metadata.snapshot.DatasetSnapshot": {
        "urn": URN,
        "aspects": [{"com.linkedin.common.Status": {"removed": False}}],
    }}}
    assert list(records_from_object(obj)) == [record("status", {"removed": False})]


def test_load_records_supports_array_jsonl_and_directory(tmp_path: Path) -> None:
    array_file = tmp_path / "baseline.json"
    array_file.write_text(json.dumps([
        {"entityType": "dataset", "entityUrn": URN, "aspectName": "status", "aspect": {"removed": False}}
    ]))
    assert len(load_records(array_file)) == 1
    output = tmp_path / "output"
    output.mkdir()
    (output / "part-0000").write_text(json.dumps(
        {"entityType": "dataset", "entityUrn": URN, "aspectName": "status", "aspect": {"removed": False}}
    ) + "\n")
    (output / "_SUCCESS").write_text("")
    assert len(load_records(output)) == 1


def test_array_reader_handles_values_across_stream_chunks(tmp_path: Path) -> None:
    file = tmp_path / "large.json"
    large_description = "x" * 70000
    file.write_text(json.dumps([
        {"entityType": "dataset", "entityUrn": URN, "aspectName": "datasetProperties", "aspect": {"description": large_description}},
        {"entityType": "dataset", "entityUrn": URN, "aspectName": "status", "aspect": {"removed": False}},
    ]))
    records = load_records(file)
    assert len(records) == 2
    assert records[0].aspect["description"] == large_description


def test_compare_equal_ignores_serialization_and_audit_fields() -> None:
    baseline = [record("status", {"removed": False, "lastModified": {"time": 12}})]
    candidate = [record("status", {"removed": False})]
    assert compare_records(baseline, candidate)["equal"] is True


def test_compare_normalizes_container_urns_by_name() -> None:
    old, new = "urn:li:container:hash", "urn:li:container:hive.UAT.hive"
    baseline = [
        record("containerProperties", {"name": "hive"}, old, "container"),
        record("container", {"container": old}),
    ]
    candidate = [
        record("containerProperties", {"name": "hive"}, new, "container"),
        record("container", {"container": new}),
    ]
    assert compare_records(baseline, candidate)["equal"] is True


def test_compare_schema_fields_are_order_independent_and_case_normalized() -> None:
    first = {"schemaName": "hive.t", "platform": "urn:li:dataPlatform:hive", "fields": [
        {"fieldPath": "b", "nativeDataType": "STRING", "description": None, "nullable": True},
        {"fieldPath": "a", "nativeDataType": "BIGINT", "description": "id", "nullable": True},
    ]}
    second = {"schemaName": "hive.t", "platform": "urn:li:dataPlatform:hive", "fields": [
        {"fieldPath": "a", "nativeDataType": "bigint", "description": "id", "nullable": True},
        {"fieldPath": "b", "nativeDataType": "string", "description": "", "nullable": True},
    ]}
    assert compare_records([record("schemaMetadata", first)], [record("schemaMetadata", second)])["equal"]


def test_compare_reports_missing_extra_and_field_difference() -> None:
    baseline = [record("status", {"removed": False}), record("subTypes", {"typeNames": ["Table"]})]
    candidate = [record("status", {"removed": True}), record("ownership", {"owners": []})]
    report = compare_records(baseline, candidate)
    assert report["equal"] is False
    assert report["summary"] == {
        "baseline_aspects": 2, "candidate_aspects": 2,
        "missing_in_candidate": 1, "extra_in_candidate": 1, "different_aspects": 1,
    }
    assert report["differences"][0]["fields"][0]["path"] == "removed"


def test_compare_reports_duplicate_aspects() -> None:
    report = compare_records(
        [record("status", {"removed": False}), record("status", {"removed": True})],
        [record("status", {"removed": False})],
    )
    assert report["equal"] is False
    assert report["duplicates"]["baseline"] == [f"{URN}#status"]


def test_unknown_record_is_rejected() -> None:
    with pytest.raises(ValueError, match="neither an MCP nor an MCE"):
        list(records_from_object({"hello": "world"}))
