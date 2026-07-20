from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple


SUPPORTED_ASPECTS = {
    "containerProperties",
    "datasetProperties",
    "schemaMetadata",
    "status",
    "container",
    "ownership",
    "subTypes",
    "viewProperties",
}


@dataclass(frozen=True)
class Record:
    entity_type: str
    urn: str
    aspect_name: str
    aspect: Any


def input_files(path: Path) -> List[Path]:
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(path)
    return sorted(
        item for item in path.rglob("part-*")
        if item.is_file() and not item.name.startswith(("_", "."))
    )


def json_objects(path: Path) -> Iterator[Mapping[str, Any]]:
    with path.open(encoding="utf-8") as stream:
        first = ""
        while True:
            char = stream.read(1)
            if not char:
                return
            if not char.isspace():
                first = char
                break
        stream.seek(0)
        if first != "[":
            for line_number, line in enumerate(stream, 1):
                if line.strip():
                    value = json.loads(line)
                    if not isinstance(value, dict):
                        raise ValueError(f"expected JSON object at {path}:{line_number}")
                    yield value
            return

        # Incremental top-level array parsing keeps large DataHub file-sink outputs bounded.
        decoder = json.JSONDecoder()
        buffer = ""
        started = False
        eof = False
        while True:
            if not eof and not buffer:
                chunk = stream.read(65536)
                eof = not chunk
                buffer += chunk
            buffer = buffer.lstrip()
            if not started:
                if not buffer:
                    if eof:
                        raise ValueError(f"unterminated JSON array in {path}")
                    continue
                if buffer[0] != "[":
                    raise ValueError(f"expected JSON array in {path}")
                buffer, started = buffer[1:], True
                continue
            buffer = buffer.lstrip()
            if buffer.startswith(","):
                buffer = buffer[1:].lstrip()
            if buffer.startswith("]"):
                return
            if not buffer and eof:
                raise ValueError(f"unterminated JSON array in {path}")
            try:
                value, offset = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                if eof:
                    raise ValueError(f"invalid JSON array in {path}")
                chunk = stream.read(65536)
                eof = not chunk
                buffer += chunk
                continue
            if not isinstance(value, dict):
                raise ValueError(f"expected JSON object in array {path}")
            yield value
            buffer = buffer[offset:]


def _entity_type(urn: str) -> str:
    if urn.startswith("urn:li:dataset:"):
        return "dataset"
    if urn.startswith("urn:li:container:"):
        return "container"
    return "unknown"


def _aspect_name(class_name: str) -> str:
    short = class_name.rsplit(".", 1)[-1]
    return short[:1].lower() + short[1:]


def records_from_object(obj: Mapping[str, Any]) -> Iterator[Record]:
    if "metadataChangeProposal" in obj:
        obj = obj["metadataChangeProposal"]
    if "entityUrn" in obj and "aspectName" in obj:
        aspect = obj.get("aspect", {})
        if isinstance(aspect, dict) and "value" in aspect:
            aspect = aspect["value"]
            if isinstance(aspect, str):
                aspect = json.loads(aspect)
        if isinstance(aspect, dict) and set(aspect) == {"json"}:
            aspect = aspect["json"]
        yield Record(
            str(obj.get("entityType") or _entity_type(str(obj["entityUrn"]))),
            str(obj["entityUrn"]),
            str(obj["aspectName"]),
            aspect,
        )
        return

    proposed = obj.get("proposedSnapshot")
    if not isinstance(proposed, dict) or len(proposed) != 1:
        raise ValueError("record is neither an MCP nor an MCE proposedSnapshot")
    snapshot = next(iter(proposed.values()))
    urn = str(snapshot["urn"])
    for wrapped_aspect in snapshot.get("aspects", []):
        if not isinstance(wrapped_aspect, dict) or len(wrapped_aspect) != 1:
            raise ValueError(f"invalid MCE aspect for {urn}")
        class_name, aspect = next(iter(wrapped_aspect.items()))
        yield Record(_entity_type(urn), urn, _aspect_name(class_name), aspect)


def load_records(path: Path) -> List[Record]:
    records: List[Record] = []
    files = input_files(path)
    if not files:
        raise ValueError(f"no input files found under {path}")
    for file in files:
        for obj in json_objects(file):
            records.extend(records_from_object(obj))
    return records


def _clean(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _clean(item)
            for key, item in sorted(value.items())
            if key not in {"__type", "created", "lastModified"}
        }
    if isinstance(value, list):
        return [_clean(item) for item in value]
    return value


def _canonical_aspect(name: str, aspect: Any, containers: Mapping[str, str]) -> Any:
    if not isinstance(aspect, dict):
        return _clean(aspect)
    if name == "schemaMetadata":
        fields = []
        for field in aspect.get("fields", []):
            fields.append({
                "fieldPath": field.get("fieldPath"),
                "nativeDataType": (field.get("nativeDataType") or "").lower(),
                "description": field.get("description") or "",
                "nullable": bool(field.get("nullable", True)),
            })
        return {
            "schemaName": aspect.get("schemaName"),
            "platform": aspect.get("platform"),
            "fields": sorted(fields, key=lambda field: str(field["fieldPath"])),
        }
    if name == "ownership":
        owners = [
            {"owner": owner.get("owner"), "type": owner.get("type", "DATAOWNER")}
            for owner in aspect.get("owners", [])
        ]
        return {"owners": sorted(owners, key=lambda owner: str(owner["owner"]))}
    if name == "subTypes":
        return {"typeNames": sorted(aspect.get("typeNames", []))}
    if name == "container":
        urn = str(aspect.get("container", ""))
        return {"container": containers.get(urn, urn)}
    return _clean(aspect)


def _container_names(records: Sequence[Record]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for record in records:
        if record.aspect_name == "containerProperties" and isinstance(record.aspect, dict):
            name = record.aspect.get("name")
            if name:
                result[record.urn] = f"container:{name}"
    return result


def _index(records: Sequence[Record], strict: bool) -> Tuple[Dict[Tuple[str, str], Any], List[str]]:
    containers = _container_names(records)
    result: Dict[Tuple[str, str], Any] = {}
    duplicates: List[str] = []
    for record in records:
        if not strict and record.aspect_name not in SUPPORTED_ASPECTS:
            continue
        entity = containers.get(record.urn, record.urn)
        key = (entity, record.aspect_name)
        canonical = _canonical_aspect(record.aspect_name, record.aspect, containers)
        if key in result and result[key] != canonical:
            duplicates.append(f"{entity}#{record.aspect_name}")
        result[key] = canonical
    return result, sorted(set(duplicates))


def _diff_values(expected: Any, actual: Any, path: str = "") -> List[Dict[str, Any]]:
    diffs: List[Dict[str, Any]] = []
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in sorted(set(expected) | set(actual)):
            child = f"{path}.{key}" if path else key
            if key not in expected:
                diffs.append({"path": child, "expected": "<missing>", "actual": actual[key]})
            elif key not in actual:
                diffs.append({"path": child, "expected": expected[key], "actual": "<missing>"})
            else:
                diffs.extend(_diff_values(expected[key], actual[key], child))
        return diffs
    if expected != actual:
        diffs.append({"path": path or "$", "expected": expected, "actual": actual})
    return diffs


def compare_records(
    baseline: Sequence[Record], candidate: Sequence[Record], strict: bool = False
) -> Dict[str, Any]:
    expected, baseline_duplicates = _index(baseline, strict)
    actual, candidate_duplicates = _index(candidate, strict)
    expected_keys, actual_keys = set(expected), set(actual)
    differences = []
    for key in sorted(expected_keys & actual_keys):
        value_diffs = _diff_values(expected[key], actual[key])
        if value_diffs:
            differences.append({"entity": key[0], "aspect": key[1], "fields": value_diffs})
    missing = [f"{urn}#{aspect}" for urn, aspect in sorted(expected_keys - actual_keys)]
    extra = [f"{urn}#{aspect}" for urn, aspect in sorted(actual_keys - expected_keys)]
    report = {
        "equal": not (missing or extra or differences or baseline_duplicates or candidate_duplicates),
        "summary": {
            "baseline_aspects": len(expected), "candidate_aspects": len(actual),
            "missing_in_candidate": len(missing), "extra_in_candidate": len(extra),
            "different_aspects": len(differences),
        },
        "missing_in_candidate": missing,
        "extra_in_candidate": extra,
        "differences": differences,
        "duplicates": {"baseline": baseline_duplicates, "candidate": candidate_duplicates},
    }
    return report


def _exact_index(records: Sequence[Record]) -> Tuple[Dict[Tuple[str, str], Any], List[str]]:
    result: Dict[Tuple[str, str], Any] = {}
    duplicates: List[str] = []
    for record in records:
        key = (record.urn, record.aspect_name)
        if key in result and result[key] != record.aspect:
            duplicates.append(f"{record.urn}#{record.aspect_name}")
        result[key] = record.aspect
    return result, sorted(set(duplicates))


def compare_records_exact(
    baseline: Sequence[Record], candidate: Sequence[Record]
) -> Dict[str, Any]:
    expected, baseline_duplicates = _exact_index(baseline)
    actual, candidate_duplicates = _exact_index(candidate)
    expected_keys, actual_keys = set(expected), set(actual)
    differences = []
    for key in sorted(expected_keys & actual_keys):
        value_diffs = _diff_values(expected[key], actual[key])
        if value_diffs:
            differences.append({"entity": key[0], "aspect": key[1], "fields": value_diffs})
    missing = [f"{urn}#{aspect}" for urn, aspect in sorted(expected_keys - actual_keys)]
    extra = [f"{urn}#{aspect}" for urn, aspect in sorted(actual_keys - expected_keys)]
    return {
        "equal": not (missing or extra or differences or baseline_duplicates or candidate_duplicates),
        "summary": {
            "baseline_aspects": len(expected), "candidate_aspects": len(actual),
            "missing_in_candidate": len(missing), "extra_in_candidate": len(extra),
            "different_aspects": len(differences),
        },
        "missing_in_candidate": missing,
        "extra_in_candidate": extra,
        "differences": differences,
        "duplicates": {"baseline": baseline_duplicates, "candidate": candidate_duplicates},
    }
