
"""Verify OpenAQ raw data using manifest-v1."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class VerifiedPayload:
    path: Path
    source_product: str
    status: str
    count: int
    sensor_id: str | None


def _within(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def verify_manifests(
    repo_root: Path,
    data_root: str = "data",
) -> tuple[list[VerifiedPayload], list[VerifiedPayload]]:
    """Verify manifests, checksums, sizes and row counts."""

    repo_root = repo_root.resolve()
    raw_root = (
        repo_root / data_root / "raw" / "openaq"
    ).resolve()

    manifest_root = raw_root / "manifests"

    if not manifest_root.is_dir():
        raise FileNotFoundError(
            f"OpenAQ manifest directory missing: {manifest_root}"
        )

    hours = []
    locations = []

    for manifest_path in sorted(
        manifest_root.rglob("*.manifest.json")
    ):
        with manifest_path.open(encoding="utf-8") as stream:
            manifest = json.load(stream)

        if manifest.get("schema_version") != "manifest-v1":
            raise ValueError(
                f"Unsupported manifest version: {manifest_path}"
            )

        if manifest.get("source") != "openaq":
            raise ValueError(
                f"Unexpected source: {manifest_path}"
            )

        if manifest.get("acquisition_mode") not in (
            "live", "replay"
        ):
            raise ValueError(
                f"Invalid acquisition_mode: {manifest_path}"
            )

        status = manifest.get("status")

        if status in ("failed", "partial"):
            raise ValueError(
                f"Raw ingestion not complete: {manifest_path}"
            )

        if status not in ("success", "empty"):
            raise ValueError(
                f"Unknown ingestion status: {manifest_path}"
            )

        rel = manifest.get("file")

        if not isinstance(rel, str) or not rel:
            raise ValueError(
                f"Missing raw file reference: {manifest_path}"
            )

        source_path = (repo_root / rel).resolve()

        if (
            not _within(source_path, raw_root)
            or not source_path.is_file()
        ):
            raise ValueError(
                f"Raw path unsafe or missing: {manifest_path}"
            )

        payload_bytes = source_path.read_bytes()

        if (
            manifest.get("bytes") is not None
            and len(payload_bytes) != manifest["bytes"]
        ):
            raise ValueError(
                f"Byte count mismatch: {source_path}"
            )

        sha = manifest.get("sha256")

        if (
            not sha
            or hashlib.sha256(payload_bytes).hexdigest() != sha
        ):
            raise ValueError(
                f"SHA256 mismatch: {source_path}"
            )

        try:
            payload = json.loads(payload_bytes)
        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise ValueError(
                f"Invalid JSON: {source_path}"
            ) from exc

        results = (
            payload.get("results")
            if isinstance(payload, dict)
            else None
        )

        if not isinstance(results, list):
            raise ValueError(
                f"OpenAQ results must be a list: {source_path}"
            )

        count = len(results)

        if manifest.get("row_count") != count:
            raise ValueError(
                f"Manifest row_count mismatch: {source_path}"
            )

        if status == "empty" and count != 0:
            raise ValueError(
                f"Empty status with nonzero records: {source_path}"
            )

        product = manifest.get("source_product")

        if product == "openaq_v3_hours":
            match = re.search(
                r"(?:^|[/\\])sensor=(\d+)(?:[/\\])",
                rel,
            )

            if match is None:
                raise ValueError(
                    f"No sensor context: {source_path}"
                )

            hours.append(
                VerifiedPayload(
                    source_path,
                    product,
                    status,
                    count,
                    match.group(1),
                )
            )

        elif product == "openaq_v3_locations":
            locations.append(
                VerifiedPayload(
                    source_path,
                    product,
                    status,
                    count,
                    None,
                )
            )

        else:
            raise ValueError(
                f"Unexpected OpenAQ product: {product}"
            )

    if not hours:
        raise ValueError("No verified hourly manifests")

    if not locations:
        raise ValueError("No verified location manifests")

    return hours, locations


def extract_sensor_metadata(
    locations: list[VerifiedPayload],
) -> list[
    tuple[str, str, float | None, float | None, str]
]:
    """Extract sensor, location and coordinate metadata."""

    lookup = {}

    for item in locations:
        with item.path.open(encoding="utf-8") as stream:
            records = json.load(stream)["results"]

        for loc in records:
            location_id = loc.get("id")
            coords = loc.get("coordinates") or {}

            for sensor in loc.get("sensors") or []:
                parameter = (
                    (sensor.get("parameter") or {}).get("name")
                )

                if (
                    location_id is None
                    or sensor.get("id") is None
                    or not parameter
                ):
                    continue

                key = (
                    str(sensor["id"]),
                    str(parameter),
                )

                row = (
                    str(sensor["id"]),
                    str(location_id),
                    coords.get("latitude"),
                    coords.get("longitude"),
                    str(parameter),
                )

                if key in lookup and row != lookup[key]:
                    raise ValueError(
                        f"Conflicting sensor metadata: {key}"
                    )

                lookup[key] = row

    return list(lookup.values())
