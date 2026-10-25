
"""Build contract-compliant fire events from eligible hotspot clusters."""

import hashlib
import math
from datetime import timedelta

from src.common.validation import timestamp, validate_events


def _valid_coordinate(value, lower, upper):
    return (
        type(value) in (int, float)
        and math.isfinite(value)
        and lower <= value <= upper
    )


def build_fire_events(clusters, region_id, algorithm_version="fire-v1"):
    """Convert ST-DBSCAN clusters into Fire Event dictionaries.

    FIRMS acquisition times have minute resolution. An exclusive event end
    is represented as the last observation time plus one minute; this is
    an observation window, not a claim about actual fire duration.
    """

    if not isinstance(region_id, str) or not region_id.strip():
        raise ValueError("region_id must be a non-empty string")

    if not isinstance(algorithm_version, str) or not algorithm_version.strip():
        raise ValueError("algorithm_version must be a non-empty string")

    events = []
    all_ids = set()

    # 1. Xu ly tung cum hotspot
    for cluster in clusters:

        if len(cluster) < 2:
            raise ValueError("A fire event requires at least two hotspots")

        ordered = sorted(
            cluster,
            key=lambda row: (
                timestamp(row["timestamp_utc"]),
                row["hotspot_id"]
            ),
        )

        coordinates_lat = []
        coordinates_lon = []
        frp_values = []
        satellites = set()
        hotspot_ids = []
        observed_times = []

        # 2. Kiem tra va thu thap du lieu moi hotspot
        for row in ordered:

            hotspot_id = row["hotspot_id"]

            if not isinstance(hotspot_id, str) or not hotspot_id.strip():
                raise ValueError("Invalid hotspot_id")

            if hotspot_id in all_ids:
                raise ValueError(
                    f"Duplicate hotspot_id across clusters: {hotspot_id}"
                )

            all_ids.add(hotspot_id)

            lat = row["latitude"]
            lon = row["longitude"]

            if (
                not _valid_coordinate(lat, -90, 90)
                or not _valid_coordinate(lon, -180, 180)
            ):
                raise ValueError(
                    f"Invalid coordinates: {hotspot_id}"
                )

            satellite = row["satellite"]

            if not isinstance(satellite, str) or not satellite.strip():
                raise ValueError(
                    f"Missing satellite for {hotspot_id}"
                )

            frp = row.get("frp_mw")

            # Khong bien FRP null thanh 0
            if frp is not None:
                if (
                    type(frp) not in (int, float)
                    or not math.isfinite(frp)
                    or frp < 0
                ):
                    raise ValueError(
                        f"Invalid FRP for {hotspot_id}"
                    )

                frp_values.append(float(frp))

            observed_times.append(
                timestamp(row["timestamp_utc"])
            )

            hotspot_ids.append(hotspot_id)
            coordinates_lat.append(float(lat))
            coordinates_lon.append(float(lon))
            satellites.add(satellite)

        # 3. Thoi gian cua cum
        start = min(observed_times)
        end = max(observed_times) + timedelta(minutes=1)

        # 4. Tao ID on dinh tu cac hotspot ID
        digest = hashlib.sha256(
            "|".join(sorted(hotspot_ids)).encode("utf-8")
        ).hexdigest()[:20]

        # 5. Tao Event theo Data Contract
        event = {
            "event_id": f"{algorithm_version}:{region_id}:{digest}",
            "event_type": "FIRE_HOTSPOT_CLUSTER",
            "region_id": region_id,
            "start_time_utc": start.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "end_time_utc": end.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "centroid_latitude": (
                sum(coordinates_lat) / len(coordinates_lat)
            ),
            "centroid_longitude": (
                sum(coordinates_lon) / len(coordinates_lon)
            ),
            "severity": "undetermined",
            "metrics": {
                "hotspot_count": len(hotspot_ids),
                "max_frp_mw": (
                    max(frp_values) if frp_values else None
                ),
                "sum_frp_mw": (
                    sum(frp_values) if frp_values else None
                ),
                "satellites": sorted(satellites),
            },
            "weather_context": None,
            "evidence_ids": hotspot_ids,
            "algorithm_version": algorithm_version,
        }

        events.append(event)

    # 6. Sap xep va kiem tra contract
    events.sort(
        key=lambda event: (
            event["start_time_utc"],
            event["event_id"]
        )
    )

    validate_events(events)

    return events
