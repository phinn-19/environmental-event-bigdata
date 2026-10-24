
"""Fire candidate filtering and ST-DBSCAN clustering (MVP v1)."""

import math
from collections import deque
from datetime import timedelta

from src.common.validation import timestamp


EARTH_RADIUS_KM = 6371.0088


# 1. Kiem tra toa do hop le
def _valid_coordinate(value, lower, upper):
    return (
        type(value) in (int, float)
        and math.isfinite(value)
        and lower <= value <= upper
    )


# 2. Tinh khoang cach dia ly
def haversine_distance_km(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two GPS points."""

    for lat in (lat1, lat2):
        if not _valid_coordinate(lat, -90, 90):
            raise ValueError("Invalid latitude")

    for lon in (lon1, lon2):
        if not _valid_coordinate(lon, -180, 180):
            raise ValueError("Invalid longitude")

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad)
        * math.sin(delta_lon / 2) ** 2
    )

    a = min(1.0, max(0.0, a))

    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


# 3. Loc hotspot candidate
def filter_fire_candidates(hotspots, config):
    """Keep eligible observations and report excluded records."""

    accepted_confidence = set(
        config["algorithms"]["fire"]["accepted_confidence"]
    )

    counts = {
        "total": 0,
        "accepted": 0,
        "rejected_confidence": 0,
        "rejected_invalid": 0,
    }

    candidates = []
    seen_ids = set()

    for hotspot in hotspots:
        counts["total"] += 1

        hotspot_id = hotspot.get("hotspot_id")

        if not isinstance(hotspot_id, str) or not hotspot_id.strip():
            counts["rejected_invalid"] += 1
            continue

        if hotspot_id in seen_ids:
            raise ValueError(f"Duplicate hotspot_id: {hotspot_id}")

        seen_ids.add(hotspot_id)

        raw_confidence = hotspot.get("confidence_raw")

        confidence = (
            raw_confidence.strip().lower()
            if isinstance(raw_confidence, str)
            else None
        )

        if confidence not in accepted_confidence:
            counts["rejected_confidence"] += 1
            continue

        lat = hotspot.get("latitude")
        lon = hotspot.get("longitude")

        if (
            not _valid_coordinate(lat, -90, 90)
            or not _valid_coordinate(lon, -180, 180)
        ):
            counts["rejected_invalid"] += 1
            continue

        try:
            timestamp(hotspot.get("timestamp_utc"))
        except (ValueError, TypeError):
            counts["rejected_invalid"] += 1
            continue

        frp = hotspot.get("frp_mw")

        if frp is not None and (
            type(frp) not in (int, float)
            or not math.isfinite(frp)
            or frp < 0
        ):
            counts["rejected_invalid"] += 1
            continue

        candidates.append(hotspot.copy())
        counts["accepted"] += 1

    candidates.sort(
        key=lambda row: (
            row["timestamp_utc"],
            row["hotspot_id"]
        )
    )

    return candidates, counts


# 4. Kiem tra hai hotspot co la hang xom khong
def are_fire_neighbors(first, second, config):
    """Two distinct hotspots are neighbors if BOTH limits are met."""

    if first["hotspot_id"] == second["hotspot_id"]:
        return False

    rules = config["algorithms"]["fire"]

    distance_km = haversine_distance_km(
        first["latitude"],
        first["longitude"],
        second["latitude"],
        second["longitude"],
    )

    time_difference = abs(
        timestamp(first["timestamp_utc"])
        - timestamp(second["timestamp_utc"])
    )

    return (
        distance_km <= rules["spatial_radius_km"]
        and time_difference <= timedelta(
            hours=rules["temporal_radius_hours"]
        )
    )


# 5. GOM HOTSPOT THANH CUM BANG ST-DBSCAN
def cluster_fire_candidates(candidates, config):
    """Cluster eligible hotspots using ST-DBSCAN MVP v1.

    min_samples includes the hotspot itself.
    Noise points are not returned as clusters.
    """

    rules = config["algorithms"]["fire"]

    if rules["algorithm"] != "st_dbscan_v1":
        raise ValueError("Unsupported fire clustering algorithm")

    min_samples = rules["min_samples"]

    if type(min_samples) is not int or min_samples < 2:
        raise ValueError("fire.min_samples must be an integer >= 2")

    # A. Sap xep de ket qua on dinh
    ordered = sorted(
        candidates,
        key=lambda item: (
            timestamp(item["timestamp_utc"]),
            item["hotspot_id"]
        )
    )

    # B. Kiem tra ID va toa do
    seen_ids = set()

    for item in ordered:
        hotspot_id = item["hotspot_id"]

        if not isinstance(hotspot_id, str) or not hotspot_id.strip():
            raise ValueError("Missing hotspot_id")

        if hotspot_id in seen_ids:
            raise ValueError(f"Duplicate hotspot_id: {hotspot_id}")

        seen_ids.add(hotspot_id)

        if (
            not _valid_coordinate(item["latitude"], -90, 90)
            or not _valid_coordinate(item["longitude"], -180, 180)
        ):
            raise ValueError(f"Invalid coordinates: {hotspot_id}")

    # C. Tim hang xom cua tung hotspot
    neighbors = [[] for _ in ordered]

    for i in range(len(ordered)):
        for j in range(i + 1, len(ordered)):

            if are_fire_neighbors(ordered[i], ordered[j], config):
                neighbors[i].append(j)
                neighbors[j].append(i)

    # D. Xac dinh core points
    # +1 vi min_samples tinh ca chinh hotspot
    core_indexes = {
        index
        for index, nearby in enumerate(neighbors)
        if len(nearby) + 1 >= min_samples
    }

    clusters = []
    expanded_cores = set()
    assigned = set()

    # E. Duyet tung diem loi de bat dau cum
    for seed in range(len(ordered)):

        if seed not in core_indexes:
            continue

        if seed in expanded_cores:
            continue

        queue = deque([seed])
        expanded_cores.add(seed)

        current_cluster = set()

        # F. Mo rong cum qua cac core point
        while queue:
            index = queue.popleft()

            if index not in assigned:
                assigned.add(index)
                current_cluster.add(index)

            for neighbor in neighbors[index]:

                # Them diem hang xom chua thuoc cum nao
                if neighbor not in assigned:
                    assigned.add(neighbor)
                    current_cluster.add(neighbor)

                # Chi core point moi mo rong cum tiep
                if (
                    neighbor in core_indexes
                    and neighbor not in expanded_cores
                ):
                    expanded_cores.add(neighbor)
                    queue.append(neighbor)

        # G. Luu cum vua tim duoc
        cluster = [
            ordered[index]
            for index in sorted(current_cluster)
        ]

        clusters.append(cluster)

    return clusters
