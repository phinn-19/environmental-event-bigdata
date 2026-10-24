
"""Merge AQ and Fire preview events into one local Event JSON."""

import json
import sys
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.validation import (
    read_json,
    validate_config,
    validate_events,
)


# 1. Doc va kiem tra file Event dau vao
def load_events(path, expected_type):

    if not path.is_file():
        raise FileNotFoundError(
            f"Khong tim thay Event JSON: {path}"
        )

    events = read_json(path)

    # Kiem tra dung Event Contract
    validate_events(events)

    # Dam bao file AQ chi chua AQ,
    # file Fire chi chua Fire
    for event in events:
        if event["event_type"] != expected_type:
            raise ValueError(
                f"Wrong event type in {path}"
            )

    return events


# 2. Dieu phoi viec ghep Event
def main():

    config = validate_config(
        ROOT / "configs/project.yaml"
    )

    demo_dir = ROOT / config["storage"]["demo"]
    events_dir = ROOT / config["storage"]["events"]

    aq_path = demo_dir / "events_aq_real_preview.json"
    fire_path = demo_dir / "events_fire_real_preview.json"

    # A. Doc AQ Events
    aq_events = load_events(
        aq_path,
        "AIR_QUALITY_ANOMALY"
    )

    # B. Doc Fire Events
    fire_events = load_events(
        fire_path,
        "FIRE_HOTSPOT_CLUSTER"
    )

    print("\n===== INPUT EVENTS =====")
    print("AQ Events:", len(aq_events))
    print("Fire Events:", len(fire_events))

    # C. Ghép hai danh sach
    all_events = aq_events + fire_events

    # D. Kiem tra vung bao cao nhat quan
    expected_region = config["integration"]["region_id"]

    for event in all_events:
        if event["region_id"] != expected_region:
            raise ValueError(
                f"Unexpected region: {event['region_id']}"
            )

    # E. Sap xep theo thoi gian
    all_events.sort(
        key=lambda event: (
            event["start_time_utc"],
            event["event_type"],
            event["event_id"]
        )
    )

    # F. Kiem tra contract tong the
    valid_count = validate_events(all_events)

    # G. Thong ke
    counts = Counter(
        event["event_type"]
        for event in all_events
    )

    print("\n===== COMBINED EVENTS =====")
    print("Total:", len(all_events))
    print("Validated:", valid_count)
    print(
        "AIR_QUALITY_ANOMALY:",
        counts["AIR_QUALITY_ANOMALY"]
    )
    print(
        "FIRE_HOTSPOT_CLUSTER:",
        counts["FIRE_HOTSPOT_CLUSTER"]
    )

    # H. Ghi file JSON dung contract
    events_dir.mkdir(parents=True, exist_ok=True)

    output_path = events_dir / "events.json"
    temp_path = events_dir / "events.json.tmp"

    try:
        temp_path.write_text(
            json.dumps(
                all_events,
                ensure_ascii=False,
                indent=2,
                allow_nan=False
            ) + "\n",
            encoding="utf-8"
        )

        # Doc lai file tam de kiem tra
        validate_events(read_json(temp_path))

        # Chi thay file dich khi JSON da hop le
        temp_path.replace(output_path)

    finally:
        if temp_path.exists():
            temp_path.unlink()

    print("\n===== OUTPUT =====")
    print("Saved to:", output_path)
    print("\nEvent merge completed.")


if __name__ == "__main__":
    main()
