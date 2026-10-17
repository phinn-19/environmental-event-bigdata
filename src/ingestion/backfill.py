"""Offline historical backfill planner; NEVER calls provider APIs or writes Raw.

Only emits a bounded plan of windows compatible with existing ingestion CLI.
Manifest-aware skipping and execution are intentionally NOT implemented in v1.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCES = ("openaq", "firms", "era5")
FIRMS_PRODUCTS = ("VIIRS_NOAA20_SP", "VIIRS_NOAA20_NRT")
MAX_PLAN_ITEMS_HARD = 10000


@dataclass(frozen=True)
class PlanItem:
    source: str
    region_id: str
    start: str  # YYYY-MM-DD inclusive UTC
    end: str  # YYYY-MM-DD exclusive UTC
    sensor_id: int | None = None
    product: str | None = None


def _date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise ValueError("dates must be valid YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError("dates must use exact YYYY-MM-DD format")
    return parsed


def plan_backfill(
    *, source: str, region_id: str, start: str, end: str,
    sensor_ids: tuple[int, ...] = (), product: str | None = None,
    max_plan_items: int = 31,
) -> tuple[PlanItem, ...]:
    """Create a bounded, deterministic, end-exclusive, OFFLINE work plan.

    Does not inspect any manifests. 'Planned' does NOT mean 'missing'.
    Does not execute tasks or promise the provider has the data.
    """
    if source not in SOURCES:
        raise ValueError("source must be openaq, firms or era5")
    if not region_id or not region_id.strip():
        raise ValueError("region_id is required")
    first, after = _date(start), _date(end)
    days = (after - first).days
    if days < 1:
        raise ValueError("end must be after start (exclusive)")
    if isinstance(max_plan_items, bool) or not isinstance(max_plan_items, int) or not 1 <= max_plan_items <= MAX_PLAN_ITEMS_HARD:
        raise ValueError("max_plan_items must be 1..10000")
    if source == "openaq":
        if product is not None:
            raise ValueError("--product applies only to FIRMS")
        if (not sensor_ids or any(type(x) is not int or x <= 0 for x in sensor_ids)
                or len(set(sensor_ids)) != len(sensor_ids)):
            raise ValueError("OpenAQ requires unique positive sensor IDs")
        expected = days * len(sensor_ids)
        step = 1
    elif source == "firms":
        if sensor_ids:
            raise ValueError("--sensor-id applies only to OpenAQ")
        if product not in FIRMS_PRODUCTS:
            raise ValueError("FIRMS requires --product VIIRS_NOAA20_SP or VIIRS_NOAA20_NRT")
        # Each FIRMS request uses at most five UTC days; no API calls made here.
        step = 5
        expected = (days + step - 1) // step
    else:
        if sensor_ids or product is not None:
            raise ValueError("sensor/product options do not apply to ERA5")
        step = 1
        expected = days
    if expected > max_plan_items:
        raise ValueError(
            f"plan has {expected} items, exceeding --max-plan-items {max_plan_items}; "
            "choose a smaller window or raise the planning cap deliberately"
        )
    items: list[PlanItem] = []
    if source == "openaq":
        for sensor in sensor_ids:
            for i in range(days):
                day = first + timedelta(days=i)
                items.append(PlanItem(source, region_id, day.isoformat(),
                                      (day + timedelta(days=1)).isoformat(), sensor_id=sensor))
    else:
        cursor = first
        while cursor < after:
            nxt = min(cursor + timedelta(days=step), after)
            items.append(PlanItem(source, region_id, cursor.isoformat(),
                                  nxt.isoformat(), product=product))
            cursor = nxt
    assert len(items) == expected
    return tuple(items)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="OFFLINE backfill plan only. No API requests, no Raw changes, no resume yet."
    )
    parser.add_argument("--source", choices=SOURCES, required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD inclusive UTC")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD exclusive UTC")
    parser.add_argument("--sensor-id", type=int, action="append", dest="sensor_ids")
    parser.add_argument("--product", choices=FIRMS_PRODUCTS)
    parser.add_argument("--config", default="configs/project.yaml")
    parser.add_argument("--max-plan-items", type=int, default=31)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)
    try:
        # Validate the region against the existing shared config; DO NOT edit it.
        from src.common.validation import validate_config
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = ROOT / config_path
        config = validate_config(config_path)
        if args.region not in config["regions"]:
            raise ValueError(f"unknown region ID: {args.region}")
        sensor_ids = tuple(args.sensor_ids or ())
        if args.source == "openaq" and not sensor_ids:
            sensor_ids = tuple(config["ingestion"]["openaq"]["pilot_sensor_ids"])
        items = plan_backfill(
            source=args.source, region_id=args.region, start=args.start, end=args.end,
            sensor_ids=sensor_ids, product=args.product, max_plan_items=args.max_plan_items,
        )
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"BACKFILL PLAN ERROR: {exc}", file=sys.stderr)
        return 2
    if args.format == "json":
        print(json.dumps({"mode": "offline_plan_only", "has_checked_manifest": False,
                          "request_count_is_estimate": True,
                          "plan_item_count": len(items),
                          "items": [asdict(item) for item in items]},
                         indent=2))
    else:
        print("OFFLINE PLAN ONLY — no network, no downloads, no raw writes")
        print("NOT manifest-aware; planned does NOT mean missing")
        print(f"Source: {args.source}; region: {args.region}; plan items: {len(items)}")
        for index, item in enumerate(items, start=1):
            label = (f"sensor={item.sensor_id}" if item.sensor_id is not None else
                     f"product={item.product}" if item.product else "5 configured variables")
            print(f"{index:03d}  {item.start} -> {item.end} (end exclusive)  {label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
