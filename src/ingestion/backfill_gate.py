"""Read-only, fail-closed execution gate for Sprint 06 backfill.

The gate never calls data providers, creates files, or dispatches downloads.
Its CANDIDATE classification is NOT permission to download; a future executor
must repeat the audit immediately before an explicitly authorized request.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Sequence

from src.common.validation import validate_config
from .backfill import FIRMS_PRODUCTS, ROOT, SOURCES, PlanItem, plan_backfill
from .backfill_audit import AuditResult, audit_item

MAX_CANDIDATES_HARD = 100


@dataclass(frozen=True)
class GateDecision:
    item: PlanItem
    action: str  # skip | hold | candidate
    reason: str


@dataclass(frozen=True)
class GateResult:
    decisions: tuple[GateDecision, ...]
    state: str  # ready_for_manual_review | blocked_review | blocked_budget | nothing_to_do
    candidate_count: int
    max_candidates: int

    @property
    def is_unblocked(self) -> bool:
        """No review or budget blocker; still does NOT authorize execution."""
        return self.state == 'ready_for_manual_review'


def evaluate_gate(
    *, root: Path, items: Sequence[PlanItem], config: dict,
    max_candidates: int = 3,
    auditor: Callable[..., AuditResult] = audit_item,
) -> GateResult:
    """Audit all plan items, then apply a whole-plan safety gate (no I/O writes).

    A single REVIEW or an exceeded candidate budget blocks the entire plan.
    A failed audit must propagate, not silently downgrade to NOT_FOUND.
    """
    if (type(max_candidates) is not int
            or not 1 <= max_candidates <= MAX_CANDIDATES_HARD):
        raise ValueError('max_candidates must be 1..100')
    if not items:
        raise ValueError('plan must contain at least one item')
    if len(set(items)) != len(items):
        raise ValueError('duplicate plan item')

    decisions: list[GateDecision] = []
    for item in items:
        result = auditor(root=root, item=item, config=config)
        if result.item != item:
            raise ValueError('audit returned different plan identity')
        if result.status == 'complete':
            action = 'skip'
        elif result.status == 'review':
            action = 'hold'
        elif result.status == 'not_found':
            action = 'candidate'
        else:
            raise ValueError('unexpected audit status; refusing to plan execution')
        decisions.append(GateDecision(item, action, result.reason))

    candidates = sum(d.action == 'candidate' for d in decisions)
    if any(d.action == 'hold' for d in decisions):
        state = 'blocked_review'
    elif candidates > max_candidates:
        state = 'blocked_budget'
    elif candidates == 0:
        state = 'nothing_to_do'
    else:
        state = 'ready_for_manual_review'
    return GateResult(tuple(decisions), state, candidates, max_candidates)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='OFFLINE backfill safety gate. No network, Raw writes or execution.'
    )
    parser.add_argument('--source', choices=SOURCES, required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--sensor-id', type=int, action='append', dest='sensor_ids')
    parser.add_argument('--product', choices=FIRMS_PRODUCTS)
    parser.add_argument('--config', default='configs/project.yaml')
    parser.add_argument('--max-plan-items', type=int, default=31)
    parser.add_argument('--max-candidates', type=int, default=3)
    parser.add_argument('--format', choices=('text', 'json'), default='text')
    args = parser.parse_args(argv)
    try:
        config_path = Path(args.config)
        if not config_path.is_absolute():
            config_path = ROOT / config_path
        config = validate_config(config_path)
        if args.region not in config['regions']:
            raise ValueError('unknown region ID')
        sensor_ids = tuple(args.sensor_ids or ())
        if args.source == 'openaq' and not sensor_ids:
            sensor_ids = tuple(config['ingestion']['openaq']['pilot_sensor_ids'])
        items = plan_backfill(
            source=args.source, region_id=args.region, start=args.start, end=args.end,
            sensor_ids=sensor_ids, product=args.product,
            max_plan_items=args.max_plan_items,
        )
        result = evaluate_gate(root=ROOT, items=items, config=config,
                               max_candidates=args.max_candidates)
    except (KeyError, OSError, TypeError, ValueError) as exc:
        print(f'BACKFILL GATE ERROR: {exc}', file=sys.stderr)
        return 2
    if args.format == 'json':
        print(json.dumps({
            'mode': 'offline_gate_only', 'no_download': True,
            'execution_authorized': False, 'state': result.state,
            'candidate_count': result.candidate_count,
            'max_candidates': result.max_candidates,
            'decisions': [asdict(d) for d in result.decisions],
        }, indent=2))
    else:
        print('OFFLINE EXECUTION GATE — NO API / NO DOWNLOAD / NO RAW WRITES')
        for number, decision in enumerate(result.decisions, 1):
            print(f'{number:03d}  {decision.item.start} -> {decision.item.end}  '
                  f'{decision.action.upper()}  {decision.reason}')
        print(f'STATE: {result.state.upper()}')
        print(f'CANDIDATES: {result.candidate_count}/{result.max_candidates}')
        print('No automatic downloads are authorized. Re-audit before any future execution.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
