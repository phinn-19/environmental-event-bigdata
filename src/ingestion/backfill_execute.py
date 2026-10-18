"""Sprint 06 OFFLINE execution rehearsal, *not* a live provider executor.

Exercises the entire planner -> audit -> gate -> immediately-before-dispatch
re-audit flow using a caller-supplied **mock** callback. The CLI only supplies a
no-op callback; it never invokes OpenAQ, FIRMS or CDS and never writes Raw.

This deliberately does NOT solve atomic reservation/locking or live byte limits;
it must not be wired to real ingestion without a separate reviewed change.
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
from .backfill_gate import GateResult, evaluate_gate


@dataclass(frozen=True)
class RehearsalResult:
    gate: GateResult
    state: str  # blocked_review | blocked_budget | nothing_to_do | approval_required | simulated | aborted_state_change
    simulated: tuple[PlanItem, ...]
    note: str


def rehearse_execution(
    *, root: Path, items: Sequence[PlanItem], config: dict,
    max_candidates: int = 3, approve_simulation: bool = False,
    auditor: Callable[..., AuditResult] = audit_item,
    mock_dispatcher: Callable[[PlanItem], None] | None = None,
) -> RehearsalResult:
    """Run fail-closed gate + read-only second audit before simulated dispatch.

    Approval only enables MOCK callbacks; NEVER real ingestion. If preflight
    contains HOLD or over-budget, no callback is invoked. The callback is
    supplied by testing/application code and has no provider integration here.
    """
    if type(approve_simulation) is not bool:
        raise ValueError('approve_simulation must be a boolean')
    gate = evaluate_gate(root=root, items=items, config=config,
                         max_candidates=max_candidates, auditor=auditor)
    if gate.state != 'ready_for_manual_review':
        return RehearsalResult(gate, gate.state, (), 'preflight gate did not authorize mock dispatch')
    if not approve_simulation:
        return RehearsalResult(gate, 'approval_required', (), 'simulation approval not provided')
    if mock_dispatcher is None:
        raise ValueError('explicit mock_dispatcher is required for a rehearsal')

    simulated: list[PlanItem] = []
    for decision in gate.decisions:
        if decision.action != 'candidate':
            continue
        # Recheck the specific item directly before dispatch (no silent replacement
        # of a newly arrived file, or treating replay manifests as live).
        refreshed = auditor(root=root, item=decision.item, config=config)
        if refreshed.item != decision.item or refreshed.status != 'not_found':
            return RehearsalResult(gate, 'aborted_state_change', tuple(simulated),
                                   'candidate changed after preflight; remaining mock work stopped')
        mock_dispatcher(decision.item)
        simulated.append(decision.item)
    return RehearsalResult(gate, 'simulated', tuple(simulated),
                           'mock callback only: no provider downloads were requested')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='OFFLINE execution rehearsal; even --approve-simulation NEVER downloads data.'
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
    parser.add_argument('--approve-simulation', action='store_true',
                        help='rehearse no-op dispatches after re-audit; NEVER calls APIs')
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
        items = plan_backfill(source=args.source, region_id=args.region,
                              start=args.start, end=args.end, sensor_ids=sensor_ids,
                              product=args.product, max_plan_items=args.max_plan_items)
        # Intentionally NO-OP: there is no real provider dispatcher in this module.
        result = rehearse_execution(root=ROOT, items=items, config=config,
                                    max_candidates=args.max_candidates,
                                    approve_simulation=args.approve_simulation,
                                    mock_dispatcher=lambda item: None)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f'BACKFILL REHEARSAL ERROR: {exc}', file=sys.stderr)
        return 2
    if args.format == 'json':
        print(json.dumps({
            'mode': 'offline_mock_rehearsal', 'network_calls': 0,
            'raw_writes': 0, 'live_execution_authorized': False,
            'state': result.state, 'note': result.note,
            'candidate_count': result.gate.candidate_count,
            'simulated_count': len(result.simulated),
            'decisions': [asdict(d) for d in result.gate.decisions],
            'simulated_items': [asdict(item) for item in result.simulated],
        }, indent=2))
    else:
        print('OFFLINE MOCK REHEARSAL — NO API / DOWNLOAD / RAW WRITES')
        for idx, decision in enumerate(result.gate.decisions, 1):
            print(f'{idx:03d}  {decision.item.start} -> {decision.item.end}  '
                  f'{decision.action.upper()}  {decision.reason}')
        print(f'GATE: {result.gate.state.upper()}')
        print(f'STATE: {result.state.upper()}')
        print(f'SIMULATED: {len(result.simulated)}/{result.gate.candidate_count}')
        print('No live execution capability is provided by this command.')
    return 0 if result.state not in ('aborted_state_change', 'blocked_review', 'blocked_budget') else 2


if __name__ == '__main__':
    raise SystemExit(main())
