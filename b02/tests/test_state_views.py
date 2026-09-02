# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for B02 state view builders (pure Python, no Dynamo runtime)."""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from b02_sketch_router.state_views import build_coarse, build_rich, build_sketch
from b02_sketch_router.workflow_state import WorkflowRecord, WorkflowTable


def _wf(wid: str, step: int, total: int, inst: int, status: str = "done",
        ctx: int = 512) -> WorkflowRecord:
    w = WorkflowRecord(workflow_id=wid, step_id=step, total_steps=total,
                       tool_status=status, tool_result_context_size=ctx,
                       last_assigned_instance=inst)
    w.progress = step / total if total else 0.0
    w.assigned_instance_history.append(inst)
    return w


def test_coarse_is_tiny():
    b = build_coarse(0, 3, 42.5)
    assert b.startswith(b"{")
    assert len(b) < 120


def test_rich_grows_with_workflows():
    wfs = [_wf(f"wf_{i:04d}", i % 8 + 1, 8, i % 4) for i in range(12)]
    b8 = build_rich(wfs[:8])
    b16 = build_rich(wfs)
    assert len(b16) > len(b8) > 500


def test_sketch_layout_and_size():
    table = WorkflowTable()
    for i in range(8):
        table.get_or_create(f"wf_{i:04d}", total_steps=8)
        rec = table.get(f"wf_{i:04d}")
        rec.step_id = i + 1
        rec.progress = (i + 1) / 8
        rec.tool_status = "done"
        rec.tool_result_context_size = 512
        rec.assign(i % 4)
    wfs = table.all()
    hot = table.affinity_hot_counts(4)
    sk = build_sketch(wfs, 4, hot)
    # 2 + 1 + 1 + 4 + 1 + 4*2 + 4*4 = 33 bytes
    assert len(sk) == 2 + 1 + 1 + 4 + 2 + 4 * 2 + 4 * 4, len(sk)
    assert hot == [2, 2, 2, 2]


def test_sketch_much_smaller_than_rich():
    wfs = [_wf(f"wf_{i:04d}", 5, 8, i % 4, ctx=1536) for i in range(8)]
    rich = len(build_rich(wfs))
    sketch = len(build_sketch(wfs, 4, [2, 2, 2, 2]))
    assert sketch < rich / 10, (sketch, rich)


def test_sketch_cap_at_16_workflows():
    wfs = [_wf(f"wf_{i:04d}", 1, 8, i % 4) for i in range(40)]
    sk = build_sketch(wfs, 4, [10, 10, 10, 10])
    assert len(sk) == 2 + 1 + 1 + 4 + 2 + 4 * 2 + 4 * 4


def test_policy_owner_pin_and_overload():
    from b02_sketch_router.policy import SketchDispatchPolicy, SketchPolicyConfig
    table = WorkflowTable()
    rec = table.get_or_create("wf_a", total_steps=8)
    rec.assign(2)
    pol = SketchDispatchPolicy(SketchPolicyConfig(alpha=1.0, gamma=10.0,
                                                  max_inflight_per_instance=8))
    d = pol.decide(table, "wf_a", {0: 0, 1: 0, 2: 3, 3: 0}, [0, 1, 2, 3])
    assert d.pin_instance == 2 and d.reason == "affinity_pin"

    pol2 = SketchDispatchPolicy(SketchPolicyConfig(alpha=1.0, gamma=10.0,
                                                   max_inflight_per_instance=2))
    d2 = pol2.decide(table, "wf_a", {0: 0, 1: 0, 2: 5, 3: 0}, [0, 1, 2, 3])
    assert d2.pin_instance is None and d2.reason == "owner_overloaded"

    d3 = pol.decide(table, "wf_new", {0: 0, 1: 0, 2: 0}, [0, 1, 2])
    assert d3.pin_instance is None and d3.reason == "no_affinity"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"{len(fns)} tests passed")
