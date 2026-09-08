# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""The three State Views at the Instance-Dispatcher boundary.

Frozen field layout from `experiments/design.md` §1.4:

- **Coarse**: compact runtime stats (JSON) — what today's dispatchers see.
- **Rich**: full per-workflow semantic state (JSON) — what an agentic
  dispatcher would *want* to see.
- **Sketch**: packed binary with the dispatching-relevant signal only:
    uint16 active_workflow_count (K, capped at 16)
    uint8  avg_progress_q      (mean progress * 100)
    uint8  max_progress_q      (max progress * 100)
    uint32 tool_status_bitset  (2 bits/wf, K<=16)
    uint16 tool_context_avail_bitmap (1 bit/wf)
    uint16 * n_instances affinity_hot_counts
    uint32 * 4 recent_workflow_hashes (crc32 of "workflow_id:step_id")

Byte sizes are the KV event headline measurement (Sketch ~= Coarse << Rich),
now computed inside a Dynamo router component.
"""

from __future__ import annotations

import json
import struct
import zlib
from typing import Sequence

from kv_event_router.workflow_state import TOOL_STATUS_BITS, WorkflowRecord

SKETCH_MAX_WORKFLOWS = 16
SKETCH_HASH_WINDOW = 4
_SKETCH_HDR = "<HBB"  # active_count, avg_progress_q, max_progress_q


def _json_bytes(payload) -> bytes:
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def build_coarse(num_waiting: int, num_running: int, kv_usage_perc: float) -> bytes:
    """Coarse view: runtime stats only, no workflow semantics."""
    return _json_bytes({
        "num_requests_waiting": num_waiting,
        "num_requests_running": num_running,
        "kv_cache_usage_perc": round(kv_usage_perc, 2),
    })


def build_rich(workflows: Sequence[WorkflowRecord]) -> bytes:
    """Rich view: the full workflow-level semantic state per instance."""
    payload = [
        {
            "workflow_id": w.workflow_id,
            "step_id": w.step_id,
            "total_steps": w.total_steps,
            "progress": round(w.progress, 4),
            "tool_status": w.tool_status,
            "last_tool_name": w.last_tool_name,
            "tool_result_context_size": w.tool_result_context_size,
            "last_assigned_instance": w.last_assigned_instance,
            "assigned_instance_history": list(w.assigned_instance_history),
        }
        for w in workflows
    ]
    return _json_bytes(payload)


def build_sketch(
    workflows: Sequence[WorkflowRecord],
    n_instances: int,
    affinity_hot_counts: Sequence[int],
) -> bytes:
    """Sketch view: packed dispatching signal (Selective signaling §1.4 frozen fields)."""
    wfs = list(workflows)[:SKETCH_MAX_WORKFLOWS]
    k = len(wfs)
    progress = [min(1.0, max(0.0, w.progress)) for w in wfs]
    avg_q = int(round(100 * (sum(progress) / k))) if k else 0
    max_q = int(round(100 * max(progress))) if k else 0

    tool_bits = 0
    ctx_bitmap = 0
    for i, w in enumerate(wfs):
        tool_bits |= TOOL_STATUS_BITS.get(w.tool_status, 0) << (2 * i)
        if w.tool_result_context_size > 0:
            ctx_bitmap |= 1 << i

    hot = [int(c) for c in list(affinity_hot_counts)[:n_instances]]
    while len(hot) < n_instances:
        hot.append(0)

    hashes = [
        zlib.crc32(f"{w.workflow_id}:{w.step_id}".encode()) & 0xFFFFFFFF
        for w in wfs[-SKETCH_HASH_WINDOW:]
    ]
    while len(hashes) < SKETCH_HASH_WINDOW:
        hashes.insert(0, 0)

    out = struct.pack(_SKETCH_HDR, k, avg_q, max_q)
    out += struct.pack("<I", tool_bits)          # 16 workflow slots = 32 bits
    out += struct.pack("<H", ctx_bitmap)         # 16 workflow slots = 16 bits
    out += struct.pack(f"<{n_instances}H", *hot)
    out += struct.pack(f"<{SKETCH_HASH_WINDOW}I", *hashes)
    return out
