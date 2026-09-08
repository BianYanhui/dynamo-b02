# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""Sketch-Dispatch policy (`experiments/design.md` §1.2).

    score(I, R) = alpha * inflight(I) + gamma * affinity_score(I, R)
    affinity_score(I, R) = -(# of R's recent steps assigned to I)

The affinity term REWARDS locality (negative score for the instance that
owns the workflow's most recent step), exactly as in the selective-signaling design's Rich/Sketch
Dispatch; the Sketch view carries this signal compactly via
affinity_hot_counts / recent_workflow_hashes. With gamma=10, affinity
dominates unless the owning instance is overloaded (max_inflight guard).
If the owner loses or is overloaded, the native Dynamo KvRouter decides
instead (keeping its KV-overlap ability), and the actual selection is
recorded for the state table.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from kv_event_router.workflow_state import WorkflowTable


@dataclass
class SketchPolicyConfig:
    alpha: float = 1.0
    gamma: float = 10.0
    max_inflight_per_instance: int = 8


@dataclass
class DispatchDecision:
    workflow_id: str | None
    pin_instance: int | None      # backend_instance_id pin, or None -> native
    reason: str                   # "affinity_pin" | "owner_overloaded" | "no_affinity"
    scores: dict[int, float] = field(default_factory=dict)
    owner_instance: int | None = None


class SketchDispatchPolicy:
    def __init__(self, config: SketchPolicyConfig | None = None) -> None:
        self.cfg = config or SketchPolicyConfig()

    def decide(
        self,
        table: WorkflowTable,
        workflow_id: str | None,
        inflight: dict[int, int],
        candidate_instances: list[int],
    ) -> DispatchDecision:
        if not workflow_id:
            return DispatchDecision(workflow_id=None, pin_instance=None,
                                    reason="no_affinity")

        rec = table.get(workflow_id)
        owner = rec.last_assigned_instance if rec else None
        if owner is None or owner not in candidate_instances:
            return DispatchDecision(workflow_id=workflow_id, pin_instance=None,
                                    reason="no_affinity", owner_instance=owner)

        scores: dict[int, float] = {}
        for inst in candidate_instances:
            s = self.cfg.alpha * inflight.get(inst, 0)
            if inst == owner:
                # Selective signaling §1.2: affinity_score = -(# of R's recent steps on I);
                # v0 counts the immediately-previous step (>= 1 for the owner).
                s -= self.cfg.gamma * 1.0
            scores[inst] = round(s, 3)

        owner_inflight = inflight.get(owner, 0)
        if owner_inflight >= self.cfg.max_inflight_per_instance:
            return DispatchDecision(workflow_id=workflow_id, pin_instance=None,
                                    reason="owner_overloaded", scores=scores,
                                    owner_instance=owner)

        best = min(scores, key=scores.get)  # type: ignore[arg-type]
        if best == owner:
            return DispatchDecision(workflow_id=workflow_id, pin_instance=owner,
                                    reason="affinity_pin", scores=scores,
                                    owner_instance=owner)
        return DispatchDecision(workflow_id=workflow_id, pin_instance=None,
                                reason="no_affinity", scores=scores,
                                owner_instance=owner)
