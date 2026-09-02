# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""B02 workflow state table.

Port of the B02 Motivation Experiment dispatcher's workflow table
(B02 `experiments/design.md` §11) into a Dynamo router component.
The dispatcher (here: the sketch router) owns the per-workflow semantic
state that feeds the Coarse/Rich/Sketch state views.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

TOOL_STATUS_BITS = {"idle": 0, "running": 1, "done": 2, "failed": 3}


@dataclass
class WorkflowRecord:
    """One agentic workflow as the dispatcher sees it (B02 §11 fields)."""

    workflow_id: str
    step_id: int = 0
    total_steps: int = 0
    progress: float = 0.0  # [0, 1]
    tool_status: str = "idle"  # idle | running | done | failed
    last_tool_name: str | None = None
    tool_result_context_size: int = 0  # tokens (dispatcher-side estimate)
    last_step_finish_time_ns: int = 0
    last_assigned_instance: int | None = None
    assigned_instance_history: list[int] = field(default_factory=list)

    def advance_step(self, tool_name: str | None, tool_result_tokens: int) -> None:
        self.step_id += 1
        if self.total_steps > 0:
            self.progress = min(1.0, self.step_id / self.total_steps)
        self.tool_status = "running"
        self.last_tool_name = tool_name
        self.tool_result_context_size = tool_result_tokens

    def finish_step(self) -> None:
        self.tool_status = "done"
        self.last_step_finish_time_ns = time.time_ns()

    def assign(self, instance_id: int) -> None:
        self.last_assigned_instance = instance_id
        self.assigned_instance_history.append(instance_id)


class WorkflowTable:
    """workflow_id -> WorkflowRecord, the dispatcher-owned state table."""

    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowRecord] = {}

    def get_or_create(self, workflow_id: str, total_steps: int = 0) -> WorkflowRecord:
        rec = self._workflows.get(workflow_id)
        if rec is None:
            rec = WorkflowRecord(workflow_id=workflow_id, total_steps=total_steps)
            self._workflows[workflow_id] = rec
        elif total_steps and not rec.total_steps:
            rec.total_steps = total_steps
        return rec

    def get(self, workflow_id: str) -> WorkflowRecord | None:
        return self._workflows.get(workflow_id)

    def all(self) -> list[WorkflowRecord]:
        return list(self._workflows.values())

    def workflows_of(self, instance_id: int) -> list[WorkflowRecord]:
        """Workflows whose most recent step was assigned to this instance."""
        return [
            w for w in self._workflows.values()
            if w.last_assigned_instance == instance_id
        ]

    def affinity_hot_counts(self, n_instances: int) -> list[int]:
        """affinity_hot_instance_counts[I] = #workflows whose last step was on I
        (B02 design.md §1.4 sketch field)."""
        counts = [0] * n_instances
        for w in self._workflows.values():
            if w.last_assigned_instance is not None and 0 <= w.last_assigned_instance < n_instances:
                counts[w.last_assigned_instance] += 1
        return counts

    def __len__(self) -> int:
        return len(self._workflows)
