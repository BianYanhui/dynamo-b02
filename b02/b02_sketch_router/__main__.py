# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""B02 Sketch Router service.

A standalone Dynamo router component that fuses B02's cost-aware semantic
state interface into Dynamo's native KV routing:

    Client -> Frontend -> B02SketchRouter -> (pin | native KvRouter) -> workers

- Workflow identity rides `x-dynamo-session-id` (frontend injects
  `agent_context.session_id`), with nvext/extra_args fallbacks.
- Sketch-Dispatch policy (B02 §1.2) pins the workflow to its owning
  instance unless that instance is overloaded; otherwise the native
  Dynamo KvRouter chooses (keeping KV-overlap awareness).
- Every tick, the Coarse/Rich/Sketch state views are built per instance
  and their byte sizes logged to state_updates.jsonl (B02 §1.4).

Usage:
    PYTHONPATH=<repo>/b02 python -m b02_sketch_router \
        --endpoint dynamo.backend.generate \
        --model-name Qwen/Qwen2.5-1.5B-Instruct \
        --model-path <hf snapshot dir> \
        --served-model-name qwen-b02
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from typing import Any, Optional

import uvloop

from dynamo.llm import KvRouter, KvRouterConfig, ModelInput, ModelType, WorkerType, register_model
from dynamo.runtime import DistributedRuntime, dynamo_worker
from dynamo.runtime.logging import configure_dynamo_logging

from b02_sketch_router.policy import DispatchDecision, SketchDispatchPolicy, SketchPolicyConfig
from b02_sketch_router.state_views import build_coarse, build_rich, build_sketch
from b02_sketch_router.workflow_state import WorkflowTable

configure_dynamo_logging()
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="B02 Sketch Router (Dynamo component)")
    ap.add_argument("--endpoint", default="dynamo.backend.generate",
                    help="backend workers' generate endpoint")
    ap.add_argument("--model-name", required=True,
                    help="the model the backend workers serve")
    ap.add_argument("--model-path", default=None,
                    help="tokenizer/model path for the frontend's preprocessing")
    ap.add_argument("--served-model-name", default=None,
                    help="model name this router registers (must differ from the "
                         "workers' own chat surface; e.g. qwen-b02)")
    ap.add_argument("--router-block-size", type=int, default=16)
    ap.add_argument("--alpha", type=float, default=1.0, help="inflight weight")
    ap.add_argument("--gamma", type=float, default=10.0, help="affinity weight")
    ap.add_argument("--max-inflight", type=int, default=8)
    ap.add_argument("--state-log-dir", default="/tmp/b02_state_logs")
    ap.add_argument("--tick-seconds", type=float, default=5.0,
                    help="state-view accounting tick")
    ap.add_argument("--kv-usage-placeholder", type=float, default=0.0,
                    help="coarse view kv usage until worker telemetry is wired")
    args = ap.parse_args()
    if args.served_model_name is None:
        args.served_model_name = f"{args.model_name}-b02"
    ns = args.endpoint.split(".")[0] if "." in args.endpoint else "dynamo"
    args.namespace = ns
    return args


def _wrap_preprocessed_request(request: dict[str, Any]) -> dict[str, Any]:
    # Same internal contract as dynamo.thunderagent_router/__main__.py:
    # the fields the backend generate endpoint expects after preprocessing.
    routing = request.get("routing")
    dp_rank = request.get("dp_rank")
    if routing is None and dp_rank is not None:
        routing = {"dp_rank": dp_rank}
    return {
        "model": request.get("model", "unknown"),
        "token_ids": request["token_ids"],
        "stop_conditions": request.get("stop_conditions", {}),
        "sampling_options": request.get("sampling_options", {}),
        "output_options": request.get("output_options", {}),
        "eos_token_ids": request.get("eos_token_ids", []),
        "annotations": request.get("annotations", []),
        "routing": routing,
        "router_config_override": request.get("router_config_override"),
        "prefill_result": request.get("prefill_result"),
        "bootstrap_info": request.get("bootstrap_info"),
        "extra_args": request.get("extra_args"),
        "mm_processor_kwargs": request.get("mm_processor_kwargs"),
        "agent_context": request.get("agent_context"),
        "request_timestamp_ms": request.get("request_timestamp_ms"),
    }


def _extract_workflow_id(request: dict[str, Any]) -> tuple[Optional[str], str]:
    """(workflow_id, source). Frontend injects agent_context from
    x-dynamo-session-id; nvext/extra_args are fallbacks."""
    ctx = request.get("agent_context")
    if isinstance(ctx, dict):
        sid = ctx.get("session_id")
        if isinstance(sid, str) and sid:
            return sid, "session"
    nv = request.get("nvext")
    if isinstance(nv, dict):
        for key in ("b02_workflow_id", "session_id", "workflow_id"):
            v = nv.get(key)
            if isinstance(v, str) and v:
                return v, "nvext"
    ea = request.get("extra_args")
    if isinstance(ea, dict):
        for key in ("b02_workflow_id", "workflow_id"):
            v = ea.get(key)
            if isinstance(v, str) and v:
                return v, "extra_args"
    return None, "none"


def _extract_worker_id(chunk: Any) -> Optional[int]:
    """Worker attribution rides routing_data.worker_id (WorkerIdInfo)."""
    if not isinstance(chunk, dict):
        return None
    rd = chunk.get("routing_data")
    if not isinstance(rd, dict):
        return None
    info = rd.get("worker_id")
    if isinstance(info, dict):
        wid = info.get("decode_worker_id")
        if not isinstance(wid, int):
            wid = info.get("prefill_worker_id")
        if isinstance(wid, int):
            return wid
    return None


class B02SketchRouterHandler:
    def __init__(self, runtime: DistributedRuntime, args: argparse.Namespace) -> None:
        self._runtime = runtime
        self._args = args
        self._kv_router: Optional[KvRouter] = None
        self._worker_client = None
        self._table = WorkflowTable()
        self._policy = SketchDispatchPolicy(SketchPolicyConfig(
            alpha=args.alpha, gamma=args.gamma,
            max_inflight_per_instance=args.max_inflight))
        self._inflight: dict[int, int] = {}
        self._n_instances = 0
        self._tick_task: Optional[asyncio.Task] = None
        self._keys_logged = False
        self._stat_total = 0
        self._stat_workflow = 0
        self._stat_passthrough = 0
        self._stat_pinned = 0
        os.makedirs(args.state_log_dir, exist_ok=True)
        self._state_log = open(os.path.join(args.state_log_dir, "state_updates.jsonl"), "a")
        self._decision_log = open(os.path.join(args.state_log_dir, "decisions.jsonl"), "a")

    # ------------------------------------------------------------- init ---
    async def initialize(self) -> None:
        worker_endpoint = self._runtime.endpoint(self._args.endpoint)
        try:
            self._kv_router = KvRouter(
                endpoint=worker_endpoint,
                block_size=self._args.router_block_size,
                kv_router_config=KvRouterConfig(),
            )
        except TypeError:
            # 1.4.2 binding signature fallback
            self._kv_router = KvRouter(worker_endpoint, self._args.router_block_size)
        self._worker_client = await worker_endpoint.client()
        logger.info("B02 sketch router initialized (endpoint=%s, block_size=%s)",
                    self._args.endpoint, self._args.router_block_size)
        self._tick_task = asyncio.create_task(self._state_accounting_loop())

    async def shutdown(self) -> None:
        if self._tick_task is not None:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass
        self._state_log.close()
        self._decision_log.close()
        logger.info("B02 sketch router shutdown complete")

    def _candidates(self) -> list[int]:
        try:
            ids = sorted(int(i) for i in self._worker_client.instance_ids())
        except Exception as exc:  # noqa: BLE001
            logger.debug("instance_ids() failed: %s", exc)
            ids = []
        if ids:
            self._n_instances = len(ids)
        return ids

    # --------------------------------------------------------- routing ---
    async def generate(self, request: dict[str, Any]):
        if self._kv_router is None:
            raise RuntimeError("B02SketchRouterHandler used before initialize()")

        if not self._keys_logged:
            self._keys_logged = True
            logger.info("b02.request_keys keys=%s", sorted(request.keys()))

        self._stat_total += 1
        workflow_id, source = _extract_workflow_id(request)
        preprocessed = _wrap_preprocessed_request(request)
        decision: DispatchDecision
        t0 = time.perf_counter()

        if workflow_id is None:
            self._stat_passthrough += 1
            decision = DispatchDecision(workflow_id=None, pin_instance=None,
                                        reason="passthrough")
            self._log_decision(decision, actual_worker=None, ttft_ms=None)
            async for chunk in await self._kv_router.generate_from_request(preprocessed):
                yield chunk
            return

        self._stat_workflow += 1
        rec = self._table.get_or_create(workflow_id)
        rec.advance_step(tool_name=None, tool_result_tokens=0)

        candidates = self._candidates() or list(self._inflight.keys())
        decision = self._policy.decide(self._table, workflow_id,
                                       dict(self._inflight), candidates)
        if decision.pin_instance is not None:
            self._stat_pinned += 1
            routing = preprocessed.get("routing") or {}
            routing["backend_instance_id"] = decision.pin_instance
            preprocessed["routing"] = routing

        logger.info("b02.dispatch workflow=%s step=%d source=%s reason=%s pin=%s "
                    "candidates=%s inflight=%s",
                    workflow_id, rec.step_id, source, decision.reason,
                    decision.pin_instance, candidates, dict(self._inflight))

        pin = decision.pin_instance
        if pin is not None:
            self._inflight[pin] = self._inflight.get(pin, 0) + 1
        actual_worker: Optional[int] = None
        ttft_ms: Optional[float] = None
        first_chunk = True
        completion_tokens = 0
        try:
            async for chunk in await self._kv_router.generate_from_request(preprocessed):
                if first_chunk:
                    first_chunk = False
                    ttft_ms = round((time.perf_counter() - t0) * 1000.0, 2)
                    wid = _extract_worker_id(chunk)
                    if wid is not None:
                        actual_worker = wid
                        rec.assign(wid)
                usage = chunk.get("completion_usage") if isinstance(chunk, dict) else None
                if isinstance(usage, dict) and isinstance(usage.get("completion_tokens"), int):
                    completion_tokens = usage["completion_tokens"]
                out_ids = chunk.get("token_ids", []) if isinstance(chunk, dict) else []
                if not completion_tokens and isinstance(out_ids, list):
                    completion_tokens = len(out_ids)
                yield chunk
        finally:
            if pin is not None:
                self._inflight[pin] = max(0, self._inflight.get(pin, 0) - 1)
            if actual_worker is None and pin is not None:
                actual_worker = pin
                rec.assign(pin)
            rec.finish_step()
            self._log_decision(decision, actual_worker=actual_worker, ttft_ms=ttft_ms,
                               completion_tokens=completion_tokens)
            try:
                self._emit_state_views()   # per-request sample (B02 state update)
            except Exception as exc:  # noqa: BLE001
                logger.debug("state view emit failed: %s", exc)

    # ------------------------------------------------- state accounting ---
    async def _state_accounting_loop(self) -> None:
        while True:
            await asyncio.sleep(self._args.tick_seconds)
            try:
                self._emit_state_views()
            except Exception as exc:  # noqa: BLE001
                logger.debug("state accounting tick failed: %s", exc)

    def _emit_state_views(self) -> None:
        candidates = self._candidates()
        if not candidates:
            return
        n = len(candidates)
        # affinity_hot_counts keyed by REAL instance ids (they are large u64s,
        # not 0..n-1) in candidate order for the sketch packing.
        hot_by_id = {inst: 0 for inst in candidates}
        for w in self._table.all():
            if w.last_assigned_instance in hot_by_id:
                hot_by_id[w.last_assigned_instance] += 1
        hot = [hot_by_id[i] for i in candidates]
        ts = time.time_ns()
        for inst in candidates:
            wfs = self._table.workflows_of(inst)
            coarse = build_coarse(num_waiting=0,
                                  num_running=self._inflight.get(inst, 0),
                                  kv_usage_perc=self._args.kv_usage_placeholder)
            rich = build_rich(wfs)
            sketch = build_sketch(wfs, n, hot)
            rec = {
                "ts_ns": ts,
                "instance": inst,
                "active_workflows": len(wfs),
                "bytes": {"coarse": len(coarse), "rich": len(rich),
                          "sketch": len(sketch)},
            }
            self._state_log.write(json.dumps(rec) + "\n")
        self._state_log.flush()

    def _log_decision(self, decision: DispatchDecision, actual_worker: Optional[int],
                      ttft_ms: Optional[float], completion_tokens: int = 0) -> None:
        rec = {
            "ts_ns": time.time_ns(),
            "workflow_id": decision.workflow_id,
            "source": decision.reason,
            "pin": decision.pin_instance,
            "actual_worker": actual_worker,
            "owner": decision.owner_instance,
            "ttft_ms": ttft_ms,
            "completion_tokens": completion_tokens,
            "scores": decision.scores,
        }
        self._decision_log.write(json.dumps(rec) + "\n")
        self._decision_log.flush()

    # ------------------------------------------------- observability -----
    async def status(self, request: Optional[dict[str, Any]] = None):
        yield {
            "status": "ready",
            "component": "b02_sketch_router",
            "workflows_tracked": len(self._table),
            "requests": {
                "total": self._stat_total,
                "workflow": self._stat_workflow,
                "passthrough": self._stat_passthrough,
                "pinned": self._stat_pinned,
            },
            "inflight": dict(self._inflight),
        }


@dynamo_worker()
async def worker(runtime: DistributedRuntime) -> None:
    args = parse_args()
    logger.info("B02 sketch router starting (backend=%s, served_as=%s)",
                args.endpoint, args.served_model_name)

    handler = B02SketchRouterHandler(runtime, args)
    await handler.initialize()

    generate_endpoint = runtime.endpoint(f"{args.namespace}.b02_sketch_router.generate")
    status_endpoint = runtime.endpoint(f"{args.namespace}.b02_sketch_router.status")

    model_path = args.model_path or args.model_name
    reg_kwargs = dict(
        model_input=ModelInput.Tokens,
        model_type=ModelType.Chat | ModelType.Completions,
        endpoint=generate_endpoint,
        model_path=model_path,
        model_name=args.served_model_name,
        worker_type=WorkerType.Aggregated,
    )
    try:
        await register_model(**reg_kwargs)
    except TypeError:
        reg_kwargs.pop("worker_type", None)
        await register_model(**reg_kwargs)
    logger.info("B02 sketch router registered model '%s' (tokenizer from %s)",
                args.served_model_name, model_path)

    try:
        await asyncio.gather(
            generate_endpoint.serve_endpoint(
                handler.generate,
                graceful_shutdown=True,
                metrics_labels=[("service", "b02_sketch_router")]),
            status_endpoint.serve_endpoint(
                handler.status,
                graceful_shutdown=True,
                metrics_labels=[("service", "b02_sketch_router")]),
        )
    finally:
        await handler.shutdown()


def main() -> None:
    uvloop.run(worker())


if __name__ == "__main__":
    main()
