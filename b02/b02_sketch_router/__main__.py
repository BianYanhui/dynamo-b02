# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""B02 selective KV-state gateway and Dynamo router service.

The current B02 design follows the paper's control-plane model:

    Instance reporter -> selective state gateway -> dispatcher hint
    Client -> Frontend -> B02SketchRouter -> native KvRouter -> workers

The native KvRouter remains the request/data-plane path.  The B02 gateway
merges repeated extensions, gives invalidations priority, suppresses
cross-instance replicas under a byte budget, and exposes an owner-side
validation hook. Request affinity pinning is disabled by default because it
is not the paper's mechanism; ``--legacy-affinity-pin`` is retained only for
an explicit A/B comparison with v0.

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
import subprocess
import sys
import time
from typing import Any, Optional

import uvloop

from dynamo.llm import KvRouter, KvRouterConfig, ModelInput, ModelType, WorkerType, register_model
from dynamo.runtime import DistributedRuntime, dynamo_worker
from dynamo.runtime.logging import configure_dynamo_logging

from b02_sketch_router.policy import DispatchDecision, SketchDispatchPolicy, SketchPolicyConfig
from b02_sketch_router.selective_signaling import (
    KVStateUpdate,
    OwnerStateRegistry,
    SelectiveKVStateGateway,
    updates_from_events,
)
from b02_sketch_router.workflow_state import WorkflowTable
from b02_sketch_router.zmq_gateway import RawZmqSelectiveRelay

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
                    help="gateway accounting tick")
    ap.add_argument("--signal-budget-bytes-per-sec", type=int, default=64 * 1024,
                    help="token-bucket budget for state signaling")
    ap.add_argument("--signal-frame-bytes", type=int, default=64,
                    help="fixed logical state frame size")
    ap.add_argument("--signal-top-k", type=int, default=64,
                    help="maximum candidate frames selected per drain")
    ap.add_argument("--signal-theta-seconds", type=float, default=30.0,
                    help="freshness decay constant in the admission utility")
    ap.add_argument("--signal-byte-penalty", type=float, default=16.0,
                    help="byte penalty lambda in the admission utility")
    ap.add_argument("--legacy-affinity-pin", action="store_true",
                    help="enable the old v0 workflow-to-worker pinning for A/B only")
    ap.add_argument("--zmq-relay-ports", default=None,
                    help="comma-separated vLLM raw ZMQ ports to relay through B02")
    ap.add_argument("--zmq-relay-scope", default="default",
                    help="compatibility scope attached to relayed KV state")
    ap.add_argument("--zmq-relay-shards", type=int, default=1,
                    help="number of process-sharded raw relays; >1 avoids the Python GIL")
    ap.add_argument("--zmq-relay-recv-hwm", type=int, default=200_000)
    ap.add_argument("--zmq-relay-max-messages-per-poll", type=int, default=512)
    ap.add_argument("--zmq-relay-max-events-per-batch", type=int, default=4096)
    ap.add_argument("--zmq-relay-drain-interval-ms", type=float, default=2.0)
    args = ap.parse_args()
    if args.zmq_relay_shards <= 0:
        ap.error("--zmq-relay-shards must be positive")
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
        self._gateway = SelectiveKVStateGateway(
            budget_bytes_per_sec=args.signal_budget_bytes_per_sec,
            frame_bytes=args.signal_frame_bytes,
            top_k=args.signal_top_k,
            theta_seconds=args.signal_theta_seconds,
            byte_penalty=args.signal_byte_penalty,
        )
        self._owner_states = OwnerStateRegistry()
        self._zmq_relay: Optional[RawZmqSelectiveRelay] = None
        self._zmq_relay_processes: list[subprocess.Popen] = []
        self._zmq_relay_ready_files: list[str] = []
        self._zmq_relay_stats_files: list[str] = []
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
        self._stat_native = 0
        self._stat_signal_ingress = 0
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
        if self._args.zmq_relay_ports:
            ports = [int(port) for port in self._args.zmq_relay_ports.split(",") if port.strip()]
            worker_ids: list[int] = []
            for _ in range(30):
                worker_ids = self._candidates()
                if len(worker_ids) >= len(ports):
                    break
                await asyncio.sleep(1.0)
            if len(worker_ids) != len(ports):
                raise RuntimeError(
                    f"B02 ZMQ relay expected {len(ports)} workers, found {worker_ids}"
                )
            if self._args.zmq_relay_shards == 1:
                self._zmq_relay = RawZmqSelectiveRelay(
                    endpoint=worker_endpoint,
                    worker_ids=worker_ids,
                    ports=ports,
                    block_size=self._args.router_block_size,
                    gateway=self._gateway,
                    compatibility_scope=self._args.zmq_relay_scope,
                    recv_hwm=self._args.zmq_relay_recv_hwm,
                    max_messages_per_poll=self._args.zmq_relay_max_messages_per_poll,
                    max_events_per_batch=self._args.zmq_relay_max_events_per_batch,
                    drain_interval_ms=self._args.zmq_relay_drain_interval_ms,
                )
                self._zmq_relay.start()
                logger.info("B02 ZMQ selective relay started (workers=%s, ports=%s)",
                            worker_ids, ports)
            else:
                self._start_sharded_relays(worker_ids, ports)
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
        if self._zmq_relay is not None:
            self._zmq_relay.shutdown()
            self._zmq_relay = None
        for process in self._zmq_relay_processes:
            if process.poll() is None:
                process.terminate()
        for process in self._zmq_relay_processes:
            try:
                process.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=1.0)
        self._zmq_relay_processes.clear()
        for path in self._zmq_relay_ready_files + self._zmq_relay_stats_files:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        self._zmq_relay_ready_files.clear()
        self._zmq_relay_stats_files.clear()
        self._state_log.close()
        self._decision_log.close()
        logger.info("B02 sketch router shutdown complete")

    def _start_sharded_relays(self, worker_ids: list[int], ports: list[int]) -> None:
        """Start independent relay processes, one shard per worker subset.

        Each shard owns its local admission state.  This preserves the
        owner-side validation safety rule while allowing raw-event processing
        to use multiple cores; cross-shard redundancy suppression is best
        effort in this explicit performance mode.
        """
        shard_count = min(self._args.zmq_relay_shards, len(worker_ids))
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        package_root = os.path.join(repo_root, "b02")
        inherited_pythonpath = os.environ.get("PYTHONPATH", "")
        pythonpath = package_root
        if inherited_pythonpath:
            pythonpath = f"{package_root}{os.pathsep}{inherited_pythonpath}"

        for shard in range(shard_count):
            shard_workers = worker_ids[shard::shard_count]
            shard_ports = ports[shard::shard_count]
            ready_file = os.path.join(
                self._args.state_log_dir, f"zmq_relay_shard_{shard}.ready"
            )
            stats_file = os.path.join(
                self._args.state_log_dir, f"zmq_relay_shard_{shard}.json"
            )
            for path in (ready_file, stats_file):
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
            command = [
                sys.executable,
                "-m",
                "b02_sketch_router.relay_worker",
                "--endpoint",
                self._args.endpoint,
                "--worker-ids",
                ",".join(str(value) for value in shard_workers),
                "--ports",
                ",".join(str(value) for value in shard_ports),
                "--block-size",
                str(self._args.router_block_size),
                "--scope",
                self._args.zmq_relay_scope,
                "--budget-bytes-per-sec",
                str(self._args.signal_budget_bytes_per_sec),
                "--frame-bytes",
                str(self._args.signal_frame_bytes),
                "--top-k",
                str(self._args.signal_top_k),
                "--theta-seconds",
                str(self._args.signal_theta_seconds),
                "--byte-penalty",
                str(self._args.signal_byte_penalty),
                "--recv-hwm",
                str(self._args.zmq_relay_recv_hwm),
                "--max-messages-per-poll",
                str(self._args.zmq_relay_max_messages_per_poll),
                "--max-events-per-batch",
                str(self._args.zmq_relay_max_events_per_batch),
                "--drain-interval-ms",
                str(self._args.zmq_relay_drain_interval_ms),
                "--ready-file",
                ready_file,
                "--stats-file",
                stats_file,
            ]
            environment = os.environ.copy()
            environment["PYTHONPATH"] = pythonpath
            process = subprocess.Popen(command, cwd=repo_root, env=environment)
            self._zmq_relay_processes.append(process)
            self._zmq_relay_ready_files.append(ready_file)
            self._zmq_relay_stats_files.append(stats_file)
            logger.info("B02 ZMQ relay shard started (pid=%s, workers=%s, ports=%s)",
                        process.pid, shard_workers, shard_ports)

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if all(os.path.exists(path) for path in self._zmq_relay_ready_files):
                return
            time.sleep(0.05)
        for process in self._zmq_relay_processes:
            if process.poll() is None:
                process.terminate()
        for process in self._zmq_relay_processes:
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                process.kill()
        self._zmq_relay_processes.clear()
        for path in self._zmq_relay_ready_files + self._zmq_relay_stats_files:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        self._zmq_relay_ready_files.clear()
        self._zmq_relay_stats_files.clear()
        raise RuntimeError("timed out waiting for process-sharded B02 relays")

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
        self._ingest_request_state(request)
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
        if self._args.legacy_affinity_pin:
            decision = self._policy.decide(self._table, workflow_id,
                                           dict(self._inflight), candidates)
        else:
            self._stat_native += 1
            decision = DispatchDecision(
                workflow_id=workflow_id,
                pin_instance=None,
                reason="native_kv",
                owner_instance=rec.last_assigned_instance,
            )
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
                self._emit_signal_snapshot()   # per-request gateway sample
            except Exception as exc:  # noqa: BLE001
                logger.debug("state view emit failed: %s", exc)

    # ------------------------------------------------- state accounting ---
    async def _state_accounting_loop(self) -> None:
        while True:
            await asyncio.sleep(self._args.tick_seconds)
            try:
                self._emit_signal_snapshot()
            except Exception as exc:  # noqa: BLE001
                logger.debug("state accounting tick failed: %s", exc)

    def _emit_signal_snapshot(self) -> None:
        rec = {
            "ts_ns": time.time_ns(),
            "gateway": self._gateway.snapshot(),
            "owner_states": self._owner_states.size,
            "relay": self._relay_snapshot(),
            "router": {
                "workflows_tracked": len(self._table),
                "inflight": dict(self._inflight),
                "legacy_affinity_pin": self._args.legacy_affinity_pin,
            },
        }
        self._state_log.write(json.dumps(rec) + "\n")
        self._state_log.flush()

    def _ingest_request_state(self, request: dict[str, Any]) -> None:
        """Accept optional reporter metadata without changing normal requests."""
        events = request.get("kv_state_events")
        if not isinstance(events, list):
            return
        for event in events:
            if isinstance(event, dict):
                self._record_owner_report(event)
        accepted = updates_from_events(events, gateway=self._gateway)
        self._stat_signal_ingress += len(events)
        if accepted:
            self._emit_signal_snapshot()

    def _state_update_from_mapping(self, mapping: dict[str, Any]) -> Optional[KVStateUpdate]:
        prefix = mapping.get("prefix_hash")
        if prefix is None:
            return None
        kind = str(mapping.get("kind", mapping.get("type", "upsert"))).lower()
        kind = "invalidate" if kind in {"removed", "blockremoved", "invalidate", "tombstone"} else "upsert"
        return KVStateUpdate(
            prefix_hash=str(prefix),
            owner_instance=int(mapping.get("owner_instance", mapping.get("worker_id", 0))),
            compatibility_scope=str(mapping.get("compatibility_scope", mapping.get("scope", "default"))),
            coverage_tokens=int(mapping.get("coverage_tokens", mapping.get("coverage", 0))),
            version=int(mapping.get("version", mapping.get("event_id", 0))),
            generated_at_ns=int(mapping.get("generated_at_ns", time.time_ns())),
            kind=kind,
            frame_bytes=int(mapping.get("frame_bytes", self._args.signal_frame_bytes)),
        )

    def _record_owner_report(self, event: dict[str, Any]) -> None:
        """Record reporter truth separately from the gateway's visible hints."""
        update = self._state_update_from_mapping(event)
        owner = int(event.get("owner_instance", event.get("worker_id", 0)))
        kind = str(event.get("kind", event.get("type", "upsert"))).lower()
        if kind in {"allblockscleared", "cleared", "clear"}:
            self._owner_states.clear_owner(owner)
        elif update is not None:
            self._owner_states.record(update, resident=not update.is_invalidation)

    async def ingest_kv_state(self, request: Optional[dict[str, Any]] = None):
        """Gateway ingress/egress endpoint for semantic KV-state frames.

        Returned frames are dispatcher hints. They are never authorization for
        reuse; the owner must validate the hint before pinning any blocks.
        """
        payload = request or {}
        events = payload.get("events")
        if not isinstance(events, list):
            events = [payload]
        accepted = updates_from_events(events, gateway=self._gateway)
        self._stat_signal_ingress += len(events)
        for event in events:
            if isinstance(event, dict):
                self._record_owner_report(event)
        frames = self._gateway.select(
            budget_bytes=payload.get("budget_bytes"),
            top_k=payload.get("top_k"),
        )
        validation_results = []
        for hint in payload.get("validate", []):
            if not isinstance(hint, dict):
                continue
            update = self._state_update_from_mapping(hint)
            if update is None:
                validation_results.append({"valid": False, "reason": "missing_prefix_hash"})
                continue
            result = self._owner_states.validate(
                update, expected_scope=hint.get("expected_scope"),
            )
            self._gateway.stats.validation_attempts += 1
            if not result.valid:
                self._gateway.stats.validation_fallbacks += 1
            validation_results.append({
                "prefix_hash": update.prefix_hash,
                "owner_instance": update.owner_instance,
                "valid": result.valid,
                "reason": result.reason,
                "actual_version": result.actual_version,
                "actual_coverage_tokens": result.actual_coverage_tokens,
            })
        self._emit_signal_snapshot()
        yield {
            "accepted": accepted,
            "frames": [frame.as_dict() for frame in frames],
            "validation": validation_results,
            "gateway": self._gateway.snapshot(),
        }

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
                "native_kv": self._stat_native,
            },
            "inflight": dict(self._inflight),
            "signal_gateway": self._gateway.snapshot(),
            "zmq_relay": self._relay_snapshot(),
            "signal_ingress_events": self._stat_signal_ingress,
            "legacy_affinity_pin": self._args.legacy_affinity_pin,
        }

    def _relay_snapshot(self) -> Optional[dict[str, Any]]:
        if self._zmq_relay is not None:
            return self._zmq_relay.snapshot()
        if self._zmq_relay_processes:
            return {
                "mode": "process_sharded",
                "shards": [
                    {
                        "pid": process.pid,
                        "returncode": process.poll(),
                        "snapshot": self._read_relay_snapshot(index),
                    }
                    for index, process in enumerate(self._zmq_relay_processes)
                ],
            }
        return None

    def _read_relay_snapshot(self, index: int) -> Optional[dict[str, Any]]:
        try:
            with open(self._zmq_relay_stats_files[index]) as handle:
                value = json.load(handle)
        except (FileNotFoundError, OSError, ValueError, IndexError):
            return None
        return value if isinstance(value, dict) else None


@dynamo_worker()
async def worker(runtime: DistributedRuntime) -> None:
    args = parse_args()
    logger.info("B02 sketch router starting (backend=%s, served_as=%s)",
                args.endpoint, args.served_model_name)

    handler = B02SketchRouterHandler(runtime, args)
    await handler.initialize()

    generate_endpoint = runtime.endpoint(f"{args.namespace}.b02_sketch_router.generate")
    status_endpoint = runtime.endpoint(f"{args.namespace}.b02_sketch_router.status")
    signal_endpoint = runtime.endpoint(f"{args.namespace}.b02_sketch_router.kv_state")

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
            signal_endpoint.serve_endpoint(
                handler.ingest_kv_state,
                graceful_shutdown=True,
                metrics_labels=[("service", "b02_selective_state_gateway")]),
        )
    finally:
        await handler.shutdown()


def main() -> None:
    uvloop.run(worker())


if __name__ == "__main__":
    main()
