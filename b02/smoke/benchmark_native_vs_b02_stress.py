#!/usr/bin/env python3
"""Stress native ZMQ and Worker-local B02 with real native envelope shapes.

The input templates come from a real Dynamo/vLLM EventBatch trace.  Each
cycle deterministically salts block hashes so replaying a trace does not turn
into an artificial duplicate-only workload.  Native and B02 receive the same
pre-encoded cycle count, envelope boundaries, and event counts; pre-encoding is
outside the timed section so the test exercises the ZMQ path rather than the
Python workload generator.
"""

from __future__ import annotations

import argparse
import base64
import json
import multiprocessing as mp
import time
from dataclasses import dataclass
from typing import Any

import msgspec
import zmq

from b02_sketch_router.prepublish import LocalKVEventSelector


class BlockStored:
    def __init__(self, **fields: Any):
        self.__dict__.update(fields)


class BlockRemoved:
    def __init__(self, **fields: Any):
        self.__dict__.update(fields)


class AllBlocksCleared:
    def __init__(self, **fields: Any):
        self.__dict__.update(fields)


def _to_event(value: dict[str, Any]) -> Any:
    fields = dict(value)
    kind = fields.pop("type", "")
    cls = {
        "BlockStored": BlockStored,
        "BlockRemoved": BlockRemoved,
        "AllBlocksCleared": AllBlocksCleared,
    }.get(kind, type(str(kind) or "UnknownKVEvent", (), {}))
    event = cls(**fields)
    event._wire_type = kind
    return event


def _raw_event(event: Any) -> dict[str, Any]:
    fields = {
        key: value for key, value in vars(event).items() if not key.startswith("_")
    }
    fields["type"] = getattr(event, "_wire_type", type(event).__name__)
    return fields


def _load_templates(path: str) -> dict[int, list[dict[str, Any]]]:
    result: dict[int, list[dict[str, Any]]] = {}
    decoder = msgspec.msgpack.Decoder()
    with open(path) as handle:
        for line in handle:
            record = json.loads(line)
            payload = base64.b64decode(record["payload_b64"])
            decoded = decoder.decode(payload)
            result.setdefault(int(record["port"]), []).append(
                {
                    "ts": float(decoded[0]),
                    "rank": decoded[2],
                    "events": decoded[1],
                    "event_count": len(decoded[1]),
                    "topic": base64.b64decode(record["topic_b64"]),
                }
            )
    return result


def _salt_batch(
    template: dict[str, Any],
    cycle: int,
    worker_index: int,
    encoder: msgspec.msgpack.Encoder,
    mapping: dict[int, int],
) -> bytes:
    """Create a fresh logical batch while retaining the native shape."""
    salt_prefix = ((cycle + 1) << 40) | ((worker_index + 1) << 32)
    salted_events: list[dict[str, Any]] = []

    def salted_hash(value: int) -> int:
        if value not in mapping:
            mapping[value] = salt_prefix | (len(mapping) + 1)
        return mapping[value]

    for raw in template["events"]:
        event = dict(raw)
        hashes = event.get("block_hashes")
        if hashes is not None:
            event["block_hashes"] = [salted_hash(int(value)) for value in hashes]
        parent = event.get("parent_block_hash")
        if parent is not None:
            event["parent_block_hash"] = salted_hash(int(parent))
        salted_events.append(event)
    return encoder.encode([template["ts"], salted_events, template["rank"]])


def _receiver(
    ports: list[int], ready: Any, stop: Any, result_queue: Any, drain_seconds: float
) -> None:
    ctx = zmq.Context()
    poller = zmq.Poller()
    sockets: dict[zmq.Socket, int] = {}
    for port in ports:
        sock = ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        sock.setsockopt(zmq.RCVHWM, 200_000)
        sock.connect(f"tcp://127.0.0.1:{port}")
        poller.register(sock, zmq.POLLIN)
        sockets[sock] = port
    decoder = msgspec.msgpack.Decoder()
    stats = {
        "received_messages": 0,
        "received_events": 0,
        "received_bytes": 0,
        "decode_errors": 0,
    }
    ready.set()
    stop_seen: float | None = None
    try:
        while True:
            if stop.is_set() and stop_seen is None:
                stop_seen = time.monotonic()
            if stop_seen is not None and time.monotonic() - stop_seen >= drain_seconds:
                break
            for sock, _ in poller.poll(20):
                for _ in range(4096):
                    try:
                        parts = sock.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    stats["received_messages"] += 1
                    stats["received_bytes"] += sum(len(part) for part in parts)
                    try:
                        stats["received_events"] += len(decoder.decode(parts[-1])[1])
                    except Exception:
                        stats["decode_errors"] += 1
    finally:
        for sock in sockets:
            sock.close(0)
        ctx.term()
    result_queue.put(stats)


@dataclass
class ProducerResult:
    worker_id: int
    input_events: int = 0
    input_messages: int = 0
    sent_messages: int = 0
    send_drops: int = 0
    output_events: int = 0
    output_messages: int = 0
    output_bytes: int = 0
    elapsed_s: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _producer(
    worker_id: int,
    worker_index: int,
    port: int,
    mode: str,
    backend: str,
    templates: list[dict[str, Any]],
    cycles: int,
    cycle_interval_s: float,
    flush_ms: float,
    go: Any,
    ready_queue: Any,
    result_queue: Any,
) -> None:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.setsockopt(zmq.SNDHWM, 100_000)
    sock.setsockopt(zmq.LINGER, 0)
    try:
        sock.bind(f"tcp://*:{port}")
    except Exception as exc:
        ready_queue.put({"worker_id": worker_id, "error": repr(exc)})
        sock.close(0)
        ctx.term()
        return

    selector = (
        LocalKVEventSelector(max_pending_events=4096, backend=backend)
        if mode != "native"
        else None
    )
    decoder = msgspec.msgpack.Decoder() if mode != "native" else None
    encoder = msgspec.msgpack.Encoder()

    # Build the identical logical workload before starting the timer.  This is
    # analogous to vLLM having already serialized an EventBatch when it calls
    # the native publisher.  Every cycle has fresh hashes, while one mapping
    # is shared across that cycle so parent/duplicate relationships remain
    # realistic.
    workload: list[tuple[bytes, int, bytes]] = []
    for cycle in range(cycles):
        cycle_mapping: dict[int, int] = {}
        for template in templates:
            workload.append(
                (
                    _salt_batch(
                        template, cycle, worker_index, encoder, cycle_mapping
                    ),
                    template["event_count"],
                    template["topic"],
                )
            )

    result = ProducerResult(worker_id)
    ready_queue.put(worker_id)
    go.wait()
    started = time.monotonic()
    next_cycle = started
    last_flush = time.monotonic_ns()
    output_seq = 0

    def send(payload: bytes, count: int, topic: bytes) -> None:
        nonlocal output_seq
        result.output_events += count
        result.output_messages += 1
        result.output_bytes += len(payload) + len(topic) + 8
        try:
            sock.send_multipart(
                (topic, output_seq.to_bytes(8, "big"), payload),
                flags=zmq.NOBLOCK,
            )
            result.sent_messages += 1
        except zmq.Again:
            result.send_drops += 1
        output_seq += 1

    try:
        workload_index = 0
        for cycle in range(cycles):
            if cycle_interval_s > 0:
                delay = next_cycle - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            next_cycle += cycle_interval_s
            for _ in templates:
                payload, event_count, topic = workload[workload_index]
                workload_index += 1
                result.input_events += event_count
                result.input_messages += 1
                if selector is None:
                    send(payload, event_count, topic)
                    continue

                assert decoder is not None
                decoded = decoder.decode(payload)
                events = [_to_event(event) for event in decoded[1]]
                selector.ingest(events)
                now = time.monotonic_ns()
                if (
                    now - last_flush >= flush_ms * 1_000_000
                    or selector.should_flush()
                ):
                    selected = selector.flush()
                    last_flush = now
                else:
                    selected = []
                if selected:
                    send(
                        encoder.encode(
                            [
                                decoded[0],
                                [_raw_event(event) for event in selected],
                                decoded[2],
                            ]
                        ),
                        len(selected),
                        topic,
                    )

        if selector is not None:
            selected = selector.flush()
            if selected:
                send(
                    encoder.encode(
                        [time.time(), [_raw_event(event) for event in selected], 0]
                    ),
                    len(selected),
                    b"kv-events",
                )
    finally:
        result.elapsed_s = time.monotonic() - started
        sock.close(0)
        ctx.term()
    result_queue.put(result.as_dict())


def run_case(
    *,
    mode: str,
    backend: str,
    trace: dict[int, list[dict[str, Any]]],
    cycles: int,
    target_events_per_s: float,
    flush_ms: float,
    base_port: int,
) -> dict[str, Any]:
    worker_ids = sorted(trace)
    total_events_per_cycle = sum(
        template["event_count"] for records in trace.values() for template in records
    )
    cycle_interval = (
        total_events_per_cycle / target_events_per_s
        if target_events_per_s > 0
        else 0.0
    )
    context = mp.get_context("fork")
    ports = [base_port + index for index in range(len(worker_ids))]
    receiver_ready = context.Event()
    receiver_stop = context.Event()
    receiver_queue = context.Queue()
    receiver = context.Process(
        target=_receiver,
        args=(ports, receiver_ready, receiver_stop, receiver_queue, 1.0),
    )
    go = context.Event()
    ready_queue = context.Queue()
    result_queue = context.Queue()
    processes: list[Any] = []
    try:
        receiver.start()
        if not receiver_ready.wait(timeout=5):
            raise RuntimeError("receiver did not become ready")
        for index, worker_id in enumerate(worker_ids):
            process = context.Process(
                target=_producer,
                args=(
                    worker_id,
                    index,
                    ports[index],
                    mode,
                    backend,
                    trace[worker_id],
                    cycles,
                    cycle_interval,
                    flush_ms,
                    go,
                    ready_queue,
                    result_queue,
                ),
            )
            processes.append(process)
            process.start()
        for _ in processes:
            ready = ready_queue.get(timeout=10)
            if isinstance(ready, dict) and ready.get("error"):
                raise RuntimeError(f"producer bind failed: {ready['error']}")
        time.sleep(0.5)
        go.set()
        started = time.monotonic()
        for process in processes:
            process.join(timeout=240)
            if process.is_alive():
                raise RuntimeError(f"producer did not finish: {process}")
        # Do not include the receiver's drain grace period in the producer
        # throughput denominator.
        producer_stats = [result_queue.get(timeout=5) for _ in processes]
        elapsed = max(
            max(item["elapsed_s"] for item in producer_stats),
            1e-9,
        )
        receiver_stop.set()
        receiver.join(timeout=10)
        if receiver.is_alive():
            raise RuntimeError("receiver did not finish")
        receiver_stats = receiver_queue.get(timeout=5)
        totals = {
            key: sum(item[key] for item in producer_stats)
            for key in producer_stats[0]
            if key not in {"worker_id", "elapsed_s"}
        }
        expected_events = total_events_per_cycle * cycles
        transport_event_loss = max(
            totals["output_events"] - receiver_stats["received_events"], 0
        )
        selector_reduced_events = max(
            expected_events - totals["output_events"], 0
        )
        return {
            "mode": mode,
            "backend": backend if mode != "native" else "native",
            "target_events_per_s": target_events_per_s,
            "cycles": cycles,
            "events_per_cycle": total_events_per_cycle,
            "expected_input_events": expected_events,
            "producer_elapsed_s": round(elapsed, 6),
            "producer_elapsed_sum_s": round(
                sum(item["elapsed_s"] for item in producer_stats), 6
            ),
            **totals,
            **receiver_stats,
            "input_events_per_s": round(totals["input_events"] / elapsed, 1),
            "output_events_per_s": round(totals["output_events"] / elapsed, 1),
            "received_events_per_s": round(
                receiver_stats["received_events"] / elapsed, 1
            ),
            "estimated_message_loss": max(
                totals["sent_messages"] - receiver_stats["received_messages"], 0
            ),
            # For native, this is the event loss at the ZMQ receiver.  For
            # B02, output_events is already reduced by local selection, so
            # using output_events avoids mislabeling selector reductions as
            # transport loss.
            "transport_event_loss": transport_event_loss,
            "selector_reduced_events": selector_reduced_events,
            "producer_details": producer_stats,
        }
    finally:
        receiver_stop.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=2)
        if receiver.is_alive():
            receiver.terminate()
        receiver.join(timeout=2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace", required=True)
    ap.add_argument(
        "--modes",
        default="native,b02-python,b02-rust",
        help="native, b02-python, and/or b02-rust",
    )
    ap.add_argument("--target-events-per-s", required=True)
    ap.add_argument("--cycles", type=int, default=1000)
    ap.add_argument("--flush-ms", type=float, default=2.0)
    ap.add_argument("--base-port", type=int, default=62000)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    trace = _load_templates(args.trace)
    rates = [
        float(item.strip())
        for item in args.target_events_per_s.split(",")
        if item.strip()
    ]
    results = []
    case_id = 0
    for rate in rates:
        for mode in [item.strip() for item in args.modes.split(",") if item.strip()]:
            if mode in {"native"}:
                backend = "python"
            elif mode in {"b02", "b02-python", "python"}:
                mode = "b02-python"
                backend = "python"
            elif mode in {"b02-rust", "rust"}:
                mode = "b02-rust"
                backend = "rust"
            else:
                raise ValueError(f"unsupported mode: {mode}")
            result = run_case(
                mode=mode,
                backend=backend,
                trace=trace,
                cycles=args.cycles,
                target_events_per_s=rate,
                flush_ms=args.flush_ms,
                base_port=args.base_port + case_id * 100,
            )
            results.append(result)
            print(json.dumps(result, sort_keys=True), flush=True)
            case_id += 1
    with open(args.out, "w") as handle:
        json.dump(results, handle, indent=2)
    print(json.dumps({"cases": len(results), "out": args.out}))


if __name__ == "__main__":
    main()
