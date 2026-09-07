#!/usr/bin/env python3
"""Replay a trace captured from native vLLM KV-event publishers.

The native and B02 modes receive the same per-publisher EventBatch sequence.
Native forwards each original payload unchanged; B02 decodes each batch,
applies the Worker-local selector, and publishes the selected batches.  This
keeps real native envelope boundaries while making the downstream comparison
repeatable.
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


def _to_event(value: Any) -> Any:
    if not isinstance(value, dict):
        raise TypeError(f"unsupported event encoding: {type(value)!r}")
    fields = dict(value)
    kind = fields.pop("type", "")
    cls = {
        "BlockStored": BlockStored,
        "BlockRemoved": BlockRemoved,
        "AllBlocksCleared": AllBlocksCleared,
    }.get(kind)
    if cls is None:
        # Keep unknown events visible to the selector as a generic event.  The
        # selector preserves events it does not classify.
        cls = type(str(kind) or "UnknownKVEvent", (), {})
    event = cls(**fields)
    event._wire_type = kind
    return event


def _raw_event(event: Any) -> dict[str, Any]:
    fields = {
        key: value for key, value in vars(event).items() if not key.startswith("_")
    }
    fields["type"] = getattr(event, "_wire_type", type(event).__name__)
    return fields


def _decode_batch(payload: bytes) -> tuple[float, list[Any], int | None]:
    decoded = msgspec.msgpack.Decoder().decode(payload)
    return float(decoded[0]), [_to_event(item) for item in decoded[1]], decoded[2]


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
                        decoded = decoder.decode(parts[-1])
                        stats["received_events"] += len(decoded[1])
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
    port: int,
    mode: str,
    records: list[dict[str, Any]],
    replay_scale: float,
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

    selector = LocalKVEventSelector(max_pending_events=4096) if mode == "b02" else None
    encoder = msgspec.msgpack.Encoder()
    result = ProducerResult(worker_id)
    ready_queue.put(worker_id)
    go.wait()
    started = time.monotonic()
    first_capture = records[0]["capture_ns"] if records else 0
    last_flush = time.monotonic_ns()
    output_seq = 0

    def send_payload(
        payload: bytes,
        event_count: int,
        topic: bytes = b"kv-events",
        sequence: int | None = None,
    ) -> None:
        nonlocal output_seq
        result.output_events += event_count
        result.output_messages += 1
        result.output_bytes += len(payload) + len(b"kv-events") + 8
        try:
            sock.send_multipart(
                (
                    topic,
                    (output_seq if sequence is None else sequence).to_bytes(8, "big"),
                    payload,
                ),
                flags=zmq.NOBLOCK,
            )
            result.sent_messages += 1
        except zmq.Again:
            result.send_drops += 1
        output_seq += 1

    try:
        for record in records:
            if replay_scale > 0 and first_capture:
                target = started + (
                    (record["capture_ns"] - first_capture) / 1_000_000_000
                ) / replay_scale
                delay = target - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

            payload = base64.b64decode(record["payload_b64"])
            ts, events, rank = _decode_batch(payload)
            result.input_messages += 1
            result.input_events += len(events)
            if selector is None:
                # Native replay forwards the exact captured payload and
                # sequence, preserving the original Dynamo wire behavior.
                send_payload(
                    payload,
                    len(events),
                    topic=base64.b64decode(record["topic_b64"]),
                    sequence=int(record["seq"]),
                )
                continue

            selector.stats.input_batches += 1
            selector.stats.input_bytes += len(payload)
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
                out_payload = encoder.encode(
                    [ts, [_raw_event(event) for event in selected], rank]
                )
                send_payload(out_payload, len(selected))

        if selector is not None:
            selected = selector.flush()
            if selected:
                out_payload = encoder.encode(
                    [time.time(), [_raw_event(event) for event in selected], 0]
                )
                send_payload(out_payload, len(selected))
    finally:
        result.elapsed_s = time.monotonic() - started
        sock.close(0)
        ctx.term()
    result_queue.put(result.as_dict())


def run_case(
    *,
    mode: str,
    trace: dict[int, list[dict[str, Any]]],
    base_port: int,
    replay_scale: float,
    flush_ms: float,
) -> dict[str, Any]:
    context = mp.get_context("fork")
    workers = sorted(trace)
    ports = [base_port + index for index in range(len(workers))]
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
    producer_processes: list[Any] = []
    try:
        receiver.start()
        if not receiver_ready.wait(timeout=5):
            raise RuntimeError("receiver did not become ready")
        for index, worker_id in enumerate(workers):
            process = context.Process(
                target=_producer,
                args=(
                    worker_id,
                    ports[index],
                    mode,
                    trace[worker_id],
                    replay_scale,
                    flush_ms,
                    go,
                    ready_queue,
                    result_queue,
                ),
            )
            producer_processes.append(process)
            process.start()
        for _ in producer_processes:
            ready = ready_queue.get(timeout=10)
            if isinstance(ready, dict) and ready.get("error"):
                raise RuntimeError(f"producer bind failed: {ready['error']}")
        time.sleep(0.5)
        go.set()
        started = time.monotonic()
        for process in producer_processes:
            process.join(timeout=180)
            if process.is_alive():
                raise RuntimeError(f"producer did not finish: {process}")
        elapsed = max(time.monotonic() - started, 1e-9)
        receiver_stop.set()
        receiver.join(timeout=10)
        if receiver.is_alive():
            raise RuntimeError("receiver did not finish")
        receiver_stats = receiver_queue.get(timeout=5)
        producer_stats = [result_queue.get(timeout=5) for _ in producer_processes]
        totals = {
            key: sum(item[key] for item in producer_stats)
            for key in producer_stats[0]
            if key != "worker_id"
        }
        return {
            "mode": mode,
            "replay_scale": replay_scale,
            "flush_ms": flush_ms,
            "workers": workers,
            "producer_elapsed_s": round(elapsed, 6),
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
            "producer_details": producer_stats,
        }
    finally:
        receiver_stop.set()
        for process in producer_processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=2)
        if receiver.is_alive():
            receiver.terminate()
        receiver.join(timeout=2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace", required=True)
    ap.add_argument("--modes", default="native,b02")
    ap.add_argument("--replay-scale", type=float, default=0.0)
    ap.add_argument("--flush-ms", type=float, default=2.0)
    ap.add_argument("--base-port", type=int, default=56000)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    trace: dict[int, list[dict[str, Any]]] = {}
    with open(args.trace) as handle:
        for line in handle:
            record = json.loads(line)
            trace.setdefault(int(record["port"]), []).append(record)
    for records in trace.values():
        records.sort(key=lambda record: record["seq"])

    results = []
    for index, mode in enumerate(
        item.strip() for item in args.modes.split(",") if item.strip()
    ):
        result = run_case(
            mode=mode,
            trace=trace,
            base_port=args.base_port + index * 100,
            replay_scale=args.replay_scale,
            flush_ms=args.flush_ms,
        )
        results.append(result)
        print(json.dumps(result, sort_keys=True), flush=True)
    with open(args.out, "w") as handle:
        json.dump(results, handle, indent=2)
    print(json.dumps({"cases": len(results), "out": args.out}))


if __name__ == "__main__":
    main()
