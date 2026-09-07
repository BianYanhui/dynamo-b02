#!/usr/bin/env python3
"""Stress the raw KV-event ZMQ path and the Worker-local B02 selector.

The benchmark uses the same three-frame shape as vLLM's ZmqEventPublisher:
``(topic, sequence, msgpack(EventBatch))``. A receiver decodes every payload,
so the result includes the Python ZMQ receive/decode funnel rather than only
socket send bandwidth.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any

import msgspec
import zmq

from b02_sketch_router.prepublish import LocalKVEventSelector


class BlockStored:
    """Small vLLM-compatible shape used by LocalKVEventSelector."""

    def __init__(self, block_hashes: list[int], parent_block_hash: int | None):
        self.block_hashes = block_hashes
        self.parent_block_hash = parent_block_hash
        self.token_ids = [block_hashes[0] & 0xFFFF, (block_hashes[0] + 1) & 0xFFFF]
        self.block_size = 2
        self.lora_name = None


def _raw(event: BlockStored) -> dict[str, Any]:
    return {
        "type": "BlockStored",
        "block_hashes": event.block_hashes,
        "parent_block_hash": event.parent_block_hash,
        "token_ids": event.token_ids,
        "block_size": event.block_size,
    }


def _build_events(
    pattern: str, worker_id: int, sequence: int, events_per_envelope: int
) -> list[BlockStored]:
    events: list[BlockStored] = []
    for event_id in range(events_per_envelope):
        block_hash = (
            worker_id * 1_000_000_000
            + sequence * events_per_envelope
            + event_id
            + 100
        )
        if pattern == "duplicate":
            block_hash = worker_id * 1_000_000 + 7
        # "unique" deliberately disables the parent chain so the selector
        # cannot merge adjacent BlockStored records.  It is the worst-case
        # CPU/input path for a worker-local pre-publish selector.
        parent = None if pattern in ("duplicate", "unique") else block_hash - 1
        events.append(BlockStored([block_hash], parent))
    return events


def _build_ring(
    pattern: str, worker_id: int, events_per_envelope: int, ring_size: int = 64
) -> tuple[list[bytes], list[list[BlockStored]]]:
    encoder = msgspec.msgpack.Encoder()
    payloads: list[bytes] = []
    event_batches: list[list[BlockStored]] = []
    for envelope_id in range(ring_size):
        events = _build_events(pattern, worker_id, envelope_id, events_per_envelope)
        payloads.append(encoder.encode([time.time(), [_raw(e) for e in events], 0]))
        event_batches.append(events)
    return payloads, event_batches


def _receiver(
    ports: list[int], ready: Any, stop: Any, result_queue: Any, drain_seconds: float
) -> None:
    context = zmq.Context()
    poller = zmq.Poller()
    sockets: dict[Any, int] = {}
    for port in ports:
        sock = context.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        sock.setsockopt(zmq.RCVHWM, 1_000_000)
        sock.connect(f"tcp://127.0.0.1:{port}")
        poller.register(sock, zmq.POLLIN)
        sockets[sock] = port

    decoder = msgspec.msgpack.Decoder()
    ready.set()
    messages = 0
    received_bytes = 0
    received_events = 0
    decode_errors = 0
    stop_seen_at: float | None = None
    try:
        while True:
            if stop.is_set() and stop_seen_at is None:
                stop_seen_at = time.monotonic()
            if stop_seen_at is not None and time.monotonic() - stop_seen_at >= drain_seconds:
                break
            for sock, _ in poller.poll(20):
                for _ in range(4096):
                    try:
                        parts = sock.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    messages += 1
                    received_bytes += sum(len(part) for part in parts)
                    try:
                        decoded = decoder.decode(parts[-1])
                        received_events += len(decoded[1])
                    except Exception:
                        decode_errors += 1
    finally:
        for sock in sockets:
            sock.close(0)
        context.term()
    result_queue.put({
        "received_messages": messages,
        "received_bytes": received_bytes,
        "received_events": received_events,
        "decode_errors": decode_errors,
    })


@dataclass
class ProducerResult:
    worker_id: int
    attempted_messages: int = 0
    sent_messages: int = 0
    send_drops: int = 0
    input_events: int = 0
    output_events: int = 0
    output_messages: int = 0
    output_bytes: int = 0

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


def _producer(
    worker_id: int,
    port: int,
    mode: str,
    pattern: str,
    events_per_envelope: int,
    duration: float,
    go: Any,
    ready_queue: Any,
    result_queue: Any,
) -> None:
    context = zmq.Context()
    sock = context.socket(zmq.PUB)
    sock.setsockopt(zmq.SNDHWM, 1_000_000)
    sock.setsockopt(zmq.LINGER, 0)
    try:
        sock.bind(f"tcp://*:{port}")
    except Exception as exc:
        # Let the parent tear down the whole case instead of waiting forever
        # for a readiness token after a stale-process/port collision.
        ready_queue.put({"worker_id": worker_id, "error": repr(exc)})
        sock.close(0)
        context.term()
        return
    payloads, event_batches = _build_ring(pattern, worker_id, events_per_envelope)
    encoder = msgspec.msgpack.Encoder()
    selector = LocalKVEventSelector(max_pending_events=4096) if mode != "native" else None
    result = ProducerResult(worker_id)
    ready_queue.put(worker_id)
    go.wait()
    deadline = time.monotonic() + duration
    last_flush = time.monotonic_ns()
    sequence = 0
    try:
        while time.monotonic() < deadline:
            selected_events: list[BlockStored]
            if pattern == "unique":
                selected_events = _build_events(
                    pattern, worker_id, sequence, events_per_envelope
                )
                payload = encoder.encode([time.time(), [_raw(e) for e in selected_events], 0])
                if selector is not None:
                    selector.ingest(selected_events)
                    selected_events = []
                    now = time.monotonic_ns()
                    if now - last_flush >= 2_000_000:
                        selected_events = selector.flush()
                        last_flush = now
                    if selected_events:
                        payload = encoder.encode(
                            [time.time(), [_raw(e) for e in selected_events], 0]
                        )
                    else:
                        payload = None
            elif selector is None:
                ring_index = sequence % len(payloads)
                payload = payloads[ring_index]
                selected_events = event_batches[ring_index]
            else:
                ring_index = sequence % len(payloads)
                selector.ingest(event_batches[ring_index])
                now = time.monotonic_ns()
                if now - last_flush >= 2_000_000:
                    selected_events = selector.flush()
                    last_flush = now
                else:
                    selected_events = []
                payload = (
                    encoder.encode([time.time(), [_raw(e) for e in selected_events], 0])
                    if selected_events else None
                )
            result.input_events += events_per_envelope
            result.attempted_messages += 1
            if payload is not None:
                result.output_events += len(selected_events)
                result.output_messages += 1
                result.output_bytes += len(payload) + len(b"kv-events") + 8
                try:
                    sock.send_multipart(
                        (b"kv-events", sequence.to_bytes(8, "big"), payload),
                        flags=zmq.NOBLOCK,
                    )
                    result.sent_messages += 1
                except zmq.Again:
                    result.send_drops += 1
            sequence += 1
        if selector is not None:
            selected_events = selector.flush()
            if selected_events:
                payload = encoder.encode([
                    time.time(), [_raw(e) for e in selected_events], 0
                ])
                result.output_events += len(selected_events)
                result.output_messages += 1
                result.output_bytes += len(payload) + len(b"kv-events") + 8
                try:
                    sock.send_multipart(
                        (b"kv-events", sequence.to_bytes(8, "big"), payload),
                        flags=zmq.NOBLOCK,
                    )
                    result.sent_messages += 1
                except zmq.Again:
                    result.send_drops += 1
    finally:
        sock.close(0)
        context.term()
    result_queue.put(result.as_dict())


def _parse_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def run_case(
    mode: str,
    publishers: int,
    events_per_envelope: int,
    pattern: str,
    duration: float,
    base_port: int,
) -> dict[str, Any]:
    context = mp.get_context("fork")
    ports = [base_port + i for i in range(publishers)]
    receiver_ready = context.Event()
    receiver_stop = context.Event()
    receiver_queue = context.Queue()
    receiver = context.Process(
        target=_receiver,
        args=(ports, receiver_ready, receiver_stop, receiver_queue, 0.5),
    )
    process_mode = mode != "b02-thread"
    producer_go: Any = context.Event() if process_mode else threading.Event()
    ready_queue: Any = context.Queue() if process_mode else queue.Queue()
    result_queue: Any = context.Queue() if process_mode else queue.Queue()
    workers: list[Any] = []
    receiver: Any = None
    try:
        receiver = context.Process(
            target=_receiver,
            args=(ports, receiver_ready, receiver_stop, receiver_queue, 0.5),
        )
        receiver.start()
        if not receiver_ready.wait(timeout=5):
            raise RuntimeError("receiver did not become ready")

        producer_mode = "native" if mode == "native" else "b02"
        for worker_id, port in enumerate(ports):
            args = (
                worker_id,
                port,
                producer_mode,
                pattern,
                events_per_envelope,
                duration,
                producer_go,
                ready_queue,
                result_queue,
            )
            if process_mode:
                worker = context.Process(target=_producer, args=args)
            else:
                worker = threading.Thread(target=_producer, args=args, daemon=True)
            workers.append(worker)
            worker.start()

        for _ in workers:
            ready = ready_queue.get(timeout=10)
            if isinstance(ready, dict) and ready.get("error"):
                raise RuntimeError(
                    f"producer {ready.get('worker_id')} failed to bind: {ready['error']}"
                )
        time.sleep(0.2)  # PUB/SUB slow-joiner settling time
        started_at = time.monotonic()
        producer_go.set()
        for worker in workers:
            worker.join(timeout=duration + 15)
            if worker.is_alive():
                raise RuntimeError(f"producer did not finish: {worker}")
        elapsed = time.monotonic() - started_at
        receiver_stop.set()
        receiver.join(timeout=5)
        if receiver.is_alive():
            raise RuntimeError("receiver did not finish")

        receiver_stats = receiver_queue.get(timeout=3)
        producer_stats = [result_queue.get(timeout=3) for _ in workers]
        totals = {
            key: sum(item[key] for item in producer_stats)
            for key in producer_stats[0]
            if key != "worker_id"
        }
        result = {
            "mode": mode,
            "pattern": pattern,
            "publishers": publishers,
            "events_per_envelope": events_per_envelope,
            "duration_s": round(elapsed, 3),
            **totals,
            **receiver_stats,
            "input_events_per_s": round(totals["input_events"] / elapsed, 1),
            "output_events_per_s": round(totals["output_events"] / elapsed, 1),
            "received_events_per_s": round(receiver_stats["received_events"] / elapsed, 1),
            "received_bytes_per_s": round(receiver_stats["received_bytes"] / elapsed, 1),
            "producer_details": producer_stats,
        }
        return result
    finally:
        receiver_stop.set()
        for worker in workers:
            if getattr(worker, "is_alive", lambda: False)():
                if process_mode:
                    worker.terminate()
                worker.join(timeout=2)
        if receiver is not None:
            if receiver.is_alive():
                receiver.terminate()
            receiver.join(timeout=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--modes", default="native,b02-thread,b02-process")
    parser.add_argument("--publishers", default="1,4,8,16")
    parser.add_argument("--envelope-events", default="1,16,64")
    parser.add_argument(
        "--pattern", choices=("duplicate", "chain", "unique"), default="chain"
    )
    parser.add_argument("--duration", type=float, default=2.0)
    parser.add_argument("--base-port", type=int, default=26000)
    parser.add_argument("--out", default="/tmp/b02_prepublish_scaling.json")
    args = parser.parse_args()

    results: list[dict[str, Any]] = []
    case_id = 0
    for events_per_envelope in _parse_ints(args.envelope_events):
        for publishers in _parse_ints(args.publishers):
            for mode in [item.strip() for item in args.modes.split(",") if item.strip()]:
                result = run_case(
                    mode,
                    publishers,
                    events_per_envelope,
                    args.pattern,
                    args.duration,
                    args.base_port + case_id * 100,
                )
                results.append(result)
                print(json.dumps(result, sort_keys=True), flush=True)
                case_id += 1
    with open(args.out, "w") as handle:
        json.dump(results, handle, indent=2)
    print(json.dumps({"cases": len(results), "out": args.out}))


if __name__ == "__main__":
    main()
