#!/usr/bin/env python3
"""Fair replay benchmark for native Dynamo versus Worker-local KV event.

Both modes consume the same deterministic event stream.  A case is defined by
the seed, publisher count, envelope size, pattern, and total event count.  The
only mode-dependent operation is whether the generated events are sent
directly to ZMQ or first pass through ``LocalKVEventSelector``.

This is an event-plane benchmark, not a model-generation benchmark.  The
receiver decodes every ZMQ envelope so the measured native path includes the
Python/ZMQ receive-and-decode funnel used by the synthetic downstream plane.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing as mp
import queue
import time
from dataclasses import dataclass
from typing import Any

import msgspec
import zmq

from kv_event_router.prepublish import LocalKVEventSelector


class BlockStored:
    """Minimal vLLM-compatible event shape used by the KV event selector."""

    def __init__(self, block_hash: int, parent_block_hash: int | None):
        self.block_hashes = [block_hash]
        self.parent_block_hash = parent_block_hash
        self.token_ids = [block_hash & 0xFFFF, (block_hash + 1) & 0xFFFF]
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
    *, seed: int, worker_id: int, sequence: int, events_per_envelope: int, pattern: str
) -> list[BlockStored]:
    events: list[BlockStored] = []
    # The arithmetic is deterministic and cheap, while the seed makes every
    # replay independently addressable.  The constants keep workers and
    # envelopes in disjoint hash ranges.
    # Keep the synthetic block hash within the signed 64-bit range accepted
    # by msgpack/vLLM while still separating seeds, workers, and envelopes.
    base = (
        (seed & 0xFFFFFFFF) * 10**6
        + worker_id * 10**12
        + sequence * events_per_envelope * 10**3
    )
    previous_hash: int | None = None
    for event_id in range(events_per_envelope):
        block_hash = base + event_id + 100
        if pattern == "duplicate":
            block_hash = (seed & 0xFFFFFFFF) * 10**6 + worker_id * 10**6 + 7
        parent = previous_hash if pattern == "chain" else None
        events.append(BlockStored(block_hash, parent))
        previous_hash = block_hash
    return events


def _update_digest(digest: Any, events: list[BlockStored]) -> None:
    for event in events:
        block_hash = event.block_hashes[0]
        parent = event.parent_block_hash or 0
        digest.update(block_hash.to_bytes(16, "big", signed=False))
        digest.update(parent.to_bytes(16, "big", signed=False))


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
    result = {
        "received_messages": 0,
        "received_bytes": 0,
        "received_events": 0,
        "decode_errors": 0,
    }
    stop_seen_at: float | None = None
    try:
        while True:
            if stop.is_set() and stop_seen_at is None:
                stop_seen_at = time.monotonic()
            if (
                stop_seen_at is not None
                and time.monotonic() - stop_seen_at >= drain_seconds
            ):
                break
            for sock, _ in poller.poll(20):
                for _ in range(4096):
                    try:
                        parts = sock.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    result["received_messages"] += 1
                    result["received_bytes"] += sum(len(part) for part in parts)
                    try:
                        decoded = decoder.decode(parts[-1])
                        result["received_events"] += len(decoded[1])
                    except Exception:
                        result["decode_errors"] += 1
    finally:
        for sock in sockets:
            sock.close(0)
        context.term()
    result_queue.put(result)


@dataclass
class ProducerResult:
    worker_id: int
    input_events: int = 0
    input_envelopes: int = 0
    sent_messages: int = 0
    send_drops: int = 0
    output_events: int = 0
    output_messages: int = 0
    output_bytes: int = 0
    elapsed_s: float = 0.0
    input_digest: str = ""

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def _producer(
    worker_id: int,
    port: int,
    mode: str,
    seed: int,
    publishers: int,
    events_per_envelope: int,
    envelopes_per_worker: int,
    pattern: str,
    target_events_per_s: float,
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
        ready_queue.put({"worker_id": worker_id, "error": repr(exc)})
        sock.close(0)
        context.term()
        return

    encoder = msgspec.msgpack.Encoder()
    selector = LocalKVEventSelector(max_pending_events=4096) if mode == "selective" else None
    result = ProducerResult(worker_id)
    digest = hashlib.sha256()
    ready_queue.put(worker_id)
    go.wait()
    started_at = time.monotonic()
    last_flush_ns = time.monotonic_ns()
    interval_s = (
        events_per_envelope * publishers / target_events_per_s
        if target_events_per_s > 0
        else 0.0
    )
    next_emit = started_at

    try:
        for sequence in range(envelopes_per_worker):
            if interval_s > 0:
                now = time.monotonic()
                if now < next_emit:
                    time.sleep(next_emit - now)
                next_emit += interval_s
                if next_emit < time.monotonic() - interval_s:
                    next_emit = time.monotonic()
            events = _build_events(
                seed=seed,
                worker_id=worker_id,
                sequence=sequence,
                events_per_envelope=events_per_envelope,
                pattern=pattern,
            )
            _update_digest(digest, events)
            result.input_events += len(events)
            result.input_envelopes += 1

            selected_events: list[BlockStored]
            if selector is None:
                selected_events = events
            else:
                selector.ingest(events)
                now = time.monotonic_ns()
                if (
                    now - last_flush_ns >= 2_000_000
                    or selector.should_flush()
                    or sequence == envelopes_per_worker - 1
                ):
                    selected_events = selector.flush()
                    last_flush_ns = now
                else:
                    selected_events = []

            if not selected_events:
                continue
            payload = encoder.encode([time.time(), [_raw(e) for e in selected_events], 0])
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
        result.elapsed_s = time.monotonic() - started_at
        result.input_digest = digest.hexdigest()
        sock.close(0)
        context.term()
    result_queue.put(result.as_dict())


def _parse_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def run_case(
    *,
    mode: str,
    seed: int,
    publishers: int,
    events_per_envelope: int,
    total_events: int,
    pattern: str,
    base_port: int,
    target_events_per_s: float = 0.0,
) -> dict[str, Any]:
    if total_events % (publishers * events_per_envelope):
        raise ValueError("total_events must divide evenly across publishers/envelopes")
    context = mp.get_context("fork")
    ports = [base_port + i for i in range(publishers)]
    envelopes_per_worker = total_events // (publishers * events_per_envelope)
    receiver_ready = context.Event()
    receiver_stop = context.Event()
    receiver_queue = context.Queue()
    receiver = context.Process(
        target=_receiver,
        args=(ports, receiver_ready, receiver_stop, receiver_queue, 1.0),
    )
    workers: list[Any] = []
    producer_go = context.Event()
    ready_queue = context.Queue()
    result_queue = context.Queue()

    try:
        receiver.start()
        if not receiver_ready.wait(timeout=5):
            raise RuntimeError("receiver did not become ready")
        for worker_id, port in enumerate(ports):
            worker = context.Process(
                target=_producer,
                args=(
                    worker_id,
                    port,
                    mode,
                    seed,
                    publishers,
                    events_per_envelope,
                    envelopes_per_worker,
                    pattern,
                    target_events_per_s,
                    producer_go,
                    ready_queue,
                    result_queue,
                ),
            )
            workers.append(worker)
            worker.start()
        for _ in workers:
            ready = ready_queue.get(timeout=10)
            if isinstance(ready, dict) and ready.get("error"):
                raise RuntimeError(f"producer bind failed: {ready['error']}")
        time.sleep(0.5)  # PUB/SUB slow-joiner settling time
        producer_go.set()
        started_at = time.monotonic()
        for worker in workers:
            worker.join(timeout=120)
            if worker.is_alive():
                raise RuntimeError(f"producer did not finish: {worker}")
        producer_elapsed = time.monotonic() - started_at
        receiver_stop.set()
        receiver.join(timeout=10)
        if receiver.is_alive():
            raise RuntimeError("receiver did not finish")

        receiver_stats = receiver_queue.get(timeout=5)
        producer_stats = [result_queue.get(timeout=5) for _ in workers]
        totals = {
            key: sum(item[key] for item in producer_stats)
            for key in producer_stats[0]
            if key not in ("worker_id", "input_digest")
        }
        input_digests = [item["input_digest"] for item in sorted(producer_stats, key=lambda x: x["worker_id"])]
        elapsed = max(producer_elapsed, 1e-9)
        return {
            "mode": mode,
            "seed": seed,
            "pattern": pattern,
            "publishers": publishers,
            "events_per_envelope": events_per_envelope,
            "total_events": total_events,
            "target_events_per_s": target_events_per_s,
            "envelopes_per_worker": envelopes_per_worker,
            "producer_elapsed_s": round(elapsed, 6),
            **totals,
            **receiver_stats,
            "input_events_per_s": round(totals["input_events"] / elapsed, 1),
            "output_events_per_s": round(totals["output_events"] / elapsed, 1),
            "received_events_per_s": round(
                receiver_stats["received_events"] / elapsed, 1
            ),
            "received_bytes_per_s": round(
                receiver_stats["received_bytes"] / elapsed, 1
            ),
            "estimated_message_loss": max(
                totals["sent_messages"] - receiver_stats["received_messages"], 0
            ),
            "input_digests": input_digests,
            "producer_details": producer_stats,
        }
    finally:
        receiver_stop.set()
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
            worker.join(timeout=2)
        if receiver.is_alive():
            receiver.terminate()
        receiver.join(timeout=2)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--modes", default="native,prepublish")
    parser.add_argument("--publishers", default="1,4")
    parser.add_argument("--envelope-events", default="1,16,64")
    parser.add_argument("--patterns", default="unique,chain")
    parser.add_argument("--total-events", type=int, default=1_048_576)
    parser.add_argument("--seed", type=int, default=20260907)
    parser.add_argument("--target-events-per-s", default="0")
    parser.add_argument("--base-port", type=int, default=46000)
    parser.add_argument("--out", default="/tmp/kv_event_native_replay.json")
    args = parser.parse_args()

    results: list[dict[str, Any]] = []
    case_id = 0
    target_rates = [
        float(item.strip())
        for item in args.target_events_per_s.split(",")
        if item.strip()
    ]
    for pattern in [item.strip() for item in args.patterns.split(",") if item.strip()]:
        for events_per_envelope in _parse_ints(args.envelope_events):
            for publishers in _parse_ints(args.publishers):
                for mode in [item.strip() for item in args.modes.split(",") if item.strip()]:
                    for target_rate in target_rates:
                        result = run_case(
                            mode=mode,
                            seed=args.seed,
                            publishers=publishers,
                            events_per_envelope=events_per_envelope,
                            total_events=args.total_events,
                            pattern=pattern,
                            base_port=args.base_port + case_id * 100,
                            target_events_per_s=target_rate,
                        )
                        results.append(result)
                        print(json.dumps(result, sort_keys=True), flush=True)
                        case_id += 1
    with open(args.out, "w") as handle:
        json.dump(results, handle, indent=2)
    print(json.dumps({"cases": len(results), "out": args.out}))


if __name__ == "__main__":
    main()
