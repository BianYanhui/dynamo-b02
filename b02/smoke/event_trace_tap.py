#!/usr/bin/env python3
"""Capture native vLLM KV-event envelopes and their size distribution.

The trace keeps the original topic, sequence, and msgpack payload so it can
be replayed without changing EventBatch boundaries.  It intentionally does
not interpret individual event variants; the envelope-size question only
needs the length of EventBatch.events.
"""

from __future__ import annotations

import argparse
import base64
import json
import signal
import time
from collections import Counter

import msgspec
import zmq


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ports", required=True, help="comma-separated ZMQ PUB ports")
    ap.add_argument("--duration", type=float, default=3600)
    ap.add_argument("--out", required=True, help="JSONL trace output")
    args = ap.parse_args()
    ports = [int(item) for item in args.ports.split(",")]

    ctx = zmq.Context()
    poller = zmq.Poller()
    sockets: dict[zmq.Socket, int] = {}
    for port in ports:
        sock = ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        sock.setsockopt(zmq.RCVHWM, 1_000_000)
        sock.connect(f"tcp://127.0.0.1:{port}")
        poller.register(sock, zmq.POLLIN)
        sockets[sock] = port

    decoder = msgspec.msgpack.Decoder()
    stopping = {"flag": False}

    def stop(_sig: int, _frame: object) -> None:
        stopping["flag"] = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    started = time.time()
    counts = Counter()
    per_port: dict[int, Counter] = {port: Counter() for port in ports}
    trace_file = open(args.out, "w")
    try:
        while time.time() - started < args.duration and not stopping["flag"]:
            ready = dict(poller.poll(500))
            for sock in ready:
                port = sockets[sock]
                while True:
                    try:
                        parts = sock.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(parts) != 3:
                        counts["malformed_messages"] += 1
                        continue
                    try:
                        decoded = decoder.decode(parts[2])
                        event_count = len(decoded[1])
                    except Exception:
                        counts["decode_errors"] += 1
                        continue

                    record = {
                        "capture_ns": time.time_ns(),
                        "port": port,
                        "topic_b64": base64.b64encode(parts[0]).decode("ascii"),
                        "seq": int.from_bytes(parts[1], "big"),
                        "payload_b64": base64.b64encode(parts[2]).decode("ascii"),
                        "event_count": event_count,
                    }
                    trace_file.write(json.dumps(record, separators=(",", ":")) + "\n")
                    counts["messages"] += 1
                    counts["events"] += event_count
                    counts["bytes"] += sum(len(part) for part in parts)
                    counts[f"envelope_{event_count}"] += 1
                    per_port[port]["messages"] += 1
                    per_port[port]["events"] += event_count
                    per_port[port][f"envelope_{event_count}"] += 1
            trace_file.flush()
    finally:
        trace_file.close()
        for sock in sockets:
            sock.close(0)
        ctx.term()

    one = counts["envelope_1"]
    summary = {
        "messages": counts["messages"],
        "events": counts["events"],
        "bytes": counts["bytes"],
        "decode_errors": counts["decode_errors"],
        "malformed_messages": counts["malformed_messages"],
        "single_event_envelopes": one,
        "single_event_envelope_ratio": round(one / max(1, counts["messages"]), 6),
        "single_event_event_ratio": round(one / max(1, counts["events"]), 6),
        "envelope_histogram": {
            str(key)[9:]: value
            for key, value in sorted(counts.items())
            if key.startswith("envelope_")
        },
        "per_port": {str(port): dict(stats) for port, stats in per_port.items()},
    }
    with open(args.out + ".summary", "w") as handle:
        json.dump(summary, handle, indent=2)
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
