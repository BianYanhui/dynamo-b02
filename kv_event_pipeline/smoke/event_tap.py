# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""Event tap: passive ZMQ SUB counter for worker KV-event streams.

Connects to each worker's kv-events publisher port, counts messages and
bytes per second. This measures the offered load of the ingestion funnel
(DynamicSubscriber -> MPSC -> decode -> KvIndexer) from issue #11899.

Output: one JSONL row per (second, port): {"ts", "port", "msgs", "bytes"}.
Prints a summary line per port at the end.
"""

from __future__ import annotations

import argparse
import json
import signal
import time

import zmq


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ports", required=True, help="comma-separated worker event ports")
    ap.add_argument("--duration", type=float, default=3600, help="seconds to run")
    ap.add_argument("--out", required=True, help="output jsonl path")
    args = ap.parse_args()
    ports = [int(p) for p in args.ports.split(",")]

    ctx = zmq.Context()
    poller = zmq.Poller()
    socks = {}
    for p in ports:
        s = ctx.socket(zmq.SUB)
        s.setsockopt(zmq.SUBSCRIBE, b"")
        s.setsockopt(zmq.RCVHWM, 200000)
        s.connect(f"tcp://127.0.0.1:{p}")
        poller.register(s, zmq.POLLIN)
        socks[s] = p

    counts = {p: [0, 0] for p in ports}  # msgs, bytes
    totals = {p: [0, 0] for p in ports}
    first_msg_ts = None
    t0 = time.time()
    window_start = time.time()
    out = open(args.out, "w")
    sample_count = 0
    stopping = {"flag": False}

    def _sigterm(_sig, _frm):
        stopping["flag"] = True
    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    try:
        while time.time() - t0 < args.duration and not stopping["flag"]:
            events = dict(poller.poll(timeout=500))
            got = False
            for s in list(events):
                p = socks[s]
                while True:
                    try:
                        parts = s.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    got = True
                    size = sum(len(x) for x in parts)
                    counts[p][0] += 1
                    counts[p][1] += size
                    if first_msg_ts is None:
                        first_msg_ts = time.time()
            if got or time.time() - window_start >= 1.0:
                if time.time() - window_start >= 1.0:
                    ts = time.time_ns()
                    for p in ports:
                        if counts[p][0] or sample_count % 5 == 0:
                            out.write(json.dumps({
                                "ts": ts, "port": p,
                                "msgs": counts[p][0], "bytes": counts[p][1]}) + "\n")
                    out.flush()
                    sample_count += 1
                    for p in ports:
                        totals[p][0] += counts[p][0]
                        totals[p][1] += counts[p][1]
                        counts[p][0] = 0
                        counts[p][1] = 0
                    window_start = time.time()
    except KeyboardInterrupt:
        pass
    finally:
        out.close()
        total_msgs = sum(t[0] for t in totals.values())
        total_bytes = sum(t[1] for t in totals.values())
        dur = (first_msg_ts and (time.time() - first_msg_ts)) or (time.time() - t0)
        summary = {
            "total_msgs": total_msgs, "total_bytes": total_bytes,
            "active_seconds": round(dur, 1),
            "msgs_per_s": round(total_msgs / max(0.1, dur), 1),
            "bytes_per_s": round(total_bytes / max(0.1, dur), 1),
            "per_port": {str(p): {"msgs": totals[p][0], "bytes": totals[p][1]}
                          for p in ports},
        }
        with open(args.out + ".summary", "w") as f:
            json.dump(summary, f, indent=2)
        print(json.dumps(summary))


if __name__ == "__main__":
    main()
