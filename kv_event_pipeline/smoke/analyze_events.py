# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""Event-volume A/B analyzer: offered funnel load per routing mode.

Funnel capacity reference: issue #11899 measured ~566k events/s ceiling on
the current direct-ZMQ ingestion path (144-core node, 256 publishers).
"""

from __future__ import annotations

import json
import os
import sys

STATE_DIR = os.environ.get("KV_EVENT_STATE_LOG_DIR", "/tmp/kv_event_events_ab")
FUNNEL_CAPACITY = 566_000  # events/s, issue #11899 headline (current path)


def load(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(len(xs) * p / 100))], 1)


def cell_stats(mode: str) -> dict:
    # Prefer the summary file; fall back to aggregating the raw tap jsonl
    # (SIGTERM-killed taps never write their summary).
    tap = load(os.path.join(STATE_DIR, f"tap_{mode}.jsonl.summary"))
    if not tap:
        rows = load(os.path.join(STATE_DIR, f"tap_{mode}.jsonl"))
        if rows:
            total_msgs = sum(r["msgs"] for r in rows)
            total_bytes = sum(r["bytes"] for r in rows)
            if rows and total_msgs > 0:
                t0 = min(r["ts"] for r in rows) / 1e9
                t1 = max(r["ts"] for r in rows) / 1e9
                dur = max(0.1, t1 - t0)
                tap = [{"total_msgs": total_msgs, "total_bytes": total_bytes,
                        "msgs_per_s": round(total_msgs / dur, 1),
                        "bytes_per_s": round(total_bytes / dur, 1)}]
    results = [r for r in load(os.path.join(STATE_DIR, f"results_events_{mode}.jsonl"))
               if r.get("ok")]
    ttft = [r["ttft_ms"] for r in results if r.get("ttft_ms") is not None]
    cached = [r["cached_tokens"] or 0 for r in results
              if r.get("cached_tokens") is not None]
    s = {
        "mode": mode,
        "requests_ok": len(results),
        "ttft_p50": pct(ttft, 50),
        "ttft_p95": pct(ttft, 95),
        "cached_mean": round(sum(cached) / len(cached), 1) if cached else None,
    }
    if tap:
        s.update({
            "total_msgs": tap[0]["total_msgs"],
            "total_bytes": tap[0]["total_bytes"],
            "msgs_per_s": tap[0]["msgs_per_s"],
            "bytes_per_s": tap[0]["bytes_per_s"],
        })
        if results:
            s["msgs_per_req"] = round(tap[0]["total_msgs"] / len(results), 1)
            s["bytes_per_req"] = round(tap[0]["total_bytes"] / len(results), 1)
    return s


def main() -> int:
    modes = ["rr", "kv", "selective"]
    stats = {m: cell_stats(m) for m in modes}
    rc = 0

    print("=" * 96)
    print("EVENT-VOLUME A/B — funnel offered load per routing mode")
    print(f"{'mode':<6}{'ok':>4}{'ttft_p50':>10}{'ttft_p95':>10}{'cached':>9}"
          f"{'msgs':>8}{'msgs/req':>10}{'bytes/req':>11}{'msgs/s':>10}")
    for m in modes:
        s = stats[m]
        print(f"{m:<6}{s['requests_ok']:>4}{str(s['ttft_p50']):>10}"
              f"{str(s['ttft_p95']):>10}{str(s['cached_mean']):>9}"
              f"{str(s.get('total_msgs','-')):>8}{str(s.get('msgs_per_req','-')):>10}"
              f"{str(s.get('bytes_per_req','-')):>11}{str(s.get('msgs_per_s','-')):>10}")

    with open(os.path.join(STATE_DIR, "events_ab_stats.json"), "w") as f:
        json.dump(stats, f, indent=2)

    print()
    print("FUNNEL EXTRAPOLATION (issue #11899: ~566k events/s ceiling, current path)")
    for m in modes:
        mpr = stats[m].get("msgs_per_req")
        if mpr:
            max_rps = FUNNEL_CAPACITY / mpr
            print(f"   {m:<5} {mpr:7.1f} events/req -> funnel saturates at "
                  f"~{max_rps:8.0f} req/s cluster-wide")

    print()
    print("VERDICT")
    base = stats["rr"].get("msgs_per_req")
    for m in ("kv", "selective"):
        v = stats[m].get("msgs_per_req")
        if base and v:
            red = 100 * (1 - v / base)
            print(f"   {m}: events/req vs rr = {v} vs {base}  ({red:+.1f}% reduction)")
    b, k = stats["selective"].get("msgs_per_req"), stats["kv"].get("msgs_per_req")
    if b and k:
        print(f"   kv_event vs native kv: {100 * (1 - b / k):+.1f}% reduction")
    if not all(stats[m].get("total_msgs") for m in modes):
        rc = 1
        print("   FAIL: missing tap data for some cells")
    if stats["rr"]["requests_ok"] == 0 or stats["selective"]["requests_ok"] == 0:
        rc = 1
        print("   FAIL: some cells served no requests")
    print("EVENT AB", "PASS" if rc == 0 else "CHECK")
    return rc


if __name__ == "__main__":
    sys.exit(main())
