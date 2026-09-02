# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""Smoke-test analyzer: stickiness, prefix reuse, state-view byte economics."""

from __future__ import annotations

import json
import os
import sys

LOG_DIR = os.environ.get("B02_STATE_LOG_DIR", "/tmp/b02_state_logs")


def load(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def main() -> int:
    results = load(os.path.join(LOG_DIR, "smoke_results.jsonl"))
    decisions = load(os.path.join(LOG_DIR, "decisions.jsonl"))
    state = load(os.path.join(LOG_DIR, "state_updates.jsonl"))
    rc = 0

    print("=" * 78)
    print("1) SERVICE HEALTH")
    ok = [r for r in results if r.get("ok")]
    print(f"   requests: {len(ok)}/{len(results)} ok")
    if len(ok) != len(results) or not results:
        rc = 1
        for r in results:
            if not r.get("ok"):
                print("   FAIL:", r.get("workflow_id"), r.get("step"), r.get("error"))

    print()
    print("2) WORKFLOW STICKINESS (B02 affinity through Dynamo pin)")
    by_wf: dict[str, list[dict]] = {}
    for r in sorted(ok, key=lambda r: (r["workflow_id"], r["step"])):
        by_wf.setdefault(r["workflow_id"], []).append(r)
    sticky_steps = 0
    comparable = 0
    for wf, rows in by_wf.items():
        prev = None
        for r in rows:
            pass  # cached_tokens tell reuse; actual pin evidence comes from router log
    pinned = [d for d in decisions if d.get("source") == "affinity_pin"]
    native = [d for d in decisions if d.get("source") == "no_affinity"]
    passthrough = [d for d in decisions if d.get("source") == "passthrough"]
    print(f"   decisions: pin={len(pinned)} native={len(native)} "
          f"passthrough={len(passthrough)}")
    if decisions and not pinned:
        rc = 1
        print("   FAIL: no affinity pins happened")

    print()
    print("3) PREFIX REUSE (vLLM cached_tokens by step)")
    steps = sorted({r["step"] for r in ok})
    for s in steps:
        rows = [r for r in ok if r["step"] == s and r.get("cached_tokens") is not None]
        if rows:
            mean = sum(r["cached_tokens"] for r in rows) / len(rows)
            print(f"   step {s}: cached_tokens mean={mean:8.1f}  (n={len(rows)})")
    first = [r for r in ok if r["step"] == 0 and r.get("cached_tokens") is not None]
    last = [r for r in ok if steps and r["step"] == max(steps) and r.get("cached_tokens") is not None]
    if first and last:
        fm = sum(r["cached_tokens"] for r in first) / len(first)
        lm = sum(r["cached_tokens"] for r in last) / len(last)
        print(f"   growth (last-first): {lm - fm:+.1f} tokens")
        if lm <= fm:
            rc = 1
            print("   FAIL: cached_tokens did not grow across steps")

    print()
    print("4) STATE-VIEW BYTE ECONOMICS (B02 headline, per update)")
    if state:
        import statistics as st
        for view in ("coarse", "rich", "sketch"):
            vals = [s["bytes"][view] for s in state if view in s.get("bytes", {})]
            if vals:
                print(f"   {view:8s} mean={st.mean(vals):8.1f}B  "
                      f"p50={st.median(vals):8.1f}B  max={max(vals):6d}B")
        rich = [s["bytes"]["rich"] for s in state if "rich" in s.get("bytes", {})]
        sk = [s["bytes"]["sketch"] for s in state if "sketch" in s.get("bytes", {})]
        co = [s["bytes"]["coarse"] for s in state if "coarse" in s.get("bytes", {})]
        if rich and sk and co:
            ratio_rich_coarse = st.mean(rich) / max(1, st.mean(co))
            ratio_sketch_coarse = st.mean(sk) / max(1, st.mean(co))
            print(f"   rich/coarse = {ratio_rich_coarse:.1f}x   "
                  f"sketch/coarse = {ratio_sketch_coarse:.2f}x")
            if ratio_sketch_coarse > 1.5 or ratio_rich_coarse < 2.0:
                rc = 1
                print("   FAIL: sketch economics not demonstrated")
    else:
        print("   (no state updates recorded)")
        rc = 1

    print()
    print("SMOKE", "PASS" if rc == 0 else "FAIL")
    return rc


if __name__ == "__main__":
    sys.exit(main())
