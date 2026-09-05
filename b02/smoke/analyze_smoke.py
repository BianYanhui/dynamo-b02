# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""Analyze the B02v1 smoke run.

The pass/fail criteria now match selective KV-state signaling.  Session
stickiness is intentionally informational because v1 leaves request routing
to the native KV-aware router.
"""

from __future__ import annotations

import json
import os
import sys

LOG_DIR = os.environ.get("B02_STATE_LOG_DIR", "/tmp/b02_state_logs")


def load(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def main() -> int:
    results = load(os.path.join(LOG_DIR, "smoke_results.jsonl"))
    decisions = load(os.path.join(LOG_DIR, "decisions.jsonl"))
    state = load(os.path.join(LOG_DIR, "state_updates.jsonl"))
    rc = 0

    print("=" * 78)
    print("1) SERVICE HEALTH")
    ok = [row for row in results if row.get("ok")]
    print(f"   requests: {len(ok)}/{len(results)} ok")
    if len(ok) != len(results) or not results:
        rc = 1
        for row in results:
            if not row.get("ok"):
                print("   FAIL:", row.get("workflow_id"), row.get("step"), row.get("error"))

    print()
    print("2) REQUEST ROUTING")
    native = [row for row in decisions if row.get("source") == "native_kv"]
    pinned = [row for row in decisions if row.get("source") == "affinity_pin"]
    passthrough = [row for row in decisions if row.get("source") == "passthrough"]
    print(f"   native_kv={len(native)} affinity_pin(legacy)={len(pinned)} "
          f"passthrough={len(passthrough)}")
    if native:
        print("   OK: v1 leaves request selection to native KV-aware routing")

    print()
    print("3) PREFIX REUSE (vLLM cached_tokens by step)")
    steps = sorted({row["step"] for row in ok if row.get("cached_tokens") is not None})
    means = {}
    for step in steps:
        rows = [row for row in ok if row["step"] == step and row.get("cached_tokens") is not None]
        means[step] = sum(row["cached_tokens"] for row in rows) / len(rows)
        print(f"   step {step}: cached_tokens mean={means[step]:8.1f} (n={len(rows)})")
    if len(means) >= 2 and means[max(means)] < means[min(means)]:
        rc = 1
        print("   FAIL: cached_tokens decreased across the workload")

    print()
    print("4) SELECTIVE SIGNALING")
    gateways = [row.get("gateway", {}) for row in state if row.get("gateway")]
    if not gateways:
        print("   (no gateway snapshots recorded; no state ingress was exercised)")
    else:
        latest = gateways[-1]
        stats = latest.get("stats", {})
        print(f"   frame_bytes={latest.get('frame_bytes')} "
              f"budget={latest.get('budget_bytes_per_sec')}B/s "
              f"pending={latest.get('pending')}")
        print("   received={received} superseded={superseded} "
              "redundant={suppressed_redundant} invalidations_sent={invalidations_sent} "
              "upserts_sent={upserts_sent}".format(**stats))

    print()
    print("SMOKE", "PASS" if rc == 0 else "FAIL")
    return rc


if __name__ == "__main__":
    sys.exit(main())
