"""Propagate the KV event publisher hook into vLLM EngineCore spawn children."""

from __future__ import annotations

import os
import sys


if (
    os.environ.get("DYN_KV_EVENT_PREPUBLISH", "0") == "1"
    and os.environ.get("DYN_KV_EVENT_WORKER", "0") == "1"
):
    try:
        from dynamo.vllm.kv_event_prepublish import install_kv_event_prepublish

        if install_kv_event_prepublish():
            print("KV event pre-publish child hook installed", file=sys.stderr)
    except Exception as exc:  # pragma: no cover - startup fallback
        print(f"KV event pre-publish child hook failed: {exc}", file=sys.stderr)
