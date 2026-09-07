"""Propagate the B02 publisher hook into vLLM EngineCore spawn children."""

from __future__ import annotations

import os
import sys


if (
    os.environ.get("DYN_B02_PREPUBLISH", "0") == "1"
    and os.environ.get("DYN_B02_WORKER", "0") == "1"
):
    try:
        from dynamo.vllm.b02_prepublish import install_b02_prepublish

        if install_b02_prepublish():
            print("B02 pre-publish child hook installed", file=sys.stderr)
    except Exception as exc:  # pragma: no cover - startup fallback
        print(f"B02 pre-publish child hook failed: {exc}", file=sys.stderr)
