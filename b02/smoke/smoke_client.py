# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""B02 sketch-router smoke client: agentic workload with session headers.

Each workflow = one Dynamo session (`x-dynamo-session-id` = workflow_id),
so the router's Sketch-Dispatch policy can pin the workflow to its owning
instance. Measurement per request: streaming TTFT, e2e, cached_tokens.
Transient 5xx / connection errors are retried (stale discovery pools).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time

import aiohttp

WORDS = ("attention cache token inference dispatch workflow state router "
         "prefix latency throughput scheduling kv block worker instance "
         "orchestration cluster serving budget affinity eviction staleness "
         "semantic interface cost signal telemetry queue batch decode prefill").split()


def _words(n: int, rng: random.Random) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(n))


def gen_trace(seed: int, n_workflows: int, steps: int,
              ctx_tokens: int, append_tokens: int) -> list[dict]:
    rng = random.Random(seed)
    workflows = []
    for w in range(n_workflows):
        ctx = _words(int(ctx_tokens / 1.3), rng)
        appends: list[str] = []
        steps_list = []
        for s in range(steps):
            prompt = (f"Task context: {ctx} "
                      + " ".join(f"Tool result {i}: {a}" for i, a in enumerate(appends))
                      + f" Now continue the task briefly. Step {s+1} of {steps}.")
            steps_list.append({"step": s, "prompt": prompt,
                               "max_tokens": rng.randint(32, 48)})
            appends.append(_words(int(append_tokens / 1.3), rng))
        workflows.append({"workflow_id": f"wf_{w:05d}", "steps": steps_list,
                          "start_delay_ms": rng.randint(0, 800)})
    return workflows


async def _do_stream(session: aiohttp.ClientSession, url: str, body: dict,
                     headers: dict, rec: dict) -> None:
    t0 = time.perf_counter()
    ttft = None
    usage = None
    async with session.post(url, json=body, headers=headers,
                            timeout=aiohttp.ClientTimeout(total=180)) as resp:
        resp.raise_for_status()
        async for raw in resp.content:
            line = raw.decode("utf-8", "replace").strip()
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()
            if data == "[DONE]":
                break
            try:
                obj = json.loads(data)
            except json.JSONDecodeError:
                continue
            if obj.get("usage"):
                usage = obj["usage"]
            ch = obj.get("choices") or []
            if ch and (ch[0].get("delta") or {}).get("content"):
                if ttft is None:
                    ttft = (time.perf_counter() - t0) * 1000.0
    rec.update(ok=True,
               ttft_ms=round(ttft, 2) if ttft else None,
               e2e_ms=round((time.perf_counter() - t0) * 1000.0, 2),
               prompt_tokens=(usage or {}).get("prompt_tokens"),
               completion_tokens=(usage or {}).get("completion_tokens"),
               cached_tokens=((usage or {}).get("prompt_tokens_details") or {}).get("cached_tokens"))


async def run(url: str, model: str, trace: list[dict], out_path: str) -> dict:
    out: list[dict] = []

    async def one_workflow(session: aiohttp.ClientSession, wf: dict) -> None:
        await asyncio.sleep(wf["start_delay_ms"] / 1000.0)
        headers = {"x-dynamo-session-id": wf["workflow_id"]}
        for st in wf["steps"]:
            body = {
                "model": model,
                "messages": [{"role": "user", "content": st["prompt"]}],
                "max_tokens": st["max_tokens"],
                "temperature": 0.7,
                "stream": True,
                "stream_options": {"include_usage": True},
            }
            rec = {"workflow_id": wf["workflow_id"], "step": st["step"]}
            ok = False
            for attempt in range(3):
                try:
                    await _do_stream(session, url, body, headers, rec)
                    ok = True
                    break
                except aiohttp.ClientResponseError as e:
                    if e.status in (500, 502, 503, 504) and attempt < 2:
                        await asyncio.sleep(2.0 * (attempt + 1))
                        continue
                    rec.update(ok=False, error=f"HTTP {e.status}")
                    break
                except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                    if attempt < 2:
                        await asyncio.sleep(2.0 * (attempt + 1))
                        continue
                    rec.update(ok=False, error=repr(e)[:120])
                    break
                except Exception as e:  # noqa: BLE001
                    rec.update(ok=False, error=repr(e)[:120])
                    break
            if not ok and "error" not in rec:
                rec.update(ok=False, error="unreachable")
            out.append(rec)

    conn = aiohttp.TCPConnector(limit=16)
    async with aiohttp.ClientSession(connector=conn) as session:
        await asyncio.gather(*[one_workflow(session, wf) for wf in trace])
    out.sort(key=lambda r: (r["workflow_id"], r["step"]))
    with open(out_path, "w") as f:
        for r in out:
            f.write(json.dumps(r) + "\n")
    ok = [r for r in out if r.get("ok")]
    return {"requests": len(out), "ok": len(ok), "failed": len(out) - len(ok)}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000/v1/chat/completions")
    ap.add_argument("--model", default="qwen-b02")
    ap.add_argument("--n-workflows", type=int, default=8)
    ap.add_argument("--steps", type=int, default=8)
    ap.add_argument("--ctx-tokens", type=int, default=1024)
    ap.add_argument("--append-tokens", type=int, default=128)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="/tmp/b02_state_logs/smoke_results.jsonl")
    args = ap.parse_args()

    trace = gen_trace(args.seed, args.n_workflows, args.steps,
                      args.ctx_tokens, args.append_tokens)
    summary = await run(args.url, args.model, trace, args.out)
    print(json.dumps(summary))


if __name__ == "__main__":
    asyncio.run(main())
