# B02v1 — Selective KV-State Signaling in Dynamo

> Fork: `BianYanhui/dynamo-b02` · branch: `b02-fusion` · Dynamo 1.4.2

## Design alignment

B02v1 follows the latest paper, *Selective KV-State Signaling for Cache-Aware
Edge LLM Dispatch*. The main mechanism is a gateway on the Instance–Dispatcher
control path, not request-session affinity:

```text
Instance reporter ──semantic KV updates──> selective gateway ──hints──> dispatcher
Client ──HTTP──> Frontend ──tokens──> B02 router ──native KvRouter──> vLLM workers
```

The gateway applies four rules:

1. **Supersession aggregation** keeps only the newest unsent update for an
   owner/prefix. Repeated extensions therefore become one frame carrying the
   final coverage.
2. **Invalidation priority** lets removal/restart tombstones bypass ordinary
   upserts, reducing stale-positive exposure.
3. **Cross-instance redundancy suppression** drops an equal-or-weaker replica
   when a compatible copy is already visible elsewhere, while preserving new
   coverage.
4. **Freshness-aware budget admission** ranks candidates using
   `exp(-(age + backlog_delay) / theta) * coverage - lambda * bytes` under a
   token-bucket byte budget. The default frame is 64 B, `theta=30 s`, and
   `lambda=16`, matching the paper's evaluation parameters.

The dispatcher treats a frame as a hint. `OwnerStateRegistry` validates owner,
compatibility scope, version, physical residency, and coverage before reuse;
failure is a normal prefill fallback and never an unsafe reuse.

## What is live in this fork

- `b02_sketch_router/selective_signaling.py` contains the gateway, event
  adapter, token-bucket admission, and owner-side validation registry.
- `b02_sketch_router/__main__.py` keeps Dynamo's native `KvRouter` request path,
  disables the old workflow pin by default, and exposes the internal endpoint
  `dynamo.b02_sketch_router.kv_state` for reporter events.
- `state_updates.jsonl` records gateway pending/visible state and signaling
  counters. `status` exposes the same snapshot.
- `--legacy-affinity-pin` is an explicit v0 A/B switch; it is off by default.

The Python gateway is the paper-aligned control-plane prototype. Dynamo's
native worker event plane is still the final production indexer path; wiring a
deployment's reporter to the `kv_state` ingress is required to feed live
semantic metadata into the gateway. Raw `BlockStored`/`BlockRemoved` events
are accepted conservatively, but reporters should provide a stable
`prefix_hash`, `owner_instance`, `compatibility_scope`, `coverage_tokens`, and
monotonic `version`.

For high raw-event rates, use `--zmq-relay-shards N` with one shard per
Worker/DP source group. Each shard runs in a separate process so raw-event
receive, decode, merge, and publish can use separate CPU cores. The default
single-process mode preserves global cross-instance redundancy suppression;
process-sharded mode keeps owner-side validation safety but performs
redundancy suppression within each shard. Use sharding only when raw ingress
is the limiting factor and validate the resulting downstream hint volume.

For the original ZMQ-ingress experiment, enable the Worker-local pre-publish
path:

```bash
DYN_B02_PREPUBLISH=1 KV_EVENTS=1 GPU_MEM_UTIL=0.60 bash /home/byh/Dynamo/dynamo/b02/smoke/cluster_up.sh
```

This wraps vLLM's publisher before its ZMQ socket. It merges adjacent local
`BlockStored` extensions, suppresses already-published local duplicates,
coalesces scheduler batches, and gives clears/removals priority over pending
positive updates. It deliberately does not suppress replicas across Workers;
that decision requires the downstream B02 gateway's global visibility. Each
Worker logs `B02 pre-publish summary` during shutdown with input/output event
and byte counts.

The selector has an optional Rust backend. `auto` is the default: it uses the
Rust extension when installed and falls back to the Python selector otherwise.
Use `DYN_B02_PREPUBLISH_BACKEND=python` to force the fallback or `rust` to fail
fast when the extension is unavailable. Build the extension in the worker
environment with:

```bash
cd /home/byh/Dynamo/dynamo/b02/rust_selector
export PATH=/home/byh/.cargo/bin:/home/byh/.local/bin:$PATH
RUSTUP_TOOLCHAIN=1.96.1 maturin build --release --manifest-path Cargo.toml \
  --interpreter /home/byh/Dynamo/.venv-dynamo/bin/python
/home/byh/Dynamo/.venv-dynamo/bin/python -m pip install --force-reinstall \
  target/wheels/b02_rust_selector-*.whl
```

The Rust backend preserves the Python event-object API and keeps Python as a
runtime fallback. It moves event-kind dispatch, hash tracking, duplicate
suppression, invalidation handling, and adjacent `BlockStored` merging into a
compiled selector; ZMQ publishing remains in the existing publisher wrapper.

## Tests

On yhs1:

```bash
cd /home/byh/Dynamo/dynamo
source /home/byh/Dynamo/.venv-dynamo/bin/activate
PYTHONPATH=b02 python b02/tests/test_selective_signaling.py
PYTHONPATH=b02 python b02/tests/test_zmq_gateway.py
PYTHONPATH=b02 python b02/tests/test_state_views.py  # legacy builder regression
PYTHONPATH=b02 python b02/tests/test_prepublish.py
```

To compare the same native-shaped trace across all paths:

```bash
PYTHONPATH=b02 python b02/smoke/benchmark_native_vs_b02_stress.py \
  --trace /tmp/native_event_trace_highconc.jsonl \
  --modes native,b02-python,b02-rust \
  --target-events-per-s 115000,150000 --cycles 1000 \
  --out /tmp/native_vs_python_rust_b02.json
```

For the full smoke test, start the existing four-worker cluster and run:

```bash
bash /home/byh/Dynamo/dynamo/b02/smoke/run_smoke.sh
```

The default smoke path now measures native KV routing plus the gateway
control-plane counters. Use `--legacy-affinity-pin` in the router command only
when reproducing the old v0 comparison.
