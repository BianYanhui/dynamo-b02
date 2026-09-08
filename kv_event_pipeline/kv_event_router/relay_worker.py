# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""One process-sharded KV event raw ZMQ relay.

This process owns a disjoint subset of Worker ZMQ publishers.  It is launched
by the router only when ``--zmq-relay-shards`` is greater than one; keeping the
relay in a separate process avoids Python's GIL on the raw-event hot path.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal

import uvloop

from dynamo.runtime import DistributedRuntime, dynamo_worker
from dynamo.runtime.logging import configure_dynamo_logging

from kv_event_router.selective_signaling import SelectiveKVStateGateway
from kv_event_router.zmq_gateway import RawZmqSelectiveRelay


configure_dynamo_logging()
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KV event process-sharded raw ZMQ relay")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--worker-ids", required=True)
    parser.add_argument("--ports", required=True)
    parser.add_argument("--block-size", type=int, required=True)
    parser.add_argument("--scope", default="default")
    parser.add_argument("--budget-bytes-per-sec", type=int, required=True)
    parser.add_argument("--frame-bytes", type=int, required=True)
    parser.add_argument("--top-k", type=int, required=True)
    parser.add_argument("--theta-seconds", type=float, required=True)
    parser.add_argument("--byte-penalty", type=float, required=True)
    parser.add_argument("--recv-hwm", type=int, required=True)
    parser.add_argument("--max-messages-per-poll", type=int, required=True)
    parser.add_argument("--max-events-per-batch", type=int, required=True)
    parser.add_argument("--drain-interval-ms", type=float, required=True)
    parser.add_argument("--ready-file", required=True)
    parser.add_argument("--stats-file", required=True)
    return parser.parse_args()


def _write_snapshot(relay: RawZmqSelectiveRelay, path: str) -> None:
    temporary = f"{path}.{os.getpid()}.tmp"
    with open(temporary, "w") as handle:
        json.dump(relay.snapshot(), handle, sort_keys=True)
    os.replace(temporary, path)


async def _stats_loop(relay: RawZmqSelectiveRelay, path: str, stopped: asyncio.Event) -> None:
    while not stopped.is_set():
        _write_snapshot(relay, path)
        try:
            await asyncio.wait_for(stopped.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass


async def _run(runtime: DistributedRuntime, args: argparse.Namespace) -> None:
    worker_ids = [int(value) for value in args.worker_ids.split(",") if value]
    ports = [int(value) for value in args.ports.split(",") if value]
    if not worker_ids or len(worker_ids) != len(ports):
        raise ValueError("worker-ids and ports must be non-empty and have equal length")

    endpoint = runtime.endpoint(args.endpoint)
    gateway = SelectiveKVStateGateway(
        budget_bytes_per_sec=args.budget_bytes_per_sec,
        frame_bytes=args.frame_bytes,
        top_k=args.top_k,
        theta_seconds=args.theta_seconds,
        byte_penalty=args.byte_penalty,
    )
    relay = RawZmqSelectiveRelay(
        endpoint=endpoint,
        worker_ids=worker_ids,
        ports=ports,
        block_size=args.block_size,
        gateway=gateway,
        compatibility_scope=args.scope,
        recv_hwm=args.recv_hwm,
        max_messages_per_poll=args.max_messages_per_poll,
        max_events_per_batch=args.max_events_per_batch,
        drain_interval_ms=args.drain_interval_ms,
    )
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stopped.set)
        except NotImplementedError:
            pass

    relay.start()
    _write_snapshot(relay, args.stats_file)
    with open(args.ready_file, "w") as handle:
        handle.write(str(os.getpid()))
    logger.info("KV event sharded relay started (workers=%s, ports=%s)", worker_ids, ports)
    stats_task = asyncio.create_task(_stats_loop(relay, args.stats_file, stopped))
    try:
        await stopped.wait()
    finally:
        stats_task.cancel()
        try:
            await stats_task
        except asyncio.CancelledError:
            pass
        _write_snapshot(relay, args.stats_file)
        relay.shutdown()
        try:
            os.remove(args.ready_file)
        except FileNotFoundError:
            pass
        logger.info("KV event sharded relay stopped (workers=%s)", worker_ids)


@dynamo_worker()
async def worker(runtime: DistributedRuntime) -> None:
    await _run(runtime, parse_args())


def main() -> None:
    uvloop.run(worker())


if __name__ == "__main__":
    main()
