# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""Raw vLLM ZMQ -> selective B02 gateway -> Dynamo KV event publisher.

The relay deliberately publishes the *same typed KV events* accepted by
``KvEventPublisher``.  This means the downstream Dynamo KvIndexer remains the
authority and does not need a B02-specific decoder.  When several stored
events for one prefix are still pending, the relay combines their block and
token lists before publishing, so a prefix chain is not broken by
supersession.  Once a block has been published, later extensions are kept as
new coverage and are not discarded.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import msgspec
import zmq

from dynamo.llm import KvEventPublisher

from b02_sketch_router.selective_signaling import KVStateUpdate, SelectiveKVStateGateway


@dataclass
class RelayStats:
    raw_messages: int = 0
    raw_events: int = 0
    decode_errors: int = 0
    accepted_events: int = 0
    superseded_events: int = 0
    duplicate_events: int = 0
    forwarded_events: int = 0
    forwarded_blocks: int = 0

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


def _signed_i64(value: Any) -> int:
    value = int(value)
    if value >= 2**63:
        value -= 2**64
    return value


def _block_lengths(token_ids: list[int], block_hashes: list[int], block_size: int) -> list[int]:
    lengths = []
    remaining = len(token_ids)
    for index in range(len(block_hashes)):
        lengths.append(max(0, min(block_size, remaining - index * block_size)))
    return lengths


class RawZmqSelectiveRelay:
    """Subscribe to vLLM raw ZMQ events and republish selected typed events."""

    def __init__(
        self,
        *,
        endpoint: Any,
        worker_ids: list[int],
        ports: list[int],
        block_size: int,
        gateway: SelectiveKVStateGateway,
        compatibility_scope: str = "default",
    ) -> None:
        if len(worker_ids) != len(ports):
            raise ValueError("worker_ids and ports must have the same length")
        self.endpoint = endpoint
        self.worker_ids = [int(value) for value in worker_ids]
        self.ports = [int(value) for value in ports]
        self.block_size = int(block_size)
        self.gateway = gateway
        self.compatibility_scope = compatibility_scope
        self.stats = RelayStats()
        self._publishers: dict[int, KvEventPublisher] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._version: defaultdict[int, int] = defaultdict(int)
        self._hash_root: dict[int, dict[int, str]] = defaultdict(dict)
        self._known_hashes: defaultdict[int, set[int]] = defaultdict(set)
        self._pending_raw: dict[tuple[int, str, str], dict[str, Any]] = {}

    def start(self) -> None:
        if self._thread is not None:
            return
        for worker_id in self.worker_ids:
            self._publishers[worker_id] = KvEventPublisher(
                endpoint=self.endpoint,
                worker_id=worker_id,
                kv_block_size=self.block_size,
                dp_rank=0,
                enable_local_indexer=False,
            )
        self._thread = threading.Thread(
            target=self._run,
            name="b02-zmq-selective-relay",
            daemon=True,
        )
        self._thread.start()

    def shutdown(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3.0)
            self._thread = None
        for publisher in self._publishers.values():
            publisher.shutdown()
        self._publishers.clear()

    def snapshot(self) -> dict[str, Any]:
        return {
            "workers": self.worker_ids,
            "ports": self.ports,
            "stats": self.stats.as_dict(),
            "gateway": self.gateway.snapshot(),
        }

    # ------------------------------------------------------------- raw input --
    def _run(self) -> None:
        context = zmq.Context()
        poller = zmq.Poller()
        sockets: dict[Any, int] = {}
        for worker_id, port in zip(self.worker_ids, self.ports):
            socket = context.socket(zmq.SUB)
            socket.setsockopt(zmq.SUBSCRIBE, b"")
            socket.setsockopt(zmq.RCVHWM, 200_000)
            socket.connect(f"tcp://127.0.0.1:{port}")
            poller.register(socket, zmq.POLLIN)
            sockets[socket] = worker_id

        try:
            while not self._stop.is_set():
                for socket, _ in poller.poll(10):
                    worker_id = sockets[socket]
                    while True:
                        try:
                            parts = socket.recv_multipart(flags=zmq.NOBLOCK)
                        except zmq.Again:
                            break
                        self.stats.raw_messages += 1
                        try:
                            payload = msgspec.msgpack.decode(parts[-1])
                            raw_events = payload[1]
                        except Exception:
                            self.stats.decode_errors += 1
                            continue
                        for raw_event in raw_events:
                            self.stats.raw_events += 1
                            self._ingest_raw(worker_id, raw_event)
                        self._drain()
                self._drain()
        finally:
            for socket in sockets:
                socket.close(0)
            context.term()

    def _next_version(self, worker_id: int) -> int:
        self._version[worker_id] += 1
        return self._version[worker_id]

    def _stored_event(self, raw: dict[str, Any]) -> tuple[dict[str, Any], list[int]]:
        hashes = [_signed_i64(value) for value in raw.get("block_hashes", [])]
        tokens = [int(value) for value in raw.get("token_ids", [])]
        block_size = int(raw.get("block_size", self.block_size))
        typed = {
            "type": "stored",
            "token_ids": tokens,
            "num_block_tokens": _block_lengths(tokens, hashes, block_size),
            "block_hashes": hashes,
            "parent_hash": (
                _signed_i64(raw["parent_block_hash"])
                if raw.get("parent_block_hash") is not None else None
            ),
        }
        if raw.get("lora_name") is not None:
            typed["lora_name"] = raw["lora_name"]
        return typed, hashes

    def _root_for(self, worker_id: int, hashes: list[int], parent: int | None) -> str:
        if parent is not None:
            return self._hash_root[worker_id].get(parent, str(parent))
        if not hashes:
            return "empty"
        return self._hash_root[worker_id].get(hashes[0], str(hashes[0]))

    @staticmethod
    def _merge_stored(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
        hashes = list(old["block_hashes"])
        existing = set(hashes)
        tokens = list(old["token_ids"])
        lengths = list(old["num_block_tokens"])
        offset = 0
        for block_hash, token_count in zip(new["block_hashes"], new["num_block_tokens"]):
            current_offset = offset
            offset += token_count
            if block_hash in existing:
                continue
            existing.add(block_hash)
            hashes.append(block_hash)
            tokens.extend(new["token_ids"][current_offset:current_offset + token_count])
            lengths.append(token_count)
        merged = dict(old)
        merged["block_hashes"] = hashes
        merged["token_ids"] = tokens
        merged["num_block_tokens"] = lengths
        return merged

    def _ingest_raw(self, worker_id: int, raw: dict[str, Any]) -> None:
        kind = str(raw.get("type", "")).lower()
        version = self._next_version(worker_id)
        if kind == "blockstored":
            typed, hashes = self._stored_event(raw)
            if not hashes:
                return
            parent = typed["parent_hash"]
            root = self._root_for(worker_id, hashes, parent)
            for block_hash in hashes:
                self._hash_root[worker_id][block_hash] = root
            key = (worker_id, self.compatibility_scope, root)
            if all(block_hash in self._known_hashes[worker_id] for block_hash in hashes):
                self.stats.duplicate_events += 1
                return
            pending = self._pending_raw.get(key)
            if pending is not None:
                typed = self._merge_stored(pending, typed)
            update = KVStateUpdate(
                prefix_hash=root,
                owner_instance=worker_id,
                compatibility_scope=self.compatibility_scope,
                coverage_tokens=len(typed["token_ids"]),
                version=version,
                generated_at_ns=time.time_ns(),
                kind="upsert",
                frame_bytes=self.gateway.frame_bytes,
                raw_event=typed,
            )
            if self.gateway.ingest(update):
                self._pending_raw[key] = typed
                self.stats.accepted_events += 1
            else:
                self.stats.superseded_events += 1
            return

        if kind in {"blockremoved", "allblockscleared"}:
            if kind == "allblockscleared":
                hashes = list(self._known_hashes[worker_id])
            else:
                hashes = [_signed_i64(value) for value in raw.get("block_hashes", [])]
            if not hashes:
                return
            typed = {"type": "removed", "block_hashes": hashes}
            root = self._root_for(worker_id, hashes, None)
            update = KVStateUpdate(
                prefix_hash=root,
                owner_instance=worker_id,
                compatibility_scope=self.compatibility_scope,
                coverage_tokens=0,
                version=version,
                generated_at_ns=time.time_ns(),
                kind="invalidate",
                frame_bytes=self.gateway.frame_bytes,
                raw_event=typed,
            )
            if self.gateway.ingest(update):
                self.stats.accepted_events += 1
            else:
                self.stats.superseded_events += 1
            for block_hash in hashes:
                self._known_hashes[worker_id].discard(block_hash)

    def _drain(self) -> None:
        for frame in self.gateway.select(top_k=64):
            raw = frame.raw_event
            if not raw:
                continue
            publisher = self._publishers.get(frame.owner_instance)
            if publisher is None:
                continue
            publisher.publish_batch([raw])
            self.stats.forwarded_events += 1
            if raw["type"] == "stored":
                self.stats.forwarded_blocks += len(raw["block_hashes"])
                self._known_hashes[frame.owner_instance].update(raw["block_hashes"])
            else:
                for block_hash in raw["block_hashes"]:
                    self._known_hashes[frame.owner_instance].discard(block_hash)
            self._pending_raw.pop(frame.key, None)
