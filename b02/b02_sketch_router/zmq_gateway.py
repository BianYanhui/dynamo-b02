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
from typing import Any, Mapping

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


def _normalized_i64_list(values: Any) -> list[int]:
    """Normalize vLLM's hash list without copying the common signed path."""
    if not values:
        return []
    if isinstance(values, list) and all(
        isinstance(value, int) and value < 2**63 for value in values
    ):
        return values
    return [_signed_i64(value) for value in values]


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
        recv_hwm: int = 200_000,
        max_messages_per_poll: int = 512,
        max_events_per_batch: int = 4096,
        drain_interval_ms: float = 2.0,
    ) -> None:
        if len(worker_ids) != len(ports):
            raise ValueError("worker_ids and ports must have the same length")
        self.endpoint = endpoint
        self.worker_ids = [int(value) for value in worker_ids]
        self.ports = [int(value) for value in ports]
        self.block_size = int(block_size)
        self.gateway = gateway
        self.compatibility_scope = compatibility_scope
        if recv_hwm <= 0 or max_messages_per_poll <= 0 or max_events_per_batch <= 0:
            raise ValueError("relay batch and HWM settings must be positive")
        if drain_interval_ms <= 0:
            raise ValueError("drain_interval_ms must be positive")
        self.recv_hwm = int(recv_hwm)
        self.max_messages_per_poll = int(max_messages_per_poll)
        self.max_events_per_batch = int(max_events_per_batch)
        self.drain_interval_ns = int(float(drain_interval_ms) * 1_000_000)
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
            "recv_hwm": self.recv_hwm,
            "max_messages_per_poll": self.max_messages_per_poll,
            "max_events_per_batch": self.max_events_per_batch,
            "drain_interval_ms": self.drain_interval_ns / 1_000_000,
            "stats": self.stats.as_dict(),
            "gateway": self.gateway.snapshot(),
        }

    # ------------------------------------------------------------- raw input --
    def _run(self) -> None:
        context = zmq.Context()
        poller = zmq.Poller()
        decoder = msgspec.msgpack.Decoder()
        sockets: dict[Any, int] = {}
        for worker_id, port in zip(self.worker_ids, self.ports):
            socket = context.socket(zmq.SUB)
            socket.setsockopt(zmq.SUBSCRIBE, b"")
            socket.setsockopt(zmq.RCVHWM, self.recv_hwm)
            socket.connect(f"tcp://127.0.0.1:{port}")
            poller.register(socket, zmq.POLLIN)
            sockets[socket] = worker_id

        try:
            messages_since_drain = 0
            events_since_drain = 0
            last_drain_ns = time.monotonic_ns()
            while not self._stop.is_set():
                for socket, _ in poller.poll(10):
                    worker_id = sockets[socket]
                    socket_messages = 0
                    while socket_messages < self.max_messages_per_poll:
                        try:
                            parts = socket.recv_multipart(flags=zmq.NOBLOCK)
                        except zmq.Again:
                            break
                        socket_messages += 1
                        messages_since_drain += 1
                        self.stats.raw_messages += 1
                        try:
                            payload = decoder.decode(parts[-1])
                            raw_events = payload[1]
                        except Exception:
                            self.stats.decode_errors += 1
                            continue
                        event_timestamp_ns = time.time_ns()
                        for raw_event in raw_events:
                            self.stats.raw_events += 1
                            self._ingest_raw(
                                worker_id, raw_event, now_ns=event_timestamp_ns
                            )
                        events_since_drain += len(raw_events)
                        now = time.monotonic_ns()
                        if (
                            events_since_drain >= self.max_events_per_batch
                            or now - last_drain_ns >= self.drain_interval_ns
                        ):
                            self._drain()
                            messages_since_drain = 0
                            events_since_drain = 0
                            last_drain_ns = now
                now = time.monotonic_ns()
                if (
                    messages_since_drain
                    and now - last_drain_ns >= self.drain_interval_ns
                ):
                    self._drain()
                    messages_since_drain = 0
                    events_since_drain = 0
                    last_drain_ns = now
            self._drain()
        finally:
            for socket in sockets:
                socket.close(0)
            context.term()

    def _next_version(self, worker_id: int) -> int:
        self._version[worker_id] += 1
        return self._version[worker_id]

    def _stored_event(self, raw: dict[str, Any]) -> tuple[dict[str, Any], list[int]]:
        hashes = _normalized_i64_list(raw.get("block_hashes"))
        raw_tokens = raw.get("token_ids") or []
        tokens = raw_tokens if isinstance(raw_tokens, list) else list(raw_tokens)
        block_size = int(raw.get("block_size", self.block_size))
        if len(hashes) == 1:
            block_lengths = [max(0, min(block_size, len(tokens)))]
        else:
            block_lengths = _block_lengths(tokens, hashes, block_size)
        typed = {
            "type": "stored",
            "token_ids": tokens,
            "num_block_tokens": block_lengths,
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
        old_hashes = old["block_hashes"]
        new_hashes = new["block_hashes"]

        # vLLM normally reports a growing prefix as one or more blocks whose
        # parent is the last block already held by the pending frame.  Keep
        # this hot path incremental: copying the whole prefix on every block
        # turns a long chain into quadratic work before it is published.
        if old_hashes and new.get("parent_hash") == old_hashes[-1]:
            old_hashes.extend(new_hashes)
            old["token_ids"].extend(new["token_ids"])
            old["num_block_tokens"].extend(new["num_block_tokens"])
            return old

        # Conservative fallback for reordered or overlapping reports.  These
        # are uncommon but must retain the original de-duplication semantics.
        hashes = list(old_hashes)
        existing = set(hashes)
        tokens = list(old["token_ids"])
        lengths = list(old["num_block_tokens"])
        offset = 0
        for block_hash, token_count in zip(new_hashes, new["num_block_tokens"]):
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

    def _ingest_raw(
        self, worker_id: int, raw: dict[str, Any], *, now_ns: int | None = None
    ) -> None:
        kind = str(raw.get("type", "")).lower()
        version = self._next_version(worker_id)
        if now_ns is None:
            now_ns = time.time_ns()
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
                generated_at_ns=now_ns,
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
                hashes = _normalized_i64_list(raw.get("block_hashes"))
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
                generated_at_ns=now_ns,
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
        if self.gateway.pending_count == 0:
            return
        batches: defaultdict[int, list[tuple[KVStateUpdate, Mapping[str, Any]]]] = defaultdict(list)
        for frame in self.gateway.select(top_k=64):
            raw = frame.raw_event
            if not raw:
                continue
            publisher = self._publishers.get(frame.owner_instance)
            if publisher is None:
                continue
            batches[frame.owner_instance].append((frame, raw))

        for owner, items in batches.items():
            publisher = self._publishers.get(owner)
            if publisher is None:
                continue
            publisher.publish_batch([raw for _, raw in items])
            self.stats.forwarded_events += len(items)
            for frame, raw in items:
                self._pending_raw.pop(frame.key, None)
                if raw["type"] == "stored":
                    self.stats.forwarded_blocks += len(raw["block_hashes"])
                    self._known_hashes[frame.owner_instance].update(raw["block_hashes"])
                else:
                    for block_hash in raw["block_hashes"]:
                        self._known_hashes[frame.owner_instance].discard(block_hash)
