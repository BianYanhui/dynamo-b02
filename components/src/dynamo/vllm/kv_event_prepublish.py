"""Worker-local KV event filtering before the vLLM ZMQ publisher.

The external KV event relay is useful for compatibility experiments, but it sees
events only after vLLM has already put them on a ZMQ socket.  This module wraps
vLLM's ``ZmqEventPublisher`` instead.  The wrapper performs only local,
lossless reductions by default:

* merge adjacent BlockStored extensions for one prefix;
* suppress a block that this worker has already published;
* coalesce several scheduler calls into one EventBatch; and
* let removals/clears invalidate pending positive updates.

Cross-worker replica suppression and byte-budget admission stay in the
downstream KV event gateway because a Worker-local publisher cannot safely know
what another Worker has already made visible.
"""

from __future__ import annotations

import copy
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


logger = logging.getLogger(__name__)


def _event_kind(event: Any) -> str:
    return type(event).__name__.lower()


def _block_hashes(event: Any) -> list[Any]:
    values = getattr(event, "block_hashes", None)
    if values is None:
        return []
    return list(values)


def _copy_event(event: Any, **updates: Any) -> Any:
    clone = copy.copy(event)
    for name, value in updates.items():
        setattr(clone, name, value)
    return clone


@dataclass
class PrePublishStats:
    input_batches: int = 0
    input_events: int = 0
    output_batches: int = 0
    output_events: int = 0
    merged_events: int = 0
    duplicate_events: int = 0
    invalidation_events: int = 0
    input_bytes: int = 0
    output_bytes: int = 0

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


class LocalKVEventSelector:
    """Lossless local event reduction for one vLLM data-parallel publisher."""

    def __init__(
        self,
        *,
        max_pending_events: int = 4096,
        backend: str | None = None,
    ) -> None:
        if max_pending_events <= 0:
            raise ValueError("max_pending_events must be positive")
        self.max_pending_events = int(max_pending_events)
        self.stats = PrePublishStats()
        requested_backend = backend or os.environ.get(
            "DYN_KV_EVENT_PREPUBLISH_BACKEND", "auto"
        )
        if requested_backend not in {"auto", "python", "rust"}:
            raise ValueError(
                "DYN_KV_EVENT_PREPUBLISH_BACKEND must be one of auto, python, rust"
            )
        self._rust_selector: Any | None = None
        self.backend = "python"
        if requested_backend in {"auto", "rust"}:
            try:
                from kv_event_selector import RustKVEventSelector
            except ImportError:
                if requested_backend == "rust":
                    raise RuntimeError(
                        "Rust KV event backend requested but kv_event_selector is not installed"
                    ) from None
            else:
                self._rust_selector = RustKVEventSelector(max_pending_events)
                self.backend = "rust"
        self._pending: list[Any] = []
        self._known_hashes: set[Any] = set()
        self._removed_hashes: set[Any] = set()

    def _sync_rust_stats(self) -> None:
        if self._rust_selector is None:
            return
        values = self._rust_selector.stats()
        for name in (
            "input_batches",
            "input_events",
            "output_batches",
            "output_events",
            "merged_events",
            "duplicate_events",
            "invalidation_events",
        ):
            setattr(self.stats, name, int(values.get(name, 0)))

    def record_input_batch(self) -> None:
        if self._rust_selector is not None:
            self._rust_selector.record_input_batch()
            self._sync_rust_stats()
        else:
            self.stats.input_batches += 1

    @property
    def pending_count(self) -> int:
        if self._rust_selector is not None:
            return int(self._rust_selector.pending_count())
        return len(self._pending)

    def ingest(self, events: list[Any]) -> None:
        if self._rust_selector is not None:
            self._rust_selector.ingest(events)
            self._sync_rust_stats()
            return
        self.stats.input_events += len(events)
        for event in events:
            kind = _event_kind(event)
            if kind == "blockstored":
                self._ingest_stored(event)
            elif kind == "blockremoved":
                self._ingest_removed(event)
            elif kind == "allblockscleared":
                # A clear supersedes all positive updates that have not yet
                # crossed the ZMQ boundary.
                self._pending.clear()
                self._known_hashes.clear()
                self._removed_hashes.clear()
                self._pending.append(event)
                self.stats.invalidation_events += 1
            else:
                self._pending.append(event)

    def flush(self) -> list[Any]:
        if self._rust_selector is not None:
            events = list(self._rust_selector.flush())
            self._sync_rust_stats()
            return events
        events = self._pending
        self._pending = []
        self.stats.output_events += len(events)
        if events:
            self.stats.output_batches += 1
        return events

    def _ingest_stored(self, event: Any) -> None:
        hashes = _block_hashes(event)
        if hashes and all(block_hash in self._known_hashes for block_hash in hashes):
            self.stats.duplicate_events += 1
            return

        # A partial duplicate is retained unchanged.  Trimming token_ids for
        # only some blocks is version-sensitive in vLLM and can corrupt the
        # block/token alignment.  The common one-block duplicate path above is
        # the hot path, while safety wins for mixed batches.
        self._removed_hashes.difference_update(hashes)
        if self._pending and _event_kind(self._pending[-1]) == "blockstored":
            previous = self._pending[-1]
            previous_hashes = _block_hashes(previous)
            parent = getattr(event, "parent_block_hash", None)
            if (
                previous_hashes
                and len(hashes) == 1
                and parent == previous_hashes[-1]
                and getattr(previous, "block_size", None)
                == getattr(event, "block_size", None)
                and getattr(previous, "lora_name", None)
                == getattr(event, "lora_name", None)
            ):
                merged = _copy_event(
                    previous,
                    block_hashes=previous_hashes + hashes,
                    token_ids=list(getattr(previous, "token_ids", []))
                    + list(getattr(event, "token_ids", [])),
                )
                previous_extra = getattr(previous, "extra_keys", None)
                event_extra = getattr(event, "extra_keys", None)
                if previous_extra is not None or event_extra is not None:
                    setattr(
                        merged,
                        "extra_keys",
                        list(previous_extra or []) + list(event_extra or []),
                    )
                self._pending[-1] = merged
                self.stats.merged_events += 1
                self._known_hashes.update(hashes)
                return

        self._pending.append(event)
        self._known_hashes.update(hashes)

    def _ingest_removed(self, event: Any) -> None:
        hashes = _block_hashes(event)
        if hashes:
            fresh = [block_hash for block_hash in hashes if block_hash not in self._removed_hashes]
            if not fresh:
                self.stats.duplicate_events += 1
                return
            if len(fresh) != len(hashes):
                event = _copy_event(event, block_hashes=fresh)
            self._removed_hashes.update(fresh)
            self._known_hashes.difference_update(fresh)
        self._pending.append(event)
        self.stats.invalidation_events += 1

    def should_flush(self) -> bool:
        if self._rust_selector is not None:
            return bool(self._rust_selector.should_flush())
        return len(self._pending) >= self.max_pending_events


class KVEventPrePublishPublisher:
    """Drop-in vLLM EventPublisher that filters before ZmqEventPublisher."""

    def __init__(
        self,
        *,
        delegate_ctor: Callable[..., Any],
        data_parallel_rank: int,
        endpoint: str = "tcp://*:5557",
        replay_endpoint: str | None = None,
        buffer_steps: int = 10_000,
        hwm: int = 100_000,
        max_queue_size: int = 100_000,
        topic: str = "",
        **kwargs: Any,
    ) -> None:
        del kwargs
        self._delegate = delegate_ctor(
            data_parallel_rank=data_parallel_rank,
            endpoint=endpoint,
            replay_endpoint=replay_endpoint,
            buffer_steps=buffer_steps,
            hwm=hwm,
            max_queue_size=max_queue_size,
            topic=topic,
        )
        self._rank = data_parallel_rank
        self._selector = LocalKVEventSelector(
            max_pending_events=int(os.environ.get("DYN_KV_EVENT_PREPUBLISH_MAX_EVENTS", "4096"))
        )
        self._flush_interval_ns = int(
            float(os.environ.get("DYN_KV_EVENT_PREPUBLISH_FLUSH_MS", "2.0")) * 1_000_000
        )
        if self._flush_interval_ns <= 0:
            raise ValueError("DYN_KV_EVENT_PREPUBLISH_FLUSH_MS must be positive")
        self._last_flush_ns = time.monotonic_ns()
        self._pending_ts: float | None = None
        self._lock = threading.Lock()
        self._encoder: Any | None = None
        self._event_batch_type: Any | None = None

    @property
    def stats(self) -> PrePublishStats:
        return self._selector.stats

    def _wire_size(self, batch: Any) -> int:
        try:
            if self._encoder is None:
                import msgspec

                self._encoder = msgspec.msgpack.Encoder()
            return len(self._encoder.encode(batch))
        except Exception:
            return 0

    def _make_batch(self, events: list[Any], timestamp: float) -> Any:
        if self._event_batch_type is None:
            from vllm.distributed.kv_events import EventBatch

            self._event_batch_type = EventBatch
        return self._event_batch_type(
            ts=timestamp,
            events=events,
            data_parallel_rank=self._rank,
        )

    def _flush_locked(self, timestamp: float | None = None) -> None:
        events = self._selector.flush()
        if not events:
            self._pending_ts = None
            return
        batch = self._make_batch(
            events,
            self._pending_ts if self._pending_ts is not None else timestamp or time.time(),
        )
        self._selector.stats.output_bytes += self._wire_size(batch)
        self._delegate.publish(batch)
        self._pending_ts = None
        self._last_flush_ns = time.monotonic_ns()

    def publish(self, batch: Any) -> None:
        with self._lock:
            self._selector.record_input_batch()
            self._selector.stats.input_bytes += self._wire_size(batch)
            if self._pending_ts is None:
                self._pending_ts = float(getattr(batch, "ts", time.time()))
            self._selector.ingest(list(getattr(batch, "events", [])))
            now = time.monotonic_ns()
            if (
                self._selector.should_flush()
                or now - self._last_flush_ns >= self._flush_interval_ns
            ):
                self._flush_locked(timestamp=getattr(batch, "ts", None))

    def shutdown(self) -> None:
        with self._lock:
            self._flush_locked()
            logger.info("KV event pre-publish summary: %s", self._selector.stats.as_dict())
        self._delegate.shutdown()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "rank": self._rank,
                "pending_events": self._selector.pending_count,
                "flush_interval_ms": self._flush_interval_ns / 1_000_000,
                "stats": self._selector.stats.as_dict(),
            }


def install_kv_event_prepublish() -> bool:
    """Install the wrapper in vLLM's publisher registry when requested.

    The install is process-local and idempotent.  Keeping it behind an env
    switch makes the baseline path exactly vLLM's native publisher.
    """

    if os.environ.get("DYN_KV_EVENT_PREPUBLISH", "0") != "1":
        return False
    try:
        from vllm.distributed.kv_events import EventPublisherFactory
    except Exception:
        logger.exception("KV event pre-publish requested but vLLM publisher import failed")
        return False

    registry = EventPublisherFactory._registry
    current = registry.get("zmq")
    if current is None:
        logger.warning("KV event pre-publish requested but vLLM has no ZMQ publisher")
        return False
    if getattr(current, "_kv_event_prepublish", False):
        return True

    def constructor(**kwargs: Any) -> KVEventPrePublishPublisher:
        return KVEventPrePublishPublisher(delegate_ctor=current, **kwargs)

    setattr(constructor, "_kv_event_prepublish", True)
    registry["zmq"] = constructor
    logger.info(
        "KV event worker-local pre-publish enabled (flush_ms=%s, max_events=%s)",
        os.environ.get("DYN_KV_EVENT_PREPUBLISH_FLUSH_MS", "2.0"),
        os.environ.get("DYN_KV_EVENT_PREPUBLISH_MAX_EVENTS", "4096"),
    )
    return True
