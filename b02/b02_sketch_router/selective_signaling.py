# SPDX-FileCopyrightText: Copyright (c) 2026 BianYanhui
# SPDX-License-Identifier: Apache-2.0

"""Selective KV-state signaling used by the B02 gateway.

This module is deliberately independent of the request router.  The router
may use the state as a hint, but the owner-side registry below is the final
authority for reuse.  The implementation follows the paper's three semantic
reductions:

* keep only the newest unsent update for an owner/prefix;
* let invalidations/tombstones bypass ordinary upserts;
* suppress a replica when an equal-or-better compatible copy is already
  visible at another owner.

The wire model is intentionally small and fixed-size by default (64 bytes in
the paper's evaluation).  ``ingest_event`` accepts the normalized metadata
produced by a reporter.  A raw Dynamo ``BlockStored`` event is also accepted
as a conservative fallback; without an explicit prefix hash, the parent hash
or block hash becomes the prefix identity.
"""

from __future__ import annotations

import math
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


DEFAULT_FRAME_BYTES = 64
DEFAULT_THETA_SECONDS = 30.0
DEFAULT_BYTE_PENALTY = 16.0


@dataclass(frozen=True)
class KVStateUpdate:
    """One logical prefix-state update at the gateway boundary."""

    prefix_hash: str
    owner_instance: int
    compatibility_scope: str = "default"
    coverage_tokens: int = 0
    version: int = 0
    generated_at_ns: int = field(default_factory=time.time_ns)
    kind: str = "upsert"  # upsert | invalidate
    frame_bytes: int = DEFAULT_FRAME_BYTES
    raw_event: Mapping[str, Any] | None = None

    @property
    def key(self) -> tuple[int, str, str]:
        return self.owner_instance, self.compatibility_scope, self.prefix_hash

    @property
    def prefix_key(self) -> tuple[str, str]:
        return self.compatibility_scope, self.prefix_hash

    @property
    def is_invalidation(self) -> bool:
        return self.kind == "invalidate"

    def supersedes(self, other: "KVStateUpdate") -> bool:
        return (self.version, self.coverage_tokens, self.generated_at_ns) > (
            other.version, other.coverage_tokens, other.generated_at_ns
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "prefix_hash": self.prefix_hash,
            "owner_instance": self.owner_instance,
            "compatibility_scope": self.compatibility_scope,
            "coverage_tokens": self.coverage_tokens,
            "version": self.version,
            "generated_at_ns": self.generated_at_ns,
            "frame_bytes": self.frame_bytes,
        }


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    reason: str
    actual_version: int | None = None
    actual_coverage_tokens: int | None = None


@dataclass
class SignalStats:
    received: int = 0
    upserts_received: int = 0
    invalidations_received: int = 0
    superseded: int = 0
    suppressed_redundant: int = 0
    invalidations_sent: int = 0
    upserts_sent: int = 0
    budget_deferred: int = 0
    bytes_sent: int = 0
    validation_attempts: int = 0
    validation_fallbacks: int = 0

    def as_dict(self) -> dict[str, int]:
        return self.__dict__.copy()


class SelectiveKVStateGateway:
    """Budgeted, freshness-aware logical KV-state gateway.

    ``budget_bytes_per_sec`` is a token-bucket budget.  ``select`` is called
    by the gateway loop (or an RPC handler) to obtain the next batch.  The
    method is deterministic for a supplied ``now_ns`` and ``budget_bytes``,
    which makes the reduction rules easy to test without a running cluster.
    """

    def __init__(
        self,
        *,
        budget_bytes_per_sec: int = 64 * 1024,
        frame_bytes: int = DEFAULT_FRAME_BYTES,
        top_k: int = 64,
        theta_seconds: float = DEFAULT_THETA_SECONDS,
        byte_penalty: float = DEFAULT_BYTE_PENALTY,
    ) -> None:
        if budget_bytes_per_sec <= 0 or frame_bytes <= 0 or top_k <= 0:
            raise ValueError("budget, frame_bytes and top_k must be positive")
        self.budget_bytes_per_sec = int(budget_bytes_per_sec)
        self.frame_bytes = int(frame_bytes)
        self.top_k = int(top_k)
        self.theta_seconds = float(theta_seconds)
        self.byte_penalty = float(byte_penalty)
        self._pending_upserts: dict[tuple[int, str, str], KVStateUpdate] = {}
        self._pending_invalidations: dict[tuple[int, str, str], KVStateUpdate] = {}
        self._visible: dict[tuple[str, str], dict[int, KVStateUpdate]] = defaultdict(dict)
        self._tokens = float(self.budget_bytes_per_sec)
        self._last_refill_ns: int | None = None
        self.stats = SignalStats()

    # ------------------------------------------------------------- ingress --
    def ingest(self, update: KVStateUpdate) -> bool:
        """Admit one semantic update into the gateway's pending state.

        Returns whether the update remains eligible for transmission.  A
        ``False`` result means it was superseded, stale, or redundant.
        """
        if update.frame_bytes != self.frame_bytes:
            update = KVStateUpdate(
                prefix_hash=update.prefix_hash,
                owner_instance=update.owner_instance,
                compatibility_scope=update.compatibility_scope,
                coverage_tokens=update.coverage_tokens,
                version=update.version,
                generated_at_ns=update.generated_at_ns,
                kind=update.kind,
                frame_bytes=self.frame_bytes,
                raw_event=update.raw_event,
            )
        self.stats.received += 1
        if update.is_invalidation:
            self.stats.invalidations_received += 1
            old = self._pending_invalidations.get(update.key)
            if old is not None and not update.supersedes(old):
                self.stats.superseded += 1
                return False
            self._pending_invalidations[update.key] = update
            # A tombstone dominates an unsent positive update for the same
            # owner/prefix, preventing stale-positive exposure.
            if update.key in self._pending_upserts:
                del self._pending_upserts[update.key]
                self.stats.superseded += 1
            return True

        self.stats.upserts_received += 1
        tombstone = self._pending_invalidations.get(update.key)
        if tombstone is not None and tombstone.version >= update.version:
            self.stats.superseded += 1
            return False

        # The visible map is updated only after transmission.  This is the
        # correct point to apply cross-instance redundancy suppression: an
        # unsent candidate must not hide a copy that the dispatcher cannot yet
        # observe.
        visible = self._visible.get(update.prefix_key, {})
        same_owner = visible.get(update.owner_instance)
        if same_owner is not None and (
            same_owner.coverage_tokens >= update.coverage_tokens
            and same_owner.version >= update.version
        ):
            self.stats.superseded += 1
            return False
        for owner, existing in visible.items():
            if owner != update.owner_instance and (
                existing.coverage_tokens >= update.coverage_tokens
                and existing.version >= update.version
            ):
                self.stats.suppressed_redundant += 1
                return False

        old = self._pending_upserts.get(update.key)
        if old is not None:
            if not update.supersedes(old):
                self.stats.superseded += 1
                return False
            self.stats.superseded += 1
        self._pending_upserts[update.key] = update
        return True

    def ingest_event(self, event: Mapping[str, Any], *, now_ns: int | None = None) -> int:
        """Normalize a reporter event and ingest its semantic updates.

        Preferred reporter fields are ``prefix_hash``, ``owner_instance``,
        ``compatibility_scope``, ``coverage_tokens`` and ``version``.  The
        Dynamo raw event names (``BlockStored``, ``BlockRemoved`` and
        ``AllBlocksCleared``) are accepted for integration experiments.
        """
        now_ns = time.time_ns() if now_ns is None else int(now_ns)
        event_type = str(event.get("kind", event.get("type", "upsert"))).lower()
        owner = int(event.get("owner_instance", event.get("worker_id", 0)))
        scope = str(event.get("compatibility_scope", event.get("scope", "default")))
        version = int(event.get("version", event.get("event_id", now_ns)))
        frame_bytes = int(event.get("frame_bytes", self.frame_bytes))
        if event_type in {"allblockscleared", "cleared", "clear"}:
            return self.clear_owner(owner, version=version, generated_at_ns=now_ns)

        kind = "invalidate" if event_type in {"removed", "blockremoved", "invalidate", "tombstone"} else "upsert"
        prefix = event.get("prefix_hash")
        block_hashes = event.get("block_hashes") or []
        if prefix is not None:
            prefixes: Iterable[Any] = [prefix]
        elif block_hashes:
            # Parent hash preserves a stable prefix identity for a stored
            # chain.  Removed events generally lack it, so each block is a
            # conservative invalidation target.
            parent = event.get("parent_block_hash")
            prefixes = [parent] if parent is not None and kind == "upsert" else [block_hashes[0]]
        else:
            return 0

        block_size = int(event.get("block_size", 0))
        coverage = int(event.get("coverage_tokens", event.get("coverage", 0)))
        if coverage <= 0:
            coverage = max(1, block_size * max(1, len(block_hashes)))
        accepted = 0
        for item in prefixes:
            update = KVStateUpdate(
                prefix_hash=str(item),
                owner_instance=owner,
                compatibility_scope=scope,
                coverage_tokens=coverage,
                version=version,
                generated_at_ns=now_ns,
                kind=kind,
                frame_bytes=frame_bytes,
                raw_event=dict(event),
            )
            accepted += int(self.ingest(update))
        return accepted

    def clear_owner(self, owner_instance: int, *, version: int = 0,
                    generated_at_ns: int | None = None) -> int:
        """Create invalidations for every currently visible/pending prefix."""
        now_ns = time.time_ns() if generated_at_ns is None else int(generated_at_ns)
        keys: set[tuple[int, str, str]] = {
            key for key in self._pending_upserts if key[0] == owner_instance
        }
        keys.update(key for key in self._pending_invalidations if key[0] == owner_instance)
        for (scope, prefix), owners in self._visible.items():
            if owner_instance in owners:
                keys.add((owner_instance, scope, prefix))
        for _, scope, prefix in keys:
            self.ingest(KVStateUpdate(
                prefix_hash=prefix,
                owner_instance=owner_instance,
                compatibility_scope=scope,
                version=version,
                generated_at_ns=now_ns,
                kind="invalidate",
                frame_bytes=self.frame_bytes,
            ))
        return len(keys)

    # ------------------------------------------------------------- egress ----
    def _refill(self, now_ns: int) -> None:
        if self._last_refill_ns is None:
            self._last_refill_ns = now_ns
            return
        elapsed = max(0, now_ns - self._last_refill_ns) / 1e9
        self._tokens = min(
            float(self.budget_bytes_per_sec),
            self._tokens + elapsed * self.budget_bytes_per_sec,
        )
        self._last_refill_ns = now_ns

    def _remove_visible(self, update: KVStateUpdate) -> None:
        owners = self._visible.get(update.prefix_key)
        if not owners:
            return
        current = owners.get(update.owner_instance)
        if current is not None and current.version <= update.version:
            del owners[update.owner_instance]
        if not owners:
            self._visible.pop(update.prefix_key, None)

    def _mark_visible(self, update: KVStateUpdate) -> None:
        owners = self._visible[update.prefix_key]
        old = owners.get(update.owner_instance)
        if old is None or update.supersedes(old):
            owners[update.owner_instance] = update

    def select(self, *, now_ns: int | None = None, budget_bytes: int | None = None,
               top_k: int | None = None) -> list[KVStateUpdate]:
        """Select the next signal frame batch.

        Invalidation frames are emitted first.  Upserts are ranked by the
        paper's freshness-aware utility with a backlog-delay estimate.
        """
        now_ns = time.time_ns() if now_ns is None else int(now_ns)
        self._refill(now_ns)
        available = int(self._tokens // self.frame_bytes)
        if budget_bytes is not None:
            available = min(available, max(0, int(budget_bytes) // self.frame_bytes))
        limit = min(available, int(top_k or self.top_k))
        if limit <= 0:
            self.stats.budget_deferred += len(self._pending_invalidations) + len(self._pending_upserts)
            return []

        selected: list[KVStateUpdate] = []
        # Tombstones are ordered oldest-first so an eviction cannot sit behind
        # a large collection of ordinary updates.
        for key, update in sorted(
            self._pending_invalidations.items(), key=lambda item: item[1].generated_at_ns
        )[:limit]:
            selected.append(update)
            del self._pending_invalidations[key]
            self._remove_visible(update)

        remaining = limit - len(selected)
        if remaining <= 0:
            self._tokens -= len(selected) * self.frame_bytes
            self.stats.invalidations_sent += len(selected)
            self.stats.bytes_sent += len(selected) * self.frame_bytes
            return selected

        pending_count = len(self._pending_upserts)
        backlog_delay = pending_count * self.frame_bytes / self.budget_bytes_per_sec
        scored: list[tuple[float, tuple[int, str, str], KVStateUpdate]] = []
        for key, update in self._pending_upserts.items():
            age = max(0.0, (now_ns - update.generated_at_ns) / 1e9)
            freshness = math.exp(-((age + backlog_delay) / self.theta_seconds))
            utility = freshness * update.coverage_tokens - self.byte_penalty * update.frame_bytes
            scored.append((utility, key, update))
        scored.sort(key=lambda row: (row[0], row[2].coverage_tokens, row[2].version), reverse=True)
        for utility, key, update in scored[:remaining]:
            selected.append(update)
            del self._pending_upserts[key]
            self._mark_visible(update)

        self._tokens -= len(selected) * self.frame_bytes
        self.stats.invalidations_sent += sum(u.is_invalidation for u in selected)
        self.stats.upserts_sent += sum(not u.is_invalidation for u in selected)
        self.stats.bytes_sent += len(selected) * self.frame_bytes
        return selected

    # ----------------------------------------------------------- inspection --
    @property
    def pending_count(self) -> int:
        return len(self._pending_upserts) + len(self._pending_invalidations)

    def snapshot(self) -> dict[str, Any]:
        return {
            "pending": self.pending_count,
            "pending_upserts": len(self._pending_upserts),
            "pending_invalidations": len(self._pending_invalidations),
            "visible_prefixes": len(self._visible),
            "budget_bytes_per_sec": self.budget_bytes_per_sec,
            "frame_bytes": self.frame_bytes,
            "stats": self.stats.as_dict(),
        }


class OwnerStateRegistry:
    """Conservative owner-side validation for advertised hints."""

    def __init__(self) -> None:
        self._states: dict[tuple[int, str, str], KVStateUpdate] = {}

    def record(self, update: KVStateUpdate, *, resident: bool = True) -> None:
        if resident:
            self._states[update.key] = update
        else:
            self._states.pop(update.key, None)

    def remove(self, owner_instance: int, prefix_hash: str,
               compatibility_scope: str = "default") -> None:
        self._states.pop((owner_instance, compatibility_scope, prefix_hash), None)

    def clear_owner(self, owner_instance: int) -> None:
        for key in list(self._states):
            if key[0] == owner_instance:
                del self._states[key]

    @property
    def size(self) -> int:
        return len(self._states)

    def validate(self, hint: KVStateUpdate, *, expected_scope: str | None = None,
                 min_coverage_tokens: int | None = None) -> ValidationResult:
        if hint.is_invalidation:
            return ValidationResult(False, "hint_is_invalidation")
        if expected_scope is not None and hint.compatibility_scope != expected_scope:
            return ValidationResult(False, "compatibility_scope_mismatch")
        actual = self._states.get(hint.key)
        if actual is None:
            return ValidationResult(False, "owner_state_missing")
        if actual.compatibility_scope != hint.compatibility_scope:
            return ValidationResult(False, "compatibility_scope_mismatch", actual.version,
                                    actual.coverage_tokens)
        if actual.version < hint.version:
            return ValidationResult(False, "version_stale", actual.version,
                                    actual.coverage_tokens)
        required = hint.coverage_tokens if min_coverage_tokens is None else min_coverage_tokens
        if actual.coverage_tokens < required:
            return ValidationResult(False, "coverage_insufficient", actual.version,
                                    actual.coverage_tokens)
        return ValidationResult(True, "validated", actual.version, actual.coverage_tokens)


def updates_from_events(events: Iterable[Mapping[str, Any]], *, gateway: SelectiveKVStateGateway,
                        now_ns: int | None = None) -> int:
    """Convenience adapter used by tests and the RPC ingress."""
    return sum(gateway.ingest_event(event, now_ns=now_ns) for event in events)
