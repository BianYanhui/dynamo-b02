import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

try:
    from b02_sketch_router.selective_signaling import (  # noqa: E402
        KVStateUpdate,
        OwnerStateRegistry,
        SelectiveKVStateGateway,
    )
except ModuleNotFoundError:
    from selective_signaling import (  # type: ignore[no-redef]
        KVStateUpdate,
        OwnerStateRegistry,
        SelectiveKVStateGateway,
    )


def _u(owner: int, prefix: str, coverage: int, version: int,
       kind: str = "upsert") -> KVStateUpdate:
    return KVStateUpdate(
        prefix_hash=prefix,
        owner_instance=owner,
        compatibility_scope="qwen:t4:fp16",
        coverage_tokens=coverage,
        version=version,
        generated_at_ns=1_000_000_000,
        kind=kind,
    )


def test_extension_supersession_keeps_one_latest_frame():
    gateway = SelectiveKVStateGateway(budget_bytes_per_sec=64, frame_bytes=64)
    for version in range(10):
        gateway.ingest(_u(1, "prefix-a", (version + 1) * 256, version))
    frames = gateway.select(now_ns=2_000_000_000, budget_bytes=64)
    assert len(frames) == 1
    assert frames[0].coverage_tokens == 2560
    assert gateway.stats.superseded == 9


def test_invalidation_bypasses_pending_upsert():
    gateway = SelectiveKVStateGateway(budget_bytes_per_sec=64, frame_bytes=64)
    gateway.ingest(_u(1, "prefix-a", 4096, 1))
    gateway.ingest(_u(1, "prefix-a", 0, 2, kind="invalidate"))
    frames = gateway.select(now_ns=2_000_000_000, budget_bytes=64)
    assert [frame.kind for frame in frames] == ["invalidate"]
    assert gateway.pending_count == 0


def test_equal_replica_is_suppressed_but_new_coverage_survives():
    gateway = SelectiveKVStateGateway(budget_bytes_per_sec=128, frame_bytes=64,
                                      byte_penalty=0.1)
    gateway.ingest(_u(1, "prefix-a", 4096, 1))
    gateway.select(now_ns=2_000_000_000, budget_bytes=64)
    assert not gateway.ingest(_u(2, "prefix-a", 4096, 1))
    assert gateway.stats.suppressed_redundant == 1
    assert gateway.ingest(_u(2, "prefix-a", 8192, 2))
    assert gateway.select(now_ns=3_000_000_000, budget_bytes=64)[0].coverage_tokens == 8192


def test_owner_validation_fails_closed():
    owner = OwnerStateRegistry()
    owner.record(_u(1, "prefix-a", 4096, 4))
    assert owner.validate(_u(1, "prefix-a", 4096, 4)).valid
    assert owner.validate(_u(1, "prefix-a", 4096, 5)).reason == "version_stale"
    assert owner.validate(_u(1, "prefix-a", 4096, 4),
                          expected_scope="different-model").reason == "compatibility_scope_mismatch"
    owner.record(_u(1, "prefix-a", 4096, 5), resident=False)
    assert not owner.validate(_u(1, "prefix-a", 4096, 4)).valid


def test_raw_dynamo_event_adapter_is_conservative():
    gateway = SelectiveKVStateGateway(budget_bytes_per_sec=128, frame_bytes=64,
                                      byte_penalty=0.1)
    accepted = gateway.ingest_event({
        "type": "BlockStored",
        "owner_instance": 7,
        "parent_block_hash": 99,
        "block_hashes": [1, 2],
        "block_size": 16,
        "event_id": 3,
    }, now_ns=10_000_000_000)
    assert accepted == 1
    frame = gateway.select(now_ns=11_000_000_000, budget_bytes=64)[0]
    assert frame.prefix_hash == "99"
    assert frame.coverage_tokens == 32


if __name__ == "__main__":
    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    for test in tests:
        test()
        print("PASS", test.__name__)
    print(len(tests), "tests passed")
