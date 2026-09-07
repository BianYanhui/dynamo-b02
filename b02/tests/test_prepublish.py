from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from b02_sketch_router.prepublish import LocalKVEventSelector


class BlockStored:
    def __init__(self, block_hashes, parent_block_hash=None, token_ids=None):
        self.block_hashes = list(block_hashes)
        self.parent_block_hash = parent_block_hash
        self.token_ids = list(token_ids or [])
        self.block_size = 2
        self.lora_name = None


class BlockRemoved:
    def __init__(self, block_hashes):
        self.block_hashes = list(block_hashes)


class AllBlocksCleared:
    pass


def test_merge_adjacent_prefix_extensions():
    selector = LocalKVEventSelector()
    selector.ingest([
        BlockStored([10], token_ids=[1, 2]),
        BlockStored([11], parent_block_hash=10, token_ids=[3, 4]),
    ])
    events = selector.flush()
    assert len(events) == 1
    assert events[0].block_hashes == [10, 11]
    assert events[0].token_ids == [1, 2, 3, 4]
    assert selector.stats.merged_events == 1


def test_duplicate_store_and_remove_are_suppressed():
    selector = LocalKVEventSelector()
    selector.ingest([BlockStored([10], token_ids=[1, 2])])
    selector.flush()
    selector.ingest([
        BlockStored([10], token_ids=[1, 2]),
        BlockRemoved([10]),
        BlockRemoved([10]),
    ])
    events = selector.flush()
    assert [type(event).__name__ for event in events] == ["BlockRemoved"]
    assert events[0].block_hashes == [10]
    assert selector.stats.duplicate_events == 2


def test_clear_drops_unsent_positive_updates():
    selector = LocalKVEventSelector()
    selector.ingest([BlockStored([10], token_ids=[1, 2]), AllBlocksCleared()])
    events = selector.flush()
    assert [type(event).__name__ for event in events] == ["AllBlocksCleared"]


if __name__ == "__main__":
    test_merge_adjacent_prefix_extensions()
    test_duplicate_store_and_remove_are_suppressed()
    test_clear_drops_unsent_positive_updates()
    print("b02 prepublish tests: PASS")
