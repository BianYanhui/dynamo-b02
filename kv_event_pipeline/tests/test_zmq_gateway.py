import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kv_event_router.zmq_gateway import RawZmqSelectiveRelay, _normalized_i64_list


def test_contiguous_chain_merge_is_incremental():
    old = {
        "type": "stored",
        "block_hashes": [1],
        "token_ids": [10, 11],
        "num_block_tokens": [2],
        "parent_hash": None,
    }
    new = {
        "type": "stored",
        "block_hashes": [2],
        "token_ids": [12, 13],
        "num_block_tokens": [2],
        "parent_hash": 1,
    }
    merged = RawZmqSelectiveRelay._merge_stored(old, new)
    assert merged is old
    assert merged["block_hashes"] == [1, 2]
    assert merged["token_ids"] == [10, 11, 12, 13]
    assert merged["num_block_tokens"] == [2, 2]


def test_reordered_merge_keeps_conservative_deduplication():
    old = {
        "type": "stored",
        "block_hashes": [1, 2],
        "token_ids": [10, 11, 12, 13],
        "num_block_tokens": [2, 2],
        "parent_hash": None,
    }
    new = {
        "type": "stored",
        "block_hashes": [2, 3],
        "token_ids": [12, 13, 14, 15],
        "num_block_tokens": [2, 2],
        "parent_hash": 99,
    }
    merged = RawZmqSelectiveRelay._merge_stored(old, new)
    assert merged["block_hashes"] == [1, 2, 3]
    assert merged["token_ids"] == [10, 11, 12, 13, 14, 15]


def test_signed_hash_fast_path_preserves_normal_lists():
    values = [1, 2, 3]
    assert _normalized_i64_list(values) is values
    assert _normalized_i64_list([2**64 - 1]) == [-1]


if __name__ == "__main__":
    for name, test in sorted(globals().items()):
        if name.startswith("test_"):
            test()
            print("PASS", name)
