from dataclasses import dataclass

def test_compute_inputs_hash_stable_for_equivalent_inputs(use_real_asset_init):
    Asset = use_real_asset_init

    class DummyAsset(Asset):
        def get(self):
            return None

    @dataclass
    class Params:
        a: int
        b: str

    inputs_a = {
        "params": Params(a=1, b="ok"),
        "mapping": {"x": 1, "y": 2},  # dict key order should not matter due to sort_keys=True
        "numbers": [1, 2, 3],
    }
    inputs_b = {
        "numbers": [1, 2, 3],
        "mapping": {"y": 2, "x": 1},  # re-ordered
        "params": Params(a=1, b="ok"),
    }

    a1 = DummyAsset(inputs=inputs_a)
    a2 = DummyAsset(inputs=inputs_b)

    assert a1.inputs_hash == a2.inputs_hash, "Hash must be invariant to dict key order with same logical content"
