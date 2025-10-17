from pathlib import Path

def test_asset_in_inputs_uses_child_cached_hash(use_real_asset_init, tmp_path):
    Asset = use_real_asset_init

    class ChildAsset(Asset):
        def __init__(self, child_hash_value: str):
            # use real init with simple inputs so it runs compute_inputs_hash, but we override get_cached_hash
            super().__init__({"note": "child"})
            self._child_hash_value = child_hash_value

        def get(self):
            return None

        # this is what compute_inputs_hash() will call when it sees an Asset in inputs
        def get_cached_hash(self):
            return self._child_hash_value

    class ParentAsset(Asset):
        def get(self):
            return None

    child_a = ChildAsset("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    child_b = ChildAsset("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

    # Parent includes a child Asset directly in its inputs (triggers the Asset branch)
    parent_with_a = ParentAsset({"child": child_a, "p": 1})
    parent_with_a_again = ParentAsset({"child": child_a, "p": 1})
    parent_with_b = ParentAsset({"child": child_b, "p": 1})

    # Same child -> same hash
    assert parent_with_a.inputs_hash == parent_with_a_again.inputs_hash

    # Different child hash -> different parent hash
    assert parent_with_a.inputs_hash != parent_with_b.inputs_hash
