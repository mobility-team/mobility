from mobility.runtime.assets.asset import Asset

def test_mixed_list_inputs_serialize_assets_and_scalars():
    class ChildAsset(Asset):
        def __init__(self, child_hash_value: str):
            super().__init__({"kind": "child"})
            self._child_hash_value = child_hash_value

        def get(self):
            return None

        def get_cached_hash(self):
            return self._child_hash_value

    class ParentAsset(Asset):
        def get(self):
            return None

    child_a = ChildAsset("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    child_b = ChildAsset("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

    parent_a = ParentAsset({"items": ["prefix", child_a, 7]})
    parent_a_again = ParentAsset({"items": ["prefix", child_a, 7]})
    parent_b = ParentAsset({"items": ["prefix", child_b, 7]})

    assert parent_a.inputs_hash == parent_a_again.inputs_hash
    assert parent_a.inputs_hash != parent_b.inputs_hash
