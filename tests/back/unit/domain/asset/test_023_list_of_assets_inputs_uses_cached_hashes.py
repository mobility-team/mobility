def test_list_of_assets_in_inputs_uses_each_child_cached_hash(use_real_asset_init):
    Asset = use_real_asset_init

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

    # Two child assets with distinct cached hashes
    c1 = ChildAsset("11111111111111111111111111111111")
    c2 = ChildAsset("22222222222222222222222222222222")
    c3 = ChildAsset("33333333333333333333333333333333")

    # Parent takes a list of Asset children -> triggers list-of-Assets branch
    parent_12 = ParentAsset({"children": [c1, c2], "flag": True})
    parent_13 = ParentAsset({"children": [c1, c3], "flag": True})
    parent_12_again = ParentAsset({"children": [c1, c2], "flag": True})

    # Order and members identical -> identical hash
    assert parent_12.inputs_hash == parent_12_again.inputs_hash

    # Changing one list element’s cached hash should change the parent hash
    assert parent_12.inputs_hash != parent_13.inputs_hash
