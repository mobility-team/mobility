from mobility.runtime.assets.asset import Asset

def test_list_of_assets_in_inputs_uses_each_child_input_hash():
    class ChildAsset(Asset):
        def __init__(self, child_hash_value: str):
            super().__init__({"child_value": child_hash_value})
        def get(self):
            return None
        def get_cached_hash(self):
            raise AssertionError("Hashing inputs must not read the disk cache")

    class ParentAsset(Asset):
        def get(self):
            return None

    c1 = ChildAsset("11111111111111111111111111111111")
    c2 = ChildAsset("22222222222222222222222222222222")
    c3 = ChildAsset("33333333333333333333333333333333")

    parent_12 = ParentAsset({"children": [c1, c2], "flag": True})
    parent_13 = ParentAsset({"children": [c1, c3], "flag": True})
    parent_12_again = ParentAsset({"children": [c1, c2], "flag": True})

    assert parent_12.inputs_hash == parent_12_again.inputs_hash

    assert parent_12.inputs_hash != parent_13.inputs_hash
