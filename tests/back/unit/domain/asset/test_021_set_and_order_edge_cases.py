def test_set_serialization_is_consistent_for_same_content(use_real_asset_init):
    Asset = use_real_asset_init

    class DummyAsset(Asset):
        def get(self):
            return None

    # Note: the current implementation converts sets to list without sorting.
    # To avoid brittleness, we only check that identical set content yields identical hashes.
    inputs1 = {"labels": {"a", "b", "c"}}
    inputs2 = {"labels": {"b", "c", "a"}}

    a = DummyAsset(inputs=inputs1)
    b = DummyAsset(inputs=inputs2)

    # Depending on Python set iteration order, these MAY differ in a flawed implementation.
    # This assertion documents the intended invariant. If it fails, it highlights a bug to fix.
    assert a.inputs_hash == b.inputs_hash, "Hash should not depend on arbitrary set iteration order"
