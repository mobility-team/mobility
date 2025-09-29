def test_calling_base_get_executes_pass_for_coverage(use_real_asset_init):
    # Use real init so we construct a bona fide Asset subclass instance
    Asset = use_real_asset_init

    class ConcreteAsset(Asset):
        def get(self):
            # Normal path returns something; not used here
            return "ok"

    instance = ConcreteAsset({"foo": 1})

    # Deliberately call the base-class abstract method to execute its 'pass' line.
    # This is safe and purely for coverage.
    result = Asset.get(instance)

    # Base 'pass' returns None implicitly
    assert result is None
