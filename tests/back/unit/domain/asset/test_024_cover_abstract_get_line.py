# tests/unit/domain/asset/test_024_cover_abstract_get_line.py
def test_calling_base_get_executes_pass_for_coverage(asset_base_class):
    Asset = asset_base_class

    class ConcreteAsset(Asset):
        def get(self):
            return "ok"

    instance = ConcreteAsset({"foo": 1})
    # Directly invoke the base abstract method body to cover the 'pass' line.
    result = Asset.get(instance)
    assert result is None
