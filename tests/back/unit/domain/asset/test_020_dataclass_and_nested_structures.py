from dataclasses import dataclass

def test_dataclass_and_nested_structures_serialization(use_real_asset_init):
    Asset = use_real_asset_init

    class DummyAsset(Asset):
        def get(self):
            return None

    @dataclass
    class Inner:
        p: int

    @dataclass
    class Outer:
        inner: Inner
        label: str

    inputs = {
        "outer": Outer(inner=Inner(p=42), label="answer"),
        "nested": {"k1": ["a", "b"], "k2": {"sub": 1}},
    }

    asset = DummyAsset(inputs=inputs)

    # Very targeted assertions: ensure hash is deterministic for repeated construction
    asset2 = DummyAsset(inputs=inputs)
    assert asset.inputs_hash == asset2.inputs_hash
