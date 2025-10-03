from pathlib import Path
from dataclasses import dataclass

def test_init_builds_inputs_and_attributes(use_real_asset_init, tmp_path):
    # Local subclass because Asset is abstract
    Asset = use_real_asset_init

    class DummyAsset(Asset):
        def get(self):
            return None

    @dataclass
    class Params:
        alpha: int
        beta: str

    inputs = {
        "data_path": Path(tmp_path / "example.csv"),
        "params": Params(alpha=1, beta="x"),
        "threshold": 0.5,
    }

    asset = DummyAsset(inputs=inputs)

    # Inputs attached to instance as attributes
    assert asset.inputs == inputs
    assert asset.data_path == inputs["data_path"]
    assert asset.params == inputs["params"]
    assert asset.threshold == 0.5

    # inputs_hash should be a 32-hex MD5 string
    assert isinstance(asset.inputs_hash, str)
    assert len(asset.inputs_hash) == 32
    assert all(c in "0123456789abcdef" for c in asset.inputs_hash)
